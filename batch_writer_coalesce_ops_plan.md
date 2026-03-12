# Plan: Coalesce DML queries in WAL streaming batch writer

## Problem

During pg-to-pg clone (`xata clone stream`), after the snapshot completes, the WAL streaming catch-up phase replays all WAL events that accumulated during the snapshot. The current `BatchWriter` builds individual SQL statements per WAL event and executes them one-by-one via `tx.Exec()` within a transaction.

For high-churn tables this is too slow. For example a single `DELETE FROM table WHERE ...` event that deletes thousands of rows results in thousands of individual `DELETE` statements, each executed separately. The overhead of parsing, planning, and executing each statement dominates, leading to very slow catch-up.

This is a similar approach that we take during snapshot (via `BulkIngestWriter`), where we use `COPY` for bulk loading. However, we can't just use `BulkIngestWriter` because it is designed to support only inserting operations (which is the only type of operations that one can have during the snapshot phase).

## Root cause

The `BatchWriter.sendBatch` → `flushQueries` → `execQueries` path executes each DML query individually:

```go
// postgres_batch_writer.go:173-174
for i, q := range queries {
    if _, err := tx.Exec(ctx, q.sql, q.args...); err != nil { ... }
}
```

Each `tx.Exec()` is a separate round-trip to the target database. With thousands of identical `DELETE FROM t WHERE "id" = $1` statements, the overhead of parsing, planning, and executing each statement individually dominates.

Additionally, the current architecture builds SQL strings *before* batching — in `ProcessWALEvent`, each WAL event is immediately converted to an individual SQL string via `walEventToQueries()`, then the SQL string is batched. This means coalescing would require either re-parsing the SQL or double-transforming.

## Proposed solution

Batch raw WAL events instead of pre-built SQL strings, then build bulk SQL at execution time.

- N DELETEs on the same table (single-column PK) become: `DELETE FROM t WHERE "id" = ANY($1::bigint[])` — one array parameter
- N DELETEs on the same table (composite PK) fall back to: `DELETE FROM t WHERE ("id","name") IN (($1,$2),($3,$4),...)` — individual parameters
- N INSERTs on the same table become: `INSERT INTO t(cols) VALUES($1,$2),($3,$4),...`

### Architecture change

**Current flow:**
```
ProcessWALEvent → walEventToQueries (builds individual SQL) → batch *query → sendBatch → execQueries (exec each SQL)
```

**Proposed flow:**
```
ProcessWALEvent → batch *walMessage (raw WAL data + schema info) → sendBatch → group by table+action → build bulk SQL → execQueries
```

Only the `BatchWriter` changes. The `BulkIngestWriter` (used during snapshot) already uses COPY and is fast — it stays unchanged.

## Detailed changes

### 1. New batch message type

**File:** `pkg/wal/processor/postgres/postgres_wal_message.go` (new)

```go
type walMessage struct {
    data       *wal.Data
    schemaInfo schemaInfo  // pre-looked-up generated columns + sequences
}
```

Implements `batch.Message` interface (`Size() int`, `IsEmpty() bool`). The `schemaInfo` (generated column names, sequence mappings) is looked up during `ProcessWALEvent` via the cached schema observer, so it's usually just a map read.

### 2. New batch sender interface

**File:** `pkg/wal/processor/postgres/postgres_writer.go`

Add `walMessageBatchSender` interface alongside existing `queryBatchSender` (which stays for BulkIngestWriter):

```go
type walMessageBatchSender interface {
    SendMessage(context.Context, *batch.WALMessage[*walMessage]) error
    Close()
}
```

### 3. Refactor BatchWriter.ProcessWALEvent

**File:** `pkg/wal/processor/postgres/postgres_batch_writer.go`

Instead of calling `walEventToQueries()` and batching `*query`:

1. Handle nil data / materialized views (skip)
2. Handle DDL events: update schema observer, create `walMessage` with DDL data
3. For DML: look up `schemaInfo` via schema observer (cached), create `walMessage` with raw `wal.Data` + `schemaInfo`
4. Send `walMessage` to batch sender

### 4. Refactor BatchWriter.sendBatch — bulk query building

**File:** `pkg/wal/processor/postgres/postgres_batch_writer.go`

Change batch type from `batch.Batch[*query]` to `batch.Batch[*walMessage]`. New flow:

1. Separate DDL and DML messages
2. For DDL: build and execute queries via existing `ddlAdapter`
3. For DML: walk messages in order, building "runs" of consecutive same-(schema, table, action) events:
   - **DELETE run (single-column PK)** → `DELETE FROM t WHERE "id" = ANY($1::type[])` — all identity values collected into a single Go slice passed as one array parameter. The PG array type is derived from the identity column's `Type` field (see type mapping below).
   - **DELETE run (composite PK)** → falls back to `DELETE FROM t WHERE ("id","name") IN (($1,$2),($3,$4),...)` with individual parameters, since `ANY` doesn't support tuple comparison. For composite PKs, respect the 65,535 parameter limit by splitting runs at ~60,000 params.
   - IS NULL identity columns become a shared WHERE prefix for both variants.
   - **INSERT run** → `INSERT INTO t(cols) OVERRIDING SYSTEM VALUE VALUES($1,$2),($3,$4),...` with ON CONFLICT clause. Sequence setval queries generated separately.
   - **UPDATE** → individual UPDATE queries (no coalescing — each row has different SET values)
   - **TRUNCATE** → pass through as individual query
4. When action changes for a table, flush the current run first (preserves INSERT→DELETE ordering)

**Coalescing only works for consecutive identical operations on the same table.** For example:

```
-- These three consecutive DELETEs coalesce into one:
DELETE FROM t WHERE id = 1
DELETE FROM t WHERE id = 2    →  DELETE FROM t WHERE id = ANY('{1,2,3}'::int[])
DELETE FROM t WHERE id = 3

-- But interleaved operations cannot be coalesced:
DELETE FROM t WHERE id = 1    →  DELETE FROM t WHERE id = 1  (flush: action changes)
INSERT INTO t (id) VALUES (1) →  INSERT INTO t (id) VALUES (1)
DELETE FROM t WHERE id = 2    →  DELETE FROM t WHERE id = 2  (new run, only 1 event)
```

The ordering constraint is necessary for correctness — the INSERT between the two DELETEs may depend on the first DELETE completing (e.g., re-inserting a just-deleted row). Reordering across action boundaries could violate data integrity.

In practice this works well for the target workload: WAL events from bulk operations on the source database (batch purges, accounting reconciliation, ETL loads) naturally produce long runs of the same operation on the same table, which coalesce effectively.
5. Execute via existing `flushQueries` / `execQueries`

**Error handling:** If a coalesced query fails, log the error with `"severity": "DATALOSS"` (consistent with existing patterns in `execQueries`, `BulkIngestWriter`, `wal_kafka_reader`, etc.), drop the failing batch, and continue. There is no fallback to individual statements — a bulk query failure typically indicates a problem that would also affect individual execution, and falling back would mask issues.

### 5. Bulk query builders

**File:** `pkg/wal/processor/postgres/postgres_wal_dml_adapter.go`

Add methods to `dmlAdapter`:

- `buildBulkDeleteQuery(events []*wal.Data) (*query, error)` — for single-column PKs, builds `DELETE ... WHERE id = ANY($1::type[])`. For composite PKs, falls back to `DELETE ... WHERE (col1, col2) IN ((...), (...))`.
- `buildBulkInsertQueries(events []*wal.Data, schemaInfo) []*query` — builds multi-value INSERT + sequence setval
- Reuse existing helpers: `filterRowColumns`, `buildOnConflictQuery`, `extractPrimaryKeyColumns`

**PG type → array type mapping for `ANY` casts:**

The identity column's `Type` field (from `wal.Column.Type`) is mapped to a PG array type for the SQL cast. Common PK types:

| Column Type | Array Cast |
|---|---|
| `integer`, `int4` | `int4[]` |
| `bigint`, `int8` | `int8[]` |
| `smallint`, `int2` | `int2[]` |
| `text`, `varchar`, `character varying` | `text[]` |
| `uuid` | `uuid[]` |

For any type not in the explicit map, fall back to appending `[]` to the type name (e.g., `numeric` → `numeric[]`), which works for most PG types.

On the Go side, identity values (from JSON-deserialized WAL data) are collected into a `[]any` slice. pgx handles encoding `[]any` when the explicit `::type[]` cast is provided in the SQL. Example:

```go
ids := make([]any, len(events))
for i, e := range events {
    ids[i] = e.Identity[0].Value
}
sql := fmt.Sprintf("DELETE FROM %s WHERE %s = ANY($1::%s)",
    table, pglib.QuoteIdentifier(colName), pgArrayType(colType))
args := []any{ids}
```

### 6. Update BatchWriter struct

```go
type BatchWriter struct {
    *Writer
    batchSender walMessageBatchSender  // was: queryBatchSender
}
```

Constructor creates `batch.NewSender[*walMessage](...)` instead of `batch.NewSender[*query](...)`.

## Files to modify

| File | Change |
|------|--------|
| `pkg/wal/processor/postgres/postgres_wal_message.go` | **New** — walMessage type |
| `pkg/wal/processor/postgres/postgres_writer.go` | Add walMessageBatchSender interface |
| `pkg/wal/processor/postgres/postgres_batch_writer.go` | Refactor ProcessWALEvent and sendBatch |
| `pkg/wal/processor/postgres/postgres_wal_dml_adapter.go` | Add bulk query builders |
| `pkg/wal/processor/postgres/postgres_batch_writer_test.go` | Update tests for new batch type |
| `pkg/wal/processor/postgres/postgres_wal_dml_adapter_bulk_test.go` | **New** — bulk builder tests |

**Unchanged:**
- `pkg/wal/processor/postgres/postgres_bulk_ingest_writer.go` — stays on `*query` with COPY
- `pkg/wal/processor/batch/` — generic framework, no changes needed
- `pkg/wal/wal_data.go` — no changes to WAL data model

## Expected impact

For the customer's workload (9,737 DELETEs in a single batch):
- **Before:** 9,737 individual `tx.Exec("DELETE FROM t WHERE id = $1", id)` calls
- **After:** 1 call: `tx.Exec("DELETE FROM t WHERE id = ANY($1::bigint[])", ids)`

This reduces network round-trips by orders of magnitude. Using `ANY` with an array parameter instead of `IN ($1, $2, ..., $N)` has additional benefits:
- **No 65,535 parameter limit** — the entire array is a single parameter, so arbitrarily large batches work without splitting.
- **Faster query planning** — the planner processes a single `ScalarArrayOpExpr` instead of optimizing a long OR chain from thousands of IN values.
- **Lower parameter binding overhead** — one array binding vs N individual bindings.
