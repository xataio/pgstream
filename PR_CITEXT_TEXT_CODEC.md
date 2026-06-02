# feat(postgres): register citext with pgtype.TextCodec

Stacks on `feat/ltree-text-copy` (which stacks on `fix/801-cube-copy`).

## Why this is separate from cube / ltree

I empirically probed each extension type with a single-row `tx.CopyFrom` (pgx binary mode) against a fresh Postgres 17 container:

| Type | binary COPY result |
| --- | --- |
| `cube` | `program limit exceeded: cube dimension is too large` |
| `ltree` | `unsupported ltree version number N` |
| `citext` | succeeds — `citext_recv` is an alias for `textrecv`, so the "binary" wire bytes ARE the text bytes |

`citext` does **not** need the text-format COPY workaround that the cube and ltree PRs put in place; binary CopyFrom round-trips it correctly. The only thing missing without an explicit codec registration is that pgx's source-side `rows.Values()` returns `[]byte` for unregistered OIDs, where the rest of the snapshot path expects `string`.

## Fix

One-line addition to `extensionTypes` in `internal/postgres/pg_utils.go`:

```go
{name: "citext", register: registerWithCodec("citext", pgtype.TextCodec{})},
```

This pins citext to `pgtype.TextCodec`, so reads return `string` like cube and ltree do — keeping the source/destination types aligned for downstream processors (transformer, filter, the WAL adapter's `serializeJSONBValue`, etc.). Notably, **`citext` does not appear in `textOnlyCopyTypes`** — the bulk ingest writer continues to route citext-only batches through binary COPY.

## Test plan

- [x] New integration test `Test_SnapshotToPostgres_CitextColumns` in `pkg/stream/integration/snapshot_pg_integration_test.go`: snapshots three rows with mixed-case emails (`Alice@Example.COM`, `bob@example.com`, `CAROL@x.io`), confirms case is preserved across the snapshot (which would catch any spurious down-casing in the codec). Covers `bulk_ingest` and `batch_writer`.
- [x] Existing tests still pass — `Test_SnapshotToPostgres_CubeColumns`, `_LtreeColumns`, `_IdentityOnlyTable`, `_IdentityAndGeneratedColumns`.
- [x] `go test ./...` passes.
