# Plan: Idempotent `--upgrade` flag for v0.9.x → v1.0 migration

## Context

Upgrading from v0.9.x to v1.0 currently requires a two-binary dance: run `pgstream destroy --migrations-only` with the old v0.9.x binary, then `pgstream init` with the new v1.0 binary. When running multiple pgstream instances in K8s pods, for example, this is operationally painful.

We want v1.0 to handle both cleanup and initialization via an `--upgrade` flag that is fully idempotent — safe to include in every pod's startup command permanently (e.g., `pgstream run --upgrade -c config.yaml`). The `--upgrade` flag implies `--init`.

## What v0.9.x leaves behind that v1.0 doesn't need

| Object | SQL to clean up |
|---|---|
| `pgstream_log_schema_create_alter_table` event trigger | `DROP EVENT TRIGGER IF EXISTS` |
| `pgstream_log_schema_drop_schema_table` event trigger | `DROP EVENT TRIGGER IF EXISTS` |
| `pgstream.log_schema()` function | `DROP FUNCTION IF EXISTS` |
| `pgstream.get_schema(text)` function | `DROP FUNCTION IF EXISTS` |
| `pgstream.refresh_schema()` function | `DROP FUNCTION IF EXISTS` |
| `pgstream.schema_log` table | `DROP TABLE IF EXISTS` |
| `pgstream.schema_migrations` table (old tracker) | `DROP TABLE IF EXISTS` |

Objects **preserved** (identical in both versions): `pgstream` schema, replication slot, `table_ids` table, XID domain + functions, `snapshot_requests` table.

## Idempotency analysis

| Pod scenario | v0.9.x state? | What happens |
|---|---|---|
| Fresh install | No | Cleanup skipped → normal init |
| First boot after upgrade | Yes (`schema_migrations` exists) | Cleanup runs → init creates v1.0 state |
| Subsequent restarts | No (already cleaned) | Cleanup skipped → init returns `ErrNoChange` |
| Multiple pods racing | N/A | All SQL is `DROP IF EXISTS`; golang-migrate uses advisory locks; replication slot handles `isDuplicateObject` |

## Concurrency safety

With multiple pgstream processes potentially starting simultaneously:
- Cleanup SQL: all `DROP IF EXISTS` — idempotent, concurrent-safe
- Migrations: golang-migrate uses PostgreSQL advisory locks
- Replication slot: `createReplicationSlot` already handles `isDuplicateObject`

## Implementation

### 1. Add `Upgrade` field to `InitConfig` and option function

**File: `pkg/stream/stream_init.go`**

```go
type InitConfig struct {
    PostgresURL               string
    ReplicationSlotName       string
    InjectorMigrationsEnabled bool
    MigrationsOnly            bool
    Upgrade                   bool  // NEW
}

func WithUpgrade() InitOption {
    return func(cfg *InitConfig) {
        cfg.Upgrade = true
    }
}
```

### 2. Add cleanup function to `stream_init.go`

**File: `pkg/stream/stream_init.go`**

Add a `cleanupV09xState` function that:
1. Checks if `pgstream.schema_migrations` exists (the v0.9.x single migration tracking table — v1.0 uses `schema_migrations_core`/`schema_migrations_injector` instead)
2. If not found → return nil (idempotent no-op)
3. If found → execute the DROP statements listed above
4. Log that v0.9.x state was detected and cleaned up

### 3. Call cleanup from `Init()` before running migrations

**File: `pkg/stream/stream_init.go`** — in `Init()`, after `createPGStreamSchema` and before building `migrationAssets`:

```go
if config.Upgrade {
    if err := cleanupV09xState(ctx, conn); err != nil {
        return fmt.Errorf("failed to clean up v0.9.x state: %w", err)
    }
}
```

### 4. Injector handling during upgrade

No auto-detection. The user's config determines whether injector migrations are enabled. Customers using injector (search targets) must have `modifiers.injector.enabled: true` in their config (which they already should if they were using it in v0.9.x).

### 5. Register `--upgrade` flag on `init` and `run` commands

**File: `cmd/root_cmd.go`**
- Add `--upgrade` flag to `initCmd`: `initCmd.Flags().Bool("upgrade", false, "Clean up v0.9.x state before initializing (idempotent, safe for repeated use)")`
- Add `--upgrade` flag to `runCmd`: `runCmd.Flags().BoolVar(&upgradeFlag, "upgrade", false, "...")`

**File: `cmd/run_cmd.go`**
- Add `var upgradeFlag = false` alongside `initFlag`
- Thread it through to `stream.Run()`

**File: `cmd/init_cmd.go`**
- Bind `--upgrade` in `initDestroyFlagBinding` and pass as `WithUpgrade()` option in `getInitOptions()`

### 6. `--upgrade` implies `--init` on `run`

**File: `cmd/run_cmd.go`**

When `upgradeFlag` is true, also set `initFlag = true`. This way customers only need `pgstream run --upgrade -c config.yaml`.

```go
// in run() or runFlagBinding:
if upgradeFlag {
    initFlag = true
}
```

**File: `pkg/stream/stream_run.go`**

Thread upgrade through to `Init()`:

```go
func Run(ctx context.Context, logger loglib.Logger, config *Config, init bool, instrumentation *otel.Instrumentation, opts ...InitOption) error {
    if init {
        if err := Init(ctx, config.GetInitConfig(opts...)); err != nil {
            return err
        }
    }
    ...
}
```

Then in `cmd/run_cmd.go`:
```go
opts := []stream.InitOption{}
if upgradeFlag {
    opts = append(opts, stream.WithUpgrade())
}
return stream.Run(ctx, ..., initFlag, ..., opts...)
```

### 7. Add tests

**File: `pkg/stream/stream_init_test.go`**

Test cases:
- `TestInit_Upgrade_WithV09xState`: Set up v0.9.x objects, run Init with Upgrade=true, verify old objects removed and v1.0 objects created
- `TestInit_Upgrade_NoV09xState`: Run Init with Upgrade=true on clean DB, verify normal init behavior
- `TestInit_Upgrade_Idempotent`: Run Init with Upgrade=true twice, verify no errors on second run
- `TestInit_Upgrade_WithInjector`: Set up v0.9.x with table_ids, run Init with Upgrade=true and InjectorMigrationsEnabled=true, verify table_ids data preserved and injector migrations applied

## Files to modify

1. `pkg/stream/stream_init.go` — core logic (cleanup function, Init changes)
2. `cmd/root_cmd.go` — flag registration
3. `cmd/run_cmd.go` — upgradeFlag variable, threading to Run()
4. `cmd/init_cmd.go` — flag binding, getInitOptions()
5. `pkg/stream/stream_run.go` — pass upgrade option through
6. `pkg/stream/stream_init_test.go` — tests

## Verification

1. **Unit tests**: Run `go test ./pkg/stream/ -run TestInit_Upgrade`
2. **Manual test with Docker**:
   - Start a PostgreSQL container
   - Use v0.9.x binary to `pgstream init --postgres-url ...`
   - Verify v0.9.x objects exist (schema_log, event triggers, etc.)
   - Switch to v1.0 binary, run `pgstream init --upgrade --postgres-url ...`
   - Verify old objects removed, v1.0 objects created
   - Run `pgstream init --upgrade --postgres-url ...` again — verify idempotent (no errors)
3. **Lint**: `make lint`
4. **Full test suite**: `make test`
