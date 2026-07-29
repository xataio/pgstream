# Running pgstream from a read replica

This guide explains how to run pgstream — both the snapshot and the replication phases —
against a **physical streaming replica** instead of the primary. This reduces load on the primary
and also avoids locking issues due to the long-running snapshots created by pgstream.

## Table of Contents

1. [Requirements](#requirements)
2. [Why DDL replication still works](#why-ddl-replication-still-works)
3. [Setup](#setup)
4. [Configuration](#configuration)
5. [Operational considerations](#operational-considerations)

## Requirements

- **PostgreSQL 16 or later.** Logical decoding on a standby does not exist before 16.
- **`wal_level = logical` on the primary _and_ on the replica.** See the warning below —
      the replica does not inherit this.
- **`hot_standby_feedback = on` on the replica.**
- **The wal2json output plugin installed on the replica's host.** It is a shared library,
      not a SQL extension, so it does not arrive through replication — it must be present on
      that machine.
- **A single-instance endpoint** for the replica, not a load-balanced reader endpoint.
- Superuser-equivalent access on the primary for the one-time `pgstream init`.

> ⚠️ **`wal_level = logical` is not inherited by the replica.** If the primary sets it with a
> command line flag, the setting never reaches `postgresql.conf` and so is not copied by
> `pg_basebackup`. The replica comes up at `wal_level = replica` and slot creation fails with
> `logical decoding requires "wal_level" >= "logical"`. Set it explicitly on the replica.

> ⚠️ **Point the source URL at a specific replica instance.** The parallel data snapshot
> exports a transaction snapshot and imports it on sibling connections, and an exported
> snapshot is instance-local. An Aurora reader (`cluster-ro`) endpoint, an RDS reader
> endpoint, or a pooler spanning instances will fail nondeterministically. See
> [Snapshots](snapshots.md#️-the-data-snapshot-source-must-be-a-single-instance);
> `pgstream check` flags this before a snapshot runs.

## How streaming DDL works

pgstream's DDL replication is **stateless**. `pgstream init` installs an event trigger
(`pgstream.emit_ddl`) that calls `pg_logical_emit_message()`, so DDL travels inline in the WAL
stream as a logical message rather than through a table that has to be read back.

You cannot install that event trigger on the replica because a standby is read only. But it's not needed. 
The event trigger runs on the *primary*, where DDL actually executes, and `pg_logical_emit_message()`
writes an ordinary WAL record. The replica replays that record like any other, so a logical slot on 
the replica decodes it for free. No pgstream state of any kind is required on the replica.

## Setup

### 1. Initialise pgstream on the primary — without a replication slot

```bash
pgstream init --postgres-url <primary-url> --migrations-only
```

This installs the pgstream schema, functions and the `emit_ddl` event trigger, all of which
reach the replica through physical replication.

`--migrations-only` is important: without it, `init` also creates a logical replication slot
on the primary. In this setup nothing ever consumes that slot, so it would pin WAL on the
primary indefinitely — the exact disk pressure you are trying to avoid. See
[CLI: init](cli.md#init).

If you already created one, drop it:

```sql
-- on the primary
SELECT pg_drop_replication_slot('pgstream_<dbname>_slot');
```

> ⚠️ Use `pg_drop_replication_slot`, **not** `pgstream destroy`. Destroy would also remove the
> pgstream schema and the `emit_ddl` event trigger, which must stay on the primary.

> ⚠️ Two different slots live on the primary and only the `slot_type` column tells them apart.
> Drop the unused **logical** slot; **keep** the replica's own **physical** slot, which is what
> stops the primary from discarding WAL the replica still needs.

### 2. Configure the replica

Both settings are required, and neither is inherited from the primary:

```
wal_level = logical
hot_standby_feedback = on
```

### 3. Create the logical replication slot on the replica

```sql
-- on the replica
SELECT pg_create_logical_replication_slot('pgstream_<dbname>_slot', 'wal2json');
```

The slot name only has to match what pgstream looks for on the source URL. If
`source.postgres.replication.replication_slot` is left empty, pgstream derives
`pgstream_<dbname>_slot` from the URL's database name; since the replica is a physical copy
its database name is the same, so the default resolves identically on both hosts.

### 4. Run pgstream against the replica

Point `source.postgres.url` at the replica and run as normal. `pgstream run` never creates a
slot, it only connects to an existing one, so a missing or invalidated slot is a startup
failure rather than something that silently recreates itself.

## Configuration

Nothing here is replica-specific except the URL: the source simply points at the replica.

```yaml
source:
  postgres:
    url: "postgres://user:pass@replica-host:5432/mydb"
    mode: snapshot_and_replication
    replication:
      replication_slot: "pgstream_mydb_slot" # the slot created on the replica
    snapshot:
      mode: full
      tables:
        - "*"
      schema:
        pgdump_pgrestore:
          clean_target_db: false

target:
  postgres:
    url: "postgres://user:pass@target-host:5432/targetdb"
```

See [Configuration](configuration.md) for the full set of options.

## Operational considerations

- **Slot invalidation is the main risk.** A logical slot on a standby can be invalidated by
  recovery conflicts, or by the primary's `max_slot_wal_keep_size`. Invalidation means a full
  resnapshot, not a resume — a slot on the primary does not have this failure mode. Monitor it:

  ```sql
  SELECT slot_name, conflicting, invalidation_reason  -- invalidation_reason is PG17+
  FROM pg_replication_slots WHERE slot_name = 'pgstream_<dbname>_slot';
  ```

- **Failover.** The slot is local to the replica, so promoting *that* node keeps it. Failing
  over to a different node, or rebuilding the replica, loses the slot and requires a new
  snapshot.

- **The primary is still required for DDL.** DDL continues to execute on the primary and the
  event trigger must remain installed there. Re-run
  `pgstream init --postgres-url <primary-url> --migrations-only` after any upgrade that adds
  migrations.
