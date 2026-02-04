# pgstream v1.0.0 Release Notes

**Release Date:** February 4th, 2026

## 🎉 Major Release: Stateless DDL Replication

This is a **major breaking release** that fundamentally transforms how pgstream handles DDL (Data Definition Language) replication. Version 1.0.0 introduces a stateless architecture that eliminates the need for schema log tables and provides a more robust, maintainable solution for tracking database schema changes.

## Table of Contents

- [Audience](#audience)
- [Summary of Required Actions](#summary-of-required-actions)
- [Breaking Changes](#breaking-changes)
- [Migration Guide](#migration-guide)
- [Required Configuration Changes](#required-configuration-changes)
- [Architecture Changes (Background)](#architecture-changes-background)
- [Support](#support)

## 👥 Audience

These release notes are primarily intended for:

- Users upgrading from **v0.x** (action required)
- Operators running pgstream in production
- Developers integrating pgstream replication output

**Important:** Upgrading from any v0.x version **requires re-initialization**.

## ✅ Summary of Required Actions

- Existing installations must be **re-initialized**
- All `schema_log`–related configuration must be removed
- Injector configuration must be updated (`schemalog_url` → `source_url`)
- Snapshot schema mode must be updated

## 🚨 Breaking Changes

### Removal of Schema Log (High Impact)

The schema log mechanism has been fully removed.

- The `pgstream.schema_log` table no longer exists
- All schema log–based tracking has been removed
- Schema changes are no longer persisted as database state

Existing installations **must migrate** to the new DDL-based model.

### Migration Framework Restructure (High Impact)

PostgreSQL migrations are now split into two independent groups.

#### Core migrations

- Location: `migrations/postgres/core/`
- Tracking table: `pgstream.schema_migrations_core`
- Provide:
  - DDL replication
  - Event triggers
  - Core replication infrastructure
- **Required for all installations**

#### Injector migrations

- Location: `migrations/postgres/injector/`
- Tracking table: `pgstream.schema_migrations_injector`
- Provide:
  - ID injection
  - Internal table ID tracking (`pgstream.table_ids`)
- **Only required when using search indexing or ID-dependent features**

### Configuration Defaults (Medium Impact)

- All `schema_log` configuration options have been removed
- LSN is now the default and only event version identifier

### Internal API Changes (Medium Impact)

- The `pkg/schemalog` package has been removed
- Processors now consume DDL events directly
- Snapshot schema generation is based on DDL events instead of schema log entries

These changes affect internal integrations and custom processors.

## 🚀 Migration Guide

### New Installations

Simply install pgstream v1.0.0 - no migration needed.

```bash
# Option 1: Initialize separately
pgstream init --config config.yaml

# Option 2: Initialize and run in one command
pgstream run --config config.yaml --init

# Option 3: Run only database migrations (without creating replication slot)
pgstream init --config config.yaml --migrations-only
```

The system will automatically install the appropriate migrations based on your configuration.

**Note:** The `--migrations-only` flag runs only the database migrations (creating the pgstream schema, tables, functions, and triggers) without creating the replication slot. This is useful when you want to set up the schema separately or when using different database credentials for migrations versus replication.

### Upgrading Existing Installations (v0.x → v1.0.0)

Upgrading requires re-initialization.

Recommended steps:

1. Stop the running pgstream instance
2. Remove existing state

   ```bash
   pgstream destroy --config config.yaml --migrations-only
   ```

   Using `--migrations-only` preserves the replication slot and the pgstream schema, along with any non migration tables (e.g., `snapshot_requests`) and can reduce downtime.

3. Download pgstream v1.0.0
4. Initialize v1.0.0

   ```bash
   pgstream init --config config.yaml

   # Or initialize and run in one command:
   pgstream run --config config.yaml --init
   ```

5. Start replication

   ```bash
   pgstream run --config config.yaml
   ```

## ⚙️ Required Configuration Changes

### Injector Configuration Rename

Old configuration:

```yaml
modifiers:
  injector:
    enabled: true
    schemalog_url: "..."
```

New configuration:

```yaml
modifiers:
  injector:
    enabled: true
    source_url: "..."
```

For PostgreSQL sources, `source_url` is not required.

### Schema Log Store Removal

PostgreSQL targets no longer require `schema_log_store_url`.

### Snapshot Schema Mode Change

The `schemalog` snapshot schema mode has been removed. It will always use `pgdump_pgrestore`.

Use:

```yaml
snapshot:
  schema:
    mode: pgdump_pgrestore
```

## 🏗️ Architecture Changes (Background)

The following sections describe the new replication model in more detail.

### Stateless DDL Replication

DDL is now captured directly from PostgreSQL using event triggers and logical WAL messages.

High-level flow:

1. DDL is captured using PostgreSQL event triggers
2. DDL events are enriched with metadata
3. DDL events are emitted as logical WAL messages
4. Schema changes are streamed inline with data changes

This removes persistent schema state while preserving event ordering.

### WAL-Based DDL Events

Schema changes are now represented as first-class WAL events.

This release introduces new Go structs describing DDL events and their associated metadata.

### Search Processor Changes

Search indexing no longer depends on the schema log.

- Field aliasing replaces schema-log-based mappings
- Column renames preserve aliases to maintain query compatibility
- Search results use actual table and column names instead of internal IDs

### Snapshot Changes

Snapshot processing has been unified for all targets. Instead of having a dedicated schema snapshot for non Postgres targets, now:

- DDL is extracted from `pg_dump` output
- DDL is converted into WAL-style DDL events
- Snapshot and live events are processed through the same pipeline

### Modular Migration Strategy

Migrations can now be applied selectively:

- Core migrations for standard replication
- Injector migrations only when search or ID tracking is required

This reduces database objects, permissions, and operational overhead.

## 💬 Support

- Issues: <https://github.com/xataio/pgstream/issues>
- Documentation: <https://github.com/xataio/pgstream/tree/main/docs>
- Discussions: <https://github.com/xataio/pgstream/discussions>
