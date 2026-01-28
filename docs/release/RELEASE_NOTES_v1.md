# pgstream v1.0.0 Release Notes

**Release Date:** January 28, 2026

## 🎉 Major Release: Stateless DDL Replication

This is a **major breaking release** that fundamentally transforms how pgstream handles DDL (Data Definition Language) replication. Version 1.0.0 introduces a stateless architecture that eliminates the need for schema log tables and provides a more robust, maintainable solution for tracking database schema changes.

---

## 🚨 Breaking Changes

### Schema Log Removal

**Impact: HIGH** - This change affects all existing pgstream installations.

- **Removed:** The `pgstream.schema_log` table and all related infrastructure
- **Removed:** All schema log-based tracking mechanisms
- **Result:** Existing installations must migrate to the new architecture

### Migration Structure Changes

**Impact: HIGH** - Changes to database migration organization.

The migration system has been completely restructured and split into two categories:

#### Core Migrations

Located in `migrations/postgres/core/`

- Basic DDL replication functionality
- Event triggers for schema change capture
- Functions for WAL-based DDL events
- **Required for all pgstream installations**

#### Injector Migrations

Located in `migrations/postgres/injector/`

- Metadata injection capabilities
- `pgstream.table_ids` table for internal ID tracking
- **Required only for search indexing use cases**

### Configuration Changes

**Impact: MEDIUM** - Configuration files need updates.

- **Removed:** `schema_log` configuration options from all components
- **Removed:** Version column selection options from injector configuration
- **Changed:** LSN (Log Sequence Number) is now the default and only version identifier for events

### API Changes

**Impact: MEDIUM** - Internal API modifications.

- **Removed:** `pkg/schemalog` package and all subpackages
- **Changed:** Processor interfaces now work with DDL events instead of schema log entries
- **Changed:** Snapshot schema generation now uses DDL event streams

---

## ✨ New Features

### Stateless DDL Tracking

The core innovation of v1.0.0 is the new stateless architecture:

**How it works:**

1. DDL changes are captured as logical messages in the PostgreSQL WAL stream
2. Table metadata is computed on-the-fly and attached to the logical message alongside the DDL query
3. Schema diffs are computed directly from DDL events during processing
4. Schema changes flow through the same replication stream as data changes

**Benefits:**

- ✅ **No external state:** No separate schema log table to maintain or query
- ✅ **Order preservation:** Schema changes are ordered with data events in the WAL stream
- ✅ **Reduced footprint:** Fewer tables and triggers on source databases
- ✅ **Simpler operations:** No schema log synchronization or consistency issues

### WAL-Based DDL Events

New DDL event structure captures schema changes directly in the WAL:

```go
type DDLEvent struct {
    DDL        string      // The DDL statement executed
    SchemaName string      // Schema where DDL was executed
    CommandTag string      // Command type (e.g., CREATE TABLE, ALTER TABLE)
    Objects    []DDLObject // Objects affected by the DDL
}

type DDLObject struct {
    Type              string      // Object type (e.g., table, index)
    Identity          string      // Full object identifier
    Schema            string      // Schema name
    OID               string      // PostgreSQL object ID
    PgstreamID        string      // Internal pgstream ID (for injector)
    Columns           []DDLColumn // Column definitions
    PrimaryKeyColumns []string    // Primary key column names
}

type DDLColumn struct {
    Attnum    int     // Column attribute number
    Name      string  // Column name
    Type      string  // PostgreSQL data type
    Nullable  bool    // Whether column accepts NULL
    Default   *string // Default value expression
    Generated bool    // Whether column is generated
    Identity  *string // Identity column type
    Unique    bool    // Whether column has unique constraint
}
```

**Key capabilities:**

- Logical message support in WAL data processing
- Real-time schema diff computation
- Automatic metadata extraction from DDL statements

### Enhanced Search Processor

**Impact: MEDIUM** - Improved search indexing capabilities.

The search processor has been completely refactored to work without schema logs:

**New features:**

- **Field aliasing:** Index mappings now use aliases to map human-readable names to internal pgstream IDs
- **Rename handling:**
  - Column renames add new aliases while preserving original storage
  - Table renames transfer all column aliases to the new table name
- **User-friendly:** Search queries use actual table/column names instead of internal IDs
- **Stable storage:** Internal immutable IDs provide storage stability across renames

### Simplified Snapshot Generation

**Impact: LOW** - Internal implementation change.

The snapshot schema generator is now dramatically simpler:

**How it works:**

1. Parse DDL statements directly from `pg_dump` output
2. Convert DDL to WAL DDL events using `restoreToWAL` function
3. Process snapshot DDL events through the same pipeline as runtime DDL
4. Single unified processing path for all schema changes

**Benefits:**

- Eliminates duplicate schema log generation code
- Consistent schema handling between snapshots and live replication
- Reduced maintenance burden

### Modular Migration System

**Impact: LOW** - Better resource utilization.

New migration structure allows selective installation:

- **Minimal deployments:** Install only core migrations for basic WAL replication
- **Search use cases:** Add injector migrations for search indexing capabilities
- **Reduced permissions:** Fewer database objects = smaller security footprint

---

## 🔧 Technical Improvements

### Architecture

- **Removed:** 12,293 lines of schema log related code
- **Added:** 7,028 lines of stateless DDL processing code
- **Net change:** ~5,200 lines removed (40% code reduction in affected areas)

### Components Updated

All major components have been refactored for DDL events:

- ✅ Filter processor
- ✅ Injector processor
- ✅ Kafka batch writer
- ✅ PostgreSQL target processor
- ✅ Transformer processor
- ✅ Search processor
- ✅ Search store
- ✅ Snapshot generator
- ✅ Replication handler

### Infrastructure

- **New:** Internal `migrator` library for managing multiple migration sets
- **Updated:** Stream initialization to apply multiple migration categories
- **Updated:** Status checker to validate multiple migration states
- **Fixed:** OpenSearch compatibility with latest version
- **Improved:** CLI with new flags for injector migration control

---

## 📊 Testing & Quality

### Test Coverage

- All unit tests updated for new architecture
- Integration tests updated and passing
- Manual testing performed across all components
- Overall test coverage maintained

### Performance

- Reduced database footprint (fewer tables and triggers)
- Eliminated schema log query overhead
- Improved memory efficiency (no schema log caching needed)
- Comparable or better performance in all benchmarks

---

## 🚀 Migration Guide

### For New Installations

Simply install pgstream v1.0.0 - no migration needed.

```bash
# Option 1: Initialize separately
pgstream init --config config.yaml

# Option 2: Initialize and run in one command
pgstream run --config config.yaml --init
```

The system will automatically install the appropriate migrations based on your configuration.

### For Existing Installations

**⚠️ Important:** Upgrading from v0.x to v1.0.0 requires running init again due to breaking changes.

#### Migration Steps

1. **Stop your current pgstream instance**

   ```bash
   # Stop running pgstream processes
   ```

2. **Clean up old pgstream state** (recommended)

   v1.0.0 initialization will not automatically remove old v0.x migrations. To clean up the old state, use `pgstream destroy` before upgrading:

   ```bash
   # Run with v0.x (before upgrading) or v1 (after upgrading)
   pgstream destroy --config config.yaml
   ```

   This will remove:
   - The `pgstream` schema, including the `schema_log` table
   - Old migration tracking tables
   - Replication slot
   - Functions and triggers from v0.x

   **Note:** If you're using snapshots, this will reset the snapshot recorder, losing any tracking of past snapshot history.

3. **Install and initialize v1.0.0**

   ```bash
   # Download new version
   ```

   **Update your configuration file** to remove v0.x-specific settings.
   See the [Configuration Updates](#configuration-updates) section below for detailed examples.

   ```bash
   # Initialize with new migrations
   pgstream init --config config.yaml

   # Or initialize and run in one command:
   pgstream run --config config.yaml --init
   ```

4. **Start replication** (if using separate init)
   ```bash
   pgstream run --config config.yaml
   ```

#### Configuration Updates

The following configuration changes are required when upgrading from v0.x to v1.0.0:

##### 1. Rename `schemalog_url` to `source_url` in Injector Configuration

The injector modifier's `schemalog_url` parameter has been renamed to `source_url`. DDL events now flow through the WAL automatically without requiring a separate schema log table.

**Important**: When using a PostgreSQL source, the `source_url` is optional (defaults to source postgres URL). However, for **non-Postgres sources** (e.g., Kafka), the `source_url` is **required** to connect to the source PostgreSQL database for DDL replication.

**Before (v0.x):**

```yaml
modifiers:
  injector:
    enabled: true
    schemalog_url: "postgres://postgres:postgres@localhost:5432?sslmode=disable"
```

**After (v1.0.0) - PostgreSQL source:**

```yaml
modifiers:
  injector:
    enabled: true
    # source_url is optional, defaults to source postgres URL
```

**After (v1.0.0) - Kafka or other non-Postgres source:**

```yaml
modifiers:
  injector:
    enabled: true
    source_url: "postgres://postgres:postgres@localhost:5432?sslmode=disable" # required for non-postgres sources
```

##### 2. Remove `schema_log_store_url` from PostgreSQL Target

PostgreSQL targets no longer need a separate schema log store URL.

**Before (v0.x):**

```yaml
target:
  postgres:
    url: "postgres://postgres:postgres@localhost:7654?sslmode=disable"
    batch:
      timeout: 5000
      size: 100
    schema_log_store_url: "postgres://postgres:postgres@localhost:5432?sslmode=disable"
```

**After (v1.0.0):**

```yaml
target:
  postgres:
    url: "postgres://postgres:postgres@localhost:7654?sslmode=disable"
    batch:
      timeout: 5000
      size: 100
    # schema_log_store_url removed
```

##### 3. Update Snapshot Schema Mode

The `schemalog` mode for snapshot schema generation has been removed. Use `pgdump_pgrestore` instead.

**Before (v0.x):**

```yaml
source:
  postgres:
    snapshot:
      mode: full
      schema:
        mode: schemalog # No longer supported
```

**After (v1.0.0):**

```yaml
source:
  postgres:
    snapshot:
      mode: full
      schema:
        pgdump_pgrestore:
          clean_target_db: true # Optional: clean target before restore
```

---

## 📚 Documentation Updates

All documentation has been updated to reflect the new architecture:

- ✅ [Architecture documentation](docs/architecture.md)
- ✅ [Configuration guide](docs/configuration.md)
- ✅ [CLI reference](docs/cli.md)
- ✅ Configuration examples updated

---

## 📞 Support

If you encounter issues during migration or have questions:

- **GitHub Issues:** https://github.com/xataio/pgstream/issues
- **Documentation:** https://github.com/xataio/pgstream/tree/main/docs
- **Discussions:** https://github.com/xataio/pgstream/discussions
