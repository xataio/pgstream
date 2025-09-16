# ⌨️ CLI Documentation

pgstream is a command-line tool for streaming PostgreSQL data changes to various targets. This document covers all available commands and their usage.

- [Installation](#installation)
- [Global Flags](#global-flags)
- [Commands](#commands)
  - [init](#init)
  - [run](#run)
  - [snapshot](#snapshot)
  - [status](#status)
  - [destroy](#destroy)
  - [tear-down](#tear-down)
  - [version](#version)
- [Configuration](#configuration)
- [Examples](#examples)
- [Troubleshooting](#troubleshooting)

## Installation

```bash
# Download the latest release
curl -L https://github.com/xataio/pgstream/releases/latest/download/pgstream-linux-amd64 -o pgstream
chmod +x pgstream
sudo mv pgstream /usr/local/bin/

# Or use go install
go install github.com/xataio/pgstream@latest

# Or build from source
git clone https://github.com/xataio/pgstream.git
cd pgstream
go build -o pgstream ./cmd

# Or install via homebrew on macOS or Linux
brew tap xataio/pgstream
brew install pgstream
```

## Global Flags

These flags are available for all commands:

| Flag             | Description                                                                         | Default |
| ---------------- | ----------------------------------------------------------------------------------- | ------- |
| `--config`, `-c` | .env or .yaml config file to use with pgstream if any                               | -       |
| `--log-level`    | Log level for the application. One of trace, debug, info, warn, error, fatal, panic | `debug` |
| `--help`, `-h`   | Show help information                                                               | -       |

## Commands

### init

Initialises pgstream, creating the replication slot and the relevant tables/functions/triggers under the configured internal pgstream schema. It performs the same operations as the `--init` flag on the `run` command.

```bash
pgstream init [flags]
```

**Description:**
The `init` command prepares your PostgreSQL database for streaming by:

- Creating a logical replication slot with the specified name
- Creating the internal pgstream schema for tracking changes
- Setting up necessary functions and triggers for change data capture
- Configuring the database objects required for logical replication

**Prerequisites:**

- PostgreSQL must have `wal_level = logical`
- User must have replication privileges (`REPLICATION` role)
- `max_replication_slots` must allow for additional slots
- User must have privileges to create schemas, functions, and triggers

**Flags:**

- `--postgres-url` - Source postgres URL where pgstream setup will be run
- `--replication-slot` - Name of the postgres replication slot to be created by pgstream on the source url

**Examples:**

```bash
pgstream init --postgres-url <source-postgres-url> --replication-slot <replication-slot-name>
pgstream init -c config.yaml
pgstream init -c config.env
```

### run

Run starts a continuous data stream from the configured source to the configured target.

```bash
pgstream run [flags]
```

**Description:**
The `run` command is the main operation mode for pgstream. It:

- Establishes a connection to the source database
- Connects to the existing replication slot (created by `init`)
- Continuously reads WAL events from the replication stream
- Processes and transforms data according to configuration
- Streams changes to configured targets (Kafka, PostgreSQL, Elasticsearch, OpenSearch)
- Runs continuously until interrupted (Ctrl+C) or receives a termination signal
- Gracefully shuts down on SIGTERM/SIGINT
- Resumes from the last confirmed WAL position

**Prerequisites:**

- Database must be initialized with `pgstream init`
- Replication slot must exist and be available
- Target systems must be accessible and properly configured
- Source database must have logical replication enabled

**Flags:**

- `--source` - Source type. One of postgres, kafka
- `--source-url` - Source URL
- `--target` - Target type. One of postgres, opensearch, elasticsearch, kafka
- `--target-url` - Target URL
- `--replication-slot` - Name of the postgres replication slot for pgstream to connect to
- `--snapshot-tables` - List of tables to snapshot if initial snapshot is required, in the format `<schema>.<table>`. If not specified, the schema `public` will be assumed. Wildcards are supported
- `--reset` - Whether to reset the target before snapshotting (only for postgres target)
- `--profile` - Whether to expose a /debug/pprof endpoint on localhost:6060
- `--init` - Whether to initialize pgstream before starting replication
- `--dump-file` - File where the pg_dump output will be written if initial snapshot is enabled when using pgdump/restore

**Examples:**

```bash
pgstream run --source postgres --source-url <source-postgres-url> --target postgres --target-url <target-postgres-url> --init
pgstream run --source postgres --source-url <source-postgres-url> --target postgres --target-url <target-postgres-url> --snapshot-tables <schema.table> --reset
pgstream run --source kafka --source-url <kafka-url> --target elasticsearch --target-url <elasticsearch-url>
pgstream run --source postgres --source-url <postgres-url> --target kafka --target-url <kafka-url>
pgstream run --config config.yaml --log-level info
pgstream run --config config.env
```

**Output Files (when `--profile` is enabled):**

- `cpu.prof` - CPU profiling data for performance analysis
- `mem.prof` - Memory allocation profiling data

### snapshot

Snapshot performs a snapshot of the configured source Postgres database into the configured target.

```bash
pgstream snapshot [flags]
```

**Description:**
The `snapshot` command creates a point-in-time copy of database tables. It:

- Connects to the source PostgreSQL database
- Reads all existing data from specified tables/schemas
- Transforms and streams the data to configured targets
- Exits after completing the snapshot operation

**Prerequisites:**

- Source PostgreSQL database must be accessible
- Target system must be accessible and properly configured
- User must have SELECT privileges on tables to be snapshotted
- For PostgreSQL targets: user must have write privileges

**Flags:**

- `--postgres-url` - Source postgres database to perform the snapshot from
- `--target` - Target type. One of postgres, opensearch, elasticsearch, kafka
- `--target-url` - Target URL
- `--tables` - List of tables to snapshot, in the format `<schema>.<table>`. If not specified, the schema `public` will be assumed. Wildcards are supported
- `--reset` - Whether to reset the target before snapshotting (only for postgres target)
- `--profile` - Whether to produce CPU and memory profile files, as well as exposing a /debug/pprof endpoint on localhost:6060
- `--dump-file` - File where the pg_dump output will be written

**Examples:**

```bash
pgstream snapshot --postgres-url <postgres-url> --target postgres --target-url <target-url> --tables <schema.table> --reset
pgstream snapshot --config config.yaml --log-level info
pgstream snapshot --config config.env
```

**Use Cases:**

- Bulk data export for analytics
- Creating test datasets
- Backfilling data after system setup

**Output Files (when `--profile` is enabled):**

- `cpu.prof` - CPU profiling data for performance analysis
- `mem.prof` - Memory allocation profiling data

### status

Checks the status of pgstream initialisation and provided configuration.

```bash
pgstream status [flags]
```

**Description:**
The `status` command provides information about:

- Replication slot status
- Internal pgstream schema and objects status
- Overall streaming infrastructure health
- Configuration validation results

**Prerequisites:**

- Access to the source PostgreSQL database
- Replication slot should exist

**Flags:**

- `--postgres-url` - Source postgres URL where pgstream has been initialised
- `--replication-slot` - Name of the postgres replication slot created by pgstream on the source url
- `--json` - Output the status in JSON format

**Examples:**

```bash
pgstream status -c pg2pg.env
pgstream status --postgres-url <postgres-url> --replication-slot <replication-slot-name>
pgstream status -c pg2pg.yaml --json
```

**Sample Output:**

```
✅ SUCCESS  pgstream status check encountered no issues
Initialisation status:
 - Pgstream schema exists: true
 - Pgstream schema_log table exists: true
 - Migration current version: 8
 - Migration status: success
 - Replication slot name: pgstream_postgres_slot
 - Replication slot plugin: wal2json
 - Replication slot database: postgres
Config status:
 - Valid: true
Transformation rules status:
 - Valid: true
Source status:
 - Reachable: true
```

### destroy

It destroys any pgstream setup, removing the replication slot and all the relevant tables/functions/triggers, along with the internal pgstream schema.

```bash
pgstream destroy [flags]
```

**Description:**
The `destroy` command cleans up all resources created by `pgstream init`:

- Drops the replication slot
- Removes the internal pgstream schema and all its objects
- Removes all pgstream-related functions and triggers
- **⚠️ Warning:** This is destructive and will lose replication position

**Prerequisites:**

- Access to the source PostgreSQL database
- User must have privileges to drop schemas, functions, and replication slots
- pgstream should be initialized (objects should exist to be destroyed)

**Flags:**

- `--postgres-url` - Source postgres URL where pgstream destroy will be run
- `--replication-slot` - Name of the postgres replication slot to be deleted by pgstream from the source url

**Examples:**

```bash
pgstream destroy --postgres-url <source-postgres-url> --replication-slot <replication-slot-name>
pgstream destroy -c config.yaml
pgstream destroy -c config.env
```

**⚠️ Important Notes:**

- This will stop any running pgstream instances using these resources
- You will lose the current replication position
- All pgstream tracking data will be permanently removed
- You can recreate resources later with `pgstream init`

### version

Displays version information for pgstream.

```bash
pgstream version
```

**Description:**
Shows the current version of the pgstream binary.

**Example:**

```bash
$ pgstream version
pgstream version v0.8.1
```

## Configuration

pgstream uses YAML or .env configuration files. The configuration can be specified via:

1. **Command-line flag:** `--config /path/to/config.yaml` or `-c /path/to/config.env`
2. **Environment variable:** `PGSTREAM_CONFIG=/path/to/config.yaml`

For more information about the configuration options, check out the [configuration documentation](configuration.md).

## Examples

### Complete Setup Workflow

```bash
# Start continuous streaming with initial snapshot and initialization step
pgstream run --source postgres --source-url "postgres://user:pass@localhost:5432/source_db" --target postgres --target-url "postgres://user:pass@localhost:5432/target_db" --replication-slot "pgstream_slot" --snapshot-tables "public.*" --init
```

### Configuration File Workflow

```bash
# 1. Create configuration file
cat > config.yaml <<EOF
source:
  postgres:
    url: "postgres://user:pass@localhost:5432/source_db"
    replication:
      replication_slot: "pgstream_slot"

target:
  postgres:
    url: "postgres://user:pass@localhost:5432/target_db"
EOF

# 2. Initialize with config file
pgstream init -c config.yaml

# 3. Check status
pgstream status -c config.yaml

# 4. Start streaming
pgstream run -c config.yaml
```

### Development with Profiling

```bash
# Run snapshot with profiling enabled
pgstream snapshot -c config.yaml --profile

# Run continuous streaming with profiling
pgstream run -c config.yaml --profile

# In another terminal, analyze performance
go tool pprof http://localhost:6060/debug/pprof/profile
go tool pprof http://localhost:6060/debug/pprof/heap
```

### Multi-target Streaming

```bash
# Stream to Kafka
pgstream run --source postgres --source-url "postgres://user:pass@localhost:5432/source_db" --target kafka --target-url "localhost:9092" --replication-slot "pgstream_slot"

# Stream from Kafka to Elasticsearch
pgstream run --source kafka --source-url "localhost:9092" --target elasticsearch --target-url "http://localhost:9200" --replication-slot "pgstream_slot"

# Stream from Kafka to OpenSearch
pgstream run --source kafka --source-url "localhost:9092" --target opensearch --target-url "http://localhost:9200" --replication-slot "pgstream_slot"
```

### Environment Variable Configuration

```bash
# Set environment variables
export PGSTREAM_POSTGRES_LISTENER_URL="postgres://user:pass@localhost:5432/source_db"
export PGSTREAM_POSTGRES_REPLICATION_SLOT_NAME="pgstream_slot"
export PGSTREAM_LOG_LEVEL="info"

# Run with environment configuration
pgstream init
pgstream status
pgstream run --source postgres --target kafka --target-url "localhost:9092"
```

## Troubleshooting

### Common Issues

**1. Initialization Failures**

```bash
# Check PostgreSQL configuration
psql "postgres://user:password@host:port/database" -c "SHOW wal_level;"
psql "postgres://user:password@host:port/database" -c "SHOW max_replication_slots;"

# Verify permissions
psql "postgres://user:password@host:port/database" -c "SELECT rolreplication FROM pg_roles WHERE rolname = 'your_user';"
```

**2. Replication Slot Conflicts**

```bash
# Check existing slots
pgstream status --postgres-url "postgres://user:pass@host:port/db" --replication-slot "slot_name"

# Remove conflicting slot if necessary
pgstream destroy --postgres-url "postgres://user:pass@host:port/db" --replication-slot "slot_name"
```

**3. Status Check Issues**

```bash
# Get JSON output for detailed debugging
pgstream status -c config.yaml --json

# Check specific postgres URL and slot
pgstream status --postgres-url "postgres://user:pass@host:port/db" --replication-slot "slot_name"
```

### Command-specific Troubleshooting

**Init Command:**

```bash
# Debug with verbose logging
pgstream init --postgres-url "postgres://user:pass@host:port/db" --replication-slot "slot_name" --log-level debug
```

**Run Command:**

```bash
# Start with initialization if needed
pgstream run -c config.yaml --init

# Run with initial snapshot
pgstream run -c config.yaml --snapshot-tables "public.users,public.orders"
```

**Snapshot Command:**

```bash
# Snapshot with schema reset (postgres target only)
pgstream snapshot -c config.yaml --reset

# Snapshot with profiling and custom dump file
pgstream snapshot -c config.yaml --profile --dump-file custom_dump.sql
```

### Best Practices

- Use `run` with the `--init` flag to ensure pgstream can properly replicate schema changes
- `snapshot` only requires read access on your Postgres source, so it's a good alternative for non invasive syncs
- If you need a snapshot and replication, use `run` with initial snapshot to prevent data loss
- Use the `status` command to validate your transformer configuration and your pgstream replication setup
- Use `destroy` carefully, since it will remove everything used by pgstream, including the replication slot

### Getting Help

For immediate help with any command:

```bash
pgstream --help
pgstream init --help
pgstream run --help
pgstream snapshot --help
pgstream status --help
pgstream destroy --help
```

- **GitHub Issues**: [https://github.com/xataio/pgstream/issues](https://github.com/xataio/pgstream/issues)
- **Documentation**: [https://github.com/xataio/pgstream/docs](https://github.com/xataio/pgstream/docs)
