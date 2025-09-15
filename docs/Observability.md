# 🔭 Observability

pgstream provides comprehensive observability through [OpenTelemetry](https://opentelemetry.io/) metrics and distributed tracing. This document outlines the available metrics and how to use them for monitoring your data streaming pipeline.

# Table of Contents

- [Overview](#overview)
- [Quick Start with Local Setup](#quick-start-with-local-setup)
- [Metrics](#metrics)
  - [WAL Replication](#wal-replication)
  - [WAL Event Processing](#wal-event-processing)
  - [Snapshot Operations](#snapshot-operations)
  - [Kafka Operations](#kafka-operations)
    - [Writer Metrics](#writer-metrics)
    - [Reader Metrics](#reader-metrics)
  - [PostgreSQL Operations](#postgresql-operations)
  - [Search Operations](#search-operations)
  - [Data Transformations](#data-transformations)
  - [Go Runtime Metrics](#go-runtime-metrics)
- [Configuration](#configuration)
  - [Enabling Metrics](#enabling-metrics)
- [Monitoring Dashboards](#monitoring-dashboards)
  - [Pre-built pgstream Dashboard](#pre-built-pgstream-dashboard)
  - [Key Performance Indicators (KPIs)](#key-performance-indicators-kpis)
- [Source PostgreSQL Monitoring](#source-postgresql-monitoring)
  - [Essential PostgreSQL Metrics](#essential-postgresql-metrics)
- [Distributed Tracing](#distributed-tracing)
- [Profiling](#profiling)
  - [Enabling Profiling](#enabling-profiling)
  - [Profiling Modes](#profiling-modes)
    - [1. HTTP Endpoint (All Commands)](#1-http-endpoint-all-commands)
    - [2. File Output (Snapshot Command Only)](#2-file-output-snapshot-command-only)
  - [Using Profiling Data](#using-profiling-data)
    - [With Go's pprof Tool](#with-gos-pprof-tool)
  - [Considerations](#considerations)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)

## Overview

pgstream instruments all major components of the data streaming pipeline with metrics and traces using OpenTelemetry. The instrumentation covers:

- WAL replication and processing
- Snapshot generation
- Kafka read/write operations
- PostgreSQL query operations
- Search indexing operations
- Data transformations
- [Go runtime metrics](https://pkg.go.dev/go.opentelemetry.io/contrib/instrumentation/runtime) (memory, GC, goroutines, etc.)
- [Go profiling](https://go.dev/blog/pprof) (CPU, memory, goroutines, etc.)

## Quick Start with Local Setup

pgstream includes a local observability setup using [SigNoz](https://signoz.io/) with a pre-configured [dashboard](../build/docker/signoz/dashboards/pgstream.json) that visualizes all the metrics described below.

To get started:

```bash
# Start the observability stack
docker-compose -f build/docker/docker-compose-signoz.yml --profile instrumentation up

# To start it alongside a different profile
docker-compose -f build/docker/docker-compose.yml -f build/docker/docker-compose-signoz.yml --profile pg2pg --profile instrumentation up

# Access SigNoz dashboard
open http://localhost:8080

# Import the pgstream dashboard
# Dashboard configuration is available in ./build/docker/signoz/dashboards/pgstream.json
```

The dashboard includes panels for all key metrics, alerting rules, and provides a comprehensive view of your pgstream deployment health and performance.

## Metrics

All metrics follow the `pgstream.*` naming convention and include relevant attributes for filtering and aggregation.

### WAL Replication

| Metric                     | Type            | Unit  | Description                                                   |
| -------------------------- | --------------- | ----- | ------------------------------------------------------------- |
| `pgstream.replication.lag` | ObservableGauge | bytes | Replication lag in bytes accrued by the pgstream WAL consumer |

**Attributes:** None

**Usage:** Monitor replication health and detect lag buildup that could indicate performance issues.

**⚠️ Important:** This metric only tracks pgstream's consumer lag. It's strongly recommended to also monitor your source PostgreSQL metrics, particularly the built-in replication lag metrics (`pg_stat_replication.flush_lag`, `pg_stat_replication.replay_lag`) to get a complete picture of replication health.

### WAL Event Processing

| Metric                               | Type      | Unit | Description                                    |
| ------------------------------------ | --------- | ---- | ---------------------------------------------- |
| `pgstream.target.processing.lag`     | Histogram | ns   | Time between WAL event creation and processing |
| `pgstream.target.processing.latency` | Histogram | ns   | Time taken to process a WAL event              |

**Attributes:**

- `target`: Target type (e.g., "postgres", "kafka", "search")

**Usage:** Monitor end-to-end processing latency for each target and identify bottlenecks in the processing pipeline.

### Snapshot Operations

| Metric                                | Type      | Unit | Description                                         |
| ------------------------------------- | --------- | ---- | --------------------------------------------------- |
| `pgstream.snapshot.generator.latency` | Histogram | ms   | Time taken to snapshot a source PostgreSQL database |

**Attributes:**

- `snapshot_schema`: List of schemas being snapshotted
- `snapshot_tables`: List of tables being snapshotted

**Usage:** Monitor snapshot performance and identify slow-running snapshot operations.

### Kafka Operations

#### Writer Metrics

| Metric                              | Type      | Unit     | Description                                  |
| ----------------------------------- | --------- | -------- | -------------------------------------------- |
| `pgstream.kafka.writer.batch.size`  | Histogram | messages | Distribution of message batch sizes          |
| `pgstream.kafka.writer.batch.bytes` | Histogram | bytes    | Distribution of message batch sizes in bytes |
| `pgstream.kafka.writer.latency`     | Histogram | ms       | Time taken to send messages to Kafka         |

#### Reader Metrics

| Metric                                    | Type      | Unit    | Description                                   |
| ----------------------------------------- | --------- | ------- | --------------------------------------------- |
| `pgstream.kafka.reader.msg.bytes`         | Histogram | bytes   | Distribution of message sizes read from Kafka |
| `pgstream.kafka.reader.fetch.latency`     | Histogram | ms      | Time taken to fetch messages from Kafka       |
| `pgstream.kafka.reader.commit.latency`    | Histogram | ms      | Time taken to commit offsets to Kafka         |
| `pgstream.kafka.reader.commit.batch.size` | Histogram | offsets | Distribution of offset batch sizes committed  |

**Attributes:** None

**Usage:** Monitor Kafka throughput, latency, and batch efficiency to optimize performance.

### PostgreSQL Operations

| Metric                              | Type      | Unit | Description                              |
| ----------------------------------- | --------- | ---- | ---------------------------------------- |
| `pgstream.postgres.querier.latency` | Histogram | ms   | Time taken to perform PostgreSQL queries |

**Attributes:**

- `query_type`: Type of SQL operation (e.g., "SELECT", "INSERT", "UPDATE", "DELETE", "tx")
- `query`: The actual SQL query (for non-transaction operations)

**Usage:** Monitor database performance and identify slow queries.

### Search Operations

| Metric                             | Type    | Unit   | Description                                  |
| ---------------------------------- | ------- | ------ | -------------------------------------------- |
| `pgstream.search.store.doc.errors` | Counter | errors | Count of failed document indexing operations |

**Attributes:**

- `severity`: Error severity level (one of "NONE", "DATALOSS", "IGNORED", "RETRIABLE")

**Usage:** Monitor search indexing health and error rates.

### Data Transformations

| Metric                         | Type      | Unit | Description                         |
| ------------------------------ | --------- | ---- | ----------------------------------- |
| `pgstream.transformer.latency` | Histogram | ms   | Time taken to transform data values |

**Attributes:**

- `transformer_type`: Type of transformer being used

**Usage:** Monitor transformation performance and identify slow transformers.

### Go Runtime Metrics

pgstream automatically collects Go runtime metrics using the [OpenTelemetry Go runtime instrumentation](https://pkg.go.dev/go.opentelemetry.io/contrib/instrumentation/runtime). These metrics are essential for monitoring application health and performance:

| Metric                         | Type      | Unit        | Description                                        |
| ------------------------------ | --------- | ----------- | -------------------------------------------------- |
| `runtime.go.mem.heap_alloc`    | Gauge     | bytes       | Bytes of allocated heap objects                    |
| `runtime.go.mem.heap_idle`     | Gauge     | bytes       | Bytes in idle (unused) heap spans                  |
| `runtime.go.mem.heap_inuse`    | Gauge     | bytes       | Bytes in in-use heap spans                         |
| `runtime.go.mem.heap_objects`  | Gauge     | objects     | Number of allocated heap objects                   |
| `runtime.go.mem.heap_released` | Gauge     | bytes       | Bytes of physical memory returned to the OS        |
| `runtime.go.mem.heap_sys`      | Gauge     | bytes       | Bytes of heap memory obtained from the OS          |
| `runtime.go.gc.count`          | Counter   | collections | Number of completed GC cycles                      |
| `runtime.go.gc.pause_ns`       | Histogram | ns          | Amount of nanoseconds in GC stop-the-world pauses  |
| `runtime.go.goroutines`        | Gauge     | goroutines  | Number of goroutines that currently exist          |
| `runtime.go.lookups`           | Counter   | lookups     | Number of pointer lookups performed by the runtime |
| `runtime.go.mem.gc_sys`        | Gauge     | bytes       | Bytes of memory in garbage collection metadata     |

**Usage:** Monitor application resource usage, detect memory leaks, track GC performance, and identify goroutine leaks.

## Configuration

### Enabling Metrics

Configure metrics collection in your pgstream configuration file:

```yaml
instrumentation:
  metrics:
    endpoint: "0.0.0.0:4317"
    collection_interval: 60 # collection interval for metrics in seconds. Defaults to 60s
  traces:
    endpoint: "0.0.0.0:4317"
    sample_ratio: 0.5 # ratio of traces that will be sampled. Must be between 0.0-1.0, where 0 is no traces sampled, and 1 is all traces sampled.
```

Or using environment variables:

```sh
PGSTREAM_METRICS_ENDPOINT="http://localhost:4317"
PGSTREAM_METRICS_COLLECTION_INTERVAL=60s
PGSTREAM_TRACES_ENDPOINT="http://localhost:4317"
PGSTREAM_TRACES_SAMPLE_RATIO=0.5
```

## Monitoring Dashboards

### Pre-built pgstream Dashboard

The included SigNoz dashboard provides:

- **Overview Panel**: System health and key metrics at a glance
- **Replication Health**: Both pgstream and PostgreSQL replication metrics
- **Processing Performance**: Latency and throughput across all components
- **Error Tracking**: Error rates and failure patterns
- **Resource Utilization**: Memory, CPU, and network usage
- **Go Runtime Health**: Memory usage, GC performance, and goroutine tracking

<img width="1656" height="1251" alt="pgstream_signoz_dashboard" src="https://github.com/user-attachments/assets/c08e5029-494d-4dc6-94e1-6ce467d3abce" />

### Key Performance Indicators (KPIs)

1. **Application Health**

   - `runtime.go.mem.heap_alloc` - Memory usage trends
   - `runtime.go.goroutines` - Goroutine count (detect leaks)
   - `runtime.go.gc.pause_ns` - GC pause times (performance impact)

2. **Replication Health**

   - `pgstream.replication.lag` - Should remain low and stable
   - `pgstream.target.processing.lag` - End-to-end processing delay
   - **PostgreSQL replication lag** - Monitor source database metrics

3. **Throughput**

   - `pgstream.kafka.writer.batch.size` - Messages per batch
   - `pgstream.kafka.writer.batch.bytes` - Bytes per batch
   - `rate(pgstream.postgres.querier.latency_count[5m])` - Query rate

4. **Latency**

   - `pgstream.target.processing.latency` (p95, p99) - Processing latency percentiles
   - `pgstream.kafka.writer.latency` (p95, p99) - Kafka write latency
   - `pgstream.postgres.querier.latency` (p95, p99) - Database query latency

5. **Error Rates**
   - `rate(pgstream.search.store.doc.errors[5m])` - Search indexing errors
   - Error logs from distributed traces

## Source PostgreSQL Monitoring

**⚠️ Critical:** pgstream metrics only track the consumer-side lag. For complete replication monitoring, you must also monitor your source PostgreSQL instance:

### Essential PostgreSQL Metrics

```sql
-- WAL generation rate
SELECT
    pg_current_wal_lsn(),
    pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) / 1024 / 1024 AS wal_mb;

-- Replication slot lag for all consumers
SELECT
    slot_name,
    active,
    pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) / 1024 / 1024 AS lag_mb
FROM pg_replication_slots;
```

## Distributed Tracing

pgstream automatically creates traces for all major operations. Traces include:

- Complete request flows from WAL event to target system
- Database query execution details
- Kafka produce/consume operations
- Transformation pipeline execution
- Error context and stack traces

Use the included SigNoz setup or tools like Jaeger/Zipkin to visualize and analyze trace data for debugging and performance optimization.

<img width="1454" height="1022" alt="pgstream_signoz_tracing" src="https://github.com/user-attachments/assets/595cfff7-d1a2-419c-afd0-739e851e1fa7" />

## Profiling

pgstream includes built-in Go profiling capabilities using Go's [`net/http/pprof`](pkg.go.dev/net/http/pprof) package for performance analysis and debugging.

### Enabling Profiling

Profiling can be enabled using the `--profile` flag on supported commands:

```bash
# Enable profiling for snapshot operations
pgstream snapshot --profile --config config.yaml

# Enable profiling for run operations
pgstream run --profile --config config.yaml
```

### Profiling Modes

#### 1. HTTP Endpoint (All Commands)

When `--profile` is enabled, pgstream exposes a profiling HTTP server at `localhost:6060` with the following endpoints:

| Endpoint                                      | Description                    |
| --------------------------------------------- | ------------------------------ |
| `http://localhost:6060/debug/pprof/`          | Profile index page             |
| `http://localhost:6060/debug/pprof/profile`   | CPU profile (30-second sample) |
| `http://localhost:6060/debug/pprof/heap`      | Memory heap profile            |
| `http://localhost:6060/debug/pprof/goroutine` | Goroutine profile              |
| `http://localhost:6060/debug/pprof/allocs`    | Memory allocation profile      |
| `http://localhost:6060/debug/pprof/block`     | Block profile                  |
| `http://localhost:6060/debug/pprof/mutex`     | Mutex profile                  |
| `http://localhost:6060/debug/pprof/trace`     | Execution trace                |

#### 2. File Output (Snapshot Command Only)

For the `snapshot` command, profiling also generates profile files:

- **`cpu.prof`** - CPU profile for the entire snapshot operation
- **`mem.prof`** - Memory profile taken at the end of the operation

### Using Profiling Data

#### With Go's pprof Tool

1. **CPU Hotspots**: Identify functions consuming the most CPU time

   ```bash
   go tool pprof -http=:8080 http://localhost:6060/debug/pprof/profile
   ```

2. **Memory Usage**: Find memory allocation patterns and potential leaks

   ```bash
   go tool pprof -http=:8080 http://localhost:6060/debug/pprof/heap
   ```

3. **Goroutine Analysis**: Debug goroutine leaks or blocking operations

   ```bash
   go tool pprof -http=:8080 http://localhost:6060/debug/pprof/goroutine
   ```

4. **Blocked Operations**:
   ```bash
   go tool pprof -http=:8080 http://localhost:6060/debug/pprof/block
   ```

### Considerations

- **Local Only**: The profiling server binds to `localhost:6060` and is not accessible externally
- **Disable in Production**: Only enable profiling for debugging/optimization sessions
- **Performance Impact**: Profiling has minimal overhead but should be used carefully

## Best Practices

1. **Use the Pre-built Dashboard**: Start with the included SigNoz dashboard for immediate visibility
2. **Monitor Both Sides**: Track both pgstream metrics AND source PostgreSQL replication metrics
3. **Watch Runtime Metrics**: Monitor memory usage, GC performance, and goroutine counts for application health
4. **Set Up Comprehensive Alerting**: Configure alerts for both application and infrastructure metrics
5. **Regular Health Checks**: Use the dashboard to perform regular health assessments
6. **Analyze Traces for Debugging**: Leverage distributed tracing for complex issue resolution
7. **Capacity Planning**: Use historical metrics data to plan for scaling needs
8. **Enable Profiling Only When Needed**: Don't run profiling continuously in production
9. **Collect Sufficient Profile Data**: Let profiling run for adequate time to collect meaningful samples

## Troubleshooting

- **High Memory Usage**: Check `runtime.go.mem.heap_alloc` and look for memory leaks in processing pipelines
- **Goroutine Leaks**: Monitor `runtime.go.goroutines` and investigate if count keeps growing
- **GC Pressure**: High `runtime.go.gc.count` rate may indicate memory allocation issues
- **High Lag**: Check both `pgstream.replication.lag` and PostgreSQL `pg_stat_replication` for the root cause
- **Write Failures**: Monitor `pgstream.kafka.writer.latency` and check Kafka connectivity
- **Slow Snapshots**: Use `pgstream.snapshot.generator.latency` with schema/table attributes to identify problematic tables
- **Query Performance**: Analyze `pgstream.postgres.querier.latency` by `query_type` to find slow operations
- **Missing Data**: Check PostgreSQL replication slot status and WAL retention policies

## Getting Help

If you encounter issues with observability setup:

1. Check the SigNoz dashboard for immediate insights
2. Review both pgstream and PostgreSQL metrics
3. Examine Go runtime metrics for application health issues
4. Use profiling to drill down into performance bottlenecks
5. Examine distributed traces for detailed execution flow
6. Consult the troubleshooting section above
7. Check PostgreSQL logs for replication related errors
