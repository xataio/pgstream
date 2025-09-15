# 🔭 Observability

pgstream provides comprehensive observability through [OpenTelemetry](https://opentelemetry.io/) metrics and distributed tracing. This document outlines the available metrics and how to use them for monitoring your data streaming pipeline.

## Overview

pgstream instruments all major components of the data streaming pipeline with metrics and traces using OpenTelemetry. The instrumentation covers:

- WAL replication and processing
- Snapshot generation
- Kafka read/write operations
- PostgreSQL query operations
- Search indexing operations
- Data transformations

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

<img width="1656" height="1251" alt="pgstream_signoz_dashboard" src="https://github.com/user-attachments/assets/c08e5029-494d-4dc6-94e1-6ce467d3abce" />

### Key Performance Indicators (KPIs)

1. **Replication Health**

   - `pgstream.replication.lag` - Should remain low and stable
   - `pgstream.target.processing.lag` - End-to-end processing delay
   - **PostgreSQL replication lag** - Monitor source database metrics

2. **Throughput**

   - `pgstream.kafka.writer.batch.size` - Messages per batch
   - `pgstream.kafka.writer.batch.bytes` - Bytes per batch
   - `rate(pgstream.postgres.querier.latency_count[5m])` - Query rate

3. **Latency**

   - `pgstream.target.processing.latency` (p95, p99) - Processing latency percentiles
   - `pgstream.kafka.writer.latency` (p95, p99) - Kafka write latency
   - `pgstream.postgres.querier.latency` (p95, p99) - Database query latency

4. **Error Rates**
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

## Best Practices

1. **Use the Pre-built Dashboard**: Start with the included SigNoz dashboard for immediate visibility
2. **Monitor Both Sides**: Track both pgstream metrics AND source PostgreSQL replication metrics
3. **Set Up Comprehensive Alerting**: Configure alerts for both application and infrastructure metrics
4. **Regular Health Checks**: Use the dashboard to perform regular health assessments
5. **Analyze Traces for Debugging**: Leverage distributed tracing for complex issue resolution
6. **Capacity Planning**: Use historical metrics data to plan for scaling needs

## Troubleshooting

- **High Lag**: Check both `pgstream.replication.lag` and PostgreSQL `pg_stat_replication` for the root cause
- **Write Failures**: Monitor `pgstream.kafka.writer.latency` and check Kafka connectivity
- **Slow Snapshots**: Use `pgstream.snapshot.generator.latency` with schema/table attributes to identify problematic tables
- **Query Performance**: Analyze `pgstream.postgres.querier.latency` by `query_type` to find slow operations
- **Missing Data**: Check PostgreSQL replication slot status and WAL retention policies

## Getting Help

If you encounter issues with observability setup:

1. Check the SigNoz dashboard for immediate insights
2. Review both pgstream and PostgreSQL metrics
3. Examine distributed traces for detailed execution flow
4. Consult the troubleshooting section above
5. Check PostgreSQL logs for replication related errors
