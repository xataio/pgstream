# 📚 `pgstream` Documentation

## Table of Contents

1. [Architecture](#architecture)
   - [WAL Listener](#wal-listener)
   - [WAL Processor](#wal-processor)
2. [Configuration](#configuration)
   - [Yaml](#yaml)
   - [Environment Variables](#environment-variables)
     - [Sources](#sources)
     - [Targets](#targets)
       - [Modifiers](#modifiers-1)
3. [Tracking Schema Changes](#tracking-schema-changes)
4. [Snapshots](#snapshots)
5. [Transformers](#transformers)
   - [Supported Transformers](#supported-transformers)
   - [Transformation Rules](#transformation-rules)
6. [Glossary](#glossary)

## Architecture

`pgstream` is constructed as a streaming pipeline, where data from one module streams into the next, eventually reaching the configured targets. It keeps track of schema changes and replicates them along with the data changes to ensure a consistent view of the source data downstream. This modular approach makes adding and integrating target implementations simple and painless.

![pgstream architecture v2](img/pgstream_diagram_v2.svg)

![pgstream architecture with kafka v2](img/pgstream_diagram_v2_kafka.svg)

At a high level the implementation is split into WAL listeners and WAL processors.

### WAL Listener

A listener is anything that listens for WAL data, regardless of the source. It has a single responsibility: consume and manage the WAL events, delegating the processing of those entries to modules that form the processing pipeline. Depending on the listener implementation, it might be required to have a checkpointer to flag the events as processed once the processor is done.

The current implementations of the listener include:

- **Postgres listener**: listens to WAL events directly from the replication slot. Since the WAL replication slot is sequential, the Postgres WAL listener is limited to run as a single process. The associated Postgres checkpointer will sync the LSN so that the replication lag doesn't grow indefinitely. It can be configured to perform an initial snapshot when pgstream is first connected to the source PostgreSQL database (see details in the [snapshots section](#snapshots)).

- **Postgres Snapshoter**: produces events by performing a snapshot of the configured PostgreSQL database, as described in the [snapshots section](#snapshots). It doesn't start continuous replication, so once all the snapshotted data has been processed, the pgstream process will stop.

- **Kafka reader**: reads WAL events from a Kafka topic. It can be configured to run concurrently by using partitions and Kafka consumer groups, applying a fan-out strategy to the WAL events. The data will be partitioned by database schema by default. The associated Kafka checkpointer will commit the message offsets per topic/partition so that the consumer group doesn't process the same message twice, and there's no lag accumulated.

### WAL Processor

A processor processes a WAL event. Depending on the implementation it might also be required to checkpoint the event once it's done processing it as described above. Wherever possible the processors are implemented to continuously consume the replication slot by using configurable memory guards, aiming to prevent the replication slot lag from growing out of control.

The current implementations of the processor include:

- **Kafka batch writer**: it writes the WAL events into a Kafka topic, using the event schema as the Kafka key for partitioning. This implementation allows to fan-out the sequential WAL events, while acting as an intermediate buffer to avoid the replication slot to grow when there are slow consumers. It has a memory guarded buffering system internally to limit the memory usage of the buffer. The buffer is sent to Kafka based on the configured linger time and maximum size. It treats both data and schema events equally, since it doesn't care about the content.

- **Search batch indexer**: it indexes the WAL events into an OpenSearch/Elasticsearch compatible search store. It implements the same kind of mechanism than the Kafka batch writer to ensure continuous processing from the listener, and it also uses a batching mechanism to minimise search store calls. The WAL event identity is used as the search store document id, and if no other version is provided, the LSN is used as the document version. Events that do not have an identity are not indexed. Schema events are stored in a separate search store index (`pgstream`), where the schema log history is kept for use within the search store (i.e, read queries).

- **Webhook notifier**: it sends a notification to any webhooks that have subscribed to the relevant wal event. It relies on a subscription HTTP server receiving the subscription requests and storing them in the shared subscription store which is accessed whenever a wal event is processed. It sends the notifications to the different subscribed webhook urls in parallel based on a configurable number of workers (client timeouts apply). Similar to the two previous processor implementations, it uses a memory guarded buffering system internally, which allows to separate the wal event processing from the webhook url sending, optimising the processor latency.

- **Postgres batch writer**: it writes the WAL events into a PostgreSQL compatible database. It implements the same kind of mechanism than the Kafka and the search batch writers to ensure continuous processing from the listener, and it also uses a batching mechanism to minimise PostgreSQL IO traffic.

In addition to the implementations described above, there are optional processor decorators, which work in conjunction with one of the main processor implementations described above. Their goal is to act as modifiers to enrich the wal event being processed. We will refer to them as modifiers.

#### Modifiers

There current implementations of the processor that act as modifier decorators are:

- **Injector**: injects some of the pgstream logic into the WAL event. This includes:

  - Data events:

    - Setting the WAL event identity. If provided, it will use the configured id finder (only available when used as a library), otherwise it will default to using the table primary key/unique not null column.
    - Setting the WAL event version. If provided, it will use the configured version finder (only available when used as a library), otherwise it will default to using the event LSN.
    - Adding pgstream IDs to all columns. This allows us to have a constant identifier for a column, so that if there are renames the column id doesn't change. This is particularly helpful for the search store, where a rename would require a reindex, which can be costly depending on the data.

  - Schema events:
    - Acknolwedging the new incoming schema in the Postgres `pgstream.schema_log` table.

- **Filter**: allows to filter out WAL events for certain schemas/tables. It can be configured by providing either an include or exclude table list. WAL events for the tables in the include list will be processed, while those for the tables in the exclude list or not present in the include list will be skipped. The format for the lists is similar to the snapshot tables, tables are expected to be schema qualified, and if not, the `public` schema will be assumed. Wildcards are supported, but not regex. Example of table list: `["test_table", "public.test_table", "test_schema.test_table", "test_schema.*", "*.test", "*.*"]`. By default, the filter will include the `pgstream.schema_log` table in the include table list, since pgstream relies on it to replicate DDL changes. It can be disabled by adding it to the exclude list.

- **Transformer**: it modifies the column values in insert/update events according to the rules defined in the configured yaml file. It can be used for anonymising data from the source Postgres database. An example of the rules definition file can be found in the repo under `transformer_rules.yaml`. The rules have per column granularity, and certain transformers from opensource sources, such as greenmask or neosync, are supported. More details can be found in the [transformers section](#transformers).

## Configuration

⚠️ Be aware that a single source and a single target must be provided or the configuration validation will fail.

### Yaml

The pgstream configuration can be provided as a yaml configuration file, which encapsulates the transformation configuration. The following sample shows the format for all supported fields.

```yaml
source:
  postgres:
    url: "postgresql://user:password@localhost:5432/mydatabase"
    mode: snapshot_and_replication # options are replication, snapshot or snapshot_and_replication
    snapshot: # when mode is snapshot or snapshot_and_replication
      mode: full # options are data_and, schema or data
      tables: ["test", "test_schema.Test", "another_schema.*"] # tables to snapshot, can be a list of table names or a pattern
      recorder:
        repeatable_snapshots: true # whether to repeat snapshots that have already been taken. Defaults to false
        postgres_url: "postgresql://user:password@localhost:5432/mytargetdatabase" # URL of the database where the snapshot status is recorded
      snapshot_workers: 4 # number of schemas to be snapshotted in parallel. Defaults to 1
      data: # when mode is full or data
        schema_workers: 4 # number of schema tables to be snapshotted in parallel. Defaults to 4
        table_workers: 4 # number of workers to snapshot a table in parallel. Defaults to 4
        batch_page_size: 1000 # number of pages to read per batch. Defaults to 1000
      schema: # when mode is full or schema
        mode: pgdump_pgrestore # options are pgdump_pgrestore or schemalog
        pgdump_pgrestore:
          clean_target_db: true # whether to clean the target database before restoring
    replication: # when mode is replication or snapshot_and_replication
      replication_slot: "pgstream_mydatabase_slot"
  kafka:
    servers: ["localhost:9092"]
    topic:
      name: "mytopic"
    consumer_group:
      id: "mygroup" # id for the kafka consumer group. Defaults to pgstream-consumer-group
      start_offset: "earliest" # options are earliest or latest. Defaults to earliest.
    tls:
      ca_cert: "/path/to/ca.crt" # path to CA certificate
      client_cert: "/path/to/client.crt" # path to client certificate
      client_key: "/path/to/client.key" # path to client key
    backoff:
      exponential:
        max_retries: 5 # maximum number of retries
        initial_interval: 1000 # initial interval in milliseconds
        max_interval: 60000 # maximum interval in milliseconds
      constant:
        max_retries: 5 # maximum number of retries
        interval: 1000 # interval in milliseconds

target:
  postgres:
    url: "postgresql://user:password@localhost:5432/mytargetdatabase"
    batch:
      timeout: 1000 # batch timeout in milliseconds. Defaults to 1s
      size: 100 # number of messages in a batch. Defaults to 100
      max_bytes: 1572864 # max size of batch in bytes (1.5MiB). Defaults to 1.5MiB
      max_queue_bytes: 204800 # max size of memory guard queue in bytes (100MiB). Defaults to 100MiB
    disable_triggers: false # whether to disable triggers on the target database. Defaults to false
    on_conflict_action: "nothing" # options are update, nothing or error. Defaults to error
  kafka:
    servers: ["localhost:9092"]
    topic:
      name: "mytopic" # name of the Kafka topic
      partitions: 1 # number of partitions for the topic. Defaults to 1
      replication_factor: 1 # replication factor for the topic. Defaults to 1
      auto_create: true # whether to automatically create the topic if it doesn't exist. Defaults to false
    tls:
      ca_cert: "/path/to/ca.crt" # path to CA certificate
      client_cert: "/path/to/client.crt" # path to client certificate
      client_key: "/path/to/client.key" # path to client key
    batch:
      timeout: 1000 # batch timeout in milliseconds. Defaults to 1s
      size: 100 # number of messages in a batch. Defaults to 100
      max_bytes: 1572864 # max size of batch in bytes (1.5MiB). Defaults to 1.5MiB
      max_queue_bytes: 204800 # max size of memory guard queue in bytes (100MiB). Defaults to 100MiB
  search:
    engine: "elasticsearch" # options are elasticsearch or opensearch
    url: "http://localhost:9200" # URL of the search engine
    batch:
      timeout: 1000 # batch timeout in milliseconds. Defaults to 1s
      size: 100 # number of messages in a batch. Defaults to 100
      max_bytes: 1572864 # max size of batch in bytes (1.5MiB). Defaults to 1.5MiB
      max_queue_bytes: 204800 # max size of memory guard queue in bytes (100MiB). Defaults to 100MiB
    backoff:
      exponential:
        max_retries: 5 # maximum number of retries
        initial_interval: 1000 # initial interval in milliseconds
        max_interval: 60000 # maximum interval in milliseconds
      constant:
        max_retries: 5 # maximum number of retries
        interval: 1000 # interval in milliseconds
  webhooks:
    subscriptions:
      store:
        url: "postgresql://user:password@localhost:5432/mydatabase" # URL of the database where the webhook subscriptions are stored
        cache:
          enabled: true # whether to enable caching for the subscription store. Defaults to false
          refresh_interval: 60 # interval in seconds to refresh the cache
      server:
        address: "localhost:9090" # address of the subscription server
        read_timeout: 60 # read timeout in seconds. Defaults to 5s
        write_timeout: 60 # write timeout in seconds. Defaults to 10s
    notifier:
      worker_count: 4 # number of notifications to be processed in parallel. Defaults to 10
      client_timeout: 1000 # timeout for the webhook client in milliseconds. Defaults to 10s

modifiers:
  injector:
    enabled: true # whether to inject pgstream metadata into the WAL events. Defaults to false
    schemalog_url: "postgres://postgres:postgres@localhost:5432?sslmode=disable" # URL of the schemalog database, if different from the source database
  filter: # one of include_tables or exclude_tables
    include_tables: # list of tables for which events should be allowed. Tables should be schema qualified. If no schema is provided, the public schema will be assumed. Wildcards "*" are supported.
      - "test"
      - "test_schema.test"
      - "another_schema.*"
    exclude_tables: # list of tables for which events should be skipped. Tables should be schema qualified. If no schema is provided, the public schema will be assumed. Wildcards "*" are supported.
      - "excluded_test"
      - "excluded_schema.test"
      - "another_excluded_schema.*"
  transformations:
    validation_mode: relaxed
    table_transformers:
      - schema: public
        table: test
        column_transformers:
          name:
            name: greenmask_firstname
            dynamic_parameters:
              gender:
                column: sex
```

### Environment Variables

Here's a list of all the environment variables that can be used to configure the individual modules, along with their descriptions and default values.

#### Sources

<details>
  <summary>Postgres Listener</summary>

| Environment Variable                        | Default                      | Required | Description                                                                                                                                                                                                                                                                                                  |
| ------------------------------------------- | ---------------------------- | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| PGSTREAM_POSTGRES_LISTENER_URL              | N/A                          | Yes      | URL of the Postgres database to connect to for replication purposes.                                                                                                                                                                                                                                         |
| PGSTREAM_POSTGRES_REPLICATION_SLOT_NAME     | "pgstream_dbname_slot"       | No       | Name of the Postgres replication slot name.                                                                                                                                                                                                                                                                  |
| PGSTREAM_POSTGRES_SNAPSHOT_TABLES           | ""                           | No       | Tables for which there will be an initial snapshot generated. The syntax supports wildcards. Tables without a schema defined will be applied the public schema. Example: for `public.test_table` and all tables in the `test_schema` schema, the value would be the following: `"test_table test_schema.\*"` |
| PGSTREAM_POSTGRES_SNAPSHOT_SCHEMA_WORKERS   | 4                            | No       | Number of tables per schema that will be processed in parallel by the snapshotting process.                                                                                                                                                                                                                  |
| PGSTREAM_POSTGRES_SNAPSHOT_TABLE_WORKERS    | 4                            | No       | Number of concurrent workers that will be used per table by the snapshotting process.                                                                                                                                                                                                                        |
| PGSTREAM_POSTGRES_SNAPSHOT_BATCH_PAGE_SIZE  | 1000                         | No       | Size of the table page range which will be processed concurrently by the table workers from `PGSTREAM_POSTGRES_SNAPSHOT_TABLE_WORKERS`.                                                                                                                                                                      |
| PGSTREAM_POSTGRES_SNAPSHOT_WORKERS          | 1                            | No       | Number of schemas that will be processed in parallel by the snapshotting process.                                                                                                                                                                                                                            |
| PGSTREAM_POSTGRES_SNAPSHOT_USE_SCHEMALOG    | False                        | No       | Forces the use of the `pgstream.schema_log` for the schema snapshot instead of using `pg_dump`/`pg_restore` for Postgres targets.                                                                                                                                                                            |
| PGSTREAM_POSTGRES_SNAPSHOT_STORE_URL        | ""                           | No       | Postgres URL for the database where the snapshot requests and their status will be tracked. A table `snapshot_requests` will be created under a `pgstream` schema.                                                                                                                                           |
| PGSTREAM_POSTGRES_SNAPSHOT_STORE_REPEATABLE | False (run), True (snapshot) | No       | Allow to repeat snapshots requests that have been already completed succesfully. If using the run command, initial snapshots won't be repeatable by default. If the snapshot command is used instead, the snapshot will be repeatable by default.                                                            |

</details>

<details>
  ##### <summary>Postgres Snapshoter</summary>

| Environment Variable                       | Default | Required | Description                                                                                                                                                                                                                                                                                                  |
| ------------------------------------------ | ------- | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| PGSTREAM_POSTGRES_SNAPSHOT_LISTENER_URL    | N/A     | Yes      | URL of the Postgres database to snapshot.                                                                                                                                                                                                                                                                    |
| PGSTREAM_POSTGRES_SNAPSHOT_TABLES          | ""      | No       | Tables for which there will be an initial snapshot generated. The syntax supports wildcards. Tables without a schema defined will be applied the public schema. Example: for `public.test_table` and all tables in the `test_schema` schema, the value would be the following: `"test_table test_schema.\*"` |
| PGSTREAM_POSTGRES_SNAPSHOT_SCHEMA_WORKERS  | 4       | No       | Number of tables per schema that will be processed in parallel by the snapshotting process.                                                                                                                                                                                                                  |
| PGSTREAM_POSTGRES_SNAPSHOT_TABLE_WORKERS   | 4       | No       | Number of concurrent workers that will be used per table by the snapshotting process.                                                                                                                                                                                                                        |
| PGSTREAM_POSTGRES_SNAPSHOT_BATCH_PAGE_SIZE | 1000    | No       | Size of the table page range which will be processed concurrently by the table workers from `PGSTREAM_POSTGRES_SNAPSHOT_TABLE_WORKERS`.                                                                                                                                                                      |
| PGSTREAM_POSTGRES_SNAPSHOT_WORKERS         | 1       | No       | Number of schemas that will be processed in parallel by the snapshotting process.                                                                                                                                                                                                                            |
| PGSTREAM_POSTGRES_SNAPSHOT_USE_SCHEMALOG   | False   | No       | Forces the use of the `pgstream.schema_log` for the schema snapshot instead of using `pg_dump`/`pg_restore` for Postgres targets.                                                                                                                                                                            |
| PGSTREAM_POSTGRES_SNAPSHOT_CLEAN_TARGET_DB | False   | No       | When using `pg_dump`/`pg_restore` to snapshot schema for Postgres targets, option to issue commands to DROP all the objects that will be restored.                                                                                                                                                           |

</details>

<details>
  <summary>Kafka Listener</summary>

| Environment Variable                               | Default  | Required         | Description                                                                                            |
| -------------------------------------------------- | -------- | ---------------- | ------------------------------------------------------------------------------------------------------ |
| PGSTREAM_KAFKA_READER_SERVERS                      | N/A      | Yes              | URLs for the Kafka servers to connect to.                                                              |
| PGSTREAM_KAFKA_TOPIC_NAME                          | N/A      | Yes              | Name of the Kafka topic to read from.                                                                  |
| PGSTREAM_KAFKA_READER_CONSUMER_GROUP_ID            | N/A      | Yes              | Name of the Kafka consumer group for the WAL Kafka reader.                                             |
| PGSTREAM_KAFKA_READER_CONSUMER_GROUP_START_OFFSET  | Earliest | No               | Kafka offset from which the consumer will start if there's no offset available for the consumer group. |
| PGSTREAM_KAFKA_TLS_ENABLED                         | False    | No               | Enable TLS connection to the Kafka servers.                                                            |
| PGSTREAM_KAFKA_TLS_CA_CERT_FILE                    | ""       | When TLS enabled | Path to the CA PEM certificate to use for Kafka TLS authentication.                                    |
| PGSTREAM_KAFKA_TLS_CLIENT_CERT_FILE                | ""       | No               | Path to the client PEM certificate to use for Kafka TLS client authentication.                         |
| PGSTREAM_KAFKA_TLS_CLIENT_KEY_FILE                 | ""       | No               | Path to the client PEM private key to use for Kafka TLS client authentication.                         |
| PGSTREAM_KAFKA_COMMIT_EXP_BACKOFF_INITIAL_INTERVAL | 0        | No               | Initial interval for the exponential backoff policy to be applied to the Kafka commit retries.         |
| PGSTREAM_KAFKA_COMMIT_EXP_BACKOFF_MAX_INTERVAL     | 0        | No               | Max interval for the exponential backoff policy to be applied to the Kafka commit retries.             |
| PGSTREAM_KAFKA_COMMIT_EXP_BACKOFF_MAX_RETRIES      | 0        | No               | Max retries for the exponential backoff policy to be applied to the Kafka commit retries.              |
| PGSTREAM_KAFKA_COMMIT_BACKOFF_INTERVAL             | 0        | No               | Constant interval for the backoff policy to be applied to the Kafka commit retries.                    |
| PGSTREAM_KAFKA_COMMIT_BACKOFF_MAX_RETRIES          | 0        | No               | Max retries for the backoff policy to be applied to the Kafka commit retries.                          |

One of exponential/constant backoff policies can be provided for the Kafka committing retry strategy. If none is provided, no retries apply.

</details>

#### Targets

<details>
  <summary>Kafka Batch Writer</summary>

| Environment Variable                    | Default | Required         | Description                                                                                         |
| --------------------------------------- | ------- | ---------------- | --------------------------------------------------------------------------------------------------- |
| PGSTREAM_KAFKA_WRITER_SERVERS           | N/A     | Yes              | URLs for the Kafka servers to connect to.                                                           |
| PGSTREAM_KAFKA_TOPIC_NAME               | N/A     | Yes              | Name of the Kafka topic to write to.                                                                |
| PGSTREAM_KAFKA_TOPIC_PARTITIONS         | 1       | No               | Number of partitions created for the Kafka topic if auto create is enabled.                         |
| PGSTREAM_KAFKA_TOPIC_REPLICATION_FACTOR | 1       | No               | Replication factor used when creating the Kafka topic if auto create is enabled.                    |
| PGSTREAM_KAFKA_TOPIC_AUTO_CREATE        | False   | No               | Auto creation of configured Kafka topic if it doesn't exist.                                        |
| PGSTREAM_KAFKA_TLS_ENABLED              | False   | No               | Enable TLS connection to the Kafka servers.                                                         |
| PGSTREAM_KAFKA_TLS_CA_CERT_FILE         | ""      | When TLS enabled | Path to the CA PEM certificate to use for Kafka TLS authentication.                                 |
| PGSTREAM_KAFKA_TLS_CLIENT_CERT_FILE     | ""      | No               | Path to the client PEM certificate to use for Kafka TLS client authentication.                      |
| PGSTREAM_KAFKA_TLS_CLIENT_KEY_FILE      | ""      | No               | Path to the client PEM private key to use for Kafka TLS client authentication.                      |
| PGSTREAM_KAFKA_WRITER_BATCH_TIMEOUT     | 1s      | No               | Max time interval at which the batch sending to Kafka is triggered.                                 |
| PGSTREAM_KAFKA_WRITER_BATCH_BYTES       | 1572864 | No               | Max size in bytes for a given batch. When this size is reached, the batch is sent to Kafka.         |
| PGSTREAM_KAFKA_WRITER_BATCH_SIZE        | 100     | No               | Max number of messages to be sent per batch. When this size is reached, the batch is sent to Kafka. |
| PGSTREAM_KAFKA_WRITER_MAX_QUEUE_BYTES   | 100MiB  | No               | Max memory used by the Kafka batch writer for inflight batches.                                     |

</details>

<details>
  <summary>Search Batch Indexer</summary>

| Environment Variable                               | Default | Required | Description                                                                                                    |
| -------------------------------------------------- | ------- | -------- | -------------------------------------------------------------------------------------------------------------- |
| PGSTREAM_OPENSEARCH_STORE_URL                      | N/A     | Yes      | URL for the opensearch store to connect to (at least one of the URLs must be provided).                        |
| PGSTREAM_ELASTICSEARCH_STORE_URL                   | N/A     | Yes      | URL for the elasticsearch store to connect to (at least one of the URLs must be provided).                     |
| PGSTREAM_SEARCH_INDEXER_BATCH_TIMEOUT              | 1s      | No       | Max time interval at which the batch sending to the search store is triggered.                                 |
| PGSTREAM_SEARCH_INDEXER_BATCH_SIZE                 | 100     | No       | Max number of messages to be sent per batch. When this size is reached, the batch is sent to the search store. |
| PGSTREAM_SEARCH_INDEXER_MAX_QUEUE_BYTES            | 100MiB  | No       | Max memory used by the search batch indexer for inflight batches.                                              |
| PGSTREAM_SEARCH_STORE_EXP_BACKOFF_INITIAL_INTERVAL | 1s      | No       | Initial interval for the exponential backoff policy to be applied to the search store operation retries.       |
| PGSTREAM_SEARCH_STORE_EXP_BACKOFF_MAX_INTERVAL     | 1min    | No       | Max interval for the exponential backoff policy to be applied to the search store operation retries.           |
| PGSTREAM_SEARCH_STORE_EXP_BACKOFF_MAX_RETRIES      | 0       | No       | Max retries for the exponential backoff policy to be applied to the search store operation retries.            |
| PGSTREAM_SEARCH_STORE_BACKOFF_INTERVAL             | 0       | No       | Constant interval for the backoff policy to be applied to the search store operation retries.                  |
| PGSTREAM_SEARCH_STORE_BACKOFF_MAX_RETRIES          | 0       | No       | Max retries for the backoff policy to be applied to the search store operation retries.                        |

One of exponential/constant backoff policies can be provided for the search indexer cleanup retry strategy. If none is provided, no retries apply.

One of exponential/constant backoff policies can be provided for the search store retry strategy. If none is provided, a default exponential backoff policy applies.

</details>

<details>
  <summary>Webhook Notifier</summary>

| Environment Variable                                       | Default | Required           | Description                                                                                                 |
| ---------------------------------------------------------- | ------- | ------------------ | ----------------------------------------------------------------------------------------------------------- |
| PGSTREAM_WEBHOOK_SUBSCRIPTION_STORE_URL                    | N/A     | Yes                | URL for the webhook subscription store to connect to.                                                       |
| PGSTREAM_WEBHOOK_SUBSCRIPTION_STORE_CACHE_ENABLED          | False   | No                 | Caching applied to the subscription store retrieval queries.                                                |
| PGSTREAM_WEBHOOK_SUBSCRIPTION_STORE_CACHE_REFRESH_INTERVAL | 60s     | When cache enabled | Interval at which the subscription store cache will be refreshed. Indicates max cache staleness.            |
| PGSTREAM_WEBHOOK_NOTIFIER_MAX_QUEUE_BYTES                  | 100MiB  | No                 | Max memory used by the webhook notifier for inflight notifications.                                         |
| PGSTREAM_WEBHOOK_NOTIFIER_WORKER_COUNT                     | 10      | No                 | Max number of concurrent workers that will send webhook notifications for a given WAL event.                |
| PGSTREAM_WEBHOOK_NOTIFIER_CLIENT_TIMEOUT                   | 10s     | No                 | Max time the notifier will wait for a response from a webhook URL before timing out.                        |
| PGSTREAM_WEBHOOK_SUBSCRIPTION_SERVER_ADDRESS               | ":9900" | No                 | Address for the subscription server to listen on.                                                           |
| PGSTREAM_WEBHOOK_SUBSCRIPTION_SERVER_READ_TIMEOUT          | 5s      | No                 | Max duration for reading an entire server request, including the body before timing out.                    |
| PGSTREAM_WEBHOOK_SUBSCRIPTION_SERVER_WRITE_TIMEOUT         | 10s     | No                 | Max duration before timing out writes of the response. It is reset whenever a new request's header is read. |

</details>

<details>
  <summary>Postgres Batch Writer</summary>

| Environment Variable                         | Default                    | Required | Description                                                                                                                                                                                                    |
| -------------------------------------------- | -------------------------- | -------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| PGSTREAM_POSTGRES_WRITER_TARGET_URL          | N/A                        | Yes      | URL for the PostgreSQL store to connect to                                                                                                                                                                     |
| PGSTREAM_POSTGRES_WRITER_BATCH_TIMEOUT       | 1s                         | No       | Max time interval at which the batch sending to PostgreSQL is triggered.                                                                                                                                       |
| PGSTREAM_POSTGRES_WRITER_BATCH_SIZE          | 100                        | No       | Max number of messages to be sent per batch. When this size is reached, the batch is sent to PostgreSQL.                                                                                                       |
| PGSTREAM_POSTGRES_WRITER_MAX_QUEUE_BYTES     | 100MiB                     | No       | Max memory used by the postgres batch writer for inflight batches.                                                                                                                                             |
| PGSTREAM_POSTGRES_WRITER_BATCH_BYTES         | 1572864                    | No       | Max size in bytes for a given batch. When this size is reached, the batch is sent to PostgreSQL.                                                                                                               |
| PGSTREAM_POSTGRES_WRITER_SCHEMALOG_STORE_URL | N/A                        | No       | URL of the store where the pgstream schemalog table which keeps track of schema changes is.                                                                                                                    |
| PGSTREAM_POSTGRES_WRITER_DISABLE_TRIGGERS    | False(run), True(snapshot) | No       | Option to disable triggers on the target PostgreSQL database while performing the snaphot/replication streaming. It defaults to false when using the run command, and to true when using the snapshot command. |
| PGSTREAM_POSTGRES_WRITER_ON_CONFLICT_ACTION  | error                      | No       | Action to apply to inserts on conflict. Options are `nothing`, `update` or `error`.                                                                                                                            |

</details>

##### Modifiers

<details>
  <summary>Injector</summary>

| Environment Variable                 | Default | Required | Description                                                    |
| ------------------------------------ | ------- | -------- | -------------------------------------------------------------- |
| PGSTREAM_INJECTOR_STORE_POSTGRES_URL | N/A     | Yes      | URL for the postgres URL where the schema log table is stored. |

</details>

<details>
  <summary>Transformer</summary>

| Environment Variable            | Default | Required | Description                                                          |
| ------------------------------- | ------- | -------- | -------------------------------------------------------------------- |
| PGSTREAM_TRANSFORMER_RULES_FILE | N/A     | No       | Filepath pointing to the yaml file containing the transformer rules. |

</details>

<details>
  <summary>Filter</summary>

| Environment Variable           | Default | Required | Description                                                                                                                                                       |
| ------------------------------ | ------- | -------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| PGSTREAM_FILTER_INCLUDE_TABLES | N/A     | No       | List of schema qualified tables for which the WAL events should be processed. If no schema is provided, `public` schema will be assumed. Wildcards are supported. |
| PGSTREAM_FILTER_EXCLUDE_TABLES | N/A     | No       | List of schema qualified tables for which the WAL events should be skipped. If no schema is provided, `public` schema will be assumed. Wildcards are supported.   |

</details>

## Tracking schema changes

One of the main differentiators of pgstream is the fact that it tracks and replicates schema changes automatically. It relies on SQL triggers that will populate a Postgres table (`pgstream.schema_log`) containing a history log of all DDL changes for a given schema. Whenever a schema change occurs, this trigger creates a new row in the schema log table with the schema encoded as a JSON value. This table tracks all the schema changes, forming a linearised change log that is then parsed and used within the pgstream pipeline to identify modifications and push the relevant changes downstream.

The detailed SQL used can be found in the [migrations folder](https://github.com/xataio/pgstream/tree/main/migrations/postgres).

The schema and data changes are part of the same linear stream - the downstream consumers always observe the schema changes as soon as they happen, before any data arrives that relies on the new schema. This prevents data loss and manual intervention.

## Snapshots

![snapshots diagram](img/pgstream_snapshot_diagram.svg)

`pgstream` supports the generation of PostgreSQL schema and data snapshots. It can be done as an initial step before starting the replication, or as a standalone mode, where a snapshot of the database is performed without any replication.

The snapshot behaviour is the same in both cases, with the only difference that if we're listening on the replication slot, we will store the current LSN before performing the snapshot, so that we can replay any operations that happened while the snapshot was ongoing.

The snapshot implementation is different for schema and data.

- Schema: depending on the configuration, it can use either the pgstream `schema_log` table to get the schema view and process it as events downstream, or rely on the `pg_dump`/`pg_restore` PostgreSQL utilities if the target is a PostgreSQL database.

- Data: it relies on transaction snapshot ids to obtain a stable view of the database tables, and paralellises the read of all the rows by dividing them into ranges using the `ctid`.

![snapshots sequence](img/pgstream_snapshot_sequence.svg)

For details on how to use and configure the snapshot mode, check the [snapshot tutorial](tutorials/postgres_snapshot.md).

## Transformers

![transformer diagram](img/pgstream_transformer_diagram.svg)

`pgstream` supports column value transformations to anonymize or mask sensitive data during replication and snapshots. This is particularly useful for compliance with data privacy regulations.

`pgstream` integrates with existing transformer open source libraries, such as [greenmask](https://github.com/GreenmaskIO/greenmask), [neosync](https://github.com/nucleuscloud/neosync) and [go-masker](https://github.com/ggwhite/go-masker), to leverage a large amount of transformation capabilities, as well as having support for custom transformations.

### Supported transformers

#### go-masker

 <details>
  <summary>masking</summary>

**Description:** Masks string values using the provided masking function.

| Supported PostgreSQL types          |
| ----------------------------------- |
| `text`, `varchar`, `char`, `bpchar` |

**Parameter Details:**

| Parameter | Type   | Default | Required | Values                                                                             |
| --------- | ------ | ------- | -------- | ---------------------------------------------------------------------------------- |
| type      | string | default | No       | custom, password, name, address, email, mobile, tel, id, credit_card, url, default |

**Example Configuration:**

```yaml
transformations:
  table_transformers:
    - schema: public
      table: users
      column_transformers:
        email:
          name: masking
          parameters:
            type: email
```

**Input-Output Examples:**

| Input Value              | Configuration Parameters | Output Value           |
| ------------------------ | ------------------------ | ---------------------- |
| `aVeryStrongPassword123` | `type: password`         | `************`         |
| `john.doe@example.com`   | `type: email`            | `joh****e@example.com` |
| `Sensitive Data`         | `type: default`          | `**************`       |

With `custom` type, the masking function is defined by the user, by providing beginning and end indexes for masking. If the input is shorter than the end index, the rest of the string will all be masked. See the third example below.

```yaml
transformations:
  table_transformers:
    - schema: public
      table: users
      column_transformers:
        email:
          name: masking
          parameters:
            type: custom
            mask_begin: "4"
            mask_end: "12"
```

**Input-Output Examples:**

| Input Value             | Output Value            |
| ----------------------- | ----------------------- |
| `1234567812345678`      | `1234********5678`      |
| `sensitive@example.com` | `sens********ample.com` |
| `sensitive`             | `sens*****`             |

If the begin index is not provided, it defaults to 0. If the end is not provided, it defaults to input length.

```yaml
transformations:
  table_transformers:
    - schema: public
      table: users
      column_transformers:
        email:
          name: masking
          parameters:
            type: custom
            mask_end: "5"
```

| Input Value             | Output Value            |
| ----------------------- | ----------------------- |
| `1234567812345678`      | `*****67812345678`      |
| `sensitive@example.com` | `*****tive@example.com` |
| `sensitive`             | `*****tive`             |

Alternatively, since input length may vary, user can provide relative beginning and end indexes, as percentages of the input length.

```yaml
transformations:
  table_transformers:
    - schema: public
      table: users
      column_transformers:
        email:
          name: masking
          parameters:
            type: custom
            mask_begin: "15%"
            mask_end: "85%"
```

| Input Value             | Output Value            |
| ----------------------- | ----------------------- |
| `1234567812345678`      | `12***********678`      |
| `sensitive@example.com` | `sen***************com` |
| `sensitive`             | `s******ve`             |

Alternatively, user can provide unmask begin and end indexes. In that case, the specified part of the input will remain unmasked, while all the rest is masked.
Mask and unmask parameters cannot be provided at the same time.

```yaml
transformations:
  table_transformers:
    - schema: public
      table: users
      column_transformers:
        email:
          name: masking
          parameters:
            type: custom
            unmask_end: "3"
```

| Input Value             | Output Value            |
| ----------------------- | ----------------------- |
| `1234567812345678`      | `123*************`      |
| `sensitive@example.com` | `sen******************` |
| `sensitive`             | `sen******`             |

</details>

#### Greenmask

 <details>
  <summary>greenmask_boolean</summary>

**Description:** Generates random or deterministic boolean values (`true` or `false`).

| Supported PostgreSQL types |
| -------------------------- |
| `boolean`                  |

| Parameter | Type   | Default | Required | Values               |
| --------- | ------ | ------- | -------- | -------------------- |
| generator | string | random  | No       | random,deterministic |

**Example Configuration:**

```yaml
transformations:
  table_transformers:
    - schema: public
      table: users
      column_transformers:
        is_active:
          name: greenmask_boolean
          parameters:
            generator: deterministic
```

**Input-Output Examples:**

| Input Value | Configuration Parameters   | Output Value               |
| ----------- | -------------------------- | -------------------------- |
| `true`      | `generator: deterministic` | `false`                    |
| `false`     | `generator: deterministic` | `true`                     |
| `true`      | `generator: random`        | `true` or `false` (random) |

</details>

 <details>
  <summary>greenmask_choice</summary>

**Description:** Randomly selects a value from a predefined list of choices.

| Supported PostgreSQL types          |
| ----------------------------------- |
| `text`, `varchar`, `char`, `bpchar` |

| Parameter | Type     | Default | Required | Values               |
| --------- | -------- | ------- | -------- | -------------------- |
| generator | string   | random  | No       | random,deterministic |
| choices   | string[] | N/A     | Yes      | N/A                  |

**Example Configuration:**

```yaml
transformations:
  table_transformers:
    - schema: public
      table: orders
      column_transformers:
        status:
          name: greenmask_choice
          parameters:
            generator: random
            choices: ["pending", "shipped", "delivered", "cancelled"]
```

**Input-Output Examples:**

| Input Value | Configuration Parameters   | Output Value         |
| ----------- | -------------------------- | -------------------- |
| `pending`   | `generator: random`        | `shipped` (random)   |
| `shipped`   | `generator: deterministic` | `pending`            |
| `delivered` | `generator: random`        | `cancelled` (random) |

</details>

 <details>
  <summary>greenmask_date</summary>

**Description:** Generates random or deterministic dates within a specified range.

| Supported PostgreSQL types         |
| ---------------------------------- |
| `date`, `timestamp`, `timestamptz` |

| Parameter | Type                  | Default | Required | Values               |
| --------- | --------------------- | ------- | -------- | -------------------- |
| generator | string                | random  | No       | random,deterministic |
| min_value | string (`yyyy-MM-dd`) | N/A     | Yes      | N/A                  |
| max_value | string (`yyyy-MM-dd`) | N/A     | Yes      | N/A                  |

**Example Configuration:**

```yaml
transformations:
  table_transformers:
    - schema: public
      table: events
      column_transformers:
        event_date:
          name: greenmask_date
          parameters:
            generator: random
            min_value: "2020-01-01"
            max_value: "2025-12-31"
```

**Input-Output Examples:**

| Input Value  | Configuration Parameters                                          | Output Value          |
| ------------ | ----------------------------------------------------------------- | --------------------- |
| `2023-01-01` | `generator: random, min_value: 2020-01-01, max_value: 2025-12-31` | `2021-05-15` (random) |
| `2022-06-15` | `generator: deterministic`                                        | `2020-01-01`          |

</details>

 <details>
  <summary>greenmask_firstname</summary>

**Description:** Generates random or deterministic first names, optionally filtered by gender.

| Supported PostgreSQL types          |
| ----------------------------------- |
| `text`, `varchar`, `char`, `bpchar` |

| Parameter | Type   | Default | Required | Values               |
| --------- | ------ | ------- | -------- | -------------------- |
| generator | string | random  | No       | random,deterministic |
| gender    | string | Any     | No       | Any,Female,Male      |

**Example Configuration:**

```yaml
transformations:
  table_transformers:
    - schema: public
      table: employees
      column_transformers:
        first_name:
          name: greenmask_firstname
          parameters:
            generator: deterministic
            gender: Female
```

**Input-Output Examples:**

| Input Name | Configuration Parameters | Output Name |
| ---------- | ------------------------ | ----------- |
| `John`     | `preserve_gender: true`  | `Michael`   |
| `Jane`     | `preserve_gender: true`  | `Emily`     |
| `Alex`     | `preserve_gender: false` | `Jordan`    |
| `Chris`    | `generator: random`      | `Taylor`    |

</details>

 <details>
  <summary>greenmask_float</summary>

**Description:** Generates random or deterministic floating-point numbers within a specified range.

| Supported PostgreSQL types |
| -------------------------- |
| `real`, `double precision` |

| Parameter | Type   | Default                                       | Required | Values               |
| --------- | ------ | --------------------------------------------- | -------- | -------------------- |
| generator | string | random                                        | No       | random,deterministic |
| min_value | float  | -3.40282346638528859811704183484516925440e+38 | No       | N/A                  |
| max_value | float  | 3.40282346638528859811704183484516925440e+38  | No       | N/A                  |

</details>

 <details>
  <summary>greenmask_integer</summary>

**Description:** Generates random or deterministic integers within a specified range.

| Supported PostgreSQL types     |
| ------------------------------ |
| `smallint`,`integer`, `bigint` |

| Parameter | Type   | Default     | Required | Values               |
| --------- | ------ | ----------- | -------- | -------------------- |
| generator | string | random      | No       | random,deterministic |
| size      | int    | 4           | No       | 2,4                  |
| min_value | int    | -2147483648 | No       | N/A                  |
| max_value | int    | 2147483647  | No       | N/A                  |

**Example Configuration:**

```yaml
transformations:
  table_transformers:
    - schema: public
      table: products
      column_transformers:
        stock_quantity:
          name: greenmask_integer
          parameters:
            generator: random
            min_value: 1
            max_value: 1000
```

</details>

 <details>
  <summary>greenmask_string</summary>

**Description:** Generates random or deterministic strings with customizable length and character set.

| Supported PostgreSQL types          |
| ----------------------------------- |
| `text`, `varchar`, `char`, `bpchar` |

| Parameter  | Type   | Default                                                        | Required | Values               |
| ---------- | ------ | -------------------------------------------------------------- | -------- | -------------------- |
| generator  | string | random                                                         | No       | random,deterministic |
| symbols    | string | abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890 | No       | N/A                  |
| min_length | int    | 1                                                              | No       | N/A                  |
| max_length | int    | 100                                                            | No       | N/A                  |

**Example Configuration:**

```yaml
transformations:
  table_transformers:
    - schema: public
      table: users
      column_transformers:
        username:
          name: greenmask_string
          parameters:
            generator: random
            min_length: 5
            max_length: 15
            symbols: "abcdefghijklmnopqrstuvwxyz1234567890"
```

</details>

 <details>
  <summary>greenmask_unix_timestamp</summary>

**Description:** Generates random or deterministic unix timestamps.

| Supported PostgreSQL types    |
| ----------------------------- |
| `smallint`,`integer`,`bigint` |

| Parameter | Type   | Default | Required | Values               |
| --------- | ------ | ------- | -------- | -------------------- |
| generator | string | random  | No       | random,deterministic |
| min_value | string | N/A     | Yes      | N/A                  |
| max_value | string | N/A     | Yes      | N/A                  |

</details>

 <details>
  <summary>greenmask_utc_timestamp</summary>

**Description:** Generates random or deterministic UTC timestamps.

| Supported PostgreSQL types |
| -------------------------- |
| `timestamp`                |

| Parameter     | Type               | Default | Required | Values                                                               |
| ------------- | ------------------ | ------- | -------- | -------------------------------------------------------------------- |
| generator     | string             | random  | No       | random,deterministic                                                 |
| truncate_part | string             | ""      | No       | nanosecond,microsecond,millisecond,second,minute,hour,day,month,year |
| min_timestamp | string (`RFC3339`) | N/A     | Yes      | N/A                                                                  |
| max_timestamp | string (`RFC3339`) | N/A     | Yes      | N/A                                                                  |

</details>

 <details>
  <summary>greenmask_uuid</summary>

**Description:** Generates random or deterministic UUIDs.

| Supported PostgreSQL types                 |
| ------------------------------------------ |
| `uuid`,`text`, `varchar`, `char`, `bpchar` |

| Parameter | Type   | Default | Required | Values               |
| --------- | ------ | ------- | -------- | -------------------- |
| generator | string | random  | No       | random,deterministic |

</details>

#### Neosync

</details>
 <details>
  <summary>neosync_email</summary>

**Description:** Anonymizes email addresses while optionally preserving length and domain.

| Supported PostgreSQL types          |
| ----------------------------------- |
| `text`, `varchar`, `char`, `bpchar` |

| Parameter            | Type     | Default | Required | Values                           |
| -------------------- | -------- | ------- | -------- | -------------------------------- |
| preserve_length      | bool     | false   | No       |                                  |
| preserve_domain      | bool     | false   | No       |                                  |
| excluded_domains     | string[] | N/A     | No       |                                  |
| max_length           | int      | 100     | No       |                                  |
| email_type           | string   | uuidv4  | No       | uuidv4,fullname,any              |
| invalid_email_action | string   | 100     | No       | reject,passthrough,null,generate |
| seed                 | int      | Rand    | No       |                                  |

**Example Configuration:**

```yaml
transformations:
  table_transformers:
    - schema: public
      table: customers
      column_transformers:
        email:
          name: neosync_email
          parameters:
            preserve_length: true
            preserve_domain: true
```

**Input-Output Examples:**

| Input Email            | Configuration Parameters                        | Output Email           |
| ---------------------- | ----------------------------------------------- | ---------------------- |
| `john.doe@example.com` | `preserve_length: true, preserve_domain: true`  | `abcd.efg@example.com` |
| `jane.doe@company.org` | `preserve_length: false, preserve_domain: true` | `random@company.org`   |
| `user123@gmail.com`    | `preserve_length: true, preserve_domain: false` | `abcde123@random.com`  |
| `invalid-email`        | `invalid_email_action: passthrough`             | `invalid-email`        |
| `invalid-email`        | `invalid_email_action: null`                    | `NULL`                 |
| `invalid-email`        | `invalid_email_action: generate`                | `generated@random.com` |

</details>

 <details>
  <summary>neosync_firstname</summary>

**Description:** Generates anonymized first names while optionally preserving length.

| Supported PostgreSQL types          |
| ----------------------------------- |
| `text`, `varchar`, `char`, `bpchar` |

| Parameter       | Type | Default | Required |
| --------------- | ---- | ------- | -------- |
| preserve_length | bool | false   | No       |
| max_length      | int  | 100     | No       |
| seed            | int  | Rand    | No       |

**Example Configuration:**

```yaml
transformations:
  table_transformers:
    - schema: public
      table: users
      column_transformers:
        first_name:
          name: neosync_firstname
          parameters:
            preserve_length: true
```

</details>
 <details>
  <summary>neosync_string</summary>

**Description:** Generates anonymized strings with customizable length.

| Supported PostgreSQL types          |
| ----------------------------------- |
| `text`, `varchar`, `char`, `bpchar` |

| Parameter       | Type | Default | Required |
| --------------- | ---- | ------- | -------- |
| preserve_length | bool | false   | No       |
| min_length      | int  | 1       | No       |
| max_length      | int  | 100     | No       |
| seed            | int  | Rand    | No       |

**Example Configuration:**

```yaml
transformations:
  table_transformers:
    - schema: public
      table: logs
      column_transformers:
        log_message:
          name: neosync_string
          parameters:
            min_length: 10
            max_length: 50
```

</details>

#### Other Transformers

 <details>
  <summary>literal_string</summary>

**Description:** Transforms all values into the given constant value.

| Supported PostgreSQL types             |
| -------------------------------------- |
| All types with a string representation |

| Parameter | Type   | Default | Required |
| --------- | ------ | ------- | -------- |
| literal   | string | N/A     | Yes      |

Below example makes all values in the JSON column `log_message` to become `{'error': null}`.
This transformer can be used for any Postgres type as long as the given string literal has the correct syntax for that type. e.g It can be "5-10-2021" for a date column, or "3.14159265" for a double precision one.

**Example Configuration:**

```yaml
transformations:
  table_transformers:
    - schema: public
      table: logs
      column_transformers:
        log_message:
          name: literal_string
          parameters:
            literal: "{'error': null}"
```

</details>
 <details>
  <summary>phone_number</summary>

**Description:** Generates anonymized phone numbers with customizable length.

| Supported PostgreSQL types          |
| ----------------------------------- |
| `text`, `varchar`, `char`, `bpchar` |

| Parameter       | Type   | Default | Required | Values                |
| --------------- | ------ | ------- | -------- | --------------------- |
| prefix          | string | ""      | No       | N/A                   |
| min_length      | int    | 6       | No       | N/A                   |
| max_length      | int    | 10      | No       | N/A                   |
| generator       | string | random  | No       | random, deterministic |

If the prefix is set, this transformer will always generate phone numbers starting with the prefix.

**Example Configuration:**

```yaml
transformations:
  table_transformers:
    - schema: public
      table: users
      column_transformers:
        phone:
          name: phone_number
          parameters:
            prefix: "+90"
            min_length: 9
            max_length: 12
            generator: deterministic
```

</details>

### Transformation rules

The rules for the transformers are defined in a dedicated yaml file, with the following format:

```yaml
transformations:
  validation_mode: <validation_mode> # Validation mode for the transformation rules. Can be one of strict, relaxed or table_level. Defaults to relaxed if not provided
  table_transformers: # List of table transformations
    - schema: <schema_name> # Name of the table schema
      table: <table_name> # Name of the table
      validation_mode: <validation_mode> # To be used when the global validation_mode is set to `table_level`. Can be one of strict or relaxed
      column_transformers: # List of column transformations
        <column_name>: # Name of the column to which the transformation will be applied
          name: <transformer_name> # Name of the transformer to be applied to the column. If no transformer needs to be applied on strict validation mode, it can be left empty or use `noop`
          parameters: # Transformer parameters as defined in the supported transformers documentation
            <transformer_parameter>: <transformer_parameter_value>
```

Below is a complete example of a transformation rules YAML file:

```yaml
transformations:
  validation_mode: table_level
  table_transformers:
    - schema: public
      table: users
      validation_mode: strict
      column_transformers:
        email:
          name: neosync_email
          parameters:
            preserve_length: true
            preserve_domain: true
        first_name:
          name: greenmask_firstname
          parameters:
            gender: Male
        username:
          name: greenmask_string
          parameters:
            generator: random
            min_length: 5
            max_length: 15
            symbols: "abcdefghijklmnopqrstuvwxyz1234567890"
    - schema: public
      table: orders
      validation_mode: relaxed
      column_transformers:
        status:
          name: greenmask_choice
          parameters:
            generator: random
            choices: ["pending", "shipped", "delivered", "cancelled"]
        order_date:
          name: greenmask_date
          parameters:
            generator: random
            min_value: "2020-01-01"
            max_value: "2025-12-31"
```

Validation mode can be set to `strict` or `relaxed` for all tables at once. Or it can be determined for each table individually, by setting the higher level `validation_mode` parameter to `table_level`. When it is set to strict, pgstream will throw an error if any of the columns in the table do not have a transformer defined. When set to relaxed, pgstream will skip any columns that do not have a transformer defined.
For details on how to use and configure the transformer, check the [transformer tutorial](tutorials/postgres_transformer.md).

## Glossary

- [CDC](https://en.wikipedia.org/wiki/Change_data_capture): Change Data Capture
- [WAL](https://www.postgresql.org/docs/current/wal-intro.html): Write Ahead Logging
- [LSN](https://pgpedia.info/l/LSN-log-sequence-number.html): Log Sequence Number
- [DDL](https://en.wikipedia.org/wiki/Data_definition_language): Data Definition Language

## Summary

`pgstream` is a versatile tool for real-time data replication and transformation. Its modular architecture and support for multiple targets make it ideal for a wide range of use cases, from analytics to compliance.

For more information, check out:

- [pgstream Tutorials](tutorials/)
- [pgstream GitHub Repository](https://github.com/xataio/pgstream)
