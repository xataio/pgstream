# ⚙️ Configuration

⚠️ Be aware that a single source and a single target must be provided or the configuration validation will fail.

## Yaml

The pgstream configuration can be provided as a yaml configuration file, which encapsulates the transformation configuration. The following sample shows the format for all supported fields.

```yaml
instrumentation:
  metrics:
    endpoint: "0.0.0.0:4317"
    collection_interval: 60 # collection interval for metrics in seconds. Defaults to 60s
  traces:
    endpoint: "0.0.0.0:4317"
    sample_ratio: 0.5 # ratio of traces that will be sampled. Must be between 0.0-1.0, where 0 is no traces sampled, and 1 is all traces sampled.

source:
  postgres:
    url: "postgresql://user:password@localhost:5432/mydatabase"
    mode: snapshot_and_replication # options are replication, snapshot or snapshot_and_replication
    snapshot: # when mode is snapshot or snapshot_and_replication
      mode: full # options are data_and, schema or data
      tables: ["test", "test_schema.Test", "another_schema.*"] # tables to snapshot, can be a list of table names or a pattern
      excluded_tables: ["test_schema.Test"] # tables to exclude for snapshot, wildcards are not supported
      recorder:
        repeatable_snapshots: true # whether to repeat snapshots that have already been taken. Defaults to false
        postgres_url: "postgresql://user:password@localhost:5432/mytargetdatabase" # URL of the database where the snapshot status is recorded
      snapshot_workers: 4 # number of schemas to be snapshotted in parallel. Defaults to 1
      data: # when mode is full or data
        schema_workers: 4 # number of schema tables to be snapshotted in parallel. Defaults to 4
        table_workers: 4 # number of workers to snapshot a table in parallel. Defaults to 4
        batch_bytes: 83886080 # bytes to read per batch (defaults to 80MiB)
      schema: # when mode is full or schema
        mode: pgdump_pgrestore # options are pgdump_pgrestore or schemalog
        pgdump_pgrestore:
          clean_target_db: true # whether to clean the target database before restoring. Defaults to false
          create_target_db: true # whether to create the database on the target postgres. Defaults to false
          include_global_db_objects: true # whether to include database global objects, such as extensions or triggers, on the schema snapshot. Defaults to false
          no_owner: false # whether to remove ownership commands from the dump. Defaults to false
          no_privileges: false # whether to prevent dumping privilege commands (grant/revoke). Defaults to false
          role: postgres # role name to be used to create the dump
          roles_snapshot_mode: # no_passwords by default. Can be set to disabled to disable roles snapshotting, or can be set to enabled to include role passwords
          exclude_security_labels: ["anon"] # list of providers whose security labels will be excluded from the snapshot. Wildcard supported.
          dump_file: pg_dump.sql # name of the file where the contents of the schema pg_dump command and output will be written for debugging purposes.
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
      timeout: 1000 # batch timeout in milliseconds. Defaults to 30s
      size: 100 # number of messages in a batch. Defaults to 20000
      max_bytes: 1572864 # max size of batch in bytes (1.5MiB). Defaults to 1.5MiB without bulk enabled, 80MiB with bulk ingest.
      max_queue_bytes: 104857600 # max size of memory guard queue in bytes (100MiB). Defaults to 100MiB
      ignore_send_errors: false # if true, log and ignore errors during batch sending. Warning: can result in consistency errors.
    schema_log_store_url: "postgresql://user:password@localhost:5432/mydatabase" # url to the postgres database where the schema log is stored to be used when performing schema change diffs
    disable_triggers: false # whether to disable triggers on the target database. Defaults to false
    on_conflict_action: "nothing" # options are update, nothing or error. Defaults to error
    bulk_ingest:
      enabled: true # whether to enable bulk ingest on the target postgres, using COPY FROM (supported for insert only workloads)
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
      max_queue_bytes: 104857600 # max size of memory guard queue in bytes (100MiB). Defaults to 100MiB
      ignore_send_errors: false # if true, log and ignore errors during batch sending. Warning: can result in consistency errors.
  search:
    engine: "elasticsearch" # options are elasticsearch or opensearch
    url: "http://localhost:9200" # URL of the search engine
    batch:
      timeout: 1000 # batch timeout in milliseconds. Defaults to 1s
      size: 100 # number of messages in a batch. Defaults to 100
      max_bytes: 1572864 # max size of batch in bytes (1.5MiB). Defaults to 1.5MiB
      max_queue_bytes: 104857600 # max size of memory guard queue in bytes (100MiB). Defaults to 100MiB
      ignore_send_errors: false # if true, log and ignore errors during batch sending. Warning: can result in consistency errors.
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

## Environment Variables

Here's a list of all the environment variables that can be used to configure the individual modules, along with their descriptions and default values.

### Sources

<details>
  <summary>Postgres Listener</summary>

| Environment Variable                                 | Default                      | Required | Description                                                                                                                                                                                                                                                                                                  |
| ---------------------------------------------------- | ---------------------------- | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| PGSTREAM_POSTGRES_LISTENER_URL                       | N/A                          | Yes      | URL of the Postgres database to connect to for replication purposes.                                                                                                                                                                                                                                         |
| PGSTREAM_POSTGRES_REPLICATION_SLOT_NAME              | "pgstream_dbname_slot"       | No       | Name of the Postgres replication slot name.                                                                                                                                                                                                                                                                  |
| PGSTREAM_POSTGRES_SNAPSHOT_TABLES                    | ""                           | No       | Tables for which there will be an initial snapshot generated. The syntax supports wildcards. Tables without a schema defined will be applied the public schema. Example: for `public.test_table` and all tables in the `test_schema` schema, the value would be the following: `"test_table test_schema.\*"` |
| PGSTREAM_POSTGRES_SNAPSHOT_EXCLUDED_TABLES           | ""                           | No       | Tables that will be excluded in the snapshot process. The syntax does not support wildcards. Tables without a schema defined will be applied the public schema.                                                                                                                                              |
| PGSTREAM_POSTGRES_SNAPSHOT_SCHEMA_WORKERS            | 4                            | No       | Number of tables per schema that will be processed in parallel by the snapshotting process.                                                                                                                                                                                                                  |
| PGSTREAM_POSTGRES_SNAPSHOT_TABLE_WORKERS             | 4                            | No       | Number of concurrent workers that will be used per table by the snapshotting process.                                                                                                                                                                                                                        |
| PGSTREAM_POSTGRES_SNAPSHOT_BATCH_BYTES               | 83886080 (80MiB)             | No       | Max batch size in bytes to be read and processed by each table worker at a time. The number of pages in the select queries will be based on this value.                                                                                                                                                      |
| PGSTREAM_POSTGRES_SNAPSHOT_WORKERS                   | 1                            | No       | Number of schemas that will be processed in parallel by the snapshotting process.                                                                                                                                                                                                                            |
| PGSTREAM_POSTGRES_SNAPSHOT_USE_SCHEMALOG             | False                        | No       | Forces the use of the `pgstream.schema_log` for the schema snapshot instead of using `pg_dump`/`pg_restore` for Postgres targets.                                                                                                                                                                            |
| PGSTREAM_POSTGRES_SNAPSHOT_CLEAN_TARGET_DB           | False                        | No       | When using `pg_dump`/`pg_restore` to snapshot schema for Postgres targets, option to issue commands to DROP all the objects that will be restored.                                                                                                                                                           |
| PGSTREAM_POSTGRES_SNAPSHOT_INCLUDE_GLOBAL_DB_OBJECTS | False                        | No       | When using `pg_dump`/`pg_restore` to snapshot schema for Postgres targets, option to snapshot all global database objects outside of the selected schema (such as extensions, triggers, etc).                                                                                                                |
| PGSTREAM_POSTGRES_SNAPSHOT_CREATE_TARGET_DB          | False                        | No       | When using `pg_dump`/`pg_restore` to snapshot schema for Postgres targets, option to create the database being restored.                                                                                                                                                                                     |
| PGSTREAM_POSTGRES_SNAPSHOT_NO_OWNER                  | False                        | No       | When using `pg_dump`/`pg_restore` to snapshot schema for Postgres targets, do not output commands to set ownership of objects to match the original database.                                                                                                                                                |
| PGSTREAM_POSTGRES_SNAPSHOT_NO_PRIVILEGES             | False                        | No       | When using `pg_dump`/`pg_restore` to snapshot schema for Postgres targets, do not output privilege related commands (grant/revoke).                                                                                                                                                                          |
| PGSTREAM_POSTGRES_SNAPSHOT_EXCLUDED_SECURITY_LABELS  | []                           | No       | When using `pg_dump`/`pg_restore` to snapshot schema for Postgres targets, list of providers whose security labels will be excluded.                                                                                                                                                                         |
| PGSTREAM_POSTGRES_SNAPSHOT_ROLE                      | ""                           | No       | When using `pg_dump`/`pg_restore` to snapshot schema for Postgres targets, role name to be used to create the dump.                                                                                                                                                                                          |
| PGSTREAM_POSTGRES_SNAPSHOT_ROLES_SNAPSHOT_MODE       | "no_passwords"               | No       | When using `pg_dump`/`pg_restore` to snapshot schema for Postgres targets, controls how roles are snapshotted. Possible values: "enabled" (snapshot all roles including passwords), "disabled" (do not snapshot roles), "no_passwords" (snapshot roles but exclude passwords).                               |
| PGSTREAM_POSTGRES_SNAPSHOT_SCHEMA_DUMP_FILE          | ""                           | No       | When using `pg_dump`/`pg_restore` to snapshot schema for Postgres targets, file where the contents of the schema pg_dump command and output will be written for debugging purposes.                                                                                                                          |
| PGSTREAM_POSTGRES_SNAPSHOT_STORE_URL                 | ""                           | No       | Postgres URL for the database where the snapshot requests and their status will be tracked. A table `snapshot_requests` will be created under a `pgstream` schema.                                                                                                                                           |
| PGSTREAM_POSTGRES_SNAPSHOT_STORE_REPEATABLE          | False (run), True (snapshot) | No       | Allow to repeat snapshots requests that have been already completed succesfully. If using the run command, initial snapshots won't be repeatable by default. If the snapshot command is used instead, the snapshot will be repeatable by default.                                                            |

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

### Targets

<details>
  <summary>Kafka Batch Writer</summary>

| Environment Variable                           | Default | Required         | Description                                                                                         |
| ---------------------------------------------- | ------- | ---------------- | --------------------------------------------------------------------------------------------------- |
| PGSTREAM_KAFKA_WRITER_SERVERS                  | N/A     | Yes              | URLs for the Kafka servers to connect to.                                                           |
| PGSTREAM_KAFKA_TOPIC_NAME                      | N/A     | Yes              | Name of the Kafka topic to write to.                                                                |
| PGSTREAM_KAFKA_TOPIC_PARTITIONS                | 1       | No               | Number of partitions created for the Kafka topic if auto create is enabled.                         |
| PGSTREAM_KAFKA_TOPIC_REPLICATION_FACTOR        | 1       | No               | Replication factor used when creating the Kafka topic if auto create is enabled.                    |
| PGSTREAM_KAFKA_TOPIC_AUTO_CREATE               | False   | No               | Auto creation of configured Kafka topic if it doesn't exist.                                        |
| PGSTREAM_KAFKA_TLS_ENABLED                     | False   | No               | Enable TLS connection to the Kafka servers.                                                         |
| PGSTREAM_KAFKA_TLS_CA_CERT_FILE                | ""      | When TLS enabled | Path to the CA PEM certificate to use for Kafka TLS authentication.                                 |
| PGSTREAM_KAFKA_TLS_CLIENT_CERT_FILE            | ""      | No               | Path to the client PEM certificate to use for Kafka TLS client authentication.                      |
| PGSTREAM_KAFKA_TLS_CLIENT_KEY_FILE             | ""      | No               | Path to the client PEM private key to use for Kafka TLS client authentication.                      |
| PGSTREAM_KAFKA_WRITER_BATCH_TIMEOUT            | 1s      | No               | Max time interval at which the batch sending to Kafka is triggered.                                 |
| PGSTREAM_KAFKA_WRITER_BATCH_BYTES              | 1572864 | No               | Max size in bytes for a given batch. When this size is reached, the batch is sent to Kafka.         |
| PGSTREAM_KAFKA_WRITER_BATCH_SIZE               | 100     | No               | Max number of messages to be sent per batch. When this size is reached, the batch is sent to Kafka. |
| PGSTREAM_KAFKA_WRITER_BATCH_IGNORE_SEND_ERRORS | False   | No               | Whether to ignore errors encountered while sending batches to the target.                           |
| PGSTREAM_KAFKA_WRITER_MAX_QUEUE_BYTES          | 100MiB  | No               | Max memory used by the Kafka batch writer for inflight batches.                                     |

</details>

<details>
  <summary>Search Batch Indexer</summary>

| Environment Variable                               | Default | Required | Description                                                                                                    |
| -------------------------------------------------- | ------- | -------- | -------------------------------------------------------------------------------------------------------------- |
| PGSTREAM_OPENSEARCH_STORE_URL                      | N/A     | Yes      | URL for the opensearch store to connect to (at least one of the URLs must be provided).                        |
| PGSTREAM_ELASTICSEARCH_STORE_URL                   | N/A     | Yes      | URL for the elasticsearch store to connect to (at least one of the URLs must be provided).                     |
| PGSTREAM_SEARCH_INDEXER_BATCH_TIMEOUT              | 1s      | No       | Max time interval at which the batch sending to the search store is triggered.                                 |
| PGSTREAM_SEARCH_INDEXER_BATCH_SIZE                 | 100     | No       | Max number of messages to be sent per batch. When this size is reached, the batch is sent to the search store. |
| PGSTREAM_SEARCH_INDEXER_BATCH_IGNORE_SEND_ERRORS   | False   | No       | Whether to ignore errors encountered while sending batches to the target.                                      |
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

| Environment Variable                              | Default                         | Required | Description                                                                                                                                                                                                    |
| ------------------------------------------------- | ------------------------------- | -------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| PGSTREAM_POSTGRES_WRITER_TARGET_URL               | N/A                             | Yes      | URL for the PostgreSQL store to connect to                                                                                                                                                                     |
| PGSTREAM_POSTGRES_WRITER_BATCH_TIMEOUT            | 30s                             | No       | Max time interval at which the batch sending to PostgreSQL is triggered.                                                                                                                                       |
| PGSTREAM_POSTGRES_WRITER_BATCH_SIZE               | 20000                           | No       | Max number of messages to be sent per batch. When this size is reached, the batch is sent to PostgreSQL.                                                                                                       |
| PGSTREAM_POSTGRES_WRITER_MAX_QUEUE_BYTES          | 100MiB                          | No       | Max memory used by the postgres batch writer for inflight batches.                                                                                                                                             |
| PGSTREAM_POSTGRES_WRITER_BATCH_BYTES              | 1.5MiB, 80MiB with bulk enabled | No       | Max size in bytes for a given batch. When this size is reached, the batch is sent to PostgreSQL.                                                                                                               |
| PGSTREAM_POSTGRES_WRITER_BATCH_IGNORE_SEND_ERRORS | False                           | No       | Whether to ignore errors encountered while sending events to the target.                                                                                                                                       |
| PGSTREAM_POSTGRES_WRITER_SCHEMALOG_STORE_URL      | N/A                             | No       | URL of the store where the pgstream schemalog table which keeps track of schema changes is.                                                                                                                    |
| PGSTREAM_POSTGRES_WRITER_DISABLE_TRIGGERS         | False(run), True(snapshot)      | No       | Option to disable triggers on the target PostgreSQL database while performing the snaphot/replication streaming. It defaults to false when using the run command, and to true when using the snapshot command. |
| PGSTREAM_POSTGRES_WRITER_ON_CONFLICT_ACTION       | error                           | No       | Action to apply to inserts on conflict. Options are `nothing`, `update` or `error`.                                                                                                                            |
| PGSTREAM_POSTGRES_WRITER_BULK_INGEST_ENABLED      | False(run), True(snapshot)      | No       | Wether to use COPY FROM on insert only workloads. It defaults to false when using the run command, and to true when using the snapshot command.                                                                |

</details>

#### Modifiers

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

### Instrumentation

<details>
  <summary>Metrics</summary>

| Environment Variable                 | Default | Required | Description                                                            |
| ------------------------------------ | ------- | -------- | ---------------------------------------------------------------------- |
| PGSTREAM_METRICS_ENDPOINT            | N/A     | No       | Endpoint where the pgstream metrics will be exported to.               |
| PGSTREAM_METRICS_COLLECTION_INTERVAL | 60s     | No       | Interval at which the pgstream metrics will be collected and exported. |

</details>

<details>
  <summary>Traces</summary>

| Environment Variable         | Default | Required | Description                                                                                                                     |
| ---------------------------- | ------- | -------- | ------------------------------------------------------------------------------------------------------------------------------- |
| PGSTREAM_TRACES_ENDPOINT     | N/A     | No       | Endpoint where the pgstream traces will be exported to.                                                                         |
| PGSTREAM_TRACES_SAMPLE_RATIO | 0       | No       | Ratio for the trace sampling. Value must be between 0.0 and 1.0, where 0.0 is no traces sampled, and 1.0 is all traces sampled. |

</details>
