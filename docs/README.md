# 📚 `pgstream` Documentation

## Table of Contents

1. [Architecture](#architecture)
   - [WAL Listener](#wal-listener)
   - [WAL Processor](#wal-processor)
2. [Configuration](#configuration)
   - [Listeners](#listeners)
     - [Postgres Listener](#postgres-listener)
     - [Postgres Snapshoter](#postgres-snapshoter)
     - [Kafka Listener](#kafka-listener)
   - [Processors](#processors)
     - [Kafka Batch Writer](#kafka-batch-writer)
     - [Search Batch Indexer](#search-batch-indexer)
     - [Webhook Notifier](#webhook-notifier)
     - [Postgres Batch Writer](#postgres-batch-writer)
     - [Injector](#injector)
     - [Transformer](#transformer)
3. [Tracking Schema Changes](#tracking-schema-changes)
4. [Snapshots](#snapshots)
5. [Transformers](#transformers)
   - [Supported Transformers](#supported-transformers)
   - [Transformation Rules](#transformation-rules)
6. [Glossary](#glossary)

## Architecture

`pgstream` is constructed as a streaming pipeline, where data from one module streams into the next, eventually reaching the configured outputs. It keeps track of schema changes and replicates them along with the data changes to ensure a consistent view of the source data downstream. This modular approach makes adding and integrating output implementations simple and painless.

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

In addition to the implementations described above, there are optional processor decorators, which work in conjunction with one of the main processor implementations described above. Their goal is to act as modifiers to enrich the wal event being processed.

There are currently two implementations of the processor that act as decorators:

- **Injector**: injects some of the pgstream logic into the WAL event. This includes:

  - Data events:

    - Setting the WAL event identity. If provided, it will use the configured id finder (only available when used as a library), otherwise it will default to using the table primary key/unique not null column.
    - Setting the WAL event version. If provided, it will use the configured version finder (only available when used as a library), otherwise it will default to using the event LSN.
    - Adding pgstream IDs to all columns. This allows us to have a constant identifier for a column, so that if there are renames the column id doesn't change. This is particularly helpful for the search store, where a rename would require a reindex, which can be costly depending on the data.

  - Schema events:
    - Acknolwedging the new incoming schema in the Postgres `pgstream.schema_log` table.

- **Transformer**: it modifies the column values in insert/update events according to the rules defined in the configured yaml file. It can be used for anonymising data from the source Postgres database. An example of the rules definition file can be found in the repo under `transformer_rules.yaml`. The rules have per column granularity, and certain transformers from opensource sources, such as greenmask or neosync, are supported. More details can be found in the [transformers section](#transformers).

## Configuration

Here's a list of all the environment variables that can be used to configure the individual modules, along with their descriptions and default values.

### Listeners

<details>
  <summary>Postgres Listener</summary>

| Environment Variable                                | Default                | Required | Description                                                                                                                                                                                                                                                                                                  |
| --------------------------------------------------- | ---------------------- | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| PGSTREAM_POSTGRES_LISTENER_URL                      | N/A                    | Yes      | URL of the Postgres database to connect to for replication purposes.                                                                                                                                                                                                                                         |
| PGSTREAM_POSTGRES_REPLICATION_SLOT_NAME             | "pgstream_dbname_slot" | No       | Name of the Postgres replication slot name.                                                                                                                                                                                                                                                                  |
| PGSTREAM_POSTGRES_LISTENER_INITIAL_SNAPSHOT_ENABLED | false                  | No       | Enables performing an initial snapshot of the Postgres database before starting to listen to the replication slot.                                                                                                                                                                                           |
| PGSTREAM_POSTGRES_INITIAL_SNAPSHOT_TABLES           | ""                     | No       | Tables for which there will be an initial snapshot generated. The syntax supports wildcards. Tables without a schema defined will be applied the public schema. Example: for `public.test_table` and all tables in the `test_schema` schema, the value would be the following: `"test_table test_schema.\*"` |
| PGSTREAM_POSTGRES_INITIAL_SNAPSHOT_SCHEMA_WORKERS   | 4                      | No       | Number of tables per schema that will be processed in parallel by the snapshotting process.                                                                                                                                                                                                                  |
| PGSTREAM_POSTGRES_INITIAL_SNAPSHOT_TABLE_WORKERS    | 4                      | No       | Number of concurrent workers that will be used per table by the snapshotting process.                                                                                                                                                                                                                        |
| PGSTREAM_POSTGRES_INITIAL_SNAPSHOT_BATCH_PAGE_SIZE  | 1000                   | No       | Size of the table page range which will be processed concurrently by the table workers from `PGSTREAM_POSTGRES_INITIAL_SNAPSHOT_TABLE_WORKERS`.                                                                                                                                                              |
| PGSTREAM_POSTGRES_INITIAL_SNAPSHOT_WORKERS          | 1                      | No       | Number of schemas that will be processed in parallel by the snapshotting process.                                                                                                                                                                                                                            |

</details>

<details>
  <summary>Postgres Snapshoter</summary>

| Environment Variable                       | Default | Required | Description                                                                                                                                                                                                                                                                                                  |
| ------------------------------------------ | ------- | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| PGSTREAM_POSTGRES_SNAPSHOT_LISTENER_URL    | N/A     | Yes      | URL of the Postgres database to snapshot.                                                                                                                                                                                                                                                                    |
| PGSTREAM_POSTGRES_SNAPSHOT_TABLES          | ""      | No       | Tables for which there will be an initial snapshot generated. The syntax supports wildcards. Tables without a schema defined will be applied the public schema. Example: for `public.test_table` and all tables in the `test_schema` schema, the value would be the following: `"test_table test_schema.\*"` |
| PGSTREAM_POSTGRES_SNAPSHOT_SCHEMA_WORKERS  | 4       | No       | Number of tables per schema that will be processed in parallel by the snapshotting process.                                                                                                                                                                                                                  |
| PGSTREAM_POSTGRES_SNAPSHOT_TABLE_WORKERS   | 4       | No       | Number of concurrent workers that will be used per table by the snapshotting process.                                                                                                                                                                                                                        |
| PGSTREAM_POSTGRES_SNAPSHOT_BATCH_PAGE_SIZE | 1000    | No       | Size of the table page range which will be processed concurrently by the table workers from `PGSTREAM_POSTGRES_SNAPSHOT_TABLE_WORKERS`.                                                                                                                                                                      |
| PGSTREAM_POSTGRES_SNAPSHOT_WORKERS         | 1       | No       | Number of schemas that will be processed in parallel by the snapshotting process.                                                                                                                                                                                                                            |

</details>

<details>
  <summary>Kafka Listener</summary>

| Environment Variable                               | Default  | Required         | Description                                                                                            |
| -------------------------------------------------- | -------- | ---------------- | ------------------------------------------------------------------------------------------------------ |
| PGSTREAM_KAFKA_SERVERS                             | N/A      | Yes              | URLs for the Kafka servers to connect to.                                                              |
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

### Processors

<details>
  <summary>Kafka Batch Writer</summary>

| Environment Variable                    | Default | Required         | Description                                                                                         |
| --------------------------------------- | ------- | ---------------- | --------------------------------------------------------------------------------------------------- |
| PGSTREAM_KAFKA_SERVERS                  | N/A     | Yes              | URLs for the Kafka servers to connect to.                                                           |
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

| Environment Variable                         | Default | Required | Description                                                                                                      |
| -------------------------------------------- | ------- | -------- | ---------------------------------------------------------------------------------------------------------------- |
| PGSTREAM_POSTGRES_WRITER_TARGET_URL          | N/A     | Yes      | URL for the PostgreSQL store to connect to                                                                       |
| PGSTREAM_POSTGRES_WRITER_BATCH_TIMEOUT       | 1s      | No       | Max time interval at which the batch sending to PostgreSQL is triggered.                                         |
| PGSTREAM_POSTGRES_WRITER_BATCH_SIZE          | 100     | No       | Max number of messages to be sent per batch. When this size is reached, the batch is sent to PostgreSQL.         |
| PGSTREAM_POSTGRES_WRITER_MAX_QUEUE_BYTES     | 100MiB  | No       | Max memory used by the postgres batch writer for inflight batches.                                               |
| PGSTREAM_POSTGRES_WRITER_BATCH_BYTES         | 1572864 | No       | Max size in bytes for a given batch. When this size is reached, the batch is sent to PostgreSQL.                 |
| PGSTREAM_POSTGRES_WRITER_SCHEMALOG_STORE_URL | N/A     | No       | URL of the store where the pgstream schemalog table which keeps track of schema changes is.                      |
| PGSTREAM_POSTGRES_WRITER_DISABLE_TRIGGERS    | False   | No       | Option to disable triggers on the target PostgreSQL database while performing the snaphot/replication streaming. |

</details>

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

## Tracking schema changes

One of the main differentiators of pgstream is the fact that it tracks and replicates schema changes automatically. It relies on SQL triggers that will populate a Postgres table (`pgstream.schema_log`) containing a history log of all DDL changes for a given schema. Whenever a schema change occurs, this trigger creates a new row in the schema log table with the schema encoded as a JSON value. This table tracks all the schema changes, forming a linearised change log that is then parsed and used within the pgstream pipeline to identify modifications and push the relevant changes downstream.

The detailed SQL used can be found in the [migrations folder](https://github.com/xataio/pgstream/tree/main/migrations/postgres).

The schema and data changes are part of the same linear stream - the downstream consumers always observe the schema changes as soon as they happen, before any data arrives that relies on the new schema. This prevents data loss and manual intervention.

## Snapshots

![snapshots diagram](img/pgstream_snapshot_diagram.svg)

`pgstream` supports the generation of PostgreSQL schema and data snapshots. It can be done as an initial step before starting the replication listener, or as a standalone mode, where a snapshot of the database is performed without any replication.

The snapshot behaviour is the same in both cases, with the only difference that if we're listening on the replication slot, we will store the current LSN before performing the snapshot, so that we can replay any operations that happened while the snapshot was ongoing.

The snapshot implementation is different for schema and data.

- Schema: depending on the configuration, it can use either the pgstream `schema_log` table to get the schema view and process it as events downstream, or rely on the `pg_dump`/`pg_restore` PostgreSQL utilities if the output is a PostgreSQL database.

- Data: it relies on transaction snapshot ids to obtain a stable view of the database tables, and paralellises the read of all the rows by dividing them into ranges using the `ctid`.

![snapshots sequence](img/pgstream_snapshot_sequence.svg)

For details on how to use and configure the snapshot mode, check the [snapshot tutorial](tutorials/postgres_snapshot.md).

## Transformers

![transformer diagram](img/pgstream_transformer_diagram.svg)

`pgstream` supports column value transformations to anonymize sensitive data during replication and snapshots. This is particularly useful for compliance with data privacy regulations.

`pgstream` integrates with existing transformer open source libraries, such as [greenmask](https://github.com/GreenmaskIO/greenmask) and [neosync](https://github.com/nucleuscloud/neosync), to leverage a large amount of transformation capabilities, as well as having support for custom transformations.

### Supported transformers

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

### Transformation rules

The rules for the transformers are defined in a dedicated yaml file, with the following format:

```yaml
transformations:
  - schema: <schema_name>
    table: <table_name>
    column_transformers:
      <column_name>:
        name: <transformer_name>
        parameters:
          <transformer_parameter>: <transformer_parameter_value>
```

Below is a complete example of a transformation rules YAML file:

```yaml
transformations:
  - schema: public
    table: users
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

For details on how to use and configure the transformer, check the [transformer tutorial](tutorials/postgres_transformer.md).

## Glossary

- [CDC](https://en.wikipedia.org/wiki/Change_data_capture): Change Data Capture
- [WAL](https://www.postgresql.org/docs/current/wal-intro.html): Write Ahead Logging
- [LSN](https://pgpedia.info/l/LSN-log-sequence-number.html): Log Sequence Number
- [DDL](https://en.wikipedia.org/wiki/Data_definition_language): Data Definition Language

## Summary

`pgstream` is a versatile tool for real-time data replication and transformation. Its modular architecture and support for multiple outputs make it ideal for a wide range of use cases, from analytics to compliance.

For more information, check out:

- [pgstream Tutorials](tutorials/)
- [pgstream GitHub Repository](https://github.com/xataio/pgstream)
