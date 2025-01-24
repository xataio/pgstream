<div align="center">
  <img src="brand-kit/banner/pgstream-banner@2x.png" alt="pgstream logo" />
</div>

<p align="center">
  <a href="https://github.com/xataio/pgstream/blob/main/LICENSE"><img src="https://img.shields.io/badge/License-Apache_2.0-green" alt="License - Apache 2.0"></a>&nbsp;
  <a href="https://github.com/xataio/pgstream/actions?query=branch%3Amain"><img src="https://github.com/xataio/pgstream/actions/workflows/build.yml/badge.svg" alt="CI Build"></a> &nbsp;
  <a href="https://pkg.go.dev/github.com/xataio/pgstream"><img src="https://pkg.go.dev/badge/github.com/xataio/pgstream.svg" alt="Go Reference"></a>&nbsp;
  <a href="https://xata.io/discord"><img src="https://img.shields.io/discord/996791218879086662?label=Discord" alt="Discord"></a> &nbsp;
  <a href="https://twitter.com/xata"><img src="https://img.shields.io/twitter/follow/xata?style=flat" alt="X (formerly Twitter) Follow" /> </a>
</p>

# pgstream - Postgres replication with DDL changes

`pgstream` is an open source CDC command-line tool and library that offers Postgres replication support with DDL changes to any provided output.

## Features

- Schema change tracking and replication of DDL changes
- Fast initial snapshots
- Modular deployment configuration, only requires Postgres
- Schema based message partitioning
- Schema filtering
- Elasticsearch/OpenSearch replication output plugin support
- Webhook support
- Automatic discovery of table primary key/unique not null columns for use as event identity
- Highly customisable modules when used as library
- Core metrics available via opentelemetry
- Extendable support for custom replication output plugins
- Continuous consumption of replication slot with configurable memory guards

## Table of Contents

- [Usage](#usage)
- [Configuration](#configuration)
- [Architecture](#architecture)
- [Limitations](#limitations)
- [Glossary](#glossary)
- [Contributing](#contributing)
- [License](#license)
- [Support](#support)

## Usage

`pgstream` can be used via the readily available CLI or as a library.

### CLI

#### Installation

##### Binaries

Binaries are available for Linux, macOS & Windows, check our [Releases](https://github.com/xataio/pgstream/releases).

##### From source

To install `pgstream` from the source, run the following command:

```sh
go install github.com/xataio/pgstream@latest
```

##### From package manager - Homebrew

To install `pgstream` with homebrew, run the following command:

```sh
# macOS or Linux
brew tap xataio/pgstream
brew install pgstream
```

#### Environment setup

If you have an environment available, with at least Postgres and whichever module resources you're planning on running, then you can skip this step. Otherwise, a docker setup is available in this repository that starts Postgres, Kafka and OpenSearch (as well as OpenSearch dashboards for easy visualisation).

```
docker-compose -f build/docker/docker-compose.yml up
```

#### Prepare the database

This will create the `pgstream` schema in the configured Postgres database, along with the tables/functions/triggers required to keep track of the schema changes. See [Tracking schema changes](#tracking-schema-changes) section for more details. It will also create a replication slot for the configured database which will be used by the pgstream service.

```
pgstream init --pgurl "postgres://postgres:postgres@localhost?sslmode=disable"
```

If you want to provide the name of the replication slot to be created instead of using the default value (`pgstream_<dbname>_slot`), you can use the `--replication-slot` flag or set the environment variable `PGSTREAM_POSTGRES_REPLICATION_SLOT_NAME`.

```
pgstream init --pgurl "postgres://postgres:postgres@localhost?sslmode=disable" --replication-slot test
```

If there are any issues or if you want to clean up the pgstream setup, you can run the following.

```
pgstream tear-down --pgurl "postgres://postgres:postgres@localhost?sslmode=disable"
```

This command will clean up all pgstream state.

#### Run pgstream

Run will require the configuration to be provided, either via environment variables, config file or a combination of both. There are some sample configuration files provided in the repo that can be used as guidelines.

Example running pgstream with Postgres -> OpenSearch:

```
pgstream run -c pg2os.env --log-level trace
```

Example running pgstream with Postgres -> Kafka, and in a separate terminal, Kafka->OpenSearch:

```
pgstream run -c pg2kafka.env --log-level trace
pgstream run -c kafka2os.env --log-level trace
```

The run command will parse the configuration provided, and initialise the configured modules. It requires at least one listener and one processor.

## Configuration

Here's a list of all the environment variables that can be used to configure the individual modules, along with their descriptions and default values.

### Listeners

<details>
  <summary>Postgres Listener</summary>

| Environment Variable                               | Default                    | Required | Description                                                                                                                                                                                                                                                                                                  |
| -------------------------------------------------- | -------------------------- | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| PGSTREAM_POSTGRES_LISTENER_URL                     | N/A                        | Yes      | URL of the Postgres database to connect to for replication purposes.                                                                                                                                                                                                                                         |
| PGSTREAM_POSTGRES_REPLICATION_SLOT_NAME            | "pgstream\_<dbname>\_slot" | No       | Name of the Postgres replication slot name.                                                                                                                                                                                                                                                                  |
| PGSTREAM_POSTGRES_INITIAL_SNAPSHOT_TABLES          | ""                         | No       | Tables for which there will be an initial snapshot generated. The syntax supports wildcards. Tables without a schema defined will be applied the public schema. Example: for `public.test_table` and all tables in the `test_schema` schema, the value would be the following: `"test_table test_schema.\*"` |
| PGSTREAM_POSTGRES_INITIAL_SNAPSHOT_SCHEMA_WORKERS  | 4                          | No       | Number of tables per schema that will be processed in parallel by the snapshotting process.                                                                                                                                                                                                                  |
| PGSTREAM_POSTGRES_INITIAL_SNAPSHOT_TABLE_WORKERS   | 4                          | No       | Number of concurrent workers that will be used per table by the snapshotting process.                                                                                                                                                                                                                        |
| PGSTREAM_POSTGRES_INITIAL_SNAPSHOT_BATCH_PAGE_SIZE | 1000                       | No       | Size of the table page range which will be processed concurrently by the table workers from `PGSTREAM_POSTGRES_INITIAL_SNAPSHOT_TABLE_WORKERS`.                                                                                                                                                              |
| PGSTREAM_POSTGRES_INITIAL_SNAPSHOT_WORKERS         | 1                          | No       | Number of schemas that will be processed in parallel by the snapshotting process.                                                                                                                                                                                                                            |

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
| -------------------------------------------------- | ------- | -------- | -------------------------------------------------------------------------------------------------------------- | --- |
| PGSTREAM_OPENSEARCH_STORE_URL                      | N/A     | Yes      | URL for the opensearch store to connect to (at least one of the URLs must be provided).                        |
| PGSTREAM_ELASTICSEARCH_STORE_URL                   | N/A     | Yes      | URL for the elasticsearch store to connect to (at least one of the URLs must be provided).                     |
| PGSTREAM_SEARCH_INDEXER_BATCH_TIMEOUT              | 1s      | No       | Max time interval at which the batch sending to the search store is triggered.                                 |
| PGSTREAM_SEARCH_INDEXER_BATCH_SIZE                 | 100     | No       | Max number of messages to be sent per batch. When this size is reached, the batch is sent to the search store. |
| PGSTREAM_SEARCH_INDEXER_MAX_QUEUE_BYTES            | 100MiB  | No       | Max memory used by the search batch indexer for inflight batches.                                              |     |
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
  <summary>Injector</summary>

| Environment Variable                 | Default | Required | Description                                                    |
| ------------------------------------ | ------- | -------- | -------------------------------------------------------------- |
| PGSTREAM_INJECTOR_STORE_POSTGRES_URL | N/A     | Yes      | URL for the postgres URL where the schema log table is stored. |

</details>

## Tracking schema changes

One of the main differentiators of pgstream is the fact that it tracks and replicates schema changes automatically. It relies on SQL triggers that will populate a Postgres table (`pgstream.schema_log`) containing a history log of all DDL changes for a given schema. Whenever a schema change occurs, this trigger creates a new row in the schema log table with the schema encoded as a JSON value. This table tracks all the schema changes, forming a linearised change log that is then parsed and used within the pgstream pipeline to identify modifications and push the relevant changes downstream.

The detailed SQL used can be found in the [migrations folder](https://github.com/xataio/pgstream/tree/main/migrations/postgres).

The schema and data changes are part of the same linear stream - the downstream consumers always observe the schema changes as soon as they happen, before any data arrives that relies on the new schema. This prevents data loss and manual intervention.

## Architecture

`pgstream` is constructed as a streaming pipeline, where data from one module streams into the next, eventually reaching the configured output plugins. It keeps track of schema changes and replicates them along with the data changes to ensure a consistent view of the source data downstream. This modular approach makes adding and integrating output plugin implementations simple and painless.

![pgstream architecture v1](docs/img/pgstream_arch_v1.png)

At a high level the implementation is split into WAL listeners and WAL processors.

### WAL Listener

A listener is anything that listens for WAL data, regardless of the source. It has a single responsibility: consume and manage the WAL events, delegating the processing of those entries to modules that form the processing pipeline. Depending on the listener implementation, it might be required to also have a checkpointer to flag the events as processed once the processor is done.

There are currently two implementations of the listener:

- **Postgres listener**: listens to WAL events directly from the replication slot. Since the WAL replication slot is sequential, the Postgres WAL listener is limited to run as a single process. The associated Postgres checkpointer will sync the LSN so that the replication lag doesn't grow indefinitely.

- **Kafka reader**: reads WAL events from a Kafka topic. It can be configured to run concurrently by using partitions and Kafka consumer groups, applying a fan-out strategy to the WAL events. The data will be partitioned by database schema by default, but can be configured when using `pgstream` as a library. The associated Kafka checkpointer will commit the message offsets per topic/partition so that the consumer group doesn't process the same message twice.

### WAL Processor

A processor processes a WAL event. Depending on the implementation it might also be required to checkpoint the event once it's done processing it as described above.

There are currently two implementations of the processor:

- **Kafka batch writer**: it writes the WAL events into a Kafka topic, using the event schema as the Kafka key for partitioning. This implementation allows to fan-out the sequential WAL events, while acting as an intermediate buffer to avoid the replication slot to grow when there are slow consumers. It has a memory guarded buffering system internally to limit the memory usage of the buffer. The buffer is sent to Kafka based on the configured linger time and maximum size. It treats both data and schema events equally, since it doesn't care about the content.

- **Search batch indexer**: it indexes the WAL events into an OpenSearch/Elasticsearch compatible search store. It implements the same kind of mechanism than the Kafka batch writer to ensure continuous processing from the listener, and it also uses a batching mechanism to minimise search store calls. The search mapping logic is configurable when used as a library. The WAL event identity is used as the search store document id, and if no other version is provided, the LSN is used as the document version. Events that do not have an identity are not indexed. Schema events are stored in a separate search store index (`pgstream`), where the schema log history is kept for use within the search store (i.e, read queries).

- **Webhook notifier**: it sends a notification to any webhooks that have subscribed to the relevant wal event. It relies on a subscription HTTP server receiving the subscription requests and storing them in the shared subscription store which is accessed whenever a wal event is processed. It sends the notifications to the different subscribed webhook urls in parallel based on a configurable number of workers (client timeouts apply). Similar to the two previous processor implementations, it uses a memory guarded buffering system internally, which allows to separate the wal event processing from the webhook url sending, optimising the processor latency.

In addition to the implementations described above, there's an optional processor decorator, the **injector**, that injects some of the pgstream logic into the WAL event. This includes:

- Data events:

  - Setting the WAL event identity. If provided, it will use the configured id finder (only available when used as a library), otherwise it will default to using the table primary key/unique not null column.
  - Setting the WAL event version. If provided, it will use the configured version finder (only available when used as a library), otherwise it will default to using the event LSN.
  - Adding pgstream IDs to all columns. This allows us to have a constant identifier for a column, so that if there are renames the column id doesn't change. This is particularly helpful for the search store, where a rename would require a reindex, which can be costly depending on the data.

- Schema events:
  - Acknolwedging the new incoming schema in the Postgres `pgstream.schema_log` table.

## Limitations

Some of the limitations of the initial release include:

- Single Kafka topic support
- Postgres plugin support limited to `wal2json`
- Data filtering limited to schema level
- No on demand data snapshots
- Primary key/unique not null column required for replication
- Kafka serialisation support limited to JSON

## Glossary

- [CDC](https://en.wikipedia.org/wiki/Change_data_capture): Change Data Capture
- [WAL](https://www.postgresql.org/docs/current/wal-intro.html): Write Ahead Logging
- [LSN](https://pgpedia.info/l/LSN-log-sequence-number.html): Log Sequence Number
- [DDL](https://en.wikipedia.org/wiki/Data_definition_language): Data Definition Language

## Contributing

We welcome contributions from the community! If you'd like to contribute to pgstream, please follow these guidelines:

- Create an issue for any questions, bug reports, or feature requests.
- Check the documentation and existing issues before opening a new issue.

### Contributing Code

1. Fork the repository.
2. Create a new branch for your feature or bug fix.
3. Make your changes and write tests if applicable.
4. Ensure your code passes linting and tests.
   - There's a [pre-commit](https://pre-commit.com/) configuration available on the root directory (`.pre-commit-config.yaml`), which can be used to validate the CI checks locally.
5. Submit a pull request.

For this project, we pledge to act and interact in ways that contribute to an open, welcoming, diverse, inclusive, and healthy community.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Support

If you have any questions, encounter issues, or need assistance, open an issue in this repository our join our [Discord](https://xata.io/discord), and our community will be happy to help.

<br>
<p align="right">Made with :heart: by <a href="https://xata.io">Xata 🦋</a></p>
