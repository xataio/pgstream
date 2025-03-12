<div align="center">
  <img src="brand-kit/banner/pgstream-banner@2x.png" alt="pgstream logo" />
</div>

<p align="center">
  <a href="https://github.com/xataio/pgstream/blob/main/LICENSE"><img src="https://img.shields.io/badge/License-Apache_2.0-green" alt="License - Apache 2.0"></a>&nbsp;
  <a href="https://github.com/xataio/pgstream/actions?query=branch%3Amain"><img src="https://github.com/xataio/pgstream/actions/workflows/build.yml/badge.svg" alt="CI Build"></a> &nbsp;
  <a href="https://pkg.go.dev/github.com/xataio/pgstream"><img src="https://pkg.go.dev/badge/github.com/xataio/pgstream.svg" alt="Go Reference"></a>&nbsp;
  <a href="https://github.com/xataio/pgstream/releases"><img src="https://img.shields.io/github/release/xataio/pgstream.svg?label=Release" alt="Release"></a> &nbsp;
  <a href="https://somsubhra.github.io/github-release-stats/?username=xataio&repository=pgstream&page=1&per_page=5"><img src="https://img.shields.io/github/downloads/xataio/pgstream/total" alt="Downloads"></a> &nbsp;
  <a href="https://goreportcard.com/report/github.com/xataio/pgstream"><img src="https://goreportcard.com/badge/github.com/xataio/pgstream" alt="Go Report Card"></a> &nbsp;
  <a href="https://xata.io/discord"><img src="https://img.shields.io/discord/996791218879086662?label=Discord&logo=discord" alt="Discord"></a> &nbsp;
  <a href="https://twitter.com/xata"><img src="https://img.shields.io/twitter/follow/xata?style=flat&color=8566ff" alt="X (formerly Twitter) Follow" /> </a>
</p>

# pgstream - Postgres replication with DDL changes

`pgstream` is an open source CDC command-line tool and library that offers Postgres replication support with DDL changes to any provided output.

## Features

- Schema change tracking and replication of DDL changes
- Fast initial snapshots
- Column value transformations
- Modular deployment configuration, only requires Postgres
- Multiple out of the box supported outputs
  - Elasticsearch/OpenSearch
  - Webhooks
  - PostgreSQL
- Kafka support with schema based partitioning
- Snapshot only mode (for when you don't need continuous replication)
- Extendable support for custom output plugins
- Automatic discovery of table primary key/unique not null columns for use as event identity
- Core metrics available via opentelemetry
- Continuous consumption of replication slot with configurable memory guards

## Table of Contents

- [Usage](#usage)
- [Configuration](#configuration)
- [Documentation](#documentation)
- [Limitations](#limitations)
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

The docker-compose file has profiles that can be used in order to bring up only the relevant containers. If for example you only want to run PostgreSQL to PostgreSQL pgstream replication you can use the `pg2pg` profile as follows:

```
docker-compose -f build/docker/docker-compose.yml --profile pg2pg up
```

You can also run multiple profiles. For example to start two PostgreSQL instances and Kafka:

```
docker-compose -f build/docker/docker-compose.yml --profile pg2pg --profile kafka up
```

List of supported docker profiles:

- pg2pg
- pg2os
- pg2webhook
- kafka

#### Prepare the database

This will create the `pgstream` schema in the configured Postgres database, along with the tables/functions/triggers required to keep track of the schema changes. See [Tracking schema changes](docs/README.md#tracking-schema-changes) section for more details. It will also create a replication slot for the configured database which will be used by the pgstream service.

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

Example running pgstream with PostgreSQL -> PostgreSQL with initial snapshot enabled:

```
pgstream run -c pg2pg.env --log-level trace
```

Example running pgstream with PostgreSQL snapshot only mode -> PostgreSQL:

```
pgstream run -c snapshot2pg.env --log-level trace
```

The run command will parse the configuration provided, and initialise the configured modules. It requires at least one listener and one processor.

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
  <summary>Postgres Batch Writer</summary>

| Environment Variable                         | Default | Required | Description                                                                                                      |
| -------------------------------------------- | ------- | -------- | ---------------------------------------------------------------------------------------------------------------- | --- |
| PGSTREAM_POSTGRES_WRITER_TARGET_URL          | N/A     | Yes      | URL for the PostgreSQL store to connect to                                                                       |
| PGSTREAM_POSTGRES_WRITER_BATCH_TIMEOUT       | 1s      | No       | Max time interval at which the batch sending to PostgreSQL is triggered.                                         |
| PGSTREAM_POSTGRES_WRITER_BATCH_SIZE          | 100     | No       | Max number of messages to be sent per batch. When this size is reached, the batch is sent to PostgreSQL.         |
| PGSTREAM_POSTGRES_WRITER_MAX_QUEUE_BYTES     | 100MiB  | No       | Max memory used by the postgres batch writer for inflight batches.                                               |     |
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

## Documentation

For more advanced usage, tutorials, and detailed configuration settings, please refer to the full [Documentation](docs/README.md).

## Limitations

Some of the limitations of the initial release include:

- Single Kafka topic support
- Postgres plugin support limited to `wal2json`
- Data filtering limited to schema level
- Primary key/unique not null column required for replication
- Kafka serialisation support limited to JSON

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
