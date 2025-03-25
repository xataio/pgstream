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

![pg2pg demo with transformers](https://github.com/user-attachments/assets/6f11b326-d8ed-44eb-b743-756910b9fedd)

## Features

- Schema change tracking and replication of DDL changes
- Multiple out of the box supported replication outputs
  - Elasticsearch/OpenSearch
  - Webhooks
  - PostgreSQL
- Initial and on demand PostgreSQL snapshots (for when you don't need continuous replication)
- Column value transformations (anonymise your data on the go!)
- Modular deployment configuration, only requires Postgres
- Kafka support with schema based partitioning
- Extendable support for custom output plugins

## Table of Contents

- [Usage](#usage)
- [Tutorials](#tutorials)
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

## Tutorials

- [PostgreSQL replication to PostgreSQL](docs/tutorials/postgres_to_postgres.md)
- [PostgreSQL replication to OpenSearch](docs/tutorials/postgres_to_opensearch.md)
- [PostgreSQL replication to webhooks](docs/tutorials/postgres_to_webhooks.md)
- [PostgreSQL replication using Kafka](docs/tutorials/postgres_kafka.md)
- [PostgreSQL snapshots](docs/tutorials/postgres_snapshot.md)
- [PostgreSQL column transformations](docs/tutorials/postgres_transformer.md)

## Documentation

For more advanced usage, implementation details, and detailed configuration settings, please refer to the full [Documentation](docs/README.md).

## Limitations

Some of the limitations of the initial release include:

- Single Kafka topic support
- Postgres plugin support limited to `wal2json`
- Limited filtering
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
