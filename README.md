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
  <a href="https://twitter.com/xata"><img src="https://img.shields.io/badge/@xata-6c47ff?label=Follow&logo=x" alt="X (formerly Twitter) Follow" /> </a>
  <a href="https://bsky.app/profile/xata.io"><img src="https://img.shields.io/badge/@xata-6c47ff?label=Follow&logo=bluesky" alt="Bluesky Follow" /> </a>
  <a href="https://www.youtube.com/@xataio"><img src="https://img.shields.io/badge/@xataio-6c47ff?label=Youtube&logo=youtube" alt="Youtube Subscribe" /> </a>
</p>

<p align="center">
  <a href="https://xata.io">
    <img src="https://img.shields.io/badge/Made%20with%20♥%20%20-%20%20by%20Xata-6c47ff?style=flat&logo=postgresql&logoColor=white" alt="Made with ♥ by Xata">
  </a>
</p>

# pgstream - Postgres replication with DDL changes

`pgstream` is an open source CDC command-line tool and library that offers Postgres replication support with DDL changes to any provided target.

![pg2pg demo with transformers](https://github.com/user-attachments/assets/6f11b326-d8ed-44eb-b743-756910b9fedd)

## Features

- Schema change tracking and replication of DDL changes
- Support for multiple out of the box targets
  - Elasticsearch/OpenSearch
  - Webhooks
  - PostgreSQL
- Initial and on demand PostgreSQL snapshots (for when you don't need continuous replication)
- Column value transformations (anonymise your data on the go!)
- Modular deployment configuration, only requires Postgres
- Kafka support with schema based partitioning
- Extendable support for custom targets

## Table of Contents

- [Usage](#usage)
- [Tutorials](#tutorials)
- [Documentation](#documentation)
- [Benchmarks](#benchmarks)
- [Limitations](#limitations)
- [Releases](#releases)
- [Contributing](#contributing)
- [License](#license)
- [Support](#support)

## Usage

`pgstream` can be used via the readily available CLI or as a library. For detailed information about the CLI usage, check out the dedicated [CLI documentation section](docs/cli.md).

### CLI Installation

```bash
# Download the latest release
curl -L https://github.com/xataio/pgstream/releases/latest/download/pgstream.linux.amd64 -o pgstream
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

### Environment setup

If you have an environment available, with at least Postgres and whichever resources you're planning on running, then you can skip this step. Otherwise, a docker setup is available in this repository with profiles that selectively start Postgres, Kafka and OpenSearch.

To run all profiles:

```
docker-compose -f build/docker/docker-compose.yml up
```

If you only want to run PostgreSQL to PostgreSQL pgstream replication you can use the `pg2pg` profile:

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

### Configuration

Pgstream source and target need to be configured appropriately before the commands can be run. This can be done:

- Using the relevant CLI flags for each command
- Using a yaml configuration file
- Using environment variables (.env file supported)

Check the [configuration documentation](docs/configuration.md) for more information about the configuration options, or check the [CLI documentation](docs/cli.md) for details on the available flags. Additionally, you can find sample files for both .env and .yaml [here](docs/examples).

If you want to configure column transformations, leveraging [greenmask](https://github.com/GreenmaskIO/greenmask), [neosync](https://github.com/nucleuscloud/neosync) and [go-masker](https://github.com/ggwhite/go-masker) open source integrations, as well as custom transformers, check the [transformation rules](docs/transformers.md#transformation-rules) configuration for more details, along with the list of [available transformers](docs/transformers.md#supported-transformers).

### Run `pgstream`

#### Replication mode

Run will start streaming data from the configured source into the configured target. By passing the `--init` flag to the run command, pgstream will initialise the pgstream state in the source Postgres database before starting replication. It will:

- Create a `pgstream` schema
- Create tables/functions/triggers to keep track of schema changes for DDL replication (see [Tracking schema changes](docs/architecture.md#tracking-schema-changes) for more details)
- Create a replication slot

Initialisation is required for pgstream replication. It can alternatively be performed by running the `pgstream init` command separately before `pgstream run`. Check out the [CLI documentation](docs/cli.md#init) for more details.

Example running pgstream replication from Postgres -> OpenSearch:

```sh
# using the environment configuration file
pgstream run -c docs/examples/pg2os.env --init --log-level trace
# using the yaml configuration file
pgstream run -c docs/examples/pg2os.yaml --init --log-level info
# using the CLI flags
pgstream run --source postgres --source-url "postgres://postgres:postgres@localhost:5432?sslmode=disable" --target opensearch --target-url "http://admin:admin@localhost:9200" --init
```

Example running pgstream with Postgres -> Kafka, and in a separate terminal, Kafka->OpenSearch:

```sh
# using the environment configuration file
pgstream run -c docs/examples/pg2kafka.env --init --log-level trace
# using the yaml configuration file
pgstream run -c docs/examples/pg2kafka.yaml --init --log-level info
# using the CLI flags
pgstream run --source postgres --source-url "postgres://postgres:postgres@localhost:5432?sslmode=disable" --target kafka --target-url "localhost:9092" --init
```

```sh
# using the environment configuration file
pgstream run -c docs/examples/kafka2os.env --init --log-level trace
# using the yaml configuration file
pgstream run -c docs/examples/kafka2os.yaml --init --log-level info
# using the CLI flags
pgstream run --source kafka --source-url "localhost:9092" --target opensearch --target-url "http://admin:admin@localhost:9200" --init
```

An initial snapshot can be performed before starting replication by providing `--snapshot-tables` flag or by setting the relevant configuration fields (check the [configuration documentation](docs/configuration.md) for more details on advanced configuration options).

Example running pgstream with PostgreSQL -> PostgreSQL with initial snapshot enabled:

```sh
# using the CLI flags
pgstream run --source postgres --source-url "postgres://postgres:postgres@localhost:5432?sslmode=disable" --target postgres --target-url "postgres://postgres:postgres@localhost:7654?sslmode=disable" --snapshot-tables test --init
```

#### Snapshot mode

pgstream can also be used to perform a point in time snapshot of the source database. This is helpful if you don't require continuous replication, but want to keep the source and target in sync by running nightly snapshots for example.

The `snapshot` command doesn't require any initialisation or pgstream specific state, since it only performs read operations on the source Postgres database.

Example running pgstream to perform a snapshot from PostgreSQL -> PostgreSQL:

```sh
# using the environment configuration file
pgstream snapshot -c docs/examples/snapshot2pg.env --log-level trace
# using the yaml configuration file
pgstream snapshot -c docs/examples/snapshot2pg.yaml --log-level info
# using the CLI flags
pgstream snapshot --postgres-url="postgres://postgres:postgres@localhost:5432?sslmode=disable" --target=postgres --target-url="postgres://postgres:postgres@localhost:7654?sslmode=disable" --tables="test" --reset
```

## Tutorials

- [PostgreSQL replication to PostgreSQL](docs/tutorials/postgres_to_postgres.md)
- [PostgreSQL replication to OpenSearch](docs/tutorials/postgres_to_opensearch.md)
- [PostgreSQL replication to webhooks](docs/tutorials/postgres_to_webhooks.md)
- [PostgreSQL replication using Kafka](docs/tutorials/postgres_kafka.md)
- [PostgreSQL snapshots](docs/tutorials/postgres_snapshot.md)
- [PostgreSQL column transformations](docs/tutorials/postgres_transformer.md)

## Documentation

For more advanced usage, implementation details, and detailed configuration settings, please refer to the full documentation below.

1. [Architecture](docs/architecture.md)
2. [Configuration](docs/configuration.md)
   - [Yaml](docs/configuration.md#yaml)
   - [Environment Variables](docs/configuration.md#environment-variables)
   - [Examples](docs/examples/)
3. [Snapshots](docs/snapshots.md)
4. [Transformers](docs/transformers.md)
   - [Supported Transformers](docs/transformers.md#supported-transformers)
   - [Transformation Rules](docs/transformers.md#transformation-rules)
5. [Observability](docs/observability.md)
6. [CLI](docs/cli.md)
7. [Privileges](docs/privileges.md)
   - [Xata](docs/xata.md)
   - [AWS](docs/aws.md)
   - [GCP CloudSQL](docs/gcp_cloudsql.md)
   - [Neon](docs/neon.md)
8. [Glossary](docs/glossary.md)

## Benchmarks

### Snapshots

<p align="left">
    <img src="docs/img/pgstream_snapshot_benchmarks.png" alt="pgstream snapshot benchmarks" width="700">
</p>

Datasets used: [IMDB database](https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/2QYZBT), [MusicBrainz database](https://musicbrainz.org/doc/MusicBrainz_Database), [Firenibble database](https://github.com/PeerDB-io/ab-scale-testing/tree/main).

All benchmarks were run using the same setup, with pgstream `v0.7.2`, pg_dump/pg_restore (PostgreSQL) 17.4 and PostgreSQL 17.4, using identical resources to ensure a fair comparison.

For more details into performance benchmarking for snapshots to PostgreSQL with `pgstream`, check out this [blogpost](https://xata.io/blog/behind-the-scenes-speeding-up-pgstream-snapshots-for-postgresql).

## Limitations

Some of the limitations of the initial release include:

- Single Kafka topic support
- Postgres plugin support limited to `wal2json`
- No row level filtering support
- Primary key/unique not null column required for replication
- Kafka serialisation support limited to JSON

## Releases

You can find below the release notes for major pgstream versions, along with migration guides whenever relevant:

- [v1.0.0](docs/release/RELEASE_NOTES_v1.md)

## Contributing

We welcome contributions from the community! If you'd like to contribute to pgstream, please follow [these guidelines](https://github.com/xataio/pgstream/tree/main?tab=contributing-ov-file) and adhere to our [code of conduct](https://github.com/xataio/pgstream?tab=coc-ov-file).

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Support

If you have any questions, encounter issues, or need assistance, open an issue in this repository our join our [Discord](https://xata.io/discord), and our community will be happy to help.

<br>
<p align="right">Made with 💜 by <a href="https://xata.io">Xata 🦋</a></p>
