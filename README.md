<p align="center">
  <a href="https://github.com/xataio/pgstream/blob/main/LICENSE"><img src="https://img.shields.io/badge/License-Apache_2.0-green" alt="License - Apache 2.0"></a>&nbsp;
  <a href="https://github.com/xataio/pgstream/actions?query=branch%3Amain"><img src="https://github.com/xataio/pgstream/actions/workflows/build.yml/badge.svg" alt="CI Build"></a> &nbsp;
  <a href="https://xata.io/discord"><img src="https://img.shields.io/discord/996791218879086662?label=Discord" alt="Discord"></a> &nbsp;
  <a href="https://twitter.com/xata"><img src="https://img.shields.io/twitter/follow/xata?style=flat" alt="X (formerly Twitter) Follow" /> </a>
</p>

# pgstream - Postgres replication with DDL changes

`pgstream` is an open source command-line tool and library that offers postgres replication with DDL changes. It keeps track of schema changes and replicates them along with the data changes to ensure a consistent view of the source data down the stream. It aims at providing a modular approach to replication where all the different stream components can be combined and used interchangeably as long as they are compatible.

## Features

- Schema change tracking and replication of DDL changes
- Optional fan out module support (i.e, Kafka)
- Schema based message partitioning
- Schema filtering
- Elasticsearch/Opensearch replication output support
- Automatic discovery of table primary key/unique not null columns for use as event identity
- Highly customisable modules when used as library
- Core metrics available via opentelemetry
- Extendable support for custom replication outputs
- Continuous consumption of replication slot with configurable memory guards for optimal usage based on traffic

## Limitations

Some of the limitations of the initial release include:

- Single Kafka topic support
- State required for DDL changes support
- Postgres plugin support limited to `wal2json`
- Data filtering limited to schema level
- No initial/automatic data replay
- Primary key/unique not null column required for replication
- Kafka serialisation support limited to JSON

## Table of Contents

- [Usage](#usage)
- [Architecture](#architecture)
- [Glossary](#glossary)
- [Contributing](#contributing)
- [License](#license)
- [Support](#support)

## Usage

`pgstream` can be used via the readily available CLI or as a library.

### CLI

For now - `go build` to build the `pgstream` binary.

#### Environment setup

If you have an environment locally available, with at least postgres and whichever module resources you're planning on running, then you can skip this step. Otherwise, a docker setup is available in this repository that starts postgres, kafka and opensearch (as well as opensearch dashboards for easy visualisation).

```
docker-compose -f build/docker/docker-compose.yml up
```

#### Prepare the database

This will create the `pgstream` schema in the configured postgres database, along with the tables/functions/triggers required to keep track of the schema changes. See [Tracking schema changes](#tracking-schema-changes) section for more details. It will also create a replication slot for the configured database which will be used by the pgstream service.

```
./pgstream init --pgurl postgres://pgstream:pgstream@localhost?sslmode=disable
```

If there are any issues or if you want to clean up the pgstream setup, you can run the following.

```
./pgstream tear-down --pgurl postgres://pgstream:pgstream@localhost?sslmode=disable
```

This command will clean up all pgstream state.

#### Start pgstream

Start will require the configuration to be provided, either via environment variables, config file or a combination of both. There are some sample configuration files provided in the repo that can be used as guidelines.

Example running pgstream with postgres->opensearch:
```
./pgstream start -c pg2os.env --log-level trace
```

Example running pgstream with postgres->kafka, and in a separate terminal, kafka->opensearch:
```
./pgstream start -c pg2kafka.env --log-level trace
./pgstream start -c kafka2os.env --log-level trace
```

The start command will parse the configuration provided, and decide based on it the modules that need to be started. It requires at least one listener and one processor to be configured.

## Tracking schema changes

In order to track schema changes, pgstream relies on functions and triggers that will populate a postgres table (`pgstream.schema_log`) containing a history log of all DDL changes for a given schema.

The detailed SQL used can be found in the [migrations folder](https://github.com/xataio/pgstream/tree/main/migrations/postgres).


## Architecture

`pgstream` relies on a series of modules connected to each other to build a stream that will allow data to reach the output plugins. At a high level the implementation is split into WAL listeners and WAL processors.

### WAL Listener

A listener is anything that listens for WAL data, regardless of the source. It doesn't have any logic beyond the listening, and relies on a processor to act on the WAL event itself. Depending on the listener implementation, it might be required to also have a checkpointer to flag the events as processed once the processor is done.

There are currently two implementations of the listener:

- **Postgres listener**: listens to WAL events directly from the replication slot. Since the WAL replication slot is sequential, the postgres WAL listener is limited to run as a single process. The associated postgres checkpointer will sync the LSN so that the replication lag doesn't grow indefinitely.

- **Kafka reader**: reads WAL events from a kafka topic. It can be configured to run concurrently by using partitions and kafka consumer groups, applying a fan-out strategy to the WAL events. The data will be partitioned by database schema by default, but can be configured when using `pgstream` as a library. The associated kafka checkpointer will commit the message offsets per topic/partition so that the consumer group doesn't process the same message twice.


### WAL Processor

A processor processes a WAL event. Depending on the implementation it might also be required to checkpoint the event once it's done processing it as described above.

There are currently two implementations of the processor:

- **Kafka batch writer**: it writes the WAL events into a kafka topic, using the event schema as the kafka key for partitioning. This implementation allows to fan-out the sequential WAL events, while acting as an intermediate buffer to avoid the replication slot to grow when there are slow consumers. It has a memory guarded buffering system internally to be able to process events from the WAL continously, and a batching mechanism that will send to kafka once the configured settings are reached. It treats both data and schema events equally, since it doesn't care about the content.

- **Search batch indexer**: it indexes the WAL events into an opensearch/elasticsearch compatible search store. It implements the same kind of mechanism than the kafka batch writer to ensure continuous processing from the listener, and it also uses a batching mechanism to minimise search store calls. The search mapping logic is configurable when used as a library. The WAL event identity is used as the search store document id, and if no other version is provided, the LSN is used as the document version. Events that do not have an identity are not indexed. Schema events are stored in a separate search store index (`pgstream`), where the schema log history is kept for use within the search store (i.e, read queries).


In addition to the two implementations described above, there's an optional processor decorator, the **translator**, that injects some of the pgstream logic into the WAL event. This includes:

- Data events:
	- Setting the WAL event identity. If provided, it will use the configured id finder (only available when used as a library), otherwise it will default to using the table primary key/unique not null column.
	- Setting the WAL event version. If provided, it will use the configured version finder (only available when used as a library), otherwise it will default to using the event LSN.
	- Adding pgstream IDs to all columns. This allows us to have a constant identifier for a column, so that if there are renames the column id doesn't change. This is particularly helpful for the search store, where a rename would require a reindex, which can be costly depending on the data.

- Schema events:
	- 	Acknolwedging the new incoming schema in the postgres `pgstream.schema_log` table.

<img width="1587" alt="Screenshot 2024-06-18 at 16 49 32" src="https://github.com/xataio/pgstream/assets/33323594/1580e3ab-109b-4ac6-a33e-0a80f8d6e454">


## Glossary

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
5. Submit a pull request.

For this project, we pledge to act and interact in ways that contribute to an open, welcoming, diverse, inclusive, and healthy community.


## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Support

If you have any questions, encounter issues, or need assistance, open an issue in this repository our join our [Discord](https://xata.io/discord), and our community will be happy to help.


<br>
<p align="right">Made with :heart: by <a href="https://xata.io">Xata 🦋</a></p>
