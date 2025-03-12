# 📚 `pgstream` Documentation

- [Architecture](#architecture)
  - [WAL Listener](#wal-listener)
  - [WAL Processor](#wal-processor)
- [Tracking schema changes](#tracking-schema-changes)
- [Snapshots](#tracking-schema-changes)
- [Transformers](#transformers)
  - [Listeners](#listeners)
  - [Processors](#processors)
- [Tutorials](#tutorials)
- [Glossary](#glossary)

## Architecture

`pgstream` is constructed as a streaming pipeline, where data from one module streams into the next, eventually reaching the configured outputs. It keeps track of schema changes and replicates them along with the data changes to ensure a consistent view of the source data downstream. This modular approach makes adding and integrating output implementations simple and painless.

![pgstream architecture v1](img/pgstream_arch_v1.png)

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

## Tracking schema changes

One of the main differentiators of pgstream is the fact that it tracks and replicates schema changes automatically. It relies on SQL triggers that will populate a Postgres table (`pgstream.schema_log`) containing a history log of all DDL changes for a given schema. Whenever a schema change occurs, this trigger creates a new row in the schema log table with the schema encoded as a JSON value. This table tracks all the schema changes, forming a linearised change log that is then parsed and used within the pgstream pipeline to identify modifications and push the relevant changes downstream.

The detailed SQL used can be found in the [migrations folder](https://github.com/xataio/pgstream/tree/main/migrations/postgres).

The schema and data changes are part of the same linear stream - the downstream consumers always observe the schema changes as soon as they happen, before any data arrives that relies on the new schema. This prevents data loss and manual intervention.

## Snapshots

`pgstream` can handle the generation of PostgreSQL snapshots, including both schema and data. The current implementations for each are:

- Schema: depending on the configuration, it can use either the pgstream `schema_log` table to get the schema view and process it as events downstream, or rely on the `pg_dump`/`pg_restore` PostgreSQL utilities.

- Data: it relies on transaction snapshot ids to obtain a stable view of the database tables, and paralellises the read of all the rows by dividing them into ranges using the `ctid`.

## Transformers

🚧 Under construction 🚧

## Tutorials

- [PostgreSQL replication to PostgreSQL](tutorials/postgres_to_postgres.md)
- PostgreSQL replication to OpenSearch 🚧
- PostgreSQL replication to webhooks 🚧
- PostgreSQL replication using Kafka 🚧
- PostgreSQL snapshots 🚧
- PostgreSQL column transformations 🚧

## Glossary

- [CDC](https://en.wikipedia.org/wiki/Change_data_capture): Change Data Capture
- [WAL](https://www.postgresql.org/docs/current/wal-intro.html): Write Ahead Logging
- [LSN](https://pgpedia.info/l/LSN-log-sequence-number.html): Log Sequence Number
- [DDL](https://en.wikipedia.org/wiki/Data_definition_language): Data Definition Language
