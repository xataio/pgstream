# 📷 Snapshots

![snapshots diagram](img/pgstream_snapshot_diagram.svg)

`pgstream` supports the generation of PostgreSQL schema and data snapshots. It can be done as an initial step before starting the replication, or as a standalone mode, where a snapshot of the database is performed without any replication.

The snapshot behaviour is the same in both cases, with the only difference that if we're listening on the replication slot, we will store the current LSN before performing the snapshot, so that we can replay any operations that happened while the snapshot was ongoing.

The snapshot implementation is different for schema and data.

- Schema: depending on the configuration, it can use either the pgstream `schema_log` table to get the schema view and process it as events downstream, or rely on the `pg_dump`/`pg_restore` PostgreSQL utilities if the target is a PostgreSQL database.

- Data: it relies on transaction snapshot ids to obtain a stable view of the database tables, and paralellises the read of all the rows by dividing them into ranges using the `ctid`.

![snapshots sequence](img/pgstream_snapshot_sequence.svg)

For more details into the snapshot implementation and performance benchmarking, check out this [blogpost](https://xata.io/blog/behind-the-scenes-speeding-up-pgstream-snapshots-for-postgresql). For details on how to use and configure the snapshot mode, check the [snapshot tutorial](tutorials/postgres_snapshot.md).
