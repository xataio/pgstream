# 🐘 PostgreSQL snapshot 📷

This tutorial will showcase the use of pgstream to snapshot data from a PostgreSQL database. For this tutorial, we'll use a PostgreSQL output.

The requirements for this tutorial are:

- A source PostgreSQL database
- A target PostgreSQL database
- pgstream (see [installation](../../README.md#installation) instructions for more details)

## Environment setup

The first step is to start the two PostgreSQL databases that will be used as source and target for the snapshot. The `pgstream` repository provides a docker installation that will be used for the purposes of this tutorial, but can be replaced by any available PostgreSQL servers.

To start the docker provided PostgreSQL servers, run the following command:

```sh
docker-compose -f build/docker/docker-compose.yml --profile pg2pg up
```

This will start two PostgreSQL databases on ports `5432` and `7654`.

## Database initialisation

Normally we need to initialise pgstream on the source database. The initialisation step creates the `pgstream` schema in the configured Postgres database, along with the tables/functions/triggers required to keep track of the schema changes. It also creates the replication slot. However, this is only required if we're going to be using the replication slot or relying on the schema log. If we're using a PostgreSQL output for the snapshot, pgstream supports using `pg_dump`/`pg_restore` for the schema snapshot, which removes the need to keep any pgstream state in the source PostgreSQL database.

If the output is not PostgreSQL, we'd need to initialise pgstream like we do normally, since it relies on the `pgstream.schema_log` table to provide a view of the schema for now. For more details on how to initialise pgstream in those cases, check out the [database initialisation](postgres_to_postgres.md#database-initialisation) section on one of the replication tutorials.

## Prepare `pgstream` configuration

### Listener

In order to run pgstream, we need to provide the configuration required to run the PostgreSQL to PostgreSQL snapshot. First, we configure the listener module that will be producing the snapshot of the source PostgreSQL database. This requires the PostgreSQL database URL, which will be the one from the docker PostgreSQL server we started and setup in the previous steps.

```sh
PGSTREAM_POSTGRES_SNAPSHOT_LISTENER_URL="postgres://postgres:postgres@localhost:5432?sslmode=disable"
```

The snapshot listener needs to be configured to indicate the tables/schemas that need to be snapshotted. If the tables are not schema qualified, the `public` schema will be assumed. Wildcards are supported. For example, `test_schema.*` will snapshot all tables in the `test_schema` schema, and `test` will snapshot the `public.test` table.

```sh
# We will snapshot all tables in the public schema.
PGSTREAM_POSTGRES_INITIAL_SNAPSHOT_TABLES="*"
```

Further configuration can be provided to optimize the performance of the snapshot process, mostly focusing on the concurrency.

```sh
# Number of tables being snapshotted in parallel for a given schema. Defaults to 4.
PGSTREAM_POSTGRES_SNAPSHOT_SCHEMA_WORKERS=4
# Number of concurrent workers that will be used per table by the snapshotting process. Defaults to 4.
PGSTREAM_POSTGRES_SNAPSHOT_TABLE_WORKERS=4
# Size of the table page range which will be processed concurrently by the table workers from PGSTREAM_POSTGRES_INITIAL_SNAPSHOT_TABLE_WORKERS. Defaults to 1000.
PGSTREAM_POSTGRES_SNAPSHOT_BATCH_PAGE_SIZE=1000
# Number of schemas that will be processed in parallel by the snapshotting process. Defaults to 1.
PGSTREAM_POSTGRES_SNAPSHOT_WORKERS=1
```

### Processor

With the listener side ready, the next step is to configure the processor. Since we want the snapshot reach a PostgreSQL database, we will set the PostgreSQL writer configuration variables. The only required value is the URL of the target database, where the snapshotted schema/data from the source database will be streamed. We use the URL of the docker PostgreSQL database we started earlier (note the port is the only difference between the source and the target PostgreSQL databases).

```sh
PGSTREAM_POSTGRES_WRITER_TARGET_URL="postgres://postgres:postgres@localhost:7654?sslmode=disable"
```

The PostgreSQL writer uses batching under the hood to reduce the number of IO calls to the target database and improve performance. The batch size and send timeout can both be configured to be able to better fit the different traffic patterns. The writer will send a batch when the timeout or the batch size is reached, whichever happens first.

```sh
# Number of DML queries that will be batched and sent together in a given transaction. It defaults to 100.
PGSTREAM_POSTGRES_WRITER_BATCH_SIZE=25

# Max delay between batch sending. The batches will be sent every 5s by default.
PGSTREAM_POSTGRES_WRITER_BATCH_TIMEOUT=5s
```

Since in this case there's no need to keep track of DDL changes, we don't need to set the schema log store variable (`PGSTREAM_POSTGRES_WRITER_SCHEMALOG_STORE_URL`).

The full configuration for this tutorial can be put into a `snapshot2pg_tutorial.env` file to be used in the next step.

```sh
# Listener config
PGSTREAM_POSTGRES_SNAPSHOT_LISTENER_URL="postgres://postgres:postgres@localhost:5432?sslmode=disable"
PGSTREAM_POSTGRES_SNAPSHOT_TABLES="*"
PGSTREAM_POSTGRES_SNAPSHOT_SCHEMA_WORKERS=4
PGSTREAM_POSTGRES_SNAPSHOT_TABLE_WORKERS=4
PGSTREAM_POSTGRES_SNAPSHOT_BATCH_PAGE_SIZE=1000
PGSTREAM_POSTGRES_SNAPSHOT_WORKERS=1

# Processor config
PGSTREAM_POSTGRES_WRITER_TARGET_URL="postgres://postgres:postgres@localhost:7654?sslmode=disable"
PGSTREAM_POSTGRES_WRITER_BATCH_SIZE=25
PGSTREAM_POSTGRES_WRITER_BATCH_TIMEOUT=5s
```

## Preparing snapshot data

Now we can connect to the source database and create a table and populate it with some data that we'll want to snapshot.

```sh
➜ psql postgresql://postgres:postgres@localhost:5432/postgres
```

```sql
CREATE TABLE test(id SERIAL PRIMARY KEY, name TEXT);
INSERT INTO test(name) VALUES('alice'),('bob'),('charlie');
```

## Run `pgstream`

With the configuration ready, we can now run pgstream. In this case we set the log level as trace to provide more context for debugging and have more visibility into what pgstream is doing under the hood. Once the snapshot finishes, the process will stop.

```sh
pgstream run -c snapshot2pg_tutorial.env --log-level trace
```

If we connect to the target database, we should now see the test table created and populated with the data from the snapshot.

```sh
➜ psql postgresql://postgres:postgres@localhost:7654/postgres
```

```sql
 \d+ test
+--------+---------+-----------+----------+--------------+-------------+
| Column | Type    | Modifiers | Storage  | Stats target | Description |
|--------+---------+-----------+----------+--------------+-------------|
| id     | integer |  not null | plain    | <null>       | <null>      |
| name   | text    |           | extended | <null>       | <null>      |
+--------+---------+-----------+----------+--------------+-------------+
Indexes:
    "test_pkey" PRIMARY KEY, btree (id)
Has OIDs: no
```

```sql
postgres@localhost:postgres> SELECT * FROM test;
+----+---------+
| id | name    |
|----+---------|
| 1  | alice   |
| 2  | bob     |
| 3  | charlie |
+----+---------+
```
