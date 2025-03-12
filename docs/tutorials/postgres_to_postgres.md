# 🐘 PostgreSQL replication to PostgreSQL 🐘

This tutorial will showcase the use of pgstream to replicate data from one PostgreSQL database to another.

The requirements for this tutorial are:

- A source PostgreSQL database
- A target PostgreSQL database
- pgstream (see [installation](../../README.md#installation) instructions for more details)

## Environment setup

The first step is to start the two PostgreSQL databases that will be used as source and target for replication. The `pgstream` repository provides a docker installation that will be used for the purposes of this tutorial, but can be replaced by any available PostgreSQL servers, as long as they have [`wal2json`](https://github.com/eulerto/wal2json) installed.

To start the docker provided PostgreSQL servers, run the following command:

```sh
docker-compose -f build/docker/docker-compose.yml --profile pg2pg up
```

This will start two PostgreSQL databases on ports `5432` and `7654`.

## Database initialisation

Once both PostgreSQL servers are up and running, the next step is to initialise pgstream on the source database. This will create the `pgstream` schema in the configured Postgres database, along with the tables/functions/triggers required to keep track of the schema changes. See [Tracking schema changes](../README.md#tracking-schema-changes) section for more details. This step will also create a replication slot on the source database which will be used by the pgstream service.

The initialisation step allows to provide both the URL of the PostgreSQL database and the name of the replication slot to be created. The PostgreSQL URL is required, but the replication slot name is optional. If not provided, it will default to `pgstream_<dbname>_slot`, where `<dbname>` is the name of the PostgreSQL database. The configuration can be provided either by using the CLI supported parameters, or using the environment variables.

For this tutorial, we'll create a replication slot with the name `pgstream_tutorial_slot`.

- Using CLI parameters:

```sh
pgstream init --pgurl "postgres://postgres:postgres@localhost:5432?sslmode=disable" --replication-slot pgstream_tutorial_slot
```

- Using environment variables:

```sh
PGSTREAM_POSTGRES_REPLICATION_SLOT_NAME=pgstream_tutorial_slot PGSTREAM_POSTGRES_LISTENER_URL=postgres://postgres:postgres@localhost:5432?sslmode=disable pgstream init
```

Successful initialisation should prompt the following message:

```
SUCCESS  pgstream initialisation complete
```

We can check the replication slot has been properly created by connecting to the source PostgreSQL database and running the following query:

```sh
➜ psql postgresql://postgres:postgres@localhost:5432/postgres
```

```sql
SELECT slot_name,plugin,slot_type,database,restart_lsn,confirmed_flush_lsn FROM pg_replication_slots;
```

Which should show a `pgstream_tutorial_slot`:

```sql
+------------------------+----------+-----------+----------+-------------+---------------------+
| slot_name              | plugin   | slot_type | database | restart_lsn | confirmed_flush_lsn |
|------------------------+----------+-----------+----------+-------------+---------------------|
| pgstream_tutorial_slot | wal2json | logical   | postgres | 0/1590E80   | 0/1590EB8           |
+------------------------+----------+-----------+----------+-------------+---------------------+
```

We can also validate the `pgstream.schema_log` table has been created:

```sql
\d+ pgstream.schema_log
+-------------+-----------------------------+----------------------------------+----------+--------------+-------------+
| Column      | Type                        | Modifiers                        | Storage  | Stats target | Description |
|-------------+-----------------------------+----------------------------------+----------+--------------+-------------|
| id          | pgstream.xid                |  not null default pgstream.xid() | extended | <null>       | <null>      |
| version     | bigint                      |  not null                        | plain    | <null>       | <null>      |
| schema_name | text                        |  not null                        | extended | <null>       | <null>      |
| schema      | jsonb                       |  not null                        | extended | <null>       | <null>      |
| created_at  | timestamp without time zone |  not null default now()          | plain    | <null>       | <null>      |
| acked       | boolean                     |  not null default false          | plain    | <null>       | <null>      |
+-------------+-----------------------------+----------------------------------+----------+--------------+-------------+
Indexes:
    "schema_log_pkey" PRIMARY KEY, btree (id)
    "schema_log_version_uniq" UNIQUE, btree (schema_name, version)
    "schema_log_name_acked" btree (schema_name, acked, id)
Has OIDs: no
```

If at any point the initialisation performed by pgstream needs to be reverted, all state will be removed by running the `tear-down` CLI command.

```sh
pgstream tear-down --pgurl "postgres://postgres:postgres@localhost:5432?sslmode=disable" --replication-slot pgstream_tutorial_slot
```

## Prepare `pgstream` configuration

### Listener

In order to run pgstream, we need to provide the configuration required to run the PostgreSQL to PostgreSQL replication. First, we configure the listener module that will be listening to the WAL on the source PostgreSQL database. This requires the PostgreSQL database URL, which will be the one from the docker PostgreSQL server we started and setup in the previous steps.

```sh
PGSTREAM_POSTGRES_LISTENER_URL="postgres://postgres:postgres@localhost:5432?sslmode=disable"
```

Since we've set a custom replication slot name, the configuration variable needs to be set accordingly so that it doesn't use the default value.

```sh
PGSTREAM_POSTGRES_REPLICATION_SLOT_NAME=pgstream_tutorial_slot
```

The PostgreSQL WAL listener can be configured to perform an initial snapshot of the existing PostgreSQL database tables before starting to listen on the replication slot. In this case, we have no existing tables, so we don't need to configure the initial snapshot.

However, if there were tables with pre-existing data that we wanted to replicate to the target PostgreSQL database, we could configure it by setting the following environment variables:

```sh
PGSTREAM_POSTGRES_LISTENER_INITIAL_SNAPSHOT_ENABLED=true

# URL of the PostgreSQL database we want to snapshot
PGSTREAM_POSTGRES_INITIAL_SNAPSHOT_STORE_URL="postgres://postgres:postgres@localhost:5432?sslmode=disable"

# List of tables we want to snapshot. If the tables are not schema qualified, the public schema will be assumed.
# Wildcards are supported.
#
# The following example will snapshot all tables in the `test_schema` and the table `test` from the public schema.
PGSTREAM_POSTGRES_INITIAL_SNAPSHOT_TABLES="test_schema.* test"
```

Further configuration can be provided to optimize the performance of the snapshot process. For more information, check the [snapshot tutorial](postgres_snapshot).

### Processor

With the listener side ready, the next step is to configure the processor. Since we want to replicate to a PostgreSQL database, we will set the PostgreSQL writer configuration variables. The only required value is the URL of the target database, where the replicated data from the source database will be written. We use the URL of the docker PostgreSQL database we started earlier (note the port is the only difference between the source and the target PostgreSQL databases).

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

For the PostgreSQL writer to keep track of DDL changes, it needs to keep track of the schema log. To enable this behaviour, an environment variable needs to be configured to point to the `pgstream.schema_log` store database. In this case, it will be the same as the source PostgreSQL database, since that's where we've initialised pgstream.

```sh
PGSTREAM_POSTGRES_WRITER_SCHEMALOG_STORE_URL="postgres://postgres:postgres@localhost:5432?sslmode=disable"
```

The full configuration for this tutorial can be put into a `pg2pg_tutorial.env` file to be used in the next step.

- Without initial snapshot

```sh
# Listener config
PGSTREAM_POSTGRES_LISTENER_URL="postgres://postgres:postgres@localhost:5432?sslmode=disable"
PGSTREAM_POSTGRES_REPLICATION_SLOT_NAME=pgstream_tutorial_slot

# Processor config
PGSTREAM_POSTGRES_WRITER_TARGET_URL="postgres://postgres:postgres@localhost:7654?sslmode=disable"
PGSTREAM_POSTGRES_WRITER_BATCH_SIZE=25
PGSTREAM_POSTGRES_WRITER_BATCH_TIMEOUT=5s
PGSTREAM_POSTGRES_WRITER_SCHEMALOG_STORE_URL="postgres://postgres:postgres@localhost:5432?sslmode=disable"
```

- With initial snapshot

```sh
# Listener config
PGSTREAM_POSTGRES_LISTENER_URL="postgres://postgres:postgres@localhost:5432?sslmode=disable"
PGSTREAM_POSTGRES_REPLICATION_SLOT_NAME=pgstream_tutorial_slot
PGSTREAM_POSTGRES_LISTENER_INITIAL_SNAPSHOT_ENABLED=true
PGSTREAM_POSTGRES_INITIAL_SNAPSHOT_STORE_URL="postgres://postgres:postgres@localhost:5432?sslmode=disable"
# Initial snapshot of all tables in the public schema
PGSTREAM_POSTGRES_INITIAL_SNAPSHOT_TABLES="*"

# Processor config
PGSTREAM_POSTGRES_WRITER_TARGET_URL="postgres://postgres:postgres@localhost:7654?sslmode=disable"
PGSTREAM_POSTGRES_WRITER_BATCH_SIZE=25
PGSTREAM_POSTGRES_WRITER_BATCH_TIMEOUT=5s
PGSTREAM_POSTGRES_WRITER_SCHEMALOG_STORE_URL="postgres://postgres:postgres@localhost:5432?sslmode=disable"
```

## Run `pgstream`

With the configuration ready, we can now run pgstream. In this case we set the log level as trace to provide more context for debugging and have more visibility into what pgstream is doing under the hood.

```sh
pgstream run -c pg2pg_tutorial.env --log-level trace
```

Now we can connect to the source database and create a table:

```sh
➜ psql postgresql://postgres:postgres@localhost:5432/postgres
```

```sql
CREATE TABLE test(id SERIAL PRIMARY KEY, name TEXT);
```

If we connect to the target database, we should now see the test table created:

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

If we insert/update some data into the source database, we should see the data updated accordingly on the target database.

```sql
INSERT INTO test(name) VALUES('alice'),('bob'),('charlie');
UPDATE test SET name='alice' WHERE name='a';
```

Similarly when performing DDL operations, they should be properly replicated on the target database.

```sql
ALTER TABLE test RENAME TO tutorial_test;
ALTER TABLE tutorial_test ADD COLUMN age INT DEFAULT 0;
ALTER TABLE tutorial_test ALTER COLUMN age TYPE bigint;
ALTER TABLE tutorial_test RENAME COLUMN age TO new_age;
ALTER TABLE tutorial_test DROP COLUMN new_age;
DROP TABLE tutorial_test;
```

## Demo

https://github.com/user-attachments/assets/32e25f8f-6aa4-49c5-8986-0b23f81826db

