# 🐘 PostgreSQL replication to PostgreSQL 🐘

## Table of Contents

1. [Introduction](#introduction)
2. [Environment Setup](#environment-setup)
3. [Database Initialization](#database-initialisation)
4. [Prepare `pgstream` Configuration](#prepare-pgstream-configuration)
   - [Listener](#listener)
   - [Processor](#processor)
5. [Run `pgstream`](#run-pgstream)
6. [Verify Replication](#verify-replication)
   - [Schema Changes](#schema-changes)
   - [Data Changes](#data-changes)
7. [Troubleshooting](#troubleshooting)
8. [Summary](#summary)

## Introduction

This tutorial demonstrates how to use pgstream to replicate data from one PostgreSQL database to another. It covers setting up the environment, configuring pgstream, and verifying the replication of both data and schema changes.

![pg2pg tutorial](../img/pgstream_tutorial_pg2pg.svg)

### Requirements

- A source PostgreSQL database
- A target PostgreSQL database
- pgstream (see [installation](../../README.md#installation) instructions for more details)

### Demo

https://github.com/user-attachments/assets/32e25f8f-6aa4-49c5-8986-0b23f81826db

Youtube link [here](https://www.youtube.com/watch?v=WCv1ZZXkUnU&list=PLf7KS0svgDP_H8x5lD8HPXK2BjhwO4ffT&index=4&pp=iAQB).

## Environment setup

The first step is to start the two PostgreSQL databases that will be used as the source and target for replication. The pgstream repository provides a Docker setup for this purpose, but any PostgreSQL servers with [`wal2json`](https://github.com/eulerto/wal2json) installed can be used.

To start the docker provided PostgreSQL servers, run the following command:

```sh
docker-compose -f build/docker/docker-compose.yml --profile pg2pg up
```

This will start two PostgreSQL databases:

- Source database on port `5432`
- Target database on port `7654`

## Database initialisation

Once both PostgreSQL servers are up and running, initialise pgstream on the source database. This step will create the `pgstream` schema in the configured Postgres database, along with the tables/functions/triggers required to keep track of the schema changes. See [Tracking schema changes](../README.md#tracking-schema-changes) section for more details. It will also create a replication slot on the source database which will be used by the pgstream service.

The initialisation step allows to provide both the URL of the PostgreSQL database and the name of the replication slot to be created. The PostgreSQL URL is required, but the replication slot name is optional. If not provided, it will default to `pgstream_<dbname>_slot`, where `<dbname>` is the name of the PostgreSQL database. The configuration can be provided either via CLI parameters or environment variables.

For this tutorial, we'll create a replication slot with the name `pgstream_tutorial_slot`.

- Using CLI parameters:

```sh
pgstream init --pgurl "postgres://postgres:postgres@localhost:5432?sslmode=disable" --replication-slot pgstream_tutorial_slot
```

- Using environment variables:

```sh
PGSTREAM_POSTGRES_REPLICATION_SLOT_NAME=pgstream_tutorial_slot \
PGSTREAM_POSTGRES_LISTENER_URL=postgres://postgres:postgres@localhost:5432?sslmode=disable \
pgstream init
```

After initialization, you should see the following message:

```
SUCCESS  pgstream initialisation complete
```

To confirm the replication slot was created, connect to the source database and run:

```sql
SELECT slot_name,plugin,slot_type,database,restart_lsn,confirmed_flush_lsn FROM pg_replication_slots;
```

You should see the `pgstream_tutorial_slot` in the output.

Additionally, verify that the `pgstream.schema_log` table was created:

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

The listener reads changes from the source database's WAL. Configure it as follows:

```sh
PGSTREAM_POSTGRES_LISTENER_URL="postgres://postgres:postgres@localhost:5432?sslmode=disable"
PGSTREAM_POSTGRES_REPLICATION_SLOT_NAME=pgstream_tutorial_slot
```

If you want to perform an initial snapshot of existing tables, add:

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

The processor writes changes to the target database. The only required value is the URL of the target database. Configure it as follows:

```sh
PGSTREAM_POSTGRES_WRITER_TARGET_URL="postgres://postgres:postgres@localhost:7654?sslmode=disable"
```

If we need to disable triggers on the target database during the replication process(ie., to avoid foreign key constraint violations), we can do so by setting the following variable:
```sh
PGSTREAM_POSTGRES_WRITER_DISABLE_TRIGGERS=true
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

Save the configuration in a file named `pg2pg_tutorial.env`:

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

## Run `pgstream`

Run pgstream with the prepared configuration. In this case we set the log level as trace to provide more context for debugging and have more visibility into what pgstream is doing under the hood.

```sh
pgstream run -c pg2pg_tutorial.env --log-level trace
```

## Verify Replication

### Schema Changes

1. Connect to the source database:

   ```sh
   psql postgresql://postgres:postgres@localhost:5432/postgres
   ```

2. Create a table:

   ```sql
   CREATE TABLE test(id SERIAL PRIMARY KEY, name TEXT);
   ```

3. Connect to the target database:

   ```sh
   psql postgresql://postgres:postgres@localhost:7654/postgres
   ```

4. Verify the table was replicated:

   ```sql
    \d+ test
   +--------+---------+-----------+----------+--------------+-------------+
   | Column | Type    | Modifiers | Storage  | Stats target | Description |
   |--------+---------+-----------+----------+--------------+-------------|
   | id     | integer |  not null | plain    | <null>       | <null>      |
   | name   | text    |           | extended | <null>       | <null>      |
   +--------+---------+-----------+----------+--------------+-------------+
   ```

Similarly when performing other DDL operations, they should be properly replicated on the target database.

```sql
ALTER TABLE test RENAME TO tutorial_test;
ALTER TABLE tutorial_test ADD COLUMN age INT DEFAULT 0;
ALTER TABLE tutorial_test ALTER COLUMN age TYPE bigint;
ALTER TABLE tutorial_test RENAME COLUMN age TO new_age;
ALTER TABLE tutorial_test DROP COLUMN new_age;
DROP TABLE tutorial_test;
```

### Data Changes

1. Insert data into the source database:

   ```sql
   INSERT INTO test(name) VALUES('alice'),('bob'),('charlie');
   ```

2. Verify the data was replicated to the target database:
   ```sql
   SELECT * FROM test;
   ```

## Troubleshooting

Here are some common issues you might encounter while following this tutorial and how to resolve them:

### 1. **Error: `Connection refused`**

- **Cause:** The PostgreSQL database is not running or the connection URL is incorrect.
- **Solution:**
  - Ensure the Docker containers for the source and target databases are running.
  - Verify the database URLs in the configuration file (`pg2pg_tutorial.env`).
  - Test the connection using `psql`:
    ```sh
    psql postgresql://postgres:postgres@localhost:5432/postgres
    ```

### 2. **Error: `Replication slot not found`**

- **Cause:** The replication slot was not created during initialization.
- **Solution:**
  - Reinitialize `pgstream` or manually create the replication slot.
  - Verify the replication slot exists by running:
    ```sql
    SELECT slot_name FROM pg_replication_slots;
    ```

### 3. **Error: `Target database does not contain replicated data`**

- **Cause:** The replication process did not complete successfully or the target database URL is incorrect.
- **Solution:**
  - Verify the target database URL in the configuration file.
  - Check the `pgstream` logs to confirm the replication process completed without errors.
  - Query the target database to ensure the data was replicated:
    ```sql
    SELECT * FROM test;
    ```

### 4. **Error: `Permission denied`**

- **Cause:** The database user does not have sufficient privileges.
- **Solution:**
  - Grant the required privileges to the database user:
    ```sql
    GRANT ALL PRIVILEGES ON DATABASE postgres TO postgres;
    ```

### 5. **Error: `Invalid configuration`**

- **Cause:** The configuration file contains invalid or missing values.
- **Solution:**
  - Double-check the `pg2pg_tutorial.env` file for typos or missing variables.
  - Refer to the [pgstream configuration documentation](https://github.com/xataio/pgstream) for details on required variables.

### 6. **Error: `Snapshot failed`**

- **Cause:** The initial snapshot process encountered an issue.
- **Solution:**
  - Ensure the `PGSTREAM_POSTGRES_LISTENER_INITIAL_SNAPSHOT_ENABLED` variable is set to `true` if a snapshot is required.
  - Check the `pgstream` logs for detailed error messages:
    ```sh
    pgstream run -c pg2pg_tutorial.env --log-level trace
    ```

If you encounter issues not listed here, consult the [pgstream documentation](https://github.com/xataio/pgstream) or open an issue on the project's GitHub repository.

## Summary

In this tutorial, we successfully configured `pgstream` to replicate data from a source PostgreSQL database to a target PostgreSQL database. We:

1. Set up the source and target PostgreSQL databases using Docker.
2. Initialized `pgstream` on the source database, creating the necessary schema and replication slot.
3. Configured the listener to capture changes from the source database's WAL.
4. Configured the processor to replicate changes to the target database.
5. Verified that both DML (data manipulation) and DDL (schema changes) were replicated correctly.

This tutorial demonstrates how `pgstream` can be used for real-time replication between PostgreSQL databases. For more advanced use cases, such as transformations or webhook integration, refer to the [pgstream tutorials](../../README.md#tutorials).
