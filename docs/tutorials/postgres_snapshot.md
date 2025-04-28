# 🐘 PostgreSQL snapshot 📷

## Table of Contents

1. [Introduction](#introduction)
2. [Environment Setup](#environment-setup)
3. [Database Initialization](#database-initialisation)
4. [Prepare `pgstream` Configuration](#prepare-pgstream-configuration)
   - [Listener](#listener)
   - [Processor](#processor)
5. [Preparing Snapshot Data](#preparing-snapshot-data)
6. [Run `pgstream`](#run-pgstream)
7. [Verify Snapshot](#verify-snapshot)
8. [Troubleshooting](#troubleshooting)
9. [Summary](#summary)

## Introduction

This tutorial will showcase the use of pgstream to snapshot data from a PostgreSQL database. For this tutorial, we'll use a PostgreSQL output.

![snapshot2pg tutorial](../img/pgstream_tutorial_snapshot2pg.svg)

### Requirements

- A source PostgreSQL database
- A target PostgreSQL database
- pgstream (see [installation](../../README.md#installation) instructions for more details)

## Environment setup

The first step is to start the two PostgreSQL databases that will be used as source and target for the snapshot. The `pgstream` repository provides a docker installation that will be used for the purposes of this tutorial, but can be replaced by any available PostgreSQL servers.

To start the docker provided PostgreSQL servers, run the following command:

```sh
# Start two PostgreSQL databases using Docker.
# The source database will run on port 5432, and the target database will run on port 7654.
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
# URL of the source PostgreSQL database. This is where the snapshot will be taken from.
PGSTREAM_POSTGRES_SNAPSHOT_LISTENER_URL="postgres://postgres:postgres@localhost:5432?sslmode=disable"
```

The snapshot listener needs to be configured to indicate the tables/schemas that need to be snapshotted. If the tables are not schema qualified, the `public` schema will be assumed. Wildcards are supported. For example, `test_schema.*` will snapshot all tables in the `test_schema` schema, and `test` will snapshot the `public.test` table.

```sh
# Tables to snapshot. Use "*" to snapshot all tables in the public schema.
PGSTREAM_POSTGRES_SNAPSHOT_TABLES="*"
```

Further configuration can be provided to optimize the performance of the snapshot process, mostly focusing on the concurrency.

```sh
# Number of tables being snapshotted in parallel for a given schema. Defaults to 4.
PGSTREAM_POSTGRES_SNAPSHOT_SCHEMA_WORKERS=4
# Number of concurrent workers that will be used per table by the snapshotting process. Defaults to 4.
PGSTREAM_POSTGRES_SNAPSHOT_TABLE_WORKERS=4
# Size of the table page range which will be processed concurrently by the table workers from PGSTREAM_POSTGRES_SNAPSHOT_TABLE_WORKERS. Defaults to 1000.
PGSTREAM_POSTGRES_SNAPSHOT_BATCH_PAGE_SIZE=1000
# Number of schemas that will be processed in parallel by the snapshotting process. Defaults to 1.
PGSTREAM_POSTGRES_SNAPSHOT_WORKERS=1
```

The snapshot listener can also be configured to record and update the status of snapshot requests in a dedicated table `snapshot_requests` under the `pgstream` schema. This allows to only perform a given snapshot once by keeping track of what's already been completed. All it's needed is the URL of the database where the table should be created. For this tutorial, we'll use the source database.

```sh
PGSTREAM_POSTGRES_SNAPSHOT_STORE_URL="postgres://postgres:postgres@localhost:5432?sslmode=disable"
```

### Processor

With the listener side ready, the next step is to configure the processor. Since we want the snapshot reach a PostgreSQL database, we will set the PostgreSQL writer configuration variables. The only required value is the URL of the target database, where the snapshotted schema/data from the source database will be streamed. We use the URL of the docker PostgreSQL database we started earlier (note the port is the only difference between the source and the target PostgreSQL databases).

```sh
PGSTREAM_POSTGRES_WRITER_TARGET_URL="postgres://postgres:postgres@localhost:7654?sslmode=disable"
```

If we need to disable triggers on the target database during the snapshot process(ie., to avoid foreign key constraint violations), we can do so by setting the following variable:

```sh
PGSTREAM_POSTGRES_WRITER_DISABLE_TRIGGERS=true
```

For further granularity, we can also configure the action that should be taken when an insert has a conflict.

```sh
# Insert on conflict action. Options are update, nothing or error (error is the default behaviour)
PGSTREAM_POSTGRES_WRITER_ON_CONFLICT_ACTION=nothing
```

The PostgreSQL writer uses batching under the hood to reduce the number of IO calls to the target database and improve performance. The batch size and send timeout can both be configured to be able to better fit the different traffic patterns. The writer will send a batch when the timeout or the batch size is reached, whichever happens first.

```sh
# Number of DML queries that will be batched and sent together in a given transaction. It defaults to 100.
PGSTREAM_POSTGRES_WRITER_BATCH_SIZE=25

# Max delay between batch sending. The batches will be sent every 5s by default.
PGSTREAM_POSTGRES_WRITER_BATCH_TIMEOUT=5s
```

Since in this case there's no need to keep track of DDL changes, we don't need to set the schema log store variable (`PGSTREAM_POSTGRES_WRITER_SCHEMALOG_STORE_URL`).

The full configuration for this tutorial can be put into a `snapshot2pg_tutorial.env` file to be used in the next step. An equivalent `snapshot2pg_tutorial.yaml` configuration can be found below the environment one, and can be used interchangeably.

```sh
# Listener config
PGSTREAM_POSTGRES_SNAPSHOT_LISTENER_URL="postgres://postgres:postgres@localhost:5432?sslmode=disable"
PGSTREAM_POSTGRES_SNAPSHOT_TABLES="*"
PGSTREAM_POSTGRES_SNAPSHOT_SCHEMA_WORKERS=4
PGSTREAM_POSTGRES_SNAPSHOT_TABLE_WORKERS=4
PGSTREAM_POSTGRES_SNAPSHOT_BATCH_PAGE_SIZE=1000
PGSTREAM_POSTGRES_SNAPSHOT_WORKERS=1
PGSTREAM_POSTGRES_SNAPSHOT_STORE_URL="postgres://postgres:postgres@localhost:5432?sslmode=disable"

# Processor config
PGSTREAM_POSTGRES_WRITER_TARGET_URL="postgres://postgres:postgres@localhost:7654?sslmode=disable"
PGSTREAM_POSTGRES_WRITER_BATCH_SIZE=25
PGSTREAM_POSTGRES_WRITER_BATCH_TIMEOUT=5s
PGSTREAM_POSTGRES_WRITER_DISABLE_TRIGGERS=true
PGSTREAM_POSTGRES_WRITER_ON_CONFLICT_ACTION=nothing
```

```yaml
source:
  postgres:
    url: "postgres://postgres:postgres@localhost:5432?sslmode=disable"
    mode: snapshot # options are replication, snapshot or snapshot_and_replication
    snapshot: # when mode is snapshot or snapshot_and_replication
      mode: full # options are data_and, schema or data
      tables: ["*"] # tables to snapshot, can be a list of table names or a pattern
      recorder:
        repeatable_snapshots: true # whether to repeat snapshots that have already been taken
        postgres_url: "postgres://postgres:postgres@localhost:5432?sslmode=disable" # URL of the database where the snapshot status is recorded
      snapshot_workers: 4 # number of schemas to be snapshotted in parallel
      data: # when mode is full or data
        schema_workers: 4 # number of schema tables to be snapshotted in parallel
        table_workers: 4 # number of workers to snapshot a table in parallel
        batch_page_size: 1000 # number of pages to read per batch
      schema: # when mode is full or schema
        mode: pgdump_pgrestore # options are pgdump_pgrestore or schemalog
        pgdump_pgrestore:
          clean_target_db: false # whether to clean the target database before restoring

target:
  postgres:
    url: "postgres://postgres:postgres@localhost:7654?sslmode=disable"
    batch:
      timeout: 5000 # batch timeout in milliseconds
      size: 25 # number of messages in a batch
    disable_triggers: true # whether to disable triggers on the target database
    on_conflict_action: "nothing" # options are update, nothing or error
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

## Perform snapshot

With the configuration ready, we can now run pgstream. In this case we set the log level as trace to provide more context for debugging and have more visibility into what pgstream is doing under the hood. Once the snapshot finishes, the process will stop.

```sh
# with the environment configuration
pgstream snapshot -c snapshot2pg_tutorial.env --log-level trace

# with the yaml configuration
pgstream snapshot -c snapshot2pg_tutorial.yaml --log-level trace

# with the CLI flags and relying on defaults
pgstream snapshot --postgres-url "postgres://postgres:postgres@localhost:5432?sslmode=disable" --target postgres --target-url "postgres://postgres:postgres@localhost:7654?sslmode=disable" --tables "*" --log-level trace
```

## Verify snapshot

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

We can also check the status of the snapshot by querying the `pgstream.snapshot_requests` table on the source database. It should contain the details of the snapshot that has been requested.

```sh
➜ psql postgresql://postgres:postgres@localhost:5432/postgres
```

```sql
postgres@localhost:postgres> SELECT * FROM pgstream.snapshot_requests;
+--------+-------------+-------------+-------------------------------+-------------------------------+-----------+--------+
| req_id | schema_name | table_names | created_at                    | updated_at                    | status    | errors |
|--------+-------------+-------------+-------------------------------+-------------------------------+-----------+--------|
| 1      | public      | ['*']       | 2025-03-18 11:27:21.683361+00 | 2025-03-18 11:27:21.843207+00 | completed | <null> |
+--------+-------------+-------------+-------------------------------+-------------------------------+-----------+--------+
```

If the snapshot has completed with errors, it will be retried on the next run of pgstream.

## Troubleshooting

Here are some common issues you might encounter while following this tutorial and how to resolve them:

### 1. **Error: `Connection refused`**

- **Cause:** The PostgreSQL database is not running or the connection URL is incorrect.
- **Solution:**
  - Ensure the Docker containers for the source and target databases are running.
  - Verify the database URLs in the configuration file (`snapshot2pg_tutorial.env`).
  - Test the connection using `psql`:
    ```sh
    psql postgresql://postgres:postgres@localhost:5432/postgres
    ```

### 2. **Error: `pgstream: replication slot not found`**

- **Cause:** The replication slot was not created during initialization.
- **Solution:**
  - Ensure the `pgstream` initialization step was completed successfully.
  - Check the replication slots in the source database:
    ```sql
    SELECT slot_name FROM pg_replication_slots;
    ```
  - If the slot is missing, reinitialize [pgstream](#database-initialisation) or manually create the slot.

### 3. **Error: `Snapshot failed`**

- **Cause:** There may be issues with the configuration or permissions.
- **Solution:**
  - Check the pgstream logs for detailed error messages:
    ```sh
    pgstream run -c snapshot2pg_tutorial.env --log-level trace
    ```
  - Ensure the source database user has the necessary permissions to read the schema and data.
  - Check the `pgstream.snapshot_requests` table for error details:
    ```sql
    SELECT * FROM pgstream.snapshot_requests;
    ```

### 4. **Error: `Target database does not contain the snapshot data`**

- **Cause:** The snapshot process did not complete successfully or the target database URL is incorrect.
- **Solution:**

  - Verify the target database URL in the configuration file.
  - Check the pgstream logs to confirm the snapshot process completed without errors.
  - Check the `pgstream.snapshot_requests` table for error details:

    ```sql
    SELECT * FROM pgstream.snapshot_requests;
    ```

  - Query the target database to ensure the data was replicated:
    ```sql
    SELECT * FROM test;
    ```

### 5. **Error: `pgstream: invalid configuration`**

- **Cause:** The configuration file contains invalid or missing values.
- **Solution:**
  - Double-check the `snapshot2pg_tutorial.env` file for typos or missing variables.
  - Refer to the [pgstream configuration documentation](../README.md#configuration) for details on required variables.

### 6. **Error: `Permission denied`**

- **Cause:** The database user does not have sufficient privileges.
- **Solution:**
  - Ensure the user has the necessary permissions to create tables, replication slots, and perform snapshots.
  - Grant the required privileges:
    ```sql
    GRANT ALL PRIVILEGES ON DATABASE postgres TO postgres;
    ```

If you encounter issues not listed here, consult the [pgstream documentation](../../README.md) or open an issue on the project's GitHub repository.

## Summary

In this tutorial, we successfully configured `pgstream` to snapshot data from a source PostgreSQL database to a target PostgreSQL database. We covered the following steps:

1. Set up the source and target PostgreSQL databases using Docker.
2. Configured the `pgstream` listener and processor for snapshotting.
3. Created a sample table in the source database and populated it with data.
4. Ran `pgstream` to perform the snapshot.
5. Verified that the data was successfully replicated to the target database.

This process demonstrates how `pgstream` can be used to efficiently snapshot data between PostgreSQL databases. For more advanced use cases, such as continuous replication or applying transformations, refer to the other [pgstream tutorials](../../README.md#tutorials).
