# 🐘 PostgreSQL replication with transformers 🔒

1. [Introduction](#introduction)
2. [Environment Setup](#environment-setup)
3. [Database Initialization](#database-initialization)
4. [Prepare `pgstream` Configuration](#prepare-pgstream-configuration)
   - [Listener](#listener)
   - [Processor](#processor)
5. [Run `pgstream`](#run-pgstream)
6. [Verify Data Transformation](#verify-data-transformation)
7. [Troubleshooting](#troubleshooting)
8. [Summary](#summary)

## Introduction

This tutorial will showcase the use of pgstream to replicate data from PostgreSQL using transformers. We'll use a target PostgreSQL database as output, but the same would apply to any of the supported outputs.

![transformer tutorial](../img/pgstream_tutorial_transformer.svg)

### Requirements

- A source PostgreSQL database
- A target PostgreSQL database
- pgstream (see [installation](../../README.md#installation) instructions for more details)

### Demo

https://github.com/user-attachments/assets/905fc81a-5172-4151-aacb-e638fb34e773

Youtube link [here](https://www.youtube.com/watch?v=6aHzmv1h_48&list=PLf7KS0svgDP_H8x5lD8HPXK2BjhwO4ffT&index=3&pp=iAQB).

## Environment setup

The first step is to start the two PostgreSQL databases that will be used as source and target for replication. The `pgstream` repository provides a docker installation that will be used for the purposes of this tutorial, but can be replaced by any available PostgreSQL servers, as long as they have [`wal2json`](https://github.com/eulerto/wal2json) installed.

To start the docker provided PostgreSQL servers, run the following command:

```sh
# Start two PostgreSQL databases using Docker.
# The source database will run on port 5432, and the target database will run on port 7654.
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

The listener configuration will be the same as the one in the PostgreSQL to PostgreSQL replication tutorial. Check the details [here](postgres_to_postgres.md#listener).

- Without initial snapshot

```sh
# Listener config
PGSTREAM_POSTGRES_LISTENER_URL="postgres://postgres:postgres@localhost:5432?sslmode=disable"
PGSTREAM_POSTGRES_REPLICATION_SLOT_NAME=pgstream_tutorial_slot
```

- With initial snapshot

```sh
# Listener config
PGSTREAM_POSTGRES_LISTENER_URL="postgres://postgres:postgres@localhost:5432?sslmode=disable"
PGSTREAM_POSTGRES_REPLICATION_SLOT_NAME=pgstream_tutorial_slot
PGSTREAM_POSTGRES_SNAPSHOT_STORE_URL="postgres://postgres:postgres@localhost:5432?sslmode=disable"
# Initial snapshot of all tables in the public schema
PGSTREAM_POSTGRES_SNAPSHOT_TABLES="*"
```

### Processor

The processor configuration will be the same as the one in the PostgreSQL to PostgreSQL replication tutorial. Check the details [here](postgres_to_postgres.md#processor).

```sh
PGSTREAM_POSTGRES_WRITER_TARGET_URL="postgres://postgres:postgres@localhost:7654?sslmode=disable"
PGSTREAM_POSTGRES_WRITER_BATCH_SIZE=25
PGSTREAM_POSTGRES_WRITER_BATCH_TIMEOUT=5s
PGSTREAM_POSTGRES_WRITER_SCHEMALOG_STORE_URL="postgres://postgres:postgres@localhost:5432?sslmode=disable"
```

In addition to the processor configuration above, we will add the transformer processor wrapper which will apply the column transformations. The only configuration required is the path to the transformer rules.

```sh
PGSTREAM_TRANSFORMER_RULES_FILE="tutorial_transformer_rules.yaml"
```

The transformer rules follow the pattern defined in the [transfomer section](../README.md#transformation-rules) of the documentation. To see all the transformers currently supported, check out the [transformers section](../README.md#suported-transformers). For this tutorial we'll be using the following transformations:

```yaml
transformations:
  - schema: public
    table: test
    column_transformers:
      email:
        name: neosync_email
        parameters:
          preserve_length: true # Ensures the transformed email has the same length as the original.
          preserve_domain: true # Keeps the domain of the original email intact.
          email_type: fullname # Specifies the type of email transformation.
      name:
        name: greenmask_firstname
        parameters:
          generator: deterministic # Ensures the same input always produces the same output.
          gender: Female # Generates female names for the transformation.
```

The full configuration for this tutorial can be put into a `pg2pg_transformer_tutorial.env` file to be used in the next step.

- Without initial snapshot

```sh
# Listener config
PGSTREAM_POSTGRES_LISTENER_URL="postgres://postgres:postgres@localhost:5432?sslmode=disable"
PGSTREAM_POSTGRES_REPLICATION_SLOT_NAME=pgstream_tutorial_slot

# Processor config
PGSTREAM_TRANSFORMER_RULES_FILE="tutorial_transformer_rules.yaml"
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
PGSTREAM_POSTGRES_SNAPSHOT_STORE_URL="postgres://postgres:postgres@localhost:5432?sslmode=disable"
# Initial snapshot of all tables in the public schema
PGSTREAM_POSTGRES_SNAPSHOT_TABLES="*"

# Processor config
PGSTREAM_TRANSFORMER_RULES_FILE="tutorial_transformer_rules.yaml"
PGSTREAM_POSTGRES_WRITER_TARGET_URL="postgres://postgres:postgres@localhost:7654?sslmode=disable"
PGSTREAM_POSTGRES_WRITER_BATCH_SIZE=25
PGSTREAM_POSTGRES_WRITER_BATCH_TIMEOUT=5s
PGSTREAM_POSTGRES_WRITER_SCHEMALOG_STORE_URL="postgres://postgres:postgres@localhost:5432?sslmode=disable"
```

## Run `pgstream`

With the configuration ready, we can now run pgstream. In this case we set the log level as trace to provide more context for debugging and have more visibility into what pgstream is doing under the hood.

**Important:** Ensure that the source and target databases are running before proceeding.

```sh
pgstream run -c pg2pg_transformer_tutorial.env --log-level trace
```

Now we can connect to the source database, create a table and start inserting data.

```sh
➜ psql postgresql://postgres:postgres@localhost:5432/postgres
```

```sql
CREATE TABLE test(id SERIAL PRIMARY KEY, name TEXT, email TEXT);
INSERT INTO test(name,email) VALUES('alice','alice@test.com'),('bob','bob@test.com'),('charlie','charlie@test.com');
SELECT * FROM test;
+----+---------+------------------+
| id | name    | email            |
|----+---------+------------------|
| 1  | alice   | alice@test.com   |
| 2  | bob     | bob@test.com     |
| 3  | charlie | charlie@test.com |
+----+---------+------------------+
```

## Verify data transformation

When we check the target PostgreSQL database, we should see the three rows have been inserted, but both the name and the emails have been anonymised.

```sh
➜ psql postgresql://postgres:postgres@localhost:7654/postgres
```

```sql
SELECT * FROM test;
+----+---------+------------------+
| id | name    | email            |
|----+---------+------------------|
| 1  | Herman  | ecdox@test.com   |
| 2  | Domingo | nab@test.com     |
| 3  | Elwin   | aafsack@test.com |
+----+---------+------------------+
```

Since we used the deterministic transformation for the name column, if we insert the same names again the value generated should be the same.

```sh
➜ psql postgresql://postgres:postgres@localhost:5432/postgres
```

```sql
INSERT INTO test(name,email) VALUES('alice','alice@test.com'),('bob','bob@test.com'),('charlie','charlie@test.com');
SELECT * FROM test;
+----+---------+------------------+
| id | name    | email            |
|----+---------+------------------|
| 1  | alice   | alice@test.com   |
| 2  | bob     | bob@test.com     |
| 3  | charlie | charlie@test.com |
| 4  | alice   | alice@test.com   |
| 5  | bob     | bob@test.com     |
| 6  | charlie | charlie@test.com |
+----+---------+------------------+
```

```sh
➜ psql postgresql://postgres:postgres@localhost:7654/postgres
```

```sql
SELECT * FROM test;
+----+---------+------------------+
| id | name    | email            |
|----+---------+------------------|
| 1  | Herman  | ecdox@test.com   |
| 2  | Domingo | nab@test.com     |
| 3  | Elwin   | aafsack@test.com |
| 4  | Herman  | rwamy@test.com   |
| 5  | Domingo | wie@test.com     |
| 6  | Elwin   | cailbni@test.com |
+----+---------+------------------+
```

To see all the transformers currently supported, check out the [transformers section](../README.md#suported-transformers).

## Troubleshooting

- **Error:** `Connection refused`

  - Ensure the Docker containers for the source and target databases are running.
  - Verify the database URLs in the configuration.

- **Error:** `Replication slot not found`

  - Check that the replication slot was created successfully during initialization.
  - Run the following query on the source database to verify:
    ```sql
    SELECT slot_name FROM pg_replication_slots;
    ```

- **Error:** `Transformation rules not applied`
  - Ensure the `PGSTREAM_TRANSFORMER_RULES_FILE` points to the correct YAML file.
  - Validate the syntax of the transformer rules using the [pgstream documentation](../README.md#transformation-rules).

## Summary

In this tutorial, we successfully configured `pgstream` to replicate data from a source PostgreSQL database to a target PostgreSQL database, applying transformations to anonymize sensitive data during replication. For more use cases, refer to the [pgstream documentation](https://github.com/xataio/pgstream).
