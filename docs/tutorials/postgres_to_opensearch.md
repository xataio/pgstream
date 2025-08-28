# 🐘 PostgreSQL replication to OpenSearch 🔍

## Table of Contents

1. [Introduction](#introduction)
2. [Environment Setup](#environment-setup)
3. [Database Initialization](#database-initialization)
4. [Prepare `pgstream` Configuration](#prepare-pgstream-configuration)
   - [Listener](#listener)
   - [Processor](#processor)
5. [Validate `pgstream` status](#validate-pgstream-status)
6. [Run `pgstream`](#run-pgstream)
7. [Verify Replication](#verify-replication)
8. [Troubleshooting](#troubleshooting)
9. [Summary](#summary)

## Introduction

This tutorial will showcase the use of pgstream to replicate data from a PostgreSQL database to an OpenSearch cluster.

![pg2os tutorial](../img/pgstream_tutorial_pg2os.svg)

### Requirements

- A source PostgreSQL database
- A target OpenSearch cluster
- pgstream (see [installation](../../README.md#installation) instructions for more details)

### Demo

https://github.com/user-attachments/assets/5a6b3daa-b57d-492f-a712-166931af89d0

Youtube link [here](https://www.youtube.com/watch?v=HC0LeLjD220&list=PLf7KS0svgDP_H8x5lD8HPXK2BjhwO4ffT&index=1&pp=iAQB).

## Environment setup

The first step is to start the PostgreSQL database that will be used as source and the OpenSearch cluster that will be the target for replication. The `pgstream` repository provides a docker installation that will be used for the purposes of this tutorial, but can be replaced by any available PostgreSQL server, as long as they have [`wal2json`](https://github.com/eulerto/wal2json) installed, and any OpenSearch cluster.

To start the docker provided PostgreSQL servers, run the following command:

```sh
docker-compose -f build/docker/docker-compose.yml --profile pg2os up
```

This will start a PostgreSQL database on ports `5432` and an OpenSearch cluster on port `9200`. It also starts OpenSearch dashboards UI on port `5601` to simplify visualisation of the OpenSearch documents, but we'll be relying on curl for this tutorial.

## Database initialisation

Once both the PostgreSQL server and the OpenSearch cluster are up and running, the next step is to initialise pgstream on the source database. This will create the `pgstream` schema in the configured Postgres database, along with the tables/functions/triggers required to keep track of the schema changes. See [Tracking schema changes](../README.md#tracking-schema-changes) section for more details. This step will also create a replication slot on the source database which will be used by the pgstream service.

The initialisation step allows to provide both the URL of the PostgreSQL database and the name of the replication slot to be created. The PostgreSQL URL is required, but the replication slot name is optional. If not provided, it will default to `pgstream_<dbname>_slot`, where `<dbname>` is the name of the PostgreSQL database. The configuration can be provided either by using the CLI supported parameters, or using the environment variables.

For this tutorial, we'll create a replication slot with the name `pgstream_tutorial_slot`.

- Using CLI parameters:

```sh
pgstream init --postgres-url "postgres://postgres:postgres@localhost:5432?sslmode=disable" --replication-slot pgstream_tutorial_slot
```

- Using environment variables:

```sh
PGSTREAM_POSTGRES_REPLICATION_SLOT_NAME=pgstream_tutorial_slot PGSTREAM_POSTGRES_LISTENER_URL=postgres://postgres:postgres@localhost:5432?sslmode=disable pgstream init
```

Successful initialisation should prompt the following message:

```
SUCCESS  pgstream initialisation complete
```

If at any point the initialisation performed by pgstream needs to be reverted, all state will be removed by running the `destroy` CLI command.

```sh
pgstream destroy --postgres-url "postgres://postgres:postgres@localhost:5432?sslmode=disable" --replication-slot pgstream_tutorial_slot
```

## Prepare `pgstream` configuration

### Listener

In order to run pgstream, we need to provide the configuration required to run the PostgreSQL to OpenSearch replication. First, we configure the listener module that will be listening to the WAL on the source PostgreSQL database. This requires the PostgreSQL database URL, which will be the one from the docker PostgreSQL server we started and setup in the previous steps.

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
# URL of the PostgreSQL database we want to snapshot
PGSTREAM_POSTGRES_SNAPSHOT_STORE_URL="postgres://postgres:postgres@localhost:5432?sslmode=disable"

# List of tables we want to snapshot. If the tables are not schema qualified, the public schema will be assumed.
# Wildcards are supported.
#
# The following example will snapshot all tables in the `test_schema` and the table `test` from the public schema.
PGSTREAM_POSTGRES_SNAPSHOT_TABLES="test_schema.* test"
```

Further configuration can be provided to optimize the performance of the snapshot process. For more information, check the [snapshot tutorial](postgres_snapshot).

### Processor

With the listener side ready, the next step is to configure the processor. Since we want to replicate to an OpenSearch cluster, we will need to set the search indexer configuration variables. The only required value is the URL of the cluster, where the replicated data from the source database will be written. We use the URL of the docker OpenSearch cluster we started earlier.

```sh
PGSTREAM_OPENSEARCH_STORE_URL="http://admin:admin@localhost:9200"
```

The search indexer uses batching under the hood to reduce the number of IO calls to OpenSearch and improve performance. The batch size and send timeout can both be configured to be able to better fit the different traffic patterns. The indexer will send a batch when the timeout or the batch size is reached, whichever happens first.

```sh
# Number of documents that will be batched and sent together in a given request. It defaults to 100.
PGSTREAM_SEARCH_INDEXER_BATCH_SIZE=25
# Max delay between batch sending. The batches will be sent every 5s by default.
PGSTREAM_SEARCH_INDEXER_BATCH_TIMEOUT=5s
```

The search indexer will apply exponential backoff by default for all the OpenSearch cluster requests. However, the backoff policy can be tweaked further to fit the behaviour to the specific needs of the application. The configuration for the exponential backoff can be altered, but it is also possible to configure a non exponential backoff instead as well. Only one of the two will be applied, giving priority to the exponential backoff configuration if both are provided.

```sh
# The amount of time the initial exponential backoff interval will apply. Defaults to 1s.
PGSTREAM_SEARCH_STORE_EXP_BACKOFF_INITIAL_INTERVAL=1s
# The max amount of time the exponential backoff will retry for. Defaults to 1m.
PGSTREAM_SEARCH_STORE_EXP_BACKOFF_MAX_INTERVAL=1m
# Maximum amount of retries the exponential backoff will retry for. Defaults to 0.
PGSTREAM_SEARCH_STORE_EXP_BACKOFF_MAX_RETRIES=0

# Constant interval that the backoff policy will apply between retries. Defaults to 0.
PGSTREAM_SEARCH_STORE_BACKOFF_INTERVAL=0
# Max number of retries the backoff policy will apply. Defaults to 0.
PGSTREAM_SEARCH_STORE_BACKOFF_MAX_RETRIES=0
```

The search indexer requires some metadata to be added to the WAL events in order to identify the id and version of the event, which will be used to index the documents. In order to add this data to the events, we need to configure the injector processor wrapper, which takes care of that, as well as injecting pgstream ids into the event columns. This pgstream ids will be used by the indexer instead of the column names, in order to keep a constant identifier when renames happen, helping with performance by preventing reindexes. Check out more details about the injector in the [architecture](../README.md#architecture) section.

The injector only needs the URL of the database where the `pgstream.schema_log` table is hosted. In our case, that's the source PostgreSQL database.

```sh
PGSTREAM_INJECTOR_STORE_POSTGRES_URL="postgres://postgres:postgres@localhost:5432?sslmode=disable"
```

The full configuration for this tutorial can be put into a `pg2os_tutorial.env` file to be used in the next step. An equivalent `pg2os_tutorial.yaml` configuration can be found below the environment one, and can be used interchangeably.

- Without initial snapshot

```sh
# Listener config
PGSTREAM_POSTGRES_LISTENER_URL="postgres://postgres:postgres@localhost:5432?sslmode=disable"
PGSTREAM_POSTGRES_REPLICATION_SLOT_NAME=pgstream_tutorial_slot

# Processor config
PGSTREAM_INJECTOR_STORE_POSTGRES_URL="postgres://postgres:postgres@localhost:5432?sslmode=disable"
PGSTREAM_OPENSEARCH_STORE_URL="http://admin:admin@localhost:9200"
PGSTREAM_SEARCH_INDEXER_BATCH_SIZE=25
PGSTREAM_SEARCH_INDEXER_BATCH_TIMEOUT=5s
```

```yaml
source:
  postgres:
    url: "postgres://postgres:postgres@localhost:5432?sslmode=disable"
    mode: replication
    replication:
      replication_slot: "pgstream_tutorial_slot"
target:
  search:
    engine: "opensearch" # options are elasticsearch or opensearch
    url: "http://localhost:9200" # URL of the search engine
    batch:
      timeout: 5000 # batch timeout in milliseconds
      size: 25 # number of messages in a batch
modifiers:
  injector:
    enabled: true # whether to inject pgstream metadata into the WAL events
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
PGSTREAM_INJECTOR_STORE_POSTGRES_URL="postgres://postgres:postgres@localhost:5432?sslmode=disable"
PGSTREAM_OPENSEARCH_STORE_URL="http://admin:admin@localhost:9200"
PGSTREAM_SEARCH_INDEXER_BATCH_SIZE=25
PGSTREAM_SEARCH_INDEXER_BATCH_TIMEOUT=5s
```

```yaml
source:
  postgres:
    url: "postgresql://user:password@localhost:5432/mydatabase"
    mode: snapshot_and_replication # options are replication, snapshot or snapshot_and_replication
    snapshot: # when mode is snapshot or snapshot_and_replication
      mode: full # options are data_and, schema or data
      tables: ["*"] # tables to snapshot, can be a list of table names or a pattern
      recorder:
        repeatable_snapshots: true # whether to repeat snapshots that have already been taken
        postgres_url: "postgres://postgres:postgres@localhost:5432?sslmode=disable" # URL of the database where the snapshot status is recorded
      schema: # when mode is full or schema
        mode: schemalog # options are pgdump_pgrestore or schemalog
    replication:
      replication_slot: "pgstream_tutorial_slot"
target:
  search:
    engine: "opensearch" # options are elasticsearch or opensearch
    url: "http://localhost:9200" # URL of the search engine
    batch:
      timeout: 5000 # batch timeout in milliseconds
      size: 25 # number of messages in a batch
modifiers:
  injector:
    enabled: true # whether to inject pgstream metadata into the WAL events
```

## Validate `pgstream` status

We can validate that the initialisation and the configuration are valid by running the `status` command before starting `pgstream`.

```sh
# using yaml configuration file
./pgstream status -c pg2os_tutorial.yaml
# using env configuration file
./pgstream status -c pg2os_tutorial.env
```

```sh
SUCCESS  pgstream status check encountered no issues
Initialisation status:
 - Pgstream schema exists: true
 - Pgstream schema_log table exists: true
 - Migration current version: 7
 - Migration status: success
 - Replication slot name: pgstream_tutorial_slot
 - Replication slot plugin: wal2json
 - Replication slot database: postgres
Config status:
 - Valid: true
Transformation rules status:
 - Valid: true
Source status:
 - Reachable: true
```

## Run `pgstream`

With the configuration ready, we can now run pgstream. In this case we set the log level as trace to provide more context for debugging and have more visibility into what pgstream is doing under the hood.

```sh
# with the environment configuration
pgstream run -c pg2os_tutorial.env --log-level trace

# with the yaml configuration
pgstream run -c pg2os_tutorial.yaml --log-level trace

# with the CLI flags and relying on defaults
pgstream run --source postgres --source-url "postgres://postgres:postgres@localhost:5432?sslmode=disable" --target opensearch --target-url "http://localhost:9200" --log-level trace
```

## Verify Replication

Now we can connect to the source database and create a table:

```sh
➜ psql postgresql://postgres:postgres@localhost:5432/postgres
```

```sql
CREATE TABLE test(id SERIAL PRIMARY KEY, name TEXT);
```

We should be able to see a `pgstream` index created in the OpenSearch cluster, along with a `public-1` index. The `pgstream` index keeps track of the schema changes, and it's the equivalent of the `pgstream.schema_log` table in PostgreSQL. The `puglic-1` index is where the data for our tables in the public schema will be indexed.

```sh
➜  ~ curl -X GET -u admin:admin http://localhost:9200/_cat/indices
green  open .opensearch-observability    -AUmVpfvQ0SyMi6-9ayTzA 1 0  0 0    208b    208b
yellow open security-auditlog-2025.03.13 ealaBDugReq6oxh-rg4bbw 1 1  7 0 105.4kb 105.4kb
yellow open public-1                     I0HbCjHsSCGxQ3O5aI2N4g 1 1  0 0    208b    208b
yellow open pgstream                     PWefOB2qQMa_Nv_47FMjMg 1 1  1 0   5.9kb   5.9kb
green  open .opendistro_security         mso1dkRtRlWFNEVGg72r3A 1 0 10 0    73kb    73kb
green  open .kibana_1                    ODNoFjfmT8enKvEi9tvRcw 1 0  0 0    208b    208b
```

If we look up the documents on the `pgstream` index we can see there's the version 1 of the schema.

```sh
➜  ~ curl -X GET -u admin:admin http://localhost:9200/pgstream/_search | jq .
```

```json
{
  "took": 73,
  "timed_out": false,
  "_shards": {
    "total": 1,
    "successful": 1,
    "skipped": 0,
    "failed": 0
  },
  "hits": {
    "total": {
      "value": 1,
      "relation": "eq"
    },
    "max_score": 1.0,
    "hits": [
      {
        "_index": "pgstream",
        "_id": "cv9bopq2e0ig0vt9s3mg",
        "_score": 1.0,
        "_source": {
          "id": "cv9bopq2e0ig0vt9s3mg",
          "version": 1,
          "schema_name": "public",
          "created_at": "2025-03-13 11:07:19.050485",
          "schema": "{\"tables\":[{\"oid\":\"16464\",\"name\":\"test\",\"columns\":[{\"name\":\"id\",\"type\":\"integer\",\"default\":\"nextval('public.test_id_seq'::regclass)\",\"nullable\":false,\"unique\":true,\"metadata\":null,\"pgstream_id\":\"cv9bopq2e0ig0vt9s3n0-1\"},{\"name\":\"name\",\"type\":\"text\",\"nullable\":true,\"unique\":false,\"metadata\":null,\"pgstream_id\":\"cv9bopq2e0ig0vt9s3n0-2\"}],\"primary_key_columns\":[\"id\"],\"pgstream_id\":\"cv9bopq2e0ig0vt9s3n0\"}]}",
          "acked": false
        }
      }
    ]
  }
}
```

And the `public-1` index (which we can refer to by its alias `public`) will show no documents yet.

```sh
➜  ~ curl -X GET -u admin:admin http://localhost:9200/public/_search | jq .
```

```json
{
  "took": 10,
  "timed_out": false,
  "_shards": {
    "total": 1,
    "successful": 1,
    "skipped": 0,
    "failed": 0
  },
  "hits": {
    "total": {
      "value": 0,
      "relation": "eq"
    },
    "max_score": null,
    "hits": []
  }
}
```

If we now start inserting data into the source database, we will be able to see the documents being indexed on the target OpenSearch cluster.

```sql
INSERT INTO test(name) VALUES('alice'),('bob'),('charlie');
```

```sh
➜  ~ curl -X GET -u admin:admin http://localhost:9200/public/_search | jq .
```

```json
{
  "took": 6,
  "timed_out": false,
  "_shards": {
    "total": 1,
    "successful": 1,
    "skipped": 0,
    "failed": 0
  },
  "hits": {
    "total": {
      "value": 3,
      "relation": "eq"
    },
    "max_score": 1.0,
    "hits": [
      {
        "_index": "public-1",
        "_id": "cv9bopq2e0ig0vt9s3n0_1",
        "_score": 1.0,
        "_source": {
          "_table": "cv9bopq2e0ig0vt9s3n0",
          "cv9bopq2e0ig0vt9s3n0-2": "alice"
        }
      },
      {
        "_index": "public-1",
        "_id": "cv9bopq2e0ig0vt9s3n0_2",
        "_score": 1.0,
        "_source": {
          "_table": "cv9bopq2e0ig0vt9s3n0",
          "cv9bopq2e0ig0vt9s3n0-2": "bob"
        }
      },
      {
        "_index": "public-1",
        "_id": "cv9bopq2e0ig0vt9s3n0_3",
        "_score": 1.0,
        "_source": {
          "_table": "cv9bopq2e0ig0vt9s3n0",
          "cv9bopq2e0ig0vt9s3n0-2": "charlie"
        }
      }
    ]
  }
}
```

As we can see in the documents, instead of column names we're relying on the pgstream ids associated to each column, so that renames don't trigger a reindex. The pgstream id of the table is used to build the pgstream ids of the columns, by appending the number to them. In this example, the pgstream id for the table `test` is `cv9bopq2e0ig0vt9s3n0`, and the `name` column pgstream id is `cv9bopq2e0ig0vt9s3n0-2`. The `id` column is not explicitly mapped, but instead used as the document `_id`, prefixed by the table pgstream id to prevent collisions with other tables. Since the `id` is the primary key for this table, it was used as the document id. If the table had a composite primary key, the individual columns would be part of the `_source`, and the document `_id` would be a combination of their values.

If we decide to update the schema of the table we will see the `pgstream` index will get a new document with a new version of the schema.

```sql
ALTER TABLE test ADD COLUMN age INT DEFAULT 0;
```

```sh
➜  ~ curl -X GET -u admin:admin http://localhost:9200/pgstream/_search | jq .
```

```json
{
  "took": 8,
  "timed_out": false,
  "_shards": {
    "total": 1,
    "successful": 1,
    "skipped": 0,
    "failed": 0
  },
  "hits": {
    "total": {
      "value": 2,
      "relation": "eq"
    },
    "max_score": 1.0,
    "hits": [
      {
        "_index": "pgstream",
        "_id": "cv9bopq2e0ig0vt9s3mg",
        "_score": 1.0,
        "_source": {
          "id": "cv9bopq2e0ig0vt9s3mg",
          "version": 1,
          "schema_name": "public",
          "created_at": "2025-03-13 11:07:19.050485",
          "schema": "{\"tables\":[{\"oid\":\"16464\",\"name\":\"test\",\"columns\":[{\"name\":\"id\",\"type\":\"integer\",\"default\":\"nextval('public.test_id_seq'::regclass)\",\"nullable\":false,\"unique\":true,\"metadata\":null,\"pgstream_id\":\"cv9bopq2e0ig0vt9s3n0-1\"},{\"name\":\"name\",\"type\":\"text\",\"nullable\":true,\"unique\":false,\"metadata\":null,\"pgstream_id\":\"cv9bopq2e0ig0vt9s3n0-2\"}],\"primary_key_columns\":[\"id\"],\"pgstream_id\":\"cv9bopq2e0ig0vt9s3n0\"}]}",
          "acked": false
        }
      },
      {
        "_index": "pgstream",
        "_id": "cv9btii2e0ig0vt9s3ng",
        "_score": 1.0,
        "_source": {
          "id": "cv9btii2e0ig0vt9s3ng",
          "version": 2,
          "schema_name": "public",
          "created_at": "2025-03-13 11:17:30.291352",
          "schema": "{\"tables\":[{\"oid\":\"16464\",\"name\":\"test\",\"columns\":[{\"name\":\"id\",\"type\":\"integer\",\"default\":\"nextval('public.test_id_seq'::regclass)\",\"nullable\":false,\"unique\":true,\"metadata\":null,\"pgstream_id\":\"cv9bopq2e0ig0vt9s3n0-1\"},{\"name\":\"name\",\"type\":\"text\",\"nullable\":true,\"unique\":false,\"metadata\":null,\"pgstream_id\":\"cv9bopq2e0ig0vt9s3n0-2\"},{\"name\":\"age\",\"type\":\"integer\",\"default\":\"0\",\"nullable\":true,\"unique\":false,\"metadata\":null,\"pgstream_id\":\"cv9bopq2e0ig0vt9s3n0-3\"}],\"primary_key_columns\":[\"id\"],\"pgstream_id\":\"cv9bopq2e0ig0vt9s3n0\"}]}",
          "acked": false
        }
      }
    ]
  }
}
```

Also, if we update the existing rows, we will see them updated on the existing document as well, along with the newly added column age (`cv9bopq2e0ig0vt9s3n0-3`). Since adding a new column with the fault doesn't trigger a table update, the existing documents will not be updated with the new column until they're updated.

```sql
UPDATE test SET name='a' WHERE name='alice';
```

```sh
➜  ~ curl -X GET -u admin:admin http://localhost:9200/public/_search | jq .
```

```json
{
  "took": 907,
  "timed_out": false,
  "_shards": {
    "total": 1,
    "successful": 1,
    "skipped": 0,
    "failed": 0
  },
  "hits": {
    "total": {
      "value": 3,
      "relation": "eq"
    },
    "max_score": 1.0,
    "hits": [
      {
        "_index": "public-1",
        "_id": "cv9bopq2e0ig0vt9s3n0_2",
        "_score": 1.0,
        "_source": {
          "_table": "cv9bopq2e0ig0vt9s3n0",
          "cv9bopq2e0ig0vt9s3n0-2": "bob"
        }
      },
      {
        "_index": "public-1",
        "_id": "cv9bopq2e0ig0vt9s3n0_3",
        "_score": 1.0,
        "_source": {
          "_table": "cv9bopq2e0ig0vt9s3n0",
          "cv9bopq2e0ig0vt9s3n0-2": "charlie"
        }
      },
      {
        "_index": "public-1",
        "_id": "cv9bopq2e0ig0vt9s3n0_1",
        "_score": 1.0,
        "_source": {
          "_table": "cv9bopq2e0ig0vt9s3n0",
          "cv9bopq2e0ig0vt9s3n0-2": "a",
          "cv9bopq2e0ig0vt9s3n0-3": 0
        }
      }
    ]
  }
}
```

Truncating a table will delete all documents associated with a table.

```sql
TRUNCATE TABLE test;
```

```sh
➜  ~ curl -X GET -u admin:admin http://localhost:9200/public/_search | jq .
```

```json
{
  "took": 2,
  "timed_out": false,
  "_shards": {
    "total": 1,
    "successful": 1,
    "skipped": 0,
    "failed": 0
  },
  "hits": {
    "total": {
      "value": 0,
      "relation": "eq"
    },
    "max_score": null,
    "hits": []
  }
}
```

And deleting a table will delete it from the pgstream schema log.

```sh
➜  ~ curl -X GET -u admin:admin http://localhost:9200/pgstream/_search | jq .
```

```json
{
  "took": 4,
  "timed_out": false,
  "_shards": {
    "total": 1,
    "successful": 1,
    "skipped": 0,
    "failed": 0
  },
  "hits": {
    "total": {
      "value": 3,
      "relation": "eq"
    },
    "max_score": 1.0,
    "hits": [
      {
        "_index": "pgstream",
        "_id": "cv9bopq2e0ig0vt9s3mg",
        "_score": 1.0,
        "_source": {
          "id": "cv9bopq2e0ig0vt9s3mg",
          "version": 1,
          "schema_name": "public",
          "created_at": "2025-03-13 11:07:19.050485",
          "schema": "{\"tables\":[{\"oid\":\"16464\",\"name\":\"test\",\"columns\":[{\"name\":\"id\",\"type\":\"integer\",\"default\":\"nextval('public.test_id_seq'::regclass)\",\"nullable\":false,\"unique\":true,\"metadata\":null,\"pgstream_id\":\"cv9bopq2e0ig0vt9s3n0-1\"},{\"name\":\"name\",\"type\":\"text\",\"nullable\":true,\"unique\":false,\"metadata\":null,\"pgstream_id\":\"cv9bopq2e0ig0vt9s3n0-2\"}],\"primary_key_columns\":[\"id\"],\"pgstream_id\":\"cv9bopq2e0ig0vt9s3n0\"}]}",
          "acked": false
        }
      },
      {
        "_index": "pgstream",
        "_id": "cv9btii2e0ig0vt9s3ng",
        "_score": 1.0,
        "_source": {
          "id": "cv9btii2e0ig0vt9s3ng",
          "version": 2,
          "schema_name": "public",
          "created_at": "2025-03-13 11:17:30.291352",
          "schema": "{\"tables\":[{\"oid\":\"16464\",\"name\":\"test\",\"columns\":[{\"name\":\"id\",\"type\":\"integer\",\"default\":\"nextval('public.test_id_seq'::regclass)\",\"nullable\":false,\"unique\":true,\"metadata\":null,\"pgstream_id\":\"cv9bopq2e0ig0vt9s3n0-1\"},{\"name\":\"name\",\"type\":\"text\",\"nullable\":true,\"unique\":false,\"metadata\":null,\"pgstream_id\":\"cv9bopq2e0ig0vt9s3n0-2\"},{\"name\":\"age\",\"type\":\"integer\",\"default\":\"0\",\"nullable\":true,\"unique\":false,\"metadata\":null,\"pgstream_id\":\"cv9bopq2e0ig0vt9s3n0-3\"}],\"primary_key_columns\":[\"id\"],\"pgstream_id\":\"cv9bopq2e0ig0vt9s3n0\"}]}",
          "acked": false
        }
      },
      {
        "_index": "pgstream",
        "_id": "cv9c05a2e0ig0vt9s3o0",
        "_score": 1.0,
        "_source": {
          "id": "cv9c05a2e0ig0vt9s3o0",
          "version": 3,
          "schema_name": "public",
          "created_at": "2025-03-13 11:23:01.250382",
          "schema": "{\"tables\":null}",
          "acked": false
        }
      }
    ]
  }
}
```

Dropping the schema would delete the `public` OpenSearch index.

## Troubleshooting

### 1. **Error: `Connection refused`**

- **Cause:** The PostgreSQL database or OpenSearch cluster is not running.
- **Solution:**
  - Ensure the Docker containers are running.
  - Verify the database and OpenSearch URLs in the configuration.

### 2. **Error: `Replication slot not found`**

- **Cause:** The replication slot was not created during initialization.
- **Solution:**
  - Reinitialize `pgstream` or manually create the replication slot.
  - Run the `pgstream status` command to validate the initialisation was successful.
  - Verify the replication slot exists by running:
    ```sql
    SELECT slot_name FROM pg_replication_slots;
    ```

### 3. **Error: `Data not replicated to OpenSearch`**

- **Cause:** The OpenSearch cluster URL is incorrect or the processor configuration is invalid.
- **Solution:**
  - Verify the OpenSearch URL in the configuration file.
  - Check the `pgstream` logs for errors:
    ```sh
    pgstream run -c pg2os_tutorial.env --log-level trace
    ```

### 4. **Error: `Permission denied`**

- **Cause:** The database user does not have sufficient privileges.
- **Solution:**
  - Grant the required privileges to the database user:
    ```sql
    GRANT ALL PRIVILEGES ON DATABASE postgres TO postgres;
    ```

If you encounter issues not listed here, consult the [pgstream documentation](https://github.com/xataio/pgstream) or open an issue on the project's GitHub repository.

## Summary

In this tutorial, we successfully configured `pgstream` to replicate data from a PostgreSQL database to an OpenSearch cluster. We:

1. Set up the source PostgreSQL database and target OpenSearch cluster.
2. Initialized `pgstream` and created a replication slot.
3. Configured the listener and processor for OpenSearch replication.
4. Verified that both schema changes and data changes were replicated correctly.

This tutorial demonstrates how `pgstream` can be used to integrate PostgreSQL with OpenSearch for real-time indexing. For more advanced use cases, refer to the [pgstream tutorials](../../README.md#tutorials).
