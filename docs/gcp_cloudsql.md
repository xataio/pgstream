# Google Cloud Provider Onboarding Guide

This guide explains how to use **pgstream** with **Google CloudSQL for Postgres**, covering **snapshots** and **replication**.

👉 Throughout this guide:

- **`pgstreamsource`** refers to the user provided in the **pgstream source URL**.
- **`pgstreamtarget`** refers to the user provided in the **pgstream target URL**.

## Table of Contents

1. [Snapshots](#snapshots)

   - [From CloudSQL Postgres](#from-cloudsql-postgres-snapshots)
   - [To CloudSQL Postgres](#to-cloudsql-postgres-snapshots)

2. [Replication](#replication)

   - [From CloudSQL Postgres](#from-cloudsql-postgres-replication)
   - [To CloudSQL Postgres](#to-cloudsql-postgres-replication)

3. [Troubleshooting](#troubleshooting)

## Snapshots

### From CloudSQL Postgres (Snapshots)

#### Quick Checklist

- [ ] Create a source user (**`pgstreamsource`**) with access to required schemas/tables.
- [ ] Decide how to handle roles:

  - Disabled → no special config.
  - Without passwords → no special config.
  - With passwords → enable `cloudsql.pg_authid_select_role` flag.

- [ ] Update YAML config with correct snapshot settings.

#### Steps

1. **User privileges**
   Ensure the **`pgstreamsource`** user (from the pgstream source URL) can access the database schema and tables you need.

2. **Roles handling**

   - **No roles** → no changes required.
   - **Roles without passwords** → no changes required.
   - **Roles with passwords** → grant `pgstreamsource` access to `pg_authid` by enabling the `cloudsql.pg_authid_select_role` database flag (via CloudSQL UI or API).

     - Reference: [GCP documentation](https://cloud.google.com/sql/docs/postgres/users)

   Without this, you’ll see:

   ```
   pg_dumpall: error: query failed: ERROR:  permission denied for table pg_authid
   ```

3. **Config when not snapshotting roles**
   If roles are disabled or not manually managed, add the following to avoid failures:

   ```yaml
   snapshot:
     schema:
       pgdump_pgrestore:
         roles_snapshot_mode: "disabled"
         no_owner: true
         no_privileges: true
   ```

   Example full configuration:

   ```yaml
   source:
     postgres:
       url: "postgresql://pgstreamsource:password@<cloudsql_address>:5432/db"
       mode: snapshot
       snapshot:
         mode: full # schema + data
         tables: ["public.*"] # all tables in the public schema
         schema:
           mode: pgdump_pgrestore
           pgdump_pgrestore:
             roles_snapshot_mode: "disabled"
             no_owner: true
             no_privileges: true
   ```

ℹ️ CloudSQL-managed roles (`cloudsqlsuperuser`, `postgres`) will not be snapshotted.

### To CloudSQL Postgres (Snapshots)

#### Quick Checklist

- [ ] Create a target user (**`pgstreamtarget`**) for the pgstream target URL.
- [ ] Grant privileges for schema/database ownership.
- [ ] Grant optional privileges depending on features (create DB, create roles, set replication role).
- [ ] If using CloudSQL, disable `disable_triggers`.

#### Steps

The **`pgstreamtarget`** user (from the pgstream target URL) must have the following privileges:

- **Schema ownership**

  ```sql
  ALTER DATABASE db OWNER TO pgstreamtarget;
  ALTER SCHEMA <schema> OWNER TO pgstreamtarget;
  ```

- **Database creation** (if `create_target_db` is enabled)

  ```sql
  ALTER ROLE pgstreamtarget CREATEDB;
  ```

- **Role creation** (if `role_snapshot_mode` is `enabled`/`no_passwords`)

  ```sql
  ALTER ROLE pgstreamtarget CREATEROLE;
  ```

  ⚠️ `pgstreamtarget` must already hold any privileges it assigns (e.g., `REPLICATION`).
  If many privileges are needed, use the `postgres` user, which is a [pseudo-superuser](https://cloud.google.com/sql/docs/postgres/users).

- **Disable triggers** (if `disable_triggers` is enabled)

  ```sql
  GRANT SET ON PARAMETER session_replication_role TO pgstreamtarget;
  ```

  Without this, pgstream fails with:

  ```
  permission denied to set parameter "session_replication_role"
  ```

  ⚠️ On CloudSQL, the `postgres` role cannot set `session_replication_role`. Always disable `disable_triggers` in this case.

## Replication

### From CloudSQL Postgres (Replication)

#### Quick Checklist

- [ ] Enable `cloudsql.logical_decoding` flag.
- [ ] Use `postgres` user for initialization.
- [ ] Grant `cloudsqlreplica` + replication privileges.
- [ ] Optionally transfer ownership to **`pgstreamsource`** for streaming.

#### Steps

1. **Enable logical replication**
   Enable the [`cloudsql.logical_decoding`](https://cloud.google.com/sql/docs/postgres/replication/configure-logical-replication) flag.

2. **Replication phases**

   - **Initialization** → requires `postgres` (superuser-like privileges).
   - **Streaming** → can switch to `pgstreamsource` (from the pgstream source URL).

#### Initialization

Initialization does the following:

- Creates `pgstream` schema
- Creates replication slot (if missing)
- Creates event triggers/functions for schema changes

Use the `postgres` role. Also grant replication privileges explicitly:

```sql
GRANT cloudsqlreplica TO postgres;
ALTER ROLE postgres REPLICATION;
```

#### Streaming

After initialization, you can:

- Keep using `postgres`, or
- Transfer ownership to a dedicated **`pgstreamsource`** role.

Example setup:

```sql
-- allow ownership transfer
GRANT pgstreamsource TO current_user;

-- transfer ownership
ALTER DATABASE db OWNER TO pgstreamsource;
ALTER SCHEMA pgstream OWNER TO pgstreamsource;
ALTER SCHEMA <schema> OWNER TO pgstreamsource;
ALTER TABLE pgstream.schema_log OWNER TO pgstreamsource;
ALTER TABLE pgstream.table_ids OWNER TO pgstreamsource;
ALTER SEQUENCE pgstream.xid_serial OWNER TO pgstreamsource;

-- replication privileges
GRANT cloudsqlreplica TO pgstreamsource;
ALTER ROLE pgstreamsource REPLICATION;
```

### To CloudSQL Postgres (Replication)

#### Quick Checklist

- [ ] Ensure **`pgstreamtarget`** has schema/database ownership.
- [ ] If combined with snapshot → follow [Snapshot target requirements](#to-cloudsql-postgres-snapshots).

#### Steps

Privileges required:

```sql
ALTER DATABASE db OWNER TO pgstreamtarget;
ALTER SCHEMA <schema> OWNER TO pgstreamtarget;
```

## Troubleshooting

| Error message                                                   | Cause                                                 | Fix                                                |
| --------------------------------------------------------------- | ----------------------------------------------------- | -------------------------------------------------- |
| `permission denied for table pg_authid`                         | Roles with passwords require pg_authid access.        | Enable `cloudsql.pg_authid_select_role` flag.      |
| `permission denied to set parameter "session_replication_role"` | CloudSQL roles can’t set replication role.            | Disable `disable_triggers`.                        |
| Role creation fails                                             | `pgstreamtarget` lacks privileges it tries to assign. | Use `postgres` or grant required privileges first. |
