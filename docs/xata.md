# Xata Database Onboarding Guide

This guide explains how to use **pgstream** with **Xata databases**, covering **snapshots** and **replication**.

👉 Throughout this guide:

- **`pgstreamsource`** refers to the user provided in the **pgstream source URL**.
- **`pgstreamtarget`** refers to the user provided in the **pgstream target URL**.

## Table of Contents

1. [Snapshots](#snapshots)

   - [From Xata Database](#from-xata-database-snapshots)
   - [To Xata Database](#to-xata-database-snapshots)

2. [Replication](#replication)

   - [From Xata Database](#from-xata-database-replication)
   - [To Xata Database](#to-xata-database-replication)

3. [Troubleshooting](#troubleshooting)

## Snapshots

### From Xata Database (Snapshots)

#### Quick Checklist

- [ ] Create a source user (**`pgstreamsource`**) with access to required schemas/tables.
- [ ] For roles without passwords → no special config needed.
- [ ] For roles with passwords → **must use the `xata` superuser**.
- [ ] Update YAML config with correct snapshot settings.

#### Steps

1. **User privileges**
   Ensure the **`pgstreamsource`** user (from the pgstream source URL) can access the database schema and tables you need.

2. **Roles handling**

   - **No roles** → no changes required.
   - **Roles without passwords** → no changes required.
   - **Roles with passwords** → **must use the `xata` user** as `pgstreamsource`.

     ⚠️ Only the `xata` user has access to the `pg_authid` table required for snapshotting roles with passwords.

     Without this, you'll see:

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
       url: "postgresql://pgstreamsource:password@<xata-host>:5432/db?sslmode=require"
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

ℹ️ Xata-managed roles (`xata`, `xata_superuser`) will not be snapshotted.

### To Xata Database (Snapshots)

#### Quick Checklist

- [ ] Create a target user (**`pgstreamtarget`**) for the pgstream target URL.
- [ ] Grant privileges for schema/database ownership.
- [ ] Grant optional privileges depending on features (create DB, create roles).
- [ ] **Use `disable_triggers: false`** (required for all non-`xata` users).

#### Steps

The **`pgstreamtarget`** user (from the pgstream target URL) must have the following privileges:

- **Schema ownership**

  ```sql
  ALTER DATABASE db OWNER TO pgstreamtarget;
  ALTER SCHEMA public OWNER TO pgstreamtarget;
  ```

- **Database creation** (if `create_target_db` is enabled)

  ```sql
  ALTER ROLE pgstreamtarget CREATEDB;
  ```

- **Role creation** (if `roles_snapshot_mode` is `enabled`/`no_passwords`)

  ```sql
  ALTER ROLE pgstreamtarget CREATEROLE;
  ```

  ⚠️ `pgstreamtarget` must already hold any privileges it assigns (e.g., `REPLICATION`).

- **Disable triggers** (required for Xata)

  ⚠️ **Only the `xata` user can set `session_replication_role`** on Xata databases. For all other users, you **must** disable trigger management in pgstream:

  ```yaml
  target:
    postgres:
      disable_triggers: false # Required for non-xata users
  ```

  ❌ If using a non-`xata` user with `disable_triggers: true`, pgstream will fail with:

  ```
  permission denied to set parameter "session_replication_role"
  ```

## Replication

### From Xata Database (Replication)

#### Quick Checklist

- [ ] Ensure logical replication is enabled on your Xata database.
- [ ] **Use the `xata` user for initialization** (only user with event trigger privileges).
- [ ] Optionally use a different **`pgstreamsource`** user for streaming.

#### Steps

1. **Enable logical replication**
   Make sure logical replication is enabled in your database. The `wal_level` must be set to `logical`. You can check the current setting by running the following:

   ```sql
   SHOW wal_level;

   ```

   ⚠️ Changing wal_level requires a full database restart.

2. **Replication phases**

   - **Initialization** → requires elevated privileges to create schemas, event triggers, and replication slots.
   - **Streaming** → can use a dedicated `pgstreamsource` user.

#### Initialization

Initialization does the following:

- Creates `pgstream` schema
- Creates replication slot (if missing)
- Creates event triggers/functions for schema changes

⚠️ **Only the `xata` user has sufficient privileges to create event triggers.** Use the `xata` user for initialization:

```yaml
source:
  postgres:
    url: "postgresql://xata:password@<xata-host>:5432/db?sslmode=require"
    mode: replication
```

#### Streaming

After initialization, you can:

- Keep using `xata`, or
- Transfer ownership to a dedicated **`pgstreamsource`** role.

```sql
-- Allow ownership transfer
GRANT pgstreamsource TO current_user;

-- Transfer ownership
ALTER DATABASE db OWNER TO pgstreamsource;
ALTER SCHEMA pgstream OWNER TO pgstreamsource;
ALTER SCHEMA <schema> OWNER TO pgstreamsource;
ALTER TABLE pgstream.schema_log OWNER TO pgstreamsource;
ALTER TABLE pgstream.table_ids OWNER TO pgstreamsource;
ALTER SEQUENCE pgstream.xid_serial OWNER TO pgstreamsource;

-- Replication privileges
ALTER ROLE pgstreamsource REPLICATION;
```

### To Xata Database (Replication)

#### Quick Checklist

- [ ] Ensure **`pgstreamtarget`** has schema/database ownership.
- [ ] If combined with snapshot → follow [Snapshot target requirements](#to-xata-database-snapshots).

#### Steps

Privileges required:

```sql
ALTER DATABASE db OWNER TO pgstreamtarget;
ALTER SCHEMA <schema> OWNER TO pgstreamtarget;
```

## Troubleshooting

| Error message                                                     | Cause                                                 | Fix                                                                                         |
| ----------------------------------------------------------------- | ----------------------------------------------------- | ------------------------------------------------------------------------------------------- |
| `permission denied for table pg_authid`                           | Roles with passwords require pg_authid access.        | Use `xata` user as source, or disable role passwords (`roles_snapshot_mode: no_passwords`). |
| `permission denied to set parameter "session_replication_role"`   | Only the `xata` user can set this parameter.          | Set `disable_triggers: false` in target config, or use the `xata` user as target.           |
| `permission denied for schema public` when transferring ownership | Target role lacks CREATE privilege on schema.         | Grant CREATE on schema to the role: `GRANT CREATE ON SCHEMA public TO role_name;`           |
| Role creation fails                                               | `pgstreamtarget` lacks privileges it tries to assign. | Grant required privileges to `pgstreamtarget` first, or use a user with more privileges.    |
