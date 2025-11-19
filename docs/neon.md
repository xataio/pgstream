# Neon Database Onboarding Guide

This guide explains how to use **pgstream** with **Neon databases**, covering **snapshots** and **replication**.

👉 Throughout this guide:

- **`pgstreamsource`** refers to the user provided in the **pgstream source URL**.
- **`pgstreamtarget`** refers to the user provided in the **pgstream target URL**.

## Table of Contents

1. [Snapshots](#snapshots)

   - [From Neon Database](#from-neon-database-snapshots)
   - [To Neon Database](#to-neon-database-snapshots)

2. [Replication](#replication)

   - [From Neon Database](#from-neon-database-replication)
   - [To Neon Database](#to-neon-database-replication)

3. [Troubleshooting](#troubleshooting)

## Snapshots

### From Neon Database (Snapshots)

#### Quick Checklist

- [ ] Create a source user (**`pgstreamsource`**) with access to required schemas/tables.
- [ ] For roles without passwords → no special config needed.
- [ ] For roles with passwords → **must use the `neondb_owner` superuser**.
- [ ] Update YAML config with correct snapshot settings.

#### Steps

1. **User privileges**
   Ensure the **`pgstreamsource`** user (from the pgstream source URL) owns the database schema and tables you want to snapshot.

2. **Roles handling**

   - **No roles** → no changes required.
   - **Roles without passwords** → no changes required.
   - **Roles with passwords** → **must use the `neondb_owner` user**.

   ⚠️ Only the `neondb_owner` user has access to the `pg_authid` table required for snapshotting roles with passwords.

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
       url: "postgresql://pgstreamsource:password@<neon-host>:5432/db?sslmode=require"
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

ℹ️ Neon-managed roles (`neondb_owner`, `neon_service`, `neon_superuser` and `cloud_admin`) will not be snapshotted.

### To Neon Database (Snapshots)

#### Quick Checklist

- [ ] Create a target user (**`pgstreamtarget`**) for the pgstream target URL.
- [ ] Grant privileges for schema/database ownership.
- [ ] Grant optional privileges depending on features (create DB, create roles).
- [ ] **Use `disable_triggers: false`** (required for all non-`neon` users).

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

- **Disable triggers** (required for Neon)

  ⚠️ **Not even the pseudo-superuser `neondb_owner` user can set `session_replication_role`** on Neon databases.

  ```yaml
  target:
    postgres:
      disable_triggers: false # Required for neon target databases
  ```

  ❌ If using `disable_triggers: true`, pgstream will fail with:

  ```
  permission denied to set parameter "session_replication_role"
  ```

## Replication

### From Neon Database (Replication)

#### Quick Checklist

- [ ] Ensure logical replication is enabled on your Neon database.
- [ ] **Use the `neondb_owner` user for initialization** (only user with event trigger privileges).
- [ ] Optionally use a different **`pgstreamsource`** user for streaming.

#### Steps

1. **Enable logical replication**

   Logical replication requires `wal_level` to be set to `logical`.

   **Check current setting:**

   ```sql
   SHOW wal_level;
   ```

   **If not set to `logical`, update it:**

   Neon enables logical replication by default on most databases. If you see `wal_level = replica`, you can enable logical replication through the Neon Console:

   1. Go to your project in the [Neon Console](https://console.neon.tech/)
   2. Navigate to **Settings** → **Replication**
   3. Enable **Logical replication**
   4. No restart is required - Neon handles this automatically

   Alternatively, you can enable it via the [Neon API](https://api-docs.neon.tech/reference/updateproject) or contact [Neon support](https://neon.tech/docs/introduction/support).

2. **Replication phases**

   - **Initialization** → requires elevated privileges to create schemas, event triggers, and replication slots.
   - **Streaming** → can use a dedicated `pgstreamsource` user.

#### Initialization

Initialization does the following:

- Creates `pgstream` schema
- Creates replication slot (if missing)
- Creates event triggers/functions for schema changes

⚠️ **Only the `neondb_owner` user has sufficient privileges to create event triggers.** Use the `neondb_owner` user for initialization:

```yaml
source:
  postgres:
    url: "postgresql://neondb_owner:password@<neon-host>:5432/db?sslmode=require"
    mode: replication
```

#### Streaming

After initialization, you can:

- Keep using `neondb_owner`, or
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

### To Neon Database (Replication)

#### Quick Checklist

- [ ] Ensure **`pgstreamtarget`** has schema/database ownership.
- [ ] If combined with snapshot → follow [Snapshot target requirements](#to-neon-database-snapshots).

#### Steps

Privileges required:

```sql
ALTER DATABASE db OWNER TO pgstreamtarget;
ALTER SCHEMA <schema> OWNER TO pgstreamtarget;
```

## Troubleshooting

| Error message                                                     | Cause                                                 | Fix                                                                                                 |
| ----------------------------------------------------------------- | ----------------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| `permission denied for table pg_authid`                           | Roles with passwords require pg_authid access.        | Use `neondb_owner` user as source, or disable role passwords (`roles_snapshot_mode: no_passwords`). |
| `permission denied to set parameter "session_replication_role"`   | No available Neon users can set this parameter.       | Set `disable_triggers: false` in target config.                                                     |
| `permission denied for schema public` when transferring ownership | Target role lacks CREATE privilege on schema.         | Grant CREATE on schema to the role: `GRANT CREATE ON SCHEMA public TO role_name;`                   |
| Role creation fails                                               | `pgstreamtarget` lacks privileges it tries to assign. | Grant required privileges to `pgstreamtarget` first, or use a user with more privileges.            |
