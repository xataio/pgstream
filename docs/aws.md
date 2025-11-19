# AWS Database Onboarding Guide

This guide explains how to use **pgstream** with **AWS RDS/Aurora databases**, covering **snapshots** and **replication**.

👉 Throughout this guide:

- **`pgstreamsource`** refers to the user provided in the **pgstream source URL**.
- **`pgstreamtarget`** refers to the user provided in the **pgstream target URL**.

## Table of Contents

1. [Snapshots](#snapshots)

   - [From AWS RDS/Aurora Database](#from-aws-rdsaurora-database-snapshots)
   - [To AWS RDS/Aurora Database](#to-aws-rdsaurora-database-snapshots)

2. [Replication](#replication)

   - [From AWS RDS/Aurora Database](#from-aws-rdsaurora-database-replication)
   - [To AWS RDS/Aurora Database](#to-aws-rdsaurora-database-replication)

3. [Troubleshooting](#troubleshooting)

## Snapshots

### From AWS RDS/Aurora Database (Snapshots)

#### Quick Checklist

- [ ] Create a source user (**`pgstreamsource`**) with access to required schemas/tables.
- [ ] For roles without passwords → no special config needed.
- [ ] Snapshot of roles with passwords is **not supported**.
- [ ] Update YAML config with correct snapshot settings.

#### Steps

1. **User privileges**
   Ensure the **`pgstreamsource`** user (from the pgstream source URL) can access the database schema and tables you need.

2. **Roles handling**

   - **No roles** → no changes required.
   - **Roles without passwords** → no changes required.
   - **Roles with passwords** → **not supported by AWS**.

     If you try to snapshot roles with passwords, you'll see:

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
       url: "postgresql://pgstreamsource:password@<aws-host>:5432/db?sslmode=require"
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

ℹ️ AWS RDS/Aurora-managed roles (`rdsadmin`, `rds_reserved`, `rds_extension`, `rds_ad`, `rds_password`, `rds_iam`, `rds_replication`, `rds_superuser` and `postgres`) will not be snapshotted.

### To AWS RDS/Aurora Database (Snapshots)

#### Quick Checklist

- [ ] Create a target user (**`pgstreamtarget`**) for the pgstream target URL.
- [ ] Grant privileges for schema/database ownership.
- [ ] Grant optional privileges depending on features (create DB, create roles).
- [ ] **Use `disable_triggers: false`** (required for all non-`rds_superuser` users).

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

- **Disable triggers** (required for AWS RDS/Aurora)

  ⚠️ **AWS RDS/Aurora only allows setting `session_replication_role`** for `rds_superuser` roles. You **must** disable trigger management in pgstream if using a non superuser role:

  ```yaml
  target:
    postgres:
      disable_triggers: false # Required for AWS RDS/Aurora target databases when using non superuser roles
  ```

  ❌ If using `disable_triggers: true` and a role without enough privileges, pgstream will fail with:

  ```
  permission denied to set parameter "session_replication_role"
  ```

## Replication

### From AWS RDS/Aurora Database (Replication)

#### Quick Checklist

- [ ] Ensure logical replication is enabled (`rds.logical_replication=1` in parameter group).
- [ ] **Use the `postgres` user or a user with `rds_superuser`** for initialization.
- [ ] Grant `rds_replication` role for replication privileges.
- [ ] Optionally use a different **`pgstreamsource`** user for streaming.

#### Steps

1. **Enable logical replication**

   Logical replication requires `wal_level` to be set to `logical`.

   **Check current setting:**

   ```sql
   SHOW wal_level;
   ```

   **If not set to `logical`, update it:**

   AWS RDS and Aurora require modifying the database parameter group:

   1. Go to the [AWS RDS Console](https://console.aws.amazon.com/rds/)
   2. Navigate to **Parameter groups** in the left sidebar
   3. Create a new parameter group or select your existing custom parameter group
      - **Note:** You cannot modify the default parameter groups
   4. Search for and modify the `rds.logical_replication` parameter:
      - Set `rds.logical_replication` to `1` (enabled)
   5. Apply the parameter group to your RDS/Aurora instance:
      - Go to your database instance
      - Click **Modify**
      - Under **Database options**, select your custom parameter group
      - Apply the changes
   6. **Reboot your database instance** for the changes to take effect

   **Verification:**

   After the reboot, verify the setting:

   ```sql
   SHOW wal_level;  -- Should return 'logical'
   SHOW rds.logical_replication;  -- Should return 'on' or '1'
   ```

   ⚠️ **Important notes:**

   - Setting `rds.logical_replication=1` automatically sets `wal_level=logical`
   - This change **requires a database reboot**
   - Logical replication may increase storage usage due to retained WAL files
   - Ensure you have adequate monitoring for replication slot lag

2. **Replication phases**

   - **Initialization** → requires elevated privileges to create schemas, event triggers, and replication slots.
   - **Streaming** → can use a dedicated `pgstreamsource` user.

#### Initialization

Initialization does the following:

- Creates `pgstream` schema
- Creates replication slot (if missing)
- Creates event triggers/functions for schema changes

⚠️ **Use a user with `rds_superuser` privileges** (typically the `postgres` user) for initialization, as creating event triggers requires elevated privileges:

```yaml
source:
  postgres:
    url: "postgresql://postgres:password@<aws-host>:5432/db?sslmode=require"
    mode: replication
```

**Required privileges for initialization user:**

```sql
-- The postgres user typically has these by default
-- If using a different user, grant:
GRANT rds_superuser TO pgstreamsource;
GRANT rds_replication TO pgstreamsource;
```

#### Streaming

After initialization, you can:

- Keep using `postgres`, or
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

-- Replication privileges (AWS RDS/Aurora specific)
GRANT rds_replication TO pgstreamsource;
```

### To AWS RDS/Aurora Database (Replication)

#### Quick Checklist

- [ ] Ensure **`pgstreamtarget`** has schema/database ownership.
- [ ] If combined with snapshot → follow [Snapshot target requirements](#to-aws-rdsaurora-database-snapshots).

#### Steps

Privileges required:

```sql
ALTER DATABASE db OWNER TO pgstreamtarget;
ALTER SCHEMA <schema> OWNER TO pgstreamtarget;
```

## Troubleshooting

| Error message                                                     | Cause                                                                          | Fix                                                                                                                      |
| ----------------------------------------------------------------- | ------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------ |
| `permission denied for table pg_authid`                           | Roles with passwords require pg_authid access.                                 | AWS RDS doesn't allow access to `pg_authid`. Disable role passwords (`roles_snapshot_mode: disabled` or `no_passwords`). |
| `permission denied to set parameter "session_replication_role"`   | AWS RDS only allows setting this parameter when using an `rds_superuser` role. | Set `disable_triggers: false` in target config or use a role with enough privileges.                                     |
| `permission denied for schema public` when transferring ownership | Target role lacks CREATE privilege on schema.                                  | Grant CREATE on schema to the role: `GRANT CREATE ON SCHEMA public TO role_name;`                                        |
| Role creation fails                                               | `pgstreamtarget` lacks privileges it tries to assign.                          | Grant required privileges to `pgstreamtarget` first, or use a user with more privileges.                                 |
| `must be superuser to create event triggers`                      | Initialization user lacks sufficient privileges.                               | Use the `postgres` user or grant `rds_superuser`: `GRANT rds_superuser TO username;`                                     |
| `cannot set parameter "rds.logical_replication"`                  | Parameter can only be set in parameter group.                                  | Modify the parameter group via AWS Console, not via SQL.                                                                 |
