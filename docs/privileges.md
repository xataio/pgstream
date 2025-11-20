# Privileges

This document summarizes the privileges required for different pgstream operations in PostgreSQL.
It is **service-agnostic**, focusing only on what pgstream itself requires.

For cloud-provider-specific instructions and constraints, please see:

- [Xata Onboarding Guide](xata.md)
- [AWS Onboarding Guide](aws.md)
- [Google Cloud (CloudSQL) Onboarding Guide](gcp_cloudsql.md)

---

## Privileges Matrix

| Mode                                     | Role                                   | Required Privileges                                                                                                                                                                                                                                     | Notes                                                                                                        |
| ---------------------------------------- | -------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| **Snapshot (Source)**                    | `pgstreamsource`                       | - **Read-only access** to schemas/tables being snapshotted <br> - _(Optional)_ Read access to `pg_authid` (if snapshotting roles with passwords)                                                                                                        | Example grants: `GRANT pg_read_all_settings TO pgstreamsource; GRANT SELECT ON pg_authid TO pgstreamsource;` |
| **Snapshot (Target)**                    | `pgstreamtarget`                       | - Ownership of database & schemas <br> - _(Optional)_ `CREATEDB` (if creating target databases) <br> - _(Optional)_ `CREATEROLE` (if restoring roles) <br> - _(Optional)_ Ability to `SET session_replication_role` (if disabling triggers during load) | Role must already hold any privileges it assigns when creating other roles.                                  |
| **Replication (Source, Initialization)** | Initialization user (e.g., `postgres`) | - Ability to create schemas, event triggers, and functions <br> - Replication privileges (`REPLICATION`)                                                                                                                                                | Requires elevated privileges because event triggers must be created.                                         |
| **Replication (Source, Streaming)**      | `pgstreamsource`                       | - Ownership of replicated database, schemas, and pgstream objects <br> - Replication privileges (`REPLICATION`)                                                                                                                                         | Can be a more restricted role than the initialization user.                                                  |
| **Replication (Target)**                 | `pgstreamtarget`                       | - Ownership of database & schemas <br> - Ability to apply DML and DDL changes                                                                                                                                                                           | If combined with snapshot, also meet snapshot target requirements.                                           |

ℹ️ **Terminology**:

- `pgstreamsource` → user defined in the **pgstream source URL**
- `pgstreamtarget` → user defined in the **pgstream target URL**
