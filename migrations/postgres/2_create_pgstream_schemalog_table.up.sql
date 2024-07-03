CREATE TABLE IF NOT EXISTS pgstream.schema_log (
    id pgstream.xid PRIMARY KEY DEFAULT pgstream.xid(),
    version BIGINT NOT NULL,
    schema_name TEXT NOT NULL,
    schema JSONB NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    acked BOOLEAN NOT NULL DEFAULT FALSE
);

-- most lookups look like:
-- `SELECT id, schema FROM pgstream.schema_log WHERE schema_name = 'foo' AND NOT acked ORDER BY id DESC LIMIT 1`
CREATE INDEX IF NOT EXISTS schema_log_name_acked ON pgstream.schema_log (schema_name, acked, id);
CREATE UNIQUE INDEX IF NOT EXISTS schema_log_version_uniq ON pgstream.schema_log(schema_name, version);
