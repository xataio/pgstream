-- Rollback: Remove index replication support (triggers, log_schema, get_schema)

-- 1. Drop index-related event triggers
DROP EVENT TRIGGER IF EXISTS pgstream_log_schema_create_alter_index;
DROP EVENT TRIGGER IF EXISTS pgstream_log_schema_drop_index;

-- 2. Restore previous version of pgstream.log_schema() without index handling
CREATE OR REPLACE FUNCTION pgstream.log_schema() RETURNS event_trigger
    LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = pg_catalog,pg_temp
AS $$
DECLARE
    rec_objid oid;
    rec_schema_name text;
    schema_version bigint;
    is_system_schema boolean;
BEGIN
    IF (pg_catalog.current_setting('pgstream.skip_log', 'TRUE') = 'TRUE') THEN
        RETURN;
    END IF;

    IF tg_tag = 'DROP SCHEMA' AND tg_event = 'sql_drop' THEN
        SELECT object_name INTO rec_schema_name FROM pg_event_trigger_dropped_objects() WHERE object_type = 'schema' LIMIT 1;
        is_system_schema := pgstream.is_system_schema(rec_schema_name);
        IF rec_schema_name IS NOT NULL AND NOT is_system_schema THEN
            SELECT COALESCE((SELECT version+1 FROM "pgstream"."schema_log" WHERE schema_name = rec_schema_name ORDER BY version DESC LIMIT 1), 1) INTO schema_version;
            INSERT INTO "pgstream"."schema_log" (version, schema_name, schema)
            VALUES (schema_version, rec_schema_name, '{"tables": null, "dropped": true}'::jsonb);
            DELETE FROM "pgstream"."schema_log" WHERE schema_name = rec_schema_name AND ((schema->'dropped')::bool IS NULL OR NOT (schema->'dropped')::bool);
            DELETE FROM "pgstream"."schema_log" WHERE (schema->'dropped')::bool AND created_at < now() - interval '7 days';
        END IF;

    ELSIF tg_tag = 'DROP TABLE' AND tg_event = 'sql_drop' THEN
        SELECT objid, schema_name INTO rec_objid, rec_schema_name FROM pg_event_trigger_dropped_objects() WHERE object_type = 'table' LIMIT 1;
        IF rec_objid IS NOT NULL THEN
            DELETE FROM "pgstream"."table_ids" WHERE oid = rec_objid;
        END IF;
        is_system_schema := pgstream.is_system_schema(rec_schema_name);
        IF rec_schema_name IS NOT NULL AND NOT is_system_schema THEN
            SELECT COALESCE((SELECT version+1 FROM "pgstream"."schema_log" WHERE schema_name = rec_schema_name ORDER BY version DESC LIMIT 1), 1) INTO schema_version;
            INSERT INTO "pgstream"."schema_log" (version, schema_name, schema)
            VALUES (schema_version, rec_schema_name, pgstream.get_schema(rec_schema_name));
        END IF;

    ELSIF tg_event = 'ddl_command_end' THEN
        IF tg_tag IN ('CREATE SCHEMA', 'CREATE TABLE', 'ALTER TABLE') THEN
            SELECT schema_name INTO rec_schema_name FROM pg_event_trigger_ddl_commands()
            WHERE object_type IN ('schema', 'table', 'table column')
            AND command_tag = tg_tag LIMIT 1;

            is_system_schema := pgstream.is_system_schema(rec_schema_name);
            IF rec_schema_name IS NOT NULL AND NOT is_system_schema THEN
                SELECT COALESCE((SELECT version+1 FROM "pgstream"."schema_log" WHERE schema_name = rec_schema_name ORDER BY version DESC LIMIT 1), 1) INTO schema_version;
                INSERT INTO "pgstream"."schema_log" (version, schema_name, schema)
                VALUES (schema_version, rec_schema_name, pgstream.get_schema(rec_schema_name));
            END IF;
        END IF;
    END IF;
END;
$$;

-- 3. Restore previous version of pgstream.get_schema() without index definitions
CREATE OR REPLACE FUNCTION pgstream.get_schema(schema_name TEXT) RETURNS jsonb
    LANGUAGE SQL
    SET search_path = pg_catalog,pg_temp
AS $$
WITH table_oids AS (
    WITH existing_oids AS (
        SELECT DISTINCT
            pg_namespace.nspname AS schema_name,
            pg_class.relname AS table_name,
            pg_class.oid AS table_oid
        FROM pg_namespace
        JOIN pg_class ON pg_namespace.oid = pg_class.relnamespace AND pg_class.relkind IN ('r', 'p')
        WHERE pg_namespace.nspname = schema_name
    )
    SELECT
        existing_oids.schema_name,
        existing_oids.table_name,
        existing_oids.table_oid,
        coalesce(pgstream.table_ids.id, pgstream.create_table_mapping(existing_oids.table_oid)) AS table_pgs_id
    FROM existing_oids
    LEFT JOIN pgstream.table_ids ON existing_oids.table_oid = pgstream.table_ids.oid
),
columns AS (
    SELECT
        table_oids.table_name AS table_name,
        table_oids.table_oid AS table_oid,
        table_oids.table_pgs_id AS table_pgs_id,
        format('%s-%s', table_oids.table_pgs_id, pg_attribute.attnum) AS column_pgs_id,
        pg_attribute.attname AS column_name,
        format_type(pg_attribute.atttypid, pg_attribute.atttypmod) AS column_type,
        pg_get_expr(pg_attrdef.adbin, pg_attrdef.adrelid) AS column_default,
        NOT (pg_attribute.attnotnull OR pg_type.typtype = 'd' AND pg_type.typnotnull) AS column_nullable,
        (EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conrelid = pg_attribute.attrelid
            AND ARRAY[pg_attribute.attnum::int] @> conkey::int[]
            AND contype = 'u'
        ) OR EXISTS (
            SELECT 1 FROM pg_index
            JOIN pg_class ON pg_class.oid = pg_index.indexrelid
            WHERE indrelid = pg_attribute.attrelid
            AND indisunique
            AND ARRAY[pg_attribute.attnum::int] @> pg_index.indkey::int[]
        )) AS column_unique,
        pg_catalog.col_description(table_oids.table_oid, pg_attribute.attnum) AS metadata
    FROM pg_attribute
    JOIN table_oids ON pg_attribute.attrelid = table_oids.table_oid
    JOIN pg_type ON pg_attribute.atttypid = pg_type.oid
    LEFT JOIN pg_attrdef ON pg_attribute.attrelid = pg_attrdef.adrelid AND pg_attribute.attnum = pg_attrdef.adnum
    WHERE pg_attribute.attnum >= 1
      AND NOT pg_attribute.attisdropped
),
by_table AS (
    SELECT
        columns.table_name,
        columns.table_oid,
        columns.table_pgs_id AS table_pgs_id,
        jsonb_agg(jsonb_build_object(
            'pgstream_id', columns.column_pgs_id,
            'name', columns.column_name,
            'type', columns.column_type,
            'default', columns.column_default,
            'nullable', columns.column_nullable,
            'unique', columns.column_unique,
            'metadata', columns.metadata
        )) AS table_columns,
        (
            SELECT COALESCE(json_agg(pg_attribute.attname), '[]'::json)
            FROM pg_index, pg_attribute
            WHERE indrelid = columns.table_oid
              AND pg_attribute.attrelid = columns.table_oid
              AND pg_attribute.attnum = ANY(pg_index.indkey)
              AND indisprimary
        ) AS primary_key_columns
    FROM columns
    GROUP BY table_name, table_oid, table_pgs_id
),
as_json AS (
    SELECT
        jsonb_build_object(
            'tables',
            jsonb_agg(jsonb_build_object(
                'oid', by_table.table_oid,
                'pgstream_id', by_table.table_pgs_id,
                'name', by_table.table_name,
                'columns', by_table.table_columns,
                'primary_key_columns', by_table.primary_key_columns
            ))
        ) AS v
    FROM by_table
)
SELECT v FROM as_json;
$$;
