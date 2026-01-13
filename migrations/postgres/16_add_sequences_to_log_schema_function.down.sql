-- Go back to previous version of log schema function without sequences
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

    IF tg_event = 'sql_drop' THEN
        IF tg_tag = 'DROP SCHEMA' THEN
            SELECT object_name INTO rec_schema_name FROM pg_event_trigger_dropped_objects() WHERE object_type = 'schema' LIMIT 1;
            is_system_schema := pgstream.is_system_schema(rec_schema_name);
            IF rec_schema_name IS NOT NULL AND NOT is_system_schema THEN
                SELECT COALESCE((SELECT version+1 FROM "pgstream"."schema_log" WHERE schema_name = rec_schema_name ORDER BY version DESC LIMIT 1), 1) INTO schema_version;
                INSERT INTO "pgstream"."schema_log" (version, schema_name, schema)
                VALUES (schema_version, rec_schema_name, '{"tables": null, "dropped": true}'::jsonb);
                DELETE FROM "pgstream"."schema_log" WHERE schema_name = rec_schema_name AND ((schema->'dropped')::bool IS NULL OR NOT (schema->'dropped')::bool);
                DELETE FROM "pgstream"."schema_log" WHERE (schema->'dropped')::bool AND created_at < now() - interval '7 days';
            END IF;
        ELSIF tg_tag IN ('DROP TABLE', 'DROP INDEX', 'DROP MATERIALIZED VIEW') THEN
            SELECT objid, schema_name
            INTO rec_objid, rec_schema_name
            FROM pg_event_trigger_dropped_objects()
            WHERE object_type IN ('table', 'index', 'materialized view')
            LIMIT 1;

            IF tg_tag = 'DROP TABLE' AND rec_objid IS NOT NULL THEN
                 DELETE FROM "pgstream"."table_ids" WHERE oid = rec_objid;
            END IF;

            is_system_schema := pgstream.is_system_schema(rec_schema_name);
            IF rec_schema_name IS NOT NULL AND NOT is_system_schema THEN
                SELECT COALESCE((SELECT version+1 FROM "pgstream"."schema_log" WHERE schema_name = rec_schema_name ORDER BY version DESC LIMIT 1), 1) INTO schema_version;
                INSERT INTO "pgstream"."schema_log" (version, schema_name, schema)
                VALUES (schema_version, rec_schema_name, pgstream.get_schema(rec_schema_name));
            END IF;
        END IF;
    ELSIF tg_event = 'ddl_command_end' THEN
        IF tg_tag = 'CREATE SCHEMA' THEN
            SELECT object_identity INTO rec_schema_name
            FROM pg_event_trigger_ddl_commands()
            WHERE object_type = 'schema'
              AND command_tag = 'CREATE SCHEMA'
            LIMIT 1;
        ELSIF tg_tag IN ('CREATE TABLE', 'ALTER TABLE', 'CREATE INDEX', 'CREATE UNIQUE INDEX', 'ALTER INDEX', 'SELECT INTO', 'CREATE TABLE AS', 'CREATE MATERIALIZED VIEW', 'ALTER MATERIALIZED VIEW') THEN
            SELECT schema_name INTO rec_schema_name
            FROM pg_event_trigger_ddl_commands()
            WHERE object_type IN ('table', 'table column', 'index', 'materialized view')
              AND command_tag = tg_tag
            LIMIT 1;
        END IF;

        is_system_schema := pgstream.is_system_schema(rec_schema_name);
        IF rec_schema_name IS NOT NULL AND NOT is_system_schema THEN
            SELECT COALESCE((SELECT version+1 FROM "pgstream"."schema_log" WHERE schema_name = rec_schema_name ORDER BY version DESC LIMIT 1), 1) INTO schema_version;
            INSERT INTO "pgstream"."schema_log" (version, schema_name, schema)
            VALUES (schema_version, rec_schema_name, pgstream.get_schema(rec_schema_name));
        END IF;
    END IF;
END;
$$;
