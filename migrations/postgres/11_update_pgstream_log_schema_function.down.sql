-- Restore the dedicated index trigger and revert pgstream.log_schema()
CREATE EVENT TRIGGER pgstream_log_schema_create_alter_index
  ON ddl_command_end
  WHEN tag IN ('CREATE INDEX', 'ALTER INDEX')
  EXECUTE FUNCTION pgstream.log_schema();

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

    ELSIF tg_tag = 'DROP INDEX' AND tg_event = 'sql_drop' THEN
        SELECT schema_name INTO rec_schema_name FROM pg_event_trigger_dropped_objects() WHERE object_type = 'index' LIMIT 1;
        is_system_schema := pgstream.is_system_schema(rec_schema_name);
        IF rec_schema_name IS NOT NULL AND NOT is_system_schema THEN
            SELECT COALESCE((SELECT version+1 FROM "pgstream"."schema_log" WHERE schema_name = rec_schema_name ORDER BY version DESC LIMIT 1), 1) INTO schema_version;
            INSERT INTO "pgstream"."schema_log" (version, schema_name, schema)
            VALUES (schema_version, rec_schema_name, pgstream.get_schema(rec_schema_name));
        END IF;

    ELSIF tg_event = 'ddl_command_end' THEN
        IF tg_tag = 'CREATE SCHEMA' THEN
            SELECT object_identity INTO rec_schema_name
            FROM pg_event_trigger_ddl_commands()
            WHERE object_type = 'schema'
              AND command_tag = 'CREATE SCHEMA'
            LIMIT 1;
        ELSIF tg_tag IN ('CREATE TABLE', 'ALTER TABLE', 'CREATE INDEX', 'ALTER INDEX') THEN
            SELECT schema_name INTO rec_schema_name
            FROM pg_event_trigger_ddl_commands()
            WHERE object_type IN ('table', 'table column', 'index')
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
