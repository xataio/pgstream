CREATE OR REPLACE FUNCTION pgstream.is_system_schema(schema_name text) RETURNS boolean AS $$
BEGIN
    RETURN schema_name IN ('pgstream', 'pgroll');
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pgstream.log_schema() RETURNS event_trigger
    LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = pg_catalog,pg_temp
    AS $$
DECLARE
    rec_objid oid; -- used for deletes
    rec_schema_name text;
    schema_version bigint;
    is_system_schema boolean;
BEGIN
    -- skip logging IF pgstream.skip_log is set
    IF (pg_catalog.current_setting('pgstream.skip_log', 'TRUE') = 'TRUE') THEN
        RETURN;
    END IF;

    -- One operation may contain many events; especially in the case when there's tables that depend on sequences, indices AND the like. We
    -- try AND be smart here AND grab the relevant schema name so we can log only once.

    -- If one executes a `CREATE x IF NOT EXISTS y;` AND the resource does in fact exist, we will not register any events. The `IF ... THEN ..`
    -- statements adjust for this case.
    IF tg_tag = 'DROP SCHEMA' AND tg_event = 'sql_drop' THEN
        SELECT object_name INTO rec_schema_name FROM pg_event_trigger_dropped_objects() WHERE object_type = 'schema' LIMIT 1;

        is_system_schema := pgstream.is_system_schema(rec_schema_name);

        IF rec_schema_name IS NOT NULL AND NOT is_system_schema THEN
            SELECT COALESCE((SELECT version+1 FROM "pgstream"."schema_log" WHERE schema_name = rec_schema_name ORDER BY version DESC LIMIT 1), 1) INTO schema_version;

            -- a dropped schema log entry is constant, has no tables AND has the dropped flag set to true.
            INSERT INTO "pgstream"."schema_log" (version, schema_name, schema) VALUES (schema_version, rec_schema_name, '{"tables": null, "dropped": true}'::jsonb);

            -- remove all log entries of current schema, with the exception of 'dropped' entry
            DELETE FROM "pgstream"."schema_log" WHERE schema_name = rec_schema_name AND ((schema->'dropped')::bool IS NULL OR NOT (schema->'dropped')::bool);

            -- remove old dropped entries in the table older than 7 days
            DELETE FROM "pgstream"."schema_log" WHERE (schema->'dropped')::bool AND created_at < now() - interval '7 days';
        END IF;
    elsif tg_tag = 'DROP TABLE' AND tg_event = 'sql_drop' THEN
        SELECT objid, schema_name INTO rec_objid, rec_schema_name FROM pg_event_trigger_dropped_objects() WHERE object_type = 'table' LIMIT 1;

        IF rec_objid IS NOT NULL THEN
            DELETE FROM "pgstream"."table_ids" WHERE oid = rec_objid;
        END IF;

        is_system_schema := pgstream.is_system_schema(rec_schema_name);

        IF rec_schema_name IS NOT NULL AND NOT is_system_schema THEN
            SELECT COALESCE((SELECT version+1 FROM "pgstream"."schema_log" WHERE schema_name = rec_schema_name ORDER BY version DESC LIMIT 1), 1) INTO schema_version;
            INSERT INTO "pgstream"."schema_log" (version, schema_name, schema) VALUES (schema_version, rec_schema_name, pgstream.get_schema(rec_schema_name));
        END IF;
    elsif tg_event = 'ddl_command_end' THEN
        IF tg_tag = 'CREATE SCHEMA' THEN
            SELECT object_identity INTO rec_schema_name FROM pg_event_trigger_ddl_commands() WHERE object_type = 'schema' AND command_tag = 'CREATE SCHEMA' LIMIT 1;
        elsif tg_tag = 'CREATE TABLE' THEN
            SELECT schema_name INTO rec_schema_name FROM pg_event_trigger_ddl_commands() WHERE object_type = 'table' AND command_tag = 'CREATE TABLE' LIMIT 1;
        elsif tg_tag = 'ALTER TABLE' THEN
            SELECT schema_name INTO rec_schema_name FROM pg_event_trigger_ddl_commands() WHERE object_type IN ('table', 'table column') AND command_tag = 'ALTER TABLE' LIMIT 1;
        END IF;

        is_system_schema := pgstream.is_system_schema(rec_schema_name);

        IF rec_schema_name IS NOT NULL AND NOT is_system_schema THEN
            SELECT COALESCE((SELECT version+1 FROM "pgstream"."schema_log" WHERE schema_name = rec_schema_name ORDER BY version DESC LIMIT 1), 1) INTO schema_version;
            INSERT INTO "pgstream"."schema_log" (version, schema_name, schema) VALUES (schema_version, rec_schema_name, pgstream.get_schema(rec_schema_name));
        END IF;
    END IF;
END;
$$;
