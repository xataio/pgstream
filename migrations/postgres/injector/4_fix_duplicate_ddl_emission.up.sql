-- Fix duplicate DDL emission for ALTER TABLE DROP operations
-- For ALTER TABLE DROP COLUMN, both sql_drop and ddl_command_end triggers fire,
-- causing the same DDL statement to be emitted twice.
-- Solution: Only emit from sql_drop for true DROP commands (DROP TABLE, DROP INDEX, etc.)
-- not for ALTER commands that happen to drop something.

CREATE OR REPLACE FUNCTION pgstream.emit_ddl() RETURNS event_trigger
    LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = pg_catalog,pg_temp
AS $$
DECLARE
    rec_schema_name text;
    ddl_command text;
    ddl_payload jsonb;
    is_system_schema boolean;
    affected_objects jsonb;
    obj_record record;
BEGIN
    -- Skip if configured to skip DDL tracking
    IF (pg_catalog.current_setting('pgstream.skip_log', 'TRUE') = 'TRUE') THEN
        RETURN;
    END IF;

    -- Capture the actual DDL command being executed
    ddl_command := current_query();

    -- Determine which schema this affects and collect all affected objects
    IF tg_event = 'sql_drop' THEN
        -- For sql_drop, only emit for true DROP commands (DROP TABLE, DROP INDEX, etc.)
        -- Skip ALTER commands that happen to drop something (like ALTER TABLE DROP COLUMN)
        -- because ddl_command_end will handle those and provide better metadata
        IF tg_tag NOT LIKE 'DROP %' THEN
            RETURN;
        END IF;

        -- For DROP events, get schema name and collect all dropped objects
        SELECT schema_name INTO rec_schema_name
        FROM pg_event_trigger_dropped_objects()
        LIMIT 1;

        -- Collect only user-facing dropped objects (exclude internal PG objects)
        -- Filter by schema to exclude system objects (pg_toast, pg_catalog, etc.)
        -- Include pgstream_id for tables from table_ids mapping
        -- Use 'original' field to exclude objects that were implicitly dropped as dependencies
        SELECT jsonb_agg(DISTINCT
            jsonb_build_object(
                'type', dropped.object_type,
                'identity', dropped.object_identity,
                'schema', dropped.schema_name,
                'oid', dropped.objid,
                'pgstream_id', COALESCE(table_ids.id::text, NULL)
            )
        )
        INTO affected_objects
        FROM (
            SELECT DISTINCT ON (object_type, object_identity)
                object_type, object_identity, schema_name, objid, original
            FROM pg_event_trigger_dropped_objects()
            WHERE original = true  -- Only objects explicitly named in DROP command
        ) dropped
        LEFT JOIN pgstream.table_ids ON dropped.objid = table_ids.oid AND dropped.object_type = 'table'
        WHERE dropped.schema_name NOT IN ('pg_toast', 'pg_catalog', 'information_schema')
          AND dropped.schema_name NOT LIKE 'pg_temp%'
          AND dropped.schema_name NOT LIKE 'pg_toast_temp%'
          AND dropped.object_type NOT IN ('default value');

        -- Clean up table_ids for dropped tables
        IF tg_tag = 'DROP TABLE' THEN
            DELETE FROM "pgstream"."table_ids"
            WHERE oid IN (
                SELECT objid
                FROM pg_event_trigger_dropped_objects()
                WHERE object_type = 'table'
            );
        END IF;

    ELSIF tg_event = 'ddl_command_end' THEN
        -- For CREATE/ALTER events, get schema name and collect all created/altered objects
        SELECT schema_name INTO rec_schema_name
        FROM pg_event_trigger_ddl_commands()
        LIMIT 1;

        -- Collect only user-facing objects (exclude internal PG objects)
        -- Filter by schema to exclude system objects
        -- For tables and table columns, create or fetch pgstream_id from table_ids mapping and include column details
        SELECT jsonb_agg(
            jsonb_build_object(
                'type', cmd.object_type,
                'identity', cmd.object_identity,
                'schema', cmd.schema_name,
                'oid', cmd.objid,
                'pgstream_id', CASE
                    WHEN cmd.object_type IN ('table', 'table column') THEN
                        COALESCE(
                            table_ids.id::text,
                            pgstream.create_table_mapping(cmd.objid)::text
                        )
                    ELSE NULL
                END
            ) || CASE
                WHEN cmd.object_type IN ('table', 'table column') THEN pgstream.get_table_metadata(cmd.objid)
                ELSE '{}'::jsonb
            END
        )
        INTO affected_objects
        FROM (
            SELECT DISTINCT ON (object_type, object_identity)
                object_type, object_identity, schema_name, objid, in_extension
            FROM pg_event_trigger_ddl_commands()
        ) cmd
        LEFT JOIN pgstream.table_ids ON cmd.objid = table_ids.oid AND cmd.object_type IN ('table', 'table column')
        WHERE cmd.schema_name NOT IN ('pg_toast', 'pg_catalog', 'information_schema')
          AND cmd.schema_name NOT LIKE 'pg_temp%'
          AND cmd.schema_name NOT LIKE 'pg_toast_temp%'
          AND NOT cmd.in_extension
          AND cmd.object_type NOT IN ('default value')
          -- Exclude implicit table row types (have typrelid > 0) but keep user-defined types
          AND NOT (cmd.object_type = 'type' AND EXISTS (
              SELECT 1 FROM pg_type t
              JOIN pg_namespace n ON t.typnamespace = n.oid
              WHERE n.nspname || '.' || t.typname = cmd.object_identity
                AND t.typrelid > 0
          ));
    END IF;

    -- Skip if no schema identified or if it's a system schema
    is_system_schema := pgstream.is_system_schema(rec_schema_name);
    IF rec_schema_name IS NULL OR is_system_schema THEN
        RETURN;
    END IF;

    -- Build the JSON payload with DDL information
    -- The DDL command itself contains all the details; objects list provides metadata
    ddl_payload := jsonb_build_object(
        'ddl', ddl_command,
        'schema_name', rec_schema_name,
        'command_tag', tg_tag,
        'objects', COALESCE(affected_objects, '[]'::jsonb)
    );

    -- Emit the DDL change as a transactional logical message
    -- The 'true' parameter makes this transactional, ensuring proper ordering with data changes
    -- The prefix 'pgstream.ddl' is used to identify these messages on the consumer side
    PERFORM pg_logical_emit_message(
        true,
        'pgstream.ddl',
        ddl_payload::text
    );
END;
$$;
