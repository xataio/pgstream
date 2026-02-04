-- Create function to emit DDL changes as logical messages for stateless schema tracking
-- This function uses pg_logical_emit_message to send DDL commands directly to the WAL stream
-- without requiring a schema_log table in the source database

-- Helper function to build table metadata (columns, primary keys, etc.)
-- Can be used for both DDL events and recovery/bootstrap scenarios
CREATE OR REPLACE FUNCTION pgstream.get_table_metadata(table_oid oid)
RETURNS jsonb
LANGUAGE SQL
STABLE
SET search_path = pg_catalog,pg_temp
AS $$
    SELECT jsonb_build_object(
        'columns', (
            SELECT jsonb_agg(
                jsonb_build_object(
                    'attnum', a.attnum,
                    'name', a.attname,
                    'type', format_type(a.atttypid, a.atttypmod),
                    'nullable', NOT a.attnotnull,
                    'default', pg_get_expr(d.adbin, d.adrelid),
                    'generated', a.attgenerated != '',
                    'identity', CASE
                        WHEN a.attidentity = 'a' THEN 'ALWAYS'
                        WHEN a.attidentity = 'd' THEN 'BY DEFAULT'
                        ELSE NULL
                    END,
                    'unique', EXISTS(
                        SELECT 1 FROM pg_index i
                        WHERE i.indrelid = table_oid
                        AND i.indisunique
                        AND ARRAY[a.attnum] <@ i.indkey::smallint[]
                    )
                ) ORDER BY a.attnum
            )
            FROM pg_attribute a
            LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
            WHERE a.attrelid = table_oid
            AND a.attnum > 0
            AND NOT a.attisdropped
        ),
        'primary_key_columns', (
            SELECT jsonb_agg(a.attname ORDER BY array_position(i.indkey, a.attnum))
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = table_oid AND i.indisprimary
        )
    );
$$;

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
        -- For DROP events, get schema name and collect all dropped objects
        SELECT schema_name INTO rec_schema_name
        FROM pg_event_trigger_dropped_objects()
        LIMIT 1;

        -- Collect only user-facing dropped objects (exclude internal PG objects)
        -- Filter by schema to exclude system objects (pg_toast, pg_catalog, etc.)
        -- Use 'original' field to exclude objects that were implicitly dropped as dependencies
        SELECT jsonb_agg(DISTINCT
            jsonb_build_object(
                'type', dropped.object_type,
                'identity', dropped.object_identity,
                'schema', dropped.schema_name,
                'oid', dropped.objid
            )
        )
        INTO affected_objects
        FROM (
            SELECT DISTINCT ON (object_type, object_identity)
                object_type, object_identity, schema_name, objid, original
            FROM pg_event_trigger_dropped_objects()
            WHERE original = true  -- Only objects explicitly named in DROP command
        ) dropped
        WHERE dropped.schema_name NOT IN ('pg_toast', 'pg_catalog', 'information_schema')
          AND dropped.schema_name NOT LIKE 'pg_temp%'
          AND dropped.schema_name NOT LIKE 'pg_toast_temp%'
          AND dropped.object_type NOT IN ('default value');

    ELSIF tg_event = 'ddl_command_end' THEN
        -- For CREATE/ALTER events, get schema name and collect all created/altered objects
        SELECT schema_name INTO rec_schema_name
        FROM pg_event_trigger_ddl_commands()
        LIMIT 1;

        -- Collect only user-facing objects (exclude internal PG objects)
        -- Filter by schema to exclude system objects
        -- For tables and table columns, include column details
        SELECT jsonb_agg(
            jsonb_build_object(
                'type', cmd.object_type,
                'identity', cmd.object_identity,
                'schema', cmd.schema_name,
                'oid', cmd.objid
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

-- Create event triggers to capture DDL events
CREATE EVENT TRIGGER pgstream_emit_ddl_drop
    ON sql_drop
    EXECUTE FUNCTION pgstream.emit_ddl();

CREATE EVENT TRIGGER pgstream_emit_ddl_command_end
    ON ddl_command_end
    EXECUTE FUNCTION pgstream.emit_ddl();
