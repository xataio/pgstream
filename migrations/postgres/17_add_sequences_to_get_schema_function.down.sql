-- Update pgstream.get_schema function to go back to previous version without sequences
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
materialized_views AS (
    SELECT DISTINCT
        pg_namespace.nspname AS schema_name,
        pg_class.relname AS table_name,
        pg_class.oid AS table_oid,
        pg_matviews.definition AS view_definition
    FROM pg_namespace
    JOIN pg_class ON pg_namespace.oid = pg_class.relnamespace AND pg_class.relkind = 'm'
    LEFT JOIN pg_matviews ON pg_matviews.schemaname = pg_namespace.nspname
        AND pg_matviews.matviewname = pg_class.relname
    WHERE pg_namespace.nspname = schema_name
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
        NOT ( pg_attribute.attgenerated = '') AS column_generated,
        pg_attribute.attgenerated AS column_generated_kind,
        pg_attribute.attidentity AS column_identity,
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
indexes AS (
    SELECT
        t.relname AS table_name,
        i.relname AS index_name,
        i.oid AS index_oid,
        pg_get_indexdef(i.oid) AS index_def,
        ix.indisunique AS is_unique,
        array_agg(a.attname ORDER BY a.attnum) AS index_columns
    FROM pg_index ix
    JOIN pg_class t ON t.oid = ix.indrelid
    JOIN pg_class i ON i.oid = ix.indexrelid
    JOIN pg_namespace n ON n.oid = t.relnamespace
    JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
    WHERE n.nspname = schema_name
        AND t.relkind IN ('r', 'p', 'm')  -- tables, partitioned tables, and materialized views
    GROUP BY t.relname, i.relname, i.oid, ix.indisunique
),
table_constraints AS (
    SELECT
        table_oids.table_name,
        jsonb_agg(jsonb_build_object(
            'name', con.conname,
            'type', CASE con.contype WHEN 'u' THEN 'UNIQUE' WHEN 'c' THEN 'CHECK' ELSE con.contype::text END,
            'definition', pg_get_constraintdef(con.oid)
        ) ORDER BY con.conname) AS constraints
    FROM pg_constraint con
    JOIN table_oids ON con.conrelid = table_oids.table_oid
    WHERE con.contype IN ('u', 'c')
    GROUP BY table_oids.table_name
),
foreign_keys AS (
    SELECT
        table_oids.table_name,
        jsonb_agg(jsonb_build_object(
            'name', con.conname,
            'definition', pg_get_constraintdef(con.oid)
        ) ORDER BY con.conname) AS fks
    FROM pg_constraint con
    JOIN table_oids ON con.conrelid = table_oids.table_oid
    WHERE con.contype = 'f'
    GROUP BY table_oids.table_name
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
            'generated', columns.column_generated,
            'generated_kind', columns.column_generated_kind,
            'identity', columns.column_identity,
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
        ) AS primary_key_columns,
        (
            SELECT COALESCE(json_agg(jsonb_build_object(
                'name', index_name,
                'columns', index_columns,
                'unique', is_unique,
                'definition', index_def
            )), '[]'::json)
            FROM indexes
            WHERE indexes.table_name = columns.table_name
        ) AS table_indexes,
        (
            SELECT COALESCE(table_constraints.constraints, '[]'::jsonb)
            FROM table_constraints
            WHERE table_constraints.table_name = columns.table_name
        ) AS table_constraints,
        (
            SELECT COALESCE(foreign_keys.fks, '[]'::jsonb)
            FROM foreign_keys
            WHERE foreign_keys.table_name = columns.table_name
        ) AS table_foreign_keys
    FROM columns
    GROUP BY table_name, table_oid, table_pgs_id
),
mv_indexes AS (
    SELECT
        mv.table_name,
        jsonb_agg(jsonb_build_object(
            'name', indexes.index_name,
            'columns', indexes.index_columns,
            'unique', indexes.is_unique,
            'definition', indexes.index_def
        )) FILTER (WHERE indexes.index_name IS NOT NULL) AS mv_indexes
    FROM materialized_views mv
    LEFT JOIN indexes ON indexes.table_name = mv.table_name
    GROUP BY mv.table_name
),
mv_aggregated AS (
    SELECT jsonb_agg(jsonb_build_object(
        'oid', mv.table_oid,
        'name', mv.table_name,
        'definition', mv.view_definition,
        'indexes', COALESCE(mv_indexes.mv_indexes, '[]'::jsonb)
    )) AS materialized_views
    FROM materialized_views mv
    LEFT JOIN mv_indexes ON mv_indexes.table_name = mv.table_name
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
                'primary_key_columns', by_table.primary_key_columns,
                'indexes', by_table.table_indexes,
                'constraints', by_table.table_constraints,
                'foreign_keys', by_table.table_foreign_keys
            )),
            'materialized_views',
            COALESCE((SELECT materialized_views FROM mv_aggregated), '[]'::jsonb)
        ) AS v
    FROM by_table
)
SELECT v FROM as_json;
$$;
