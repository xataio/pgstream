-- this function is called each time a change to a given schema is made. It will store the result of the schema change
-- which will then be replicated. The output structure is mapped in the codebase, please take care if editing.
--
-- We have the first step `with table_oids as ( ... )` in order to grab IDs that have already been generated, and
-- insert those that don't yet have IDs. It's done like this to help with performance.

-- It adds the column generated flag to the column
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
                 RIGHT JOIN pg_class ON pg_namespace.oid = pg_class.relnamespace AND pg_class.relkind IN ('r', 'p')
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
             NOT ( pg_attribute.attnotnull OR pg_type.typtype = 'd' AND pg_type.typnotnull) AS column_nullable,
			 NOT ( pg_attribute.attgenerated = '') AS column_generated,
             (EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conrelid = pg_attribute.attrelid
                AND ARRAY[pg_attribute.attnum::int] @> conkey::int[]
                AND contype = 'u'
              ) OR EXISTS (
                SELECT 1
                FROM pg_index
                JOIN pg_class ON pg_class.oid = pg_index.indexrelid
                WHERE indrelid = pg_attribute.attrelid
                AND indisunique
                AND ARRAY[pg_attribute.attnum::int] @> pg_index.indkey::int[]
             )) AS column_unique,
             pg_catalog.col_description(table_oids.table_oid,pg_attribute.attnum) AS metadata
         FROM pg_attribute
                  JOIN table_oids ON pg_attribute.attrelid = table_oids.table_oid
                  JOIN pg_type ON pg_attribute.atttypid = pg_type.oid
                  LEFT JOIN pg_attrdef ON pg_attribute.attrelid = pg_attrdef.adrelid AND pg_attribute.attnum = pg_attrdef.adnum
         WHERE pg_attribute.attnum >= 1 -- less than 1 is reserved for system resources
           AND NOT pg_attribute.attisdropped -- will be `true` if column is being dropped
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
                     'unique', columns.column_unique,
                     'metadata', columns.metadata
                 )) AS table_columns,
             (
                SELECT COALESCE(json_agg(pg_attribute.attname), '[]'::json)
                FROM pg_index, pg_attribute
                WHERE
                    indrelid = columns.table_oid AND
                    pg_attribute.attrelid = columns.table_oid AND
                    pg_attribute.attnum = any(pg_index.indkey)
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
