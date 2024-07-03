CREATE OR REPLACE FUNCTION pgstream.refresh_schema(schema_to_refresh text) RETURNS void
    LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = pg_catalog,pg_temp
    AS $$
DECLARE
    schema_version bigint;
    is_system_schema boolean;
BEGIN
  is_system_schema := pgstream.is_system_schema(schema_to_refresh);

  IF schema_to_refresh IS NOT NULL and NOT IS_SYSTEM_SCHEMA THEN
    SELECT COUNT(*)+1 INTO schema_version
      FROM "pgstream"."schema_log"
      where schema_name = schema_to_refresh;

    INSERT INTO "pgstream"."schema_log" (version, schema_name, schema)
      VALUES (schema_version, schema_to_refresh, pgstream.get_schema(schema_to_refresh));

    RESET pgstream.skip_log;
  END IF;
END;
$$;
