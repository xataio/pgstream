-- Add pg_temp schema to system schemas list
CREATE OR REPLACE FUNCTION pgstream.is_system_schema(schema_name text) RETURNS boolean AS $$
BEGIN
    RETURN schema_name IN ('pgstream', 'pgroll', 'pg_temp');
END;
$$ LANGUAGE plpgsql;
