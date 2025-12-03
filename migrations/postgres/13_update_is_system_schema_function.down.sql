-- Restore previous version of is_system_schema function
CREATE OR REPLACE FUNCTION pgstream.is_system_schema(schema_name text) RETURNS boolean AS $$
BEGIN
    RETURN schema_name IN ('pgstream', 'pgroll');
END;
$$ LANGUAGE plpgsql;
