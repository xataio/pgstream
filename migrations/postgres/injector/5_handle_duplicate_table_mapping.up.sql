CREATE OR REPLACE FUNCTION pgstream.create_table_mapping(table_oid oid) RETURNS pgstream.xid
    LANGUAGE SQL
    SET search_path = pg_catalog,pg_temp
    AS $$
    INSERT INTO pgstream.table_ids (oid) VALUES (table_oid)
    ON CONFLICT (oid) DO UPDATE SET oid = EXCLUDED.oid
    RETURNING id;
$$;
