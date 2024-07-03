-- table_ids stores the mapping between the pgstream table id and the Postgres table oid
CREATE TABLE IF NOT EXISTS pgstream.table_ids (
    id pgstream.xid PRIMARY KEY DEFAULT pgstream.xid(),
    oid BIGINT NOT NULL UNIQUE
);

CREATE OR REPLACE FUNCTION pgstream.create_table_mapping(table_oid oid) RETURNS pgstream.xid
    LANGUAGE SQL
    SET search_path = pg_catalog,pg_temp
    AS $$
    INSERT INTO pgstream.table_ids (oid) VALUES (table_oid) RETURNING id;
$$;
