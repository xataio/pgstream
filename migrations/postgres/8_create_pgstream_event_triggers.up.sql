CREATE EVENT TRIGGER pgstream_log_schema_create_alter_table ON ddl_command_end EXECUTE FUNCTION pgstream.log_schema();
CREATE EVENT TRIGGER pgstream_log_schema_drop_schema_table ON sql_drop WHEN tag IN ('DROP TABLE', 'DROP SCHEMA') EXECUTE FUNCTION pgstream.log_schema();
