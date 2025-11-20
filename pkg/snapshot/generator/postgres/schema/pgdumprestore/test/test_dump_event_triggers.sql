CREATE EVENT TRIGGER pgstream_log_schema_drop_index ON sql_drop         WHEN TAG IN ('DROP INDEX')   EXECUTE FUNCTION pgstream.log_schema();
