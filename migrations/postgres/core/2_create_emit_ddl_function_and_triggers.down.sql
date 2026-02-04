-- Rollback: Remove DDL emission function and event triggers

DROP EVENT TRIGGER IF EXISTS pgstream_emit_ddl_drop;
DROP EVENT TRIGGER IF EXISTS pgstream_emit_ddl_command_end;
DROP FUNCTION IF EXISTS pgstream.emit_ddl();
