-- dropping the role fails if there are multiple databases using pgstream in the Postgres server
REVOKE USAGE ON SCHEMA pgstream FROM pgstreamrole;
DROP ROLE IF EXISTS pgstreamrole;
