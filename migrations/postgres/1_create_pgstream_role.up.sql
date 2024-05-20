-- Setup a role to own and execute pgstream triggers and functions

-- Handle exception in case pgstreamrole already exists
DO $$
    BEGIN
        CREATE ROLE pgstreamrole;
    EXCEPTION WHEN duplicate_object THEN RAISE NOTICE '%, skipping', SQLERRM USING ERRCODE = SQLSTATE;
    END
$$;

GRANT usage ON SCHEMA pgstream TO pgstreamrole;
