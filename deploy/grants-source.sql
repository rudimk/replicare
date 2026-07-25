-- Least-privilege grants for the replicare daemon role on a SOURCE Postgres
-- (CLAUDE.md §12). replicare installs trigger-based CDC (a dedicated `replicare`
-- schema with per-table delta/track tables and trigger functions) and reads the
-- replicated tables. It needs NO superuser, NO REPLICATION attribute, and NO
-- wal_level change.
--
-- Run as a role that owns (or can grant on) the replicated schemas/tables, e.g.:
--   psql -v ON_ERROR_STOP=1 \
--        -v role=replicare -v db=app -v app_schema=public \
--        -f grants-source.sql "host=... dbname=app user=admin"
--
-- Repeat the per-schema / per-table sections for every schema and table you
-- replicate (or grant at the schema level with ALTER DEFAULT PRIVILEGES).

-- 1. The daemon role. Create it if it does not exist (set a password out of band).
--    Only LOGIN is required; nothing else.
-- CREATE ROLE :role LOGIN PASSWORD 'CHANGE-ME';

-- 2. Capture machinery lives in a dedicated `replicare` schema. Two options:
--
--    (a) Grant CREATE on the database so replicare creates the schema itself:
GRANT CREATE ON DATABASE :"db" TO :"role";
--
--    (b) OR pre-create the schema owned by the daemon role, avoiding any
--        database-level grant (the "pre-created schema we own" option, §12).
--        The migration runner only issues CREATE SCHEMA when it is absent, so a
--        pre-created schema needs no CREATE-on-database privilege:
-- CREATE SCHEMA IF NOT EXISTS replicare AUTHORIZATION :role;

-- 3. USAGE on each schema that contains a replicated table (prerequisite to
--    referencing its tables).
GRANT USAGE ON SCHEMA :"app_schema" TO :"role";

-- 4. TRIGGER (to install capture) and SELECT (to read + re-read rows) on each
--    replicated table. TRIGGER alone is enough to CREATE TRIGGER.
GRANT SELECT, TRIGGER ON ALL TABLES IN SCHEMA :"app_schema" TO :"role";

-- Make future tables in the schema inherit the same grants (optional convenience).
ALTER DEFAULT PRIVILEGES IN SCHEMA :"app_schema" GRANT SELECT, TRIGGER ON TABLES TO :"role";

-- --- Teardown asymmetry (documented, §12) ---
-- CREATE TRIGGER needs only the TRIGGER privilege, but DROP TRIGGER (part of
-- `replicare capture remove`) requires TABLE OWNERSHIP. A least-privilege role can
-- install and run capture, but fully uninstalling the trigger needs table
-- ownership (or the owner's help). Dropping replicare's own delta/track tables and
-- functions works with the daemon role, since it owns those objects.
