-- Least-privilege grants for the replicare daemon role on a TARGET Postgres
-- (CLAUDE.md §12). replicare replicates DATA ONLY into pre-existing target tables
-- (you create/migrate the target schema with your own tooling — §7). It needs
-- only DML on those tables: no DDL, no ownership, no superuser.
--
-- If the StateStore lives on this same database (a common choice), also run the
-- state-store grants at the bottom.
--
--   psql -v ON_ERROR_STOP=1 \
--        -v role=replicare -v app_schema=public \
--        -f grants-target.sql "host=... dbname=warehouse user=admin"

-- 1. USAGE on each schema holding a target table.
GRANT USAGE ON SCHEMA :"app_schema" TO :"role";

-- 2. DML on the (pre-existing) target tables: the apply path upserts and deletes.
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA :"app_schema" TO :"role";

-- Inherit the same grants for future target tables (optional convenience).
ALTER DEFAULT PRIVILEGES IN SCHEMA :"app_schema"
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO :"role";

-- --- Optional: StateStore on this database ---
-- replicare's own operational state (sync progress, cursors, ownership lock)
-- lives in a dedicated `replicare_state` schema it creates and owns. If you point
-- state_store at this database, grant it the ability to create that schema:
GRANT CREATE ON DATABASE :"db" TO :"role";
--   OR pre-create it owned by the daemon role to avoid the database-level grant:
-- CREATE SCHEMA IF NOT EXISTS replicare_state AUTHORIZATION :role;
