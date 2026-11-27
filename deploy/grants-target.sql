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

-- --- Faster initial copy: deferred target indexes (on by default) ---
-- replicare takes each table's plain indexes down for the initial copy and builds
-- them again afterwards, which needs ownership of the table (or membership of the
-- role that owns it). With only the DML grants above, tables keep their indexes and
-- copy more slowly, with a warning; nothing fails. To let it defer them, either run
-- replicare as the role that created the target tables, or grant it the owning role
-- (pass -v owner_role=<the role that owns the tables> and uncomment):
-- GRANT :"owner_role" TO :"role";

-- --- The target's own triggers (on by default) ---
-- replicare keeps the target's own triggers from firing on its writes. With the right
-- to run its sessions in replica mode, ordinary triggers are skipped and nothing on the
-- target changes; that right comes with superuser or rds_superuser, or on Postgres 15+
-- with this grant (uncomment):
-- GRANT SET ON PARAMETER session_replication_role TO :"role";
-- Triggers that fire even in replica mode (ENABLE ALWAYS), or all of them without it,
-- are switched off while the sync runs instead, which needs ownership of their tables
-- (the owner_role grant below). A target without triggers needs neither.

-- --- Optional: StateStore on this database ---
-- replicare's own operational state (sync progress, cursors, ownership lock)
-- lives in a dedicated `replicare_state` schema it creates and owns. If you point
-- state_store at this database, grant it the ability to create that schema:
GRANT CREATE ON DATABASE :"db" TO :"role";
--   OR pre-create it owned by the daemon role to avoid the database-level grant:
-- CREATE SCHEMA IF NOT EXISTS replicare_state AUTHORIZATION :role;
