-- Least-privilege grants for the replicare daemon role on a SOURCE MySQL
-- (mysql-plan §MM3, CLAUDE.md §12). replicare installs trigger-based CDC (a
-- dedicated `replicare` DATABASE with per-table delta/track tables, and three
-- AFTER triggers per replicated table) and reads the replicated tables. It needs
-- NO SUPER, NO REPLICATION SLAVE, and NO binlog access.
--
-- Run as an admin (e.g. root). Replace the role/host and the replicated database
-- name(s) to match your deployment. Repeat the per-database grants for every
-- database you replicate from.

-- 1. The daemon role. Create it if it does not exist (set a real password).
CREATE USER IF NOT EXISTS 'replicare'@'%' IDENTIFIED BY 'CHANGE-ME';

-- 2. Capture machinery lives in a dedicated `replicare` database. Grant the role
--    full ownership-equivalent rights there so it can create/drop the registry,
--    delta, and track tables and run the migration runner.
--    (Or pre-create the database and GRANT ALL on it — same effect.)
GRANT ALL PRIVILEGES ON `replicare`.* TO 'replicare'@'%';

-- 3. On each replicated database: SELECT (read + re-read rows) and TRIGGER
--    (install AND remove capture — unlike Postgres, MySQL DROP TRIGGER needs only
--    the TRIGGER privilege, not table ownership, so the daemon role can fully
--    uninstall). No INSERT/UPDATE/DELETE on the user tables is required.
GRANT SELECT, TRIGGER ON `app`.* TO 'replicare'@'%';

-- 4. The migration runner and capture install use a session named lock
--    (GET_LOCK/RELEASE_LOCK) to serialize concurrent syncs sharing a source.
--    GET_LOCK needs no special privilege — nothing extra to grant.

FLUSH PRIVILEGES;

-- --- Notes (§12) ---
-- * NO SUPER / SYSTEM_VARIABLES_ADMIN: the session-variable canonicalization
--   (time_zone, sql_mode, character_set_results) is session-level and needs no
--   privilege.
-- * The capture triggers are created with DEFINER = CURRENT_USER (the daemon
--   role), so application writers fire them without needing any grant on
--   `replicare`; the trigger's INSERT into the delta table runs as the definer.
-- * `local_infile` (for the target's LOAD DATA fast path) is a server system
--   variable, not a grant — enable it on the TARGET server's config, not here.
