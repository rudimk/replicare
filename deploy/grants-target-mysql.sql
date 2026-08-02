-- Least-privilege grants for the replicare daemon role on a TARGET MySQL
-- (mysql-plan §MM8, CLAUDE.md §12). replicare writes DATA ONLY to the
-- pre-existing target tables (§7: the target schema is the user's
-- responsibility). It creates NO objects on the target except session-scoped
-- TEMPORARY staging tables, and it NEVER touches the target's indexes,
-- constraints, types, or DDL.
--
-- Run as an admin (e.g. root). Replace the role/host and the replicated database
-- name(s) to match your deployment. Repeat the per-database grants for every
-- database you replicate into.

-- 1. The daemon role. Create it if it does not exist (set a real password). If
--    the same role connects to source and target, create it once.
CREATE USER IF NOT EXISTS 'replicare'@'%' IDENTIFIED BY 'CHANGE-ME';

-- 2. On each replicated database: the four data verbs on the pre-existing target
--    tables, plus CREATE TEMPORARY TABLES for the faithful apply path (delta
--    apply and the non-empty-target merge load stage the re-read into a session
--    TEMP table, then INSERT ... ON DUPLICATE KEY UPDATE). No CREATE/ALTER/DROP
--    on the user tables is needed or granted.
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE TEMPORARY TABLES ON `warehouse`.* TO 'replicare'@'%';

-- 3. If the state store lives on this MySQL server, note that v1's ONLY
--    StateStore backend is Postgres (CLAUDE.md §9) — the daemon's operational
--    state does not live on MySQL. Nothing to grant here for state.

FLUSH PRIVILEGES;

-- --- Notes (§12) ---
-- * NO SUPER / SYSTEM_VARIABLES_ADMIN: the session-variable canonicalization on
--   the write connection (time_zone, a strict-safe sql_mode that keeps
--   STRICT_*_TABLES but permits zero-dates, NO_AUTO_VALUE_ON_ZERO) is all
--   session-level and needs no privilege.
-- * local_infile: the initial-copy fast path is LOAD DATA LOCAL INFILE. That
--   requires the TARGET server to allow local-infile loads — a server system
--   variable (`local_infile=ON`), NOT a grant. Set it in the target's config
--   (my.cnf) or via `SET GLOBAL local_infile=1`. The driver only ever streams
--   from an in-process reader, never a server file path, so `secure_file_priv`
--   does not restrict it. When local_infile is off, replicare falls back to a
--   batched multi-row INSERT (slower, correctness-equivalent).
-- * FOREIGN_KEY_CHECKS: a cyclic NOT NULL FK component is loaded/applied with a
--   session-local `SET FOREIGN_KEY_CHECKS=0` plus a mandatory pre-commit orphan
--   verification (mysql-plan §0.2). SET SESSION on this variable needs no
--   special privilege.
