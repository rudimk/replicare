# Operating `replicare`

This page is the operator's view of running `replicare` in production. It covers what a sync goes
through from start to finish, what to check before pointing it at a production source, which knobs
to turn when it falls behind, what to watch on the source, every metric, span and log event it
emits, as well as how it behaves through outages, restarts, pauses and promotions. How it works
underneath is in [Architecture](architecture.md).

## Lifecycle of a sync

Every sync goes through the same five stages, in order:

1. Pre-flight. `replicare validate <config>`(and the daemon at startup) introspects the source
   and target and classifies every column pair. Identical or widening pairs are fine, risky / lossy
   pairs get a warning, and an incompatible or missing type blocks the sync, which then refuses to
   start. It also reports FK components, a giant-component warning, dangling FK edges, and skipped
   no-key tables. Fix anything that blocks before running.
2. Capture first. The daemon installs trigger-based CDC on the source's selected tables, so
   changes start queuing into per-table delta tables straight away.
3. Initial copy. A chunked, parallel copy of each FK component(parents first) runs while
   capture is active. It's resumable - a restart carries on from the last checkpoint, not from the
   top.
4. Cutover to streaming. Once a component is copied, it flips to continuous streaming; each pass
   re-reads the current source values of dirty keys and applies them idempotently, in FK order.
5. Purge, retention & reseed. Consumed deltas are purged, and a target that falls too far behind
   is reseeded(see below).

## Before a migration: checklist

Before pointing `replicare` at a production source, we'd recommend working through this list. Most
of the items are information to gather(sizing, traffic, quiet windows), while a few have to be true
before starting. Run the queries on the source unless stated otherwise. `replicare validate <config>`
then checks the rest(types, foreign-key loops, keys) and reports anything that would block.

### Postgres

The first four items are about sizing and traffic.

1. Total database size.
   ```sql
   SELECT pg_size_pretty(pg_database_size(current_database()));
   ```
2. Row counts and sizes per table. `row_estimate` comes from planner statistics; `-1`(or `0` on
   older versions) means the table has never been analysed, so run `ANALYZE` first for real numbers.
   ```sql
   SELECT n.nspname AS schema, c.relname AS table_name,
          c.reltuples::bigint                            AS row_estimate,
          pg_size_pretty(pg_total_relation_size(c.oid)) AS total,
          pg_size_pretty(pg_indexes_size(c.oid))        AS indexes
   FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
   WHERE c.relkind IN ('r', 'p')
     AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'replicare')
     AND n.nspname NOT LIKE 'pg_toast%'
   ORDER BY pg_total_relation_size(c.oid) DESC;
   ```
3. Write volume over 24 hours. The counters only ever go up, so run this twice, 24 hours apart,
   and subtract. Every row inserted, updated or deleted in a replicated table adds one row to
   `replicare`'s change tracking, so `inserts + updates + deletes` per day is roughly the daily
   change-tracking volume(and the work streaming has to keep up with).
   ```sql
   SELECT now() AS sampled_at,
          sum(n_tup_ins) AS inserts, sum(n_tup_upd) AS updates, sum(n_tup_del) AS deletes,
          (SELECT xact_commit + xact_rollback FROM pg_stat_database
            WHERE datname = current_database()) AS transactions
   FROM pg_stat_user_tables;
   ```
   For the busiest tables, run `SELECT relname, n_tup_ins + n_tup_upd + n_tup_del AS writes FROM
   pg_stat_user_tables ORDER BY writes DESC LIMIT 20;`.
4. Low-traffic windows(daily and weekly). Busy periods followed by lulls let streaming catch up,
   and a quiet window is the best time for the final cutover.

The rest of the items have to be true before starting.

5. Every table has a primary key or a `UNIQUE` constraint. `replicare` tracks changes by key; it
   uses the primary key, or else the first `UNIQUE` constraint, and a bare `CREATE UNIQUE INDEX`
   without a constraint doesn't count. Tables listed by this query are skipped(with a warning), not
   replicated:
   ```sql
   SELECT n.nspname AS schema, c.relname AS table_name
   FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
   WHERE c.relkind IN ('r', 'p')
     AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'replicare')
     AND n.nspname NOT LIKE 'pg_toast%'
     AND NOT EXISTS (SELECT 1 FROM pg_constraint k
                     WHERE k.conrelid = c.oid AND k.contype IN ('p', 'u'))
   ORDER BY 1, 2;
   ```
6. Extensions. List them on the source and confirm the target has every one the replicated
   tables use(column types, defaults). `replicare` never installs extensions, so a missing one
   fails pre-flight.
   ```sql
   SELECT extname, extversion FROM pg_extension ORDER BY 1;
   ```
7. Materialised views(and plain views) aren't replicated - only table data is. Create them on
   the target from the schema dump, then `REFRESH MATERIALIZED VIEW` there after the initial copy
   and again just before cutover.
   ```sql
   SELECT schemaname, matviewname FROM pg_matviews ORDER BY 1, 2;
   ```
8. Large objects aren't replicated. Their contents live in `pg_largeobject`, not in a replicated
   table; a table's `oid` column is copied, but the object it points to isn't. Anything above 0
   here needs a separate plan:
   ```sql
   SELECT count(*) FROM pg_largeobject_metadata;
   ```
9. Schema changes are frozen for the whole replication. DDL isn't replicated, so a schema change
   on the source has to be applied to the target by hand, and it can break the pipeline. During
   the initial copy there's an extra reason; an `ALTER TABLE` waits for `replicare`'s copy read of
   that table to finish, and the application's queries on that table queue behind the `ALTER`.
10. Regular vacuum and maintenance jobs. Find out what runs and when - autovacuum settings,
    scheduled `VACUUM`, and especially heavy jobs that lock whole tables(`VACUUM FULL`, `CLUSTER`,
    `pg_repack`). Plain `VACUUM` / autovacuum is fine at any time, while the heavy jobs should be
    paused during the initial copy and kept out of peak hours afterwards. See
    [Vacuum and other maintenance](#vacuum-and-other-maintenance).
    ```sql
    SHOW autovacuum;
    -- scheduled jobs, if pg_cron is installed:
    SELECT jobid, schedule, command FROM cron.job;
    -- recent vacuum activity and the tables with the most dead rows:
    SELECT relname, last_vacuum, last_autovacuum, n_dead_tup
    FROM pg_stat_user_tables ORDER BY n_dead_tup DESC LIMIT 20;
    ```
    Also ask about jobs outside the database(cron, CI pipelines, the hosting provider's
    maintenance windows).
11. No long-open transactions. Sessions left open, especially `idle in transaction` ones, stop
    vacuum from cleaning up(so `replicare`'s change-tracking tables bloat) and make trigger setup
    wait. This should normally return nothing:
    ```sql
    SELECT pid, usename, state, now() - xact_start AS open_for, left(query, 80) AS last_query
    FROM pg_stat_activity
    WHERE xact_start < now() - interval '5 minutes' AND pid <> pg_backend_pid()
    ORDER BY xact_start;
    ```
12. Versions. Run `SELECT version();` on both sides. The source can be old; the target should be
    the same major version or newer.
13. Privileges and disk. The source role needs the grants in
    [Least-privilege grants](#least-privilege-grants), and the source needs room for the
    change-tracking tables to grow with the write volume from item 3(more if a target falls
    behind, up to the retention cap).
14. Sequences at cutover. Row values are copied, but sequence counters aren't. Plan
    `replicare reseed-sequences`(or `sync_sequences: true`) before the target takes writes - see
    [Sequences & identity counters](#sequences--identity-counters-dr--promotion).

### MySQL

The numbers match the Postgres list. Postgres items 4(quiet windows), 9(schema freeze),
13(privileges and disk) and 14(sequences) apply as written; large objects(8) don't exist in MySQL,
and the version check(12) is part of item 6. Here are the MySQL versions of the rest, plus two
MySQL-only items:

- (1) Total size.
   ```sql
   SELECT ROUND(SUM(data_length + index_length) / 1024 / 1024) AS size_mb
   FROM information_schema.TABLES WHERE table_schema = '<DB_NAME>';
   ```
- (2) Row counts, sizes and storage engine per table. `table_rows` is InnoDB's estimate.
   ```sql
   SELECT table_name, engine, table_rows AS row_estimate,
          ROUND(data_length / 1024 / 1024)  AS data_mb,
          ROUND(index_length / 1024 / 1024) AS index_mb
   FROM information_schema.TABLES
   WHERE table_schema = '<DB_NAME>' AND table_type = 'BASE TABLE'
   ORDER BY data_length + index_length DESC;
   ```
   Every table has to be InnoDB, since pre-flight blocks MyISAM / MEMORY tables.
- (3) Write volume over 24 hours. These are server-wide counters(covering all the databases on the
   server); run this twice, 24 hours apart, and subtract.
   ```sql
   SHOW GLOBAL STATUS WHERE Variable_name IN
     ('Innodb_rows_inserted', 'Innodb_rows_updated', 'Innodb_rows_deleted', 'Com_commit', 'Com_rollback');
   ```
- (5) Every table has a primary key or a `UNIQUE` key(tables without one are skipped):
   ```sql
   SELECT t.table_name FROM information_schema.TABLES t
   WHERE t.table_schema = '<DB_NAME>' AND t.table_type = 'BASE TABLE'
     AND NOT EXISTS (SELECT 1 FROM information_schema.TABLE_CONSTRAINTS c
                     WHERE c.table_schema = t.table_schema AND c.table_name = t.table_name
                       AND c.constraint_type IN ('PRIMARY KEY', 'UNIQUE'));
   ```
- (6) Extensions don't exist in MySQL. Instead, check that it's MySQL 5.7 or
   newer(5.6 is best-effort only; `SELECT VERSION();` - MariaDB isn't supported). On the target, check that `local_infile` is
   `ON`(`SHOW GLOBAL VARIABLES LIKE 'local_infile';`), and that the target tables have no extra
   `UNIQUE` keys beyond the one `replicare` uses, since pre-flight blocks them(see
   [the MySQL page](mysql.md#requirements)).
- (7) Materialised views don't exist in MySQL, and plain views aren't replicated(they come over
   with the schema dump).
- (MySQL only) Existing triggers. Tables can keep their own triggers. From MySQL 5.7.2 a table may
   have several triggers per timing and event, so `replicare`'s triggers sit alongside the
   application's and both fire. Only a source older than 5.7.2(in practice, a best-effort 5.6)
   allows a single trigger per timing and event, and there a table with its own
   `AFTER INSERT/UPDATE/DELETE` trigger blocks capture. They're worth listing either way, to know
   what else runs on each write:
   ```sql
   SELECT event_object_table, trigger_name, action_timing, event_manipulation
   FROM information_schema.TRIGGERS WHERE trigger_schema = '<DB_NAME>';
   ```
- (9) The schema freeze also covers online schema-change tools(`pt-online-schema-change`,
   `gh-ost`). They rebuild a table and swap it in, and the swap leaves `replicare`'s triggers
   behind on the old table; `pt-online-schema-change` also adds triggers of its own.
- (10) Maintenance jobs(the MySQL counterpart of vacuum). MySQL has no `VACUUM`; the jobs that
    matter are table rebuilds - `OPTIMIZE TABLE`, `ALTER TABLE … FORCE` / `ENGINE=InnoDB` - run
    from cron or the event scheduler. A rebuild locks the table briefly at the start and the end.
    During the initial copy it waits for `replicare`'s copy read, and the application queues
    behind it, so pause rebuilds during the copy.
    ```sql
    SELECT event_schema, event_name, interval_value, interval_field, event_definition
    FROM information_schema.EVENTS;
    ```
- (11) No long-open transactions. They hold back InnoDB's purge(the history list grows, and
    `replicare`'s change-tracking tables bloat) and make trigger setup wait:
    ```sql
    SELECT trx_mysql_thread_id, trx_started, trx_state, LEFT(trx_query, 80) AS query
    FROM information_schema.INNODB_TRX ORDER BY trx_started;
    ```
- (MySQL only) A Postgres database for `replicare`'s state is still required for a MySQL to MySQL
    sync(see [the MySQL page](mysql.md#1-a-mysql-sync-still-needs-a-postgres-for-state)).

## Tuning knobs, per sync under `tuning:`

Each sync has its own `tuning:` block, with these knobs:

| Knob | Meaning | Default |
|---|---|---|
| `drain_interval` | Time between streaming drain passes. A longer interval coalesces more changes per PK(less source load), but raises lag. | 1s |
| `drain_batch` | The most dirty deltas applied per table, per pass. Together with `drain_interval`, this sets the per-table streaming ceiling. | 1000 |
| `apply_concurrency` | How many of a component's tables apply concurrently in each pass. `1` is sequential; anything higher spreads the work across the copy-worker pool. Raise it when several tables backlog at once. | 1 |
| `retention.max_age` | Cap on the age of the oldest unconsumed delta before a lagging target is reseeded. | 24h |
| `retention.max_bytes` | Cap on the delta table's on-disk size before a reseed. | off |
| `pool.max_source_connections` / `pool.max_target_connections` | Connection caps; the copy worker pool is sized from these. | 4 each |

The defaults favour low source pressure. For more throughput, raise the pool caps and shorten
`drain_interval`; to reduce re-read load under heavy churn, lengthen `drain_interval` instead.

### A single high-volume table that lags

Streaming applies at most `drain_batch` deltas per table, per pass, so a single table's ceiling is
roughly `drain_batch / drain_interval` rows a second(for example `1000 / 1s`, which is about 1000
rows/s). When one big table(say a high-churn `events`) falls behind while the rest stay at zero
backlog, that's the ceiling being hit. There are two levers, to be tried in this order:

1. Shorten `drain_interval`(`1s`, then `200ms`, then `100ms`). More passes a second means
   linearly more throughput, at the cost of more frequent re-read `SELECT`s on the source. It's
   the right lever when "Apply batch latency (p95)" is well under the interval(the drain is
   interval-bound).
2. Raise `drain_batch`(`1000`, then `5000` or more). That means more rows per pass. For a
   genuinely large table it's more efficient than a tiny interval - one COPY-to-staging and upsert
   of 5000 rows amortises round-trips far better than five of 1000 - and it's the right lever when
   apply latency is near or above the interval(the drain is apply-bound). The cost is a larger
   re-read and apply on each pass, so more source and target work in one go.

Watch `- rate(replicare_delta_backlog_rows[5m])` per table; a positive value means the backlog is
shrinking, so the table is catching up.

### Several tables backlogged at once

The `drain_batch` / `drain_interval` levers above are per-table ceilings on a single-threaded
drain; by default, a pass applies each component's tables one at a time. When several tables are
backlogged together, raise `apply_concurrency` instead. It applies a component's tables
concurrently, across a pool built from the copy workers(which sit idle during streaming), so the
tables no longer queue behind each other.

It's bounded by the connection caps. The pool has one primary pair plus up to
`apply_concurrency-1` worker pairs, and the worker pairs come from `pool.max_source_connections` /
`max_target_connections`, so raise those alongside it. FK order within a component is still
honoured on each pass(a child that lands before its parent hits a transient FK error and retries),
and cyclic components on MySQL, which apply as one whole-component transaction, stay sequential.

This doesn't speed up a *single* hot table; that's still down to `drain_batch` / `drain_interval`
and the target's capacity. What it removes is the queuing across tables when many of them are hot.
As always, it only helps until the target's write capacity saturates.

## Source footprint: the thing to watch

`replicare` writes delta and track tables to the source - a database we may not own - so keeping
that footprint bounded matters just as much as keeping the target up to date. The headline signals,
per target / table, are:

- `replicare_delta_backlog_rows` / `replicare_delta_backlog_bytes` - the unconsumed work.
- `replicare_delta_oldest_unconsumed_age_seconds` - how stale the queue is.
- `replicare_target_up` - reachability(1 or 0).
- `replicare_reseed_total` - forced reseeds.

A slow or unreachable target holds up the delta queue, since nothing is purged until every target
has consumed it. Left alone, that queue would keep growing on the source, so `replicare` caps
retention by age and / or size. A target over the cap is marked as needing a reseed(needs-reseed);
its consumption record is reset, the unpinned deltas are purged, and it's brought back with a fresh
full copy. This is on by default - it trades a re-copy of one target for a protected source. Watch
the `retention.cap_approaching` logs, which escalate from INFO to WARN to ERROR as the backlog nears
the cap.

> ⚠️ There's one caveat here, the `xmin` horizon. A long idle-in-transaction session(or any other
> held-back `xmin`) on the source stops autovacuum from reclaiming purged delta rows, so the delta
> table can bloat *even though* `replicare` purges it. `replicare` bounds the logical backlog
> regardless, but physical bloat under a pinned `xmin` is surfaced, not fixed; the way out is to
> hunt down the idle transaction on the source.

MySQL(InnoDB) has a direct equivalent. InnoDB doesn't shrink the tablespace on `DELETE`, and a
long-running source transaction inflates the history list, blocking purge of rows it can still
see - the same shape as the `xmin` horizon. The same headline metrics apply(the delta / track
tables live in a dedicated `replicare` database on the MySQL source), retention and reseed protect
the source in exactly the same way, and the fix is the same: end the long-running source
transaction. See [the MySQL engine page](mysql.md).

Redis has no source footprint at all. Its CDC is capture-less; `replicare` writes nothing to the
source(no delta / track tables, no `replicare` schema), so the delta-backlog metrics, retention and
`xmin`-style bloat simply don't apply. What's worth watching instead is delete-propagation latency.
Deletes are found by the periodic target-vs-source sweep, so a source `DEL` only converges after
the next sweep. `replicare_delete_reconciliation_lag_seconds`(the sweep duration) and
`replicare_deletes_reconciled_total` are the Redis-specific signals, and the cadence is tuned with
`delete_sweep_interval`. Upserts converge faster, through the rolling reconciliation scan. See
[the Redis engine page](redis.md).

## Vacuum and other maintenance

Plain `VACUUM` and autovacuum never conflict with streaming. While streaming, `replicare` only runs
ordinary statements on the source: the trigger's `INSERT` into its change-tracking table on every
application write, `SELECT`s that read those tables and re-read changed rows, and batched
`DELETE`s that clear changes every target has applied. None of these takes a lock that a plain
vacuum blocks, or that blocks a plain vacuum. The same holds on the target, where `replicare` only
inserts, updates and deletes rows.

Trigger setup is the one place a plain vacuum can get in the way, and only at startup, when a
trigger actually has to be created or replaced(a new table, or a trigger someone removed). Adding a
trigger needs a lock that a running vacuum holds. `replicare` waits at most 2 seconds for it, backs
off and retries, and after repeated failures stops with an error naming the table, rather than
holding the application up. A normal restart, with every trigger already in place, takes no lock on
application tables at all.

Heavy maintenance that locks a whole table(`VACUUM FULL`, `CLUSTER`, `pg_repack`, `ALTER TABLE`)
behaves differently depending on which table it hits:

- On an application table, it blocks `replicare`'s reads of that table just like it blocks the
  application's queries, so `replicare` adds no extra harm. During streaming, `replicare`'s reads
  are short, so the job only waits a moment for one to finish.
- On one of `replicare`'s change-tracking tables(for example during a database-wide
  `VACUUM FULL`), it briefly blocks the application's writes to the matching table, because the
  trigger can't record the change until the job finishes. These tables are small, so the pause is
  short, but it's still best kept out of peak hours.

During the initial copy, two more things apply:

- Copy reads are long. Each chunk, and each table loaded in one statement(a self-referencing table
  whose link column can't be emptied), keeps a read open for as long as it runs, and while it runs,
  vacuum can't remove rows that read might still need to see, anywhere in the database.
- An `ALTER TABLE`(or `VACUUM FULL` / `CLUSTER`) on a table being copied waits for that read to
  finish, and the application's queries on the table queue behind it. Pause heavy maintenance and
  schema migrations during the initial copy, or run them with a `lock_timeout`.

Long-open transactions are the real vacuum problem. `replicare`'s change-tracking tables get
constant inserts and deletes, and rely on autovacuum to reclaim the space. A transaction left open
on the source(typically an application session `idle in transaction`) stops vacuum from cleaning
up after it anywhere in the database, so those tables grow even though `replicare` keeps clearing
them(see [the `xmin`-horizon caveat](#source-footprint-the-thing-to-watch)). Streaming keeps
working; the cost is disk. Ending the stuck session fixes it.

MySQL has no `VACUUM`. Its equivalents are table rebuilds(`OPTIMIZE TABLE`,
`ALTER TABLE … FORCE`), which behave like the heavy jobs above, and long transactions holding back
InnoDB's purge, which behave like the open-transaction problem.

## When a target goes down

A drain against an unreachable target fires all three channels at once: an errored trace span with
backlog attributes, an `ERROR target.unreachable` log, and `replicare_target_up=0` alongside a
climbing backlog series. The daemon keeps retrying on every pass, and nothing is lost. If the target
stays down long enough to go over the retention cap, it's reseeded on recovery.

## Whole-DB vs replicated size

`replicare` emits two size figures for each endpoint, and they answer different questions:

- Whole-database size, `replicare_source_db_bytes` / `replicare_target_db_bytes`. This is
  Postgres `pg_database_size`, or a MySQL sum over `information_schema.tables`, and it covers the
  entire database the connection points at, *including* everything that isn't the replicated data.
- Replicated-data size, `replicare_source_replicated_bytes` /
  `replicare_target_replicated_bytes`. This is `SUM(pg_total_relation_size)`(heap, indexes and
  TOAST) on Postgres, or MySQL `data_length + index_length`, over just the sync's selected tables.
  Tables that don't exist at an endpoint contribute nothing, and a sync with no relational
  selection reports `0`.

The two whole-database gauges usually differ, even while the replicated-data gauges converge. On a
healthy, caught-up single-to-single Postgres sync, `source_db_bytes` is typically larger than
`target_db_bytes`, even though the data is identical. That's expected, and it isn't drift; there
are three reasons for it:

1. Trigger CDC writes to the source. `replicare`'s own `replicare` schema lives on the *source*:
   per-table delta and track tables, plus trigger functions. Under churn those delta tables are a
   high-write queue that bloats before autovacuum / purge reclaims it, and none of this exists on
   the target.
2. Unreplicated tables count too. `pg_database_size` covers the whole database, so any source table
   that isn't selected(audit logs, staging tables, keyless tables skipped for lacking a PK)
   inflates the source figure but never the target's.
3. The state store is a separate database. When the Postgres `StateStore` lives on the target
   host, it's its own database(e.g. `replicare_state`), so `pg_database_size(current_database())`
   for the target's data database doesn't count it.

The replicated-data gauges strip all of that away - same tables, same size function on both
sides - so they're the right series for a like-for-like "has the target caught up to the
source?" comparison. Watch `source_replicated_bytes` against `target_replicated_bytes` for convergence, and
`source_db_bytes`(against `source_replicated_bytes`) for the capture / bloat footprint on a source
we may not own(see [Source footprint](#source-footprint-the-thing-to-watch)).

## Metrics reference

Every metric `replicare` exposes on `/metrics`(Prometheus text format; port `9090` in the demo,
configurable under `observability:`) is listed below. These names, types and labels are the single
source of truth locked in the observability contract, so a scrape of a running daemon matches this
table exactly.

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `replicare_rows_copied_total` | counter | `sync`, `table` | Rows copied during the initial copy, counted once they're committed on the target(a retried chunk counts once). Tables linked in a loop count too; rows re-copied only to catch up with a busy source don't. Restarts from 0 with the daemon. |
| `replicare_initial_copy_rows_target` | gauge | `sync`, `table` | Estimated total rows to copy(the progress denominator), from the source's own statistics(Postgres `pg_class.reltuples`, MySQL `TABLE_ROWS`; no scan). Set at bring-up for tables this run still has to copy, and left unset for a table that's never been analysed. |
| `replicare_throughput_rows_per_second` | gauge | `sync` | Current apply / copy throughput. `0` on an idle, caught-up pass. |
| `replicare_replication_lag_seconds` | gauge | `sync`, `target`, `table`, `component` | Replication lag, the age of the oldest unconsumed delta(`0` when caught up). |
| `replicare_delta_backlog_rows` | gauge | `sync`, `target`, `table`, `component` | Unconsumed delta rows(the queue depth). |
| `replicare_delta_backlog_bytes` | gauge | `sync`, `target`, `table`, `component` | Estimated unconsumed delta bytes. |
| `replicare_delta_oldest_unconsumed_age_seconds` | gauge | `sync`, `target`, `table`, `component` | Age of the oldest unconsumed delta(how stale the queue is). |
| `replicare_delta_purged_total` | counter | `sync`, `table` | Delta rows purged after consumption. |
| `replicare_reseed_total` | counter | `sync`, `target` | Forced reseeds triggered by the retention cap. |
| `replicare_target_up` | gauge | `sync`, `target` | Target reachability(`1` = up, `0` = down). |
| `replicare_source_up` | gauge | `sync` | Source reachability(`1` = up, `0` = down). |
| `replicare_source_db_bytes` | gauge | `sync` | Source whole-database size in bytes(Postgres `pg_database_size`, MySQL `information_schema`; unset for engines that don't report it). Includes `replicare`'s own capture schema, delta bloat and any unreplicated tables; see [Whole-DB vs replicated size](#whole-db-vs-replicated-size). |
| `replicare_target_db_bytes` | gauge | `sync`, `target` | Target whole-database size in bytes(same engine support). |
| `replicare_source_replicated_bytes` | gauge | `sync` | Size in bytes of just the replicated tables on the source(`SUM(pg_total_relation_size)` / MySQL `data_length+index_length` over the selection). The like-for-like source / target figure; see [Whole-DB vs replicated size](#whole-db-vs-replicated-size). |
| `replicare_target_replicated_bytes` | gauge | `sync`, `target` | Size in bytes of just the replicated tables on the target(same engine support). |
| `replicare_apply_batch_seconds` | histogram | `sync`, `target` | Apply-batch duration per drain pass. |
| `replicare_errors_total` | counter | `sync`, `category` | Errors by category(e.g. `drain`, `retention`). |
| `replicare_table_phase_info` | gauge | `sync`, `table`, `phase` | Table lifecycle phase as an info gauge, with value `1` on the active `phase` label(`initial_copy` / `streaming`) and exactly one series per table. It's present from bring-up, so it's the series to key dashboards on. |
| `replicare_delete_reconciliation_lag_seconds` | gauge | `sync`, `target`, `table` | Redis only. Duration of the last completed delete sweep(how stale capture-less deletes can be). Unset for Postgres / MySQL. |
| `replicare_deletes_reconciled_total` | counter | `sync`, `target`, `table` | Redis only. Keys `DEL`ed by the target-vs-source delete sweep. Unset for Postgres / MySQL. |

The per-table backlog / lag gauges also carry a `component` label, set to the table's FK connected
component(its first member), so lag and backlog can be rolled up per
component(`max by (component) (replicare_replication_lag_seconds)`).

The headline signals to watch are `replicare_source_up`, `replicare_target_up`,
`replicare_delta_backlog_rows`, `replicare_delta_oldest_unconsumed_age_seconds` and
`replicare_replication_lag_seconds`. Together, they answer "is the target healthy, and how far
behind is it?" - see [Source footprint](#source-footprint-the-thing-to-watch) and
[When a target goes down](#when-a-target-goes-down).

A ready-to-import Grafana dashboard over these metrics(per-table backlog / lag, catch-up,
initial-copy progress, throughput and errors, with a configurable Prometheus data source) lives at
[Grafana dashboard](grafana-dashboard.md).

### OpenTelemetry traces

Traces are exported over OTLP/gRPC when `observability.otlp_endpoint` is set. The span names come
from a fixed registry:

| Span | Covers |
|---|---|
| `replicare.initial_copy.chunk` | one chunk of the initial copy |
| `replicare.capture.install` | installing trigger capture on the source |
| `replicare.delta.drain` | one streaming drain pass for a table / target |
| `replicare.apply.component` | applying one FK component's pass on the target |
| `replicare.reseed` | a forced / retention reseed |
| `replicare.preflight` | the start-up compatibility check |

A drain / apply span against an unreachable target is recorded with error status, plus the backlog /
age attributes, so the stall and its blast radius are visible in the trace timeline.

### Structured logs with slog

Logs are JSON or text(`logging.format`) and levelled(`logging.level`), with a stable `event`
field to grep / alert on:

| Event | Level | Meaning |
|---|---|---|
| `capture.installed` / `capture.removed` | INFO | capture lifecycle |
| `table.cutover_to_streaming` | INFO | a table finished its initial copy |
| `retention.cap_approaching` | INFO, then WARN, then ERROR | delta backlog nearing the retention cap(escalates as it gets closer) |
| `target.unreachable` / `target.recovered` | ERROR / INFO | target connectivity, cross-channel with the `target_up` gauge and the errored span |
| `target.needs_reseed` | ERROR | a target was sacrificed to the retention cap and will re-copy |
| `preflight.blocked` | ERROR | a sync refused to start on a blocking incompatibility |

A degrading target shows up in metrics, traces *and* logs at once - never in just one channel.

## The status HTTP API

The daemon serves an operator HTTP surface, bound to `observability.status_addr`(e.g. `:8080`):

- `GET /status` - a JSON snapshot per sync / target / table, read from the state store: the
  phase(initial_copy / streaming), rows copied against the total, replication lag, and the last error.
- `GET /healthz` - a liveness probe(200 when the daemon is up), for Kubernetes / load balancers.
- `GET /metrics` - the Prometheus scrape. It's served here too, and additionally on
  `observability.metrics_addr` when that's set to a different address(the conventional split).

The CLI's `replicare status` reads the same state-store snapshot and adds live signals on top,
covered in the next section.

## Checking progress from the CLI, no Grafana required

Sometimes Grafana can't be deployed next to `replicare` - for example, when the daemon runs inside
the source cluster because that's the only network path to the source databases. In that case the
CLI is the dashboard. Two read-only commands answer "what is it doing, and has it caught up?"
directly against the config, with no metrics stack:

```sh
replicare status config.yml            # phase, lag, LIVE row counts + delta backlog
replicare status config.yml --watch 5s # refresh in place until Ctrl-C
replicare verify config.yml            # source<->target convergence spot-check (exit 0 = converged)
```

`status` is [live by default](cli.md#status-config---json---sync-name---no-live---watch-dur). Each
sync prints a one-line health headline(phase · last healthy pass · last data applied + rows ·
backlog) over a per-(table, target) grid. Two separate time signals keep a caught-up sync legible:
`SEEN`, the time since the last healthy pass(a liveness heartbeat, small while it's running), and
`LAST_SYNC` / `ROWS`, which say when data was last applied to that table and how many rows. A
healthy idle sync shows a recent `SEEN` with a growing `LAST_SYNC`; that's normal, not lag. Live
mode adds `SRC_ROWS` / `TGT_ROWS` and the per-target delta `BACKLOG`, where `0` means caught up,
`N (age)` means waiting, and `-` means unknown(it couldn't be read, and a `live: partial` note says
why). An unknown backlog is never conflated with `0`. `--no-live` drops the source / target
connections, but keeps the state-store signals(phase, `SEEN`, `LAST_SYNC`, reseed).

`verify` is a [read-only convergence check](cli.md#verify-config---json---sync-name---watch-dur).
It counts and content-fingerprints every replicated unit on both ends, reports `ok` /
`drift-count` / `drift-checksum` per table, and exits non-zero on drift. It's the same
count-and-checksum convergence logic the load-test harnesses use, folded into the shipped binary.
The fingerprint is by value content, including for Redis, where it hashes each key's type and
canonical logical value(not just which keys exist), so a changed value on an existing key is
caught, not only missing / extra keys. A paused sync(`enabled: false`) is tagged `[PAUSED]` in the
output, since convergence of a static source and target says nothing about whether the pipeline is
running.

> ⚠️ Redis TTL is excluded from the fingerprint; it's replicated as a relative value, so the exact
> time remaining differs by design. Stream consumer groups / PEL aren't deep-verified yet either.

Both commands take `--json`(for scripting) and `--watch <dur>`(repeat on an interval). Both are
read-only and install nothing, so they're safe to run against a source we may not own.

Running the CLI inside the pod needs no shell. The container image is minimal, so it's best not to
rely on a shell being there; invoke the binary directly instead, which `kubectl exec` does without
any shell:

```sh
kubectl exec <pod> -- /usr/local/bin/replicare status /etc/replicare/config.yml
kubectl exec <pod> -- /usr/local/bin/replicare verify /etc/replicare/config.yml
```

The runtime image is [Chainguard `wolfi-base`](https://github.com/rudimk/replicare/blob/main/Dockerfile),
which *does* include `/bin/sh` for an interactive session(`kubectl exec -it <pod> -- /bin/sh`), but
the direct-exec form above is all that's needed for a progress check.

## Running multiple pipelines in one instance

One `replicare` config, and one daemon process, can run several syncs across different engines at
once. The single-engine rule applies per sync, not per config: each sync's source and targets must
share an engine, but a config may hold a Postgres sync and a Redis sync side by side, and the daemon
runs them concurrently(each under its own single-active ownership lock).

A complete, annotated version of the example below is at
[`../examples/postgres-and-redis.yml`](https://github.com/rudimk/replicare/blob/main/examples/postgres-and-redis.yml).

```yaml
state_store:                     # ALWAYS Postgres — even for a Redis-only deployment
  engine: postgres
  postgres: { host: pg-state, port: 5432, database: replicare_state, user: replicare, password: ${STATE_PW} }
sources:
  pg-src:    { engine: postgres, postgres: { host: pg-source,  port: 5432, database: app, user: replicare, password: ${PG_PW} } }
  redis-src: { engine: redis,    redis:    { mode: standalone, nodes: [redis-source:6379] } }
targets:
  pg-dst:    { engine: postgres, postgres: { host: pg-target,  port: 5432, database: app, user: replicare, password: ${PG_TGT_PW} } }
  redis-dst: { engine: redis,    redis:    { mode: standalone, nodes: [redis-target:6379] } }
syncs:
  - name: pg-pipeline
    source: pg-src
    targets: [pg-dst]
    include: ["public.*"]
  - name: redis-pipeline
    source: redis-src
    targets: [redis-dst]
    include: ["*"]
```

`replicare run config.yml` streams both. `replicare status config.yml` shows both: the Postgres
sync with row counts and delta backlog, and the Redis sync with key counts and a `-` backlog, since
Redis has no source-side queue. `replicare verify config.yml` checks both, and any command can be
scoped to one of them with `--sync pg-pipeline`.

> ⚠️ The state store is always Postgres(the only backend in v1), even for a Redis to Redis pipeline
> where no data touches Postgres. That's a documented requirement, not a bug; see
> [redis-statestore.md](redis-statestore.md). Without a Postgres to spare, it's the one piece of
> infrastructure a Redis-only deployment still needs.

### Pausing one pipeline

Setting `enabled: false` on a sync pauses just that pipeline while the others keep running - for
example, to work on the Postgres pipeline above while Redis keeps streaming:

```yaml
syncs:
  - name: pg-pipeline
    source: pg-src
    targets: [pg-dst]
    enabled: false            # paused; skipped at startup, no ownership lock taken
    include: ["public.*"]
  - name: redis-pipeline      # still runs
    source: redis-src
    targets: [redis-dst]
    include: ["*"]
```

The pause is data-loss-free. The paused sync's source capture stays installed, so deltas keep
queuing, and setting `enabled: true` again(then restarting) drains the backlog. It's start-time
only, though; pausing and unpausing are a config change plus a daemon restart, not a live command.
The full semantics are in [Pausing a sync](configuration.md#pausing-a-sync-enabled), in the
configuration reference. On Kubernetes, flip it with `helm upgrade`(see
[kubernetes.md](kubernetes.md#configuring-the-daemon)).

> ⚠️ Since a pause isn't a live toggle, a long one lets the source-side delta tables grow and
> suspends the [source-footprint protection](#source-footprint-the-thing-to-watch), because
> retention runs in the streaming loop, which is skipped. Pause for maintenance / cutover, not
> indefinitely.

## Forcing a reseed

To re-copy a target from scratch(e.g. after out-of-band divergence), run:

```sh
replicare reseed <config> --sync <name> --target <name>
```

This flags the target in the state store, and the running daemon re-copies it on its next pass,
then resumes streaming. The command signals the owning daemon rather than acting directly, which
honours single-active ownership.

## Sequences & identity counters: DR / promotion

`replicare` replicates row values faithfully; the integer `id` a sequence produced lands on the
target verbatim(`OVERRIDING SYSTEM VALUE` on Postgres, the explicit value on MySQL). What it
doesn't replicate is the sequence object's counter(Postgres `last_value`, MySQL's per-table
`AUTO_INCREMENT`), because that's schema-object state, and `replicare` is data-only.

For a passive replica, this is harmless. The target never calls `nextval()` for a replicated
table(the applier always supplies explicit ids), so the stale counter just sits unused. It only
bites once the target starts generating ids, which means a DR promotion / failover: the app
inserts, `nextval()` returns a value at or near 1, and it collides with the replicated rows at id
`1..N`.

Before opening a promoted node to application writes, advance its counters past the data:

```sh
replicare reseed-sequences <config> --sync <name> [--target <name>] [--dry-run]
```

For every replicated table, this sets the owned identity / serial / `AUTO_INCREMENT` counter to
`max(id)+1` on that target(`--dry-run` reports what it *would* set, and writes nothing). Tables
with no locally-allocated counter(UUID / natural / composite PK) are skipped. A few things to know
about it:

- Order matters. Run it after replication into the node has stopped, and before the app can
  write; a write in that gap could still grab a colliding id.
- It uses `max(id)+1`, not the source's `last_value`. The promoted node's data *is* the replicated
  rows, so `max(id)+1` can't collide; any ids the source allocated beyond its max(gaps, or rows
  lost in the DR window) belong to rows that aren't present, so re-issuing them is harmless.
- It needs an extra privilege, a grant beyond the DML set: Postgres `UPDATE` on the sequence,
  MySQL `ALTER` on the table(see [Least-privilege grants](#least-privilege-grants)). A missing
  grant fails loudly. The command only reads the target's own `max(id)`, and never touches the
  source.

> ⚠️ `reseed-sequences` is for passive / one-way / DR setups only. It refuses a target that's an
> active-active cluster member, since there the same `max(id)+1` would collapse every node onto the
> same counter and silently drop rows via last-write-wins. Active-active id allocation is a schema
> concern, so use globally-unique keys(UUID v7 / ULID) instead; see
> [active-active, Rule 1](active-active.md#rule-1-use-globally-unique-ids). (This isn't the same as
> `reseed`, which re-copies a target's data.)

### Keeping counters warm continuously: `sync_sequences`

The promotion-time `reseed-sequences` run above is the guarantee. To keep the counters advanced
*during* streaming as well, so a failover has nothing sequence-related left to do, opt the sync
into continuous sequence syncing on the neutral config layer:

```yaml
syncs:
  - name: app-to-dr
    source: primary
    targets: [dr]
    sync_sequences: true      # optional; default false
```

While streaming, the daemon advances each replicated table's owned counter on the target to
`max(id)+1` on a lazy interval(the same mechanism as `reseed-sequences`). Since a passive
replica's applier always supplies explicit ids(`OVERRIDING SYSTEM VALUE`), which never advance
the sequence, setting it over and over is idempotent and simply keeps the counter ahead of the data.

- It's opt-in, and off by default. Unset or `false` behaves exactly as it did before.
- It makes the privilege steady-state. The daemon's *target* connection needs the same DR-only
  grant the command needs, now continuously: Postgres `UPDATE` on each owned sequence, MySQL
  `ALTER` on each table. Without it the advance fails, but only on a best-effort basis; the
  failure is logged(`stream.pass_error` with a `sequence sync` message) and retried on the next
  interval, and it never aborts the streaming pass or stops data replication.
- It's still passive / one-way / DR only. The same mesh refusal as the command applies, enforced
  at config load; `sync_sequences: true` on a sync whose target is also an active-active cluster
  member is rejected by `validate` / `run` up front, since advancing a mesh counter is silent data
  loss.
- It's not a substitute for the runbook. It narrows the promotion gap, but the cutover order(stop
  replication, then open to writes) doesn't change. Run `reseed-sequences` as the final
  belt-and-braces step for certainty at the instant of promotion.

## Runtime type errors

If the target rejects a value(a type or constraint error), `replicare` halts the affected
component loudly(logs and telemetry) rather than skipping or guessing. The offending row stays
dirty, and re-applies automatically once the cause is fixed(e.g. a missing target type /
extension, or a schema mismatch).

## Restarts & ownership

Each sync runs under a single-active `pg_advisory_lock`; a second daemon pointed at the same sync
stands by. State(copy progress, cursors) is checkpointed in the Postgres state store, which is
always Postgres regardless of the data engine, so MySQL syncs restart and resume through the same
mechanism. A restarted or rescheduled process resumes cleanly from the last checkpoint, and an
ungraceful kill replays at-least-once and converges idempotently.

One daemon can run Postgres, MySQL and Redis syncs side by side(each sync stays single-engine). A
Redis sync only checkpoints a coarse phase(snapshot-complete per unit, with no resumable SCAN
cursor), so a crash mid-copy re-runs the whole `SCAN` idempotently rather than resuming a chunk;
see [the Redis engine page](redis.md).

## Connection resilience & the liveness probe

A dropped source or target connection - an RDS / managed failover, an idle NAT / LB reap, a brief
network blip - recovers on its own, with no restart needed. All connections set a bounded dial
timeout and TCP keepalive, so a dead peer surfaces as an error quickly instead of hanging. After any
failed drain pass, the daemon health-checks both endpoints and reconnects whichever is down;
Postgres re-dials its single connection and re-applies session GUCs, while MySQL and Redis rebuild
their pooled clients. A reconnect that can't succeed yet, because the endpoint is genuinely down,
is retried on every pass, so the daemon never crash-loops on an outage - it heals when the endpoint
returns. Recovery shows up as a `stream.reconnected` event in `/status` and the logs.

`/healthz` reports streaming liveness, as a backstop for a true wedge(e.g. a hung query that no
timeout caught). It's deliberately *not* target reachability; a loop that keeps cycling, including
one correctly retrying against a down target, stays healthy, since that state shows up in
`replicare_target_up` and the backlog metrics instead. The probe only fails when a streaming loop
completes no pass within `observability.stall_timeout`(default `2m`), so Kubernetes restarts a pod
whose loop has stopped iterating. Set `stall_timeout` above the slowest expected drain pass(a large
`drain_batch` on a slow target lengthens a pass); a negative value disables the check. Liveness
tracking only starts once a sync reaches streaming, so a long initial copy is never mistaken for a
wedge.

## Least-privilege grants

For Postgres, see
[`../deploy/grants-source.sql`](https://github.com/rudimk/replicare/blob/main/deploy/grants-source.sql) and
[`../deploy/grants-target.sql`](https://github.com/rudimk/replicare/blob/main/deploy/grants-target.sql).
Note the documented asymmetry: installing capture needs only the `TRIGGER` privilege, but
`capture remove`(dropping the trigger) requires table ownership. For the true zero-database-grant
path, pre-create the `replicare` capture schema(and, if the state store lives there, the
`replicare_state` schema) owned by the daemon role; the migration runner then issues no
`CREATE SCHEMA` and needs no `CREATE`-on-database privilege. See the
[Postgres engine page](postgres.md#least-privilege).

For MySQL, use
[`../deploy/grants-source-mysql.sql`](https://github.com/rudimk/replicare/blob/main/deploy/grants-source-mysql.sql) and
[`../deploy/grants-target-mysql.sql`](https://github.com/rudimk/replicare/blob/main/deploy/grants-target-mysql.sql).
MySQL has no such asymmetry; the `TRIGGER` privilege covers both `CREATE TRIGGER` and
`DROP TRIGGER`, so the daemon role can fully uninstall capture on its own.

For Redis, use the ACL presets
[`../deploy/acl-source-redis.txt`](https://github.com/rudimk/replicare/blob/main/deploy/acl-source-redis.txt) and
[`../deploy/acl-target-redis.txt`](https://github.com/rudimk/replicare/blob/main/deploy/acl-target-redis.txt).
The source role is read-only(`+ping +info +scan +type +dump +pttl +exists +memory|usage`), while
the target role needs `+ping +info +restore +del +scan +exists +module|list`. Both roles need
`+info`(pre-flight reads `INFO server`), and in cluster mode both also need
`+cluster|shards +cluster|slots`, with the same user granted on every master. See
[the Redis engine page](redis.md).

> ⚠️ Watch out for `RESTORE`. It's `@dangerous`, and has to be granted explicitly with `+restore`,
> or every apply fails loudly.

`reseed-sequences` needs one extra target grant beyond the DML set, and only on the node it's run
against(a DR promotion step): Postgres `UPDATE` on each replicated table's owned sequence(or
ownership of it), and MySQL `ALTER` on each replicated table. It only reads the target's own
`max(id)`, so it needs no source privilege. Grant it as part of the failover runbook, not the
steady-state role - unless `sync_sequences: true` is on, in which case the daemon needs it all the
time(see [Keeping counters warm continuously](#keeping-counters-warm-continuously-sync_sequences)).

## Load & convergence testing

Three manual load-and-verify harnesses drive `replicare` against broad, high-volume, realistic
data, and assert that the target converges with the source under continuous churn. They're
developer / test tools, run from a checkout(`go run ./test/…`), and *not* part of the shipped
binary:

- Postgres - [`test/loadgen`](load-testing-postgres.md). It seeds a rich 10-table schema chosen to
  exercise the hard paths(FK components, composite / UUID / text PKs, PK-changing `SKU` renames,
  `GENERATED` columns, a keyless table it must skip, and optional FK cycles under `--cyclic`),
  churns random inserts / updates / deletes, and `verify`s a per-table row count and ordered
  content checksum.
- MySQL - [`test/loadgen-mysql`](load-testing-mysql.md). The MySQL sibling of the Postgres harness:
  the same 10-table schema in MySQL types(`AUTO_INCREMENT`, `JSON`, a composite `VARCHAR` PK,
  `GENERATED … STORED`, a keyless table), generated with MySQL-5.7-safe SQL(no `generate_series` /
  CTE / window functions), churned the same way, and `verify`d with a cross-version,
  order-independent content checksum(`BIT_XOR`-folded per-row hashes).
- Redis - [`test/loadgen-redis`](load-testing-redis.md). It seeds every value type(string / hash /
  list / set / zset / stream, plus big and TTL'd keys) and a source-only `lgskip:*` cohort the sync
  must exclude, then churns value mutations, `RENAME`s and heavy `DEL`s(the delete-reconciliation
  stress), and `verify`s a version-independent, type-aware content hash plus TTL presence. It
  covers standalone, sentinel and cluster.

All three support active-active load testing via `--node-id`(each writer node claims a disjoint
key slice, so a mesh converges to the clean union); `--node-id 0` is the default
single-writer(active-passive) behaviour. Each README documents the full end-to-end rig(two databases, a Postgres
state store, a config, the daemon, and a churn-and-verify loop) and the active-active workflow.
`task loadgen:*`, `task loadgen-mysql:*` and `task loadgen-redis:*` are the shortcuts. These are
separate from the automated Go suite(`task test:integration`).
