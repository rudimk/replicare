# Changelog

All notable changes to replicare are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/), and the project aims to follow
[Semantic Versioning](https://semver.org/).

## [Unreleased]

- **Fixed: the catch-up ETA read "not catching up" for about 10 minutes after a start.** The
  first reading after a start fills in each table's backlog in turn, which on hundreds of tables
  takes minutes, and the ETA took a sample after each table, so its history began with an almost
  empty backlog that seemed to grow. It now takes one sample per complete reading of every table.

## [0.8.4]

- **The backlog, lag and catch-up ETA show from the start, whatever the passes are doing.** They
  used to come from the end of each pass, on the streaming connections: a long pass left the
  dashboard blank, and after a restart the first reading could fail on a connection dropped during
  start-up. A monitor now reads them every 30 seconds(or less often on a large schema) on a source
  and a target connection of its own, along with the database sizes, and marks the target caught up.
- **Streaming no longer reads every change queue in full on every pass.** Each pass counted every
  table's waiting changes twice, once for retention and once for the dashboard, which on a deep
  queue meant reading all of it(918,000 changes for one table in production) on a busy disk. The
  oldest waiting change is now found from the front of the queue, and a queue of 100,000 changes or
  more is counted from Postgres's own running statistics instead of row by row. The lag stays exact.
- **Independent groups of tables apply side by side.** With `apply_concurrency` above 1, groups of
  linked tables now run in parallel across the connection pool, instead of one after another with
  only the tables inside a group in parallel; a group of several tables still takes the free pairs
  for its own tables. One big table no longer waits behind every other group. With the default of
  1, the order is unchanged.
- **A progress line every minute, during a pass too.** `stream.progress` was only written when a
  pass finished, so a long pass made the logs go silent. It's now written every minute with the rows
  applied so far(counted as each table commits) and how long the current pass has run.
- **Fixed: a slow size or estimate query could close the streaming connection.** These had a time
  limit in `replicare`, and the database driver gives up on a query past its limit by closing the
  connection, which the next pass then failed on. On Postgres the limit is now kept by the server,
  which cancels just the statement. Both connections are also checked, and opened again if needed,
  before the first pass.
- **Fixed: the check after catch-up could break the streaming connection.** When the check found
  rows that differ, it queued them for streaming to write again on the connection streaming itself
  was using, at the same moment as a pass. Both then failed(`conn busy`) and the connection could be
  left closed. The check now queues them on a connection of its own.

## [0.8.3]

- **Fixed: a large `drain_batch` broke streaming.** To apply a batch of changes, `replicare`
  read the changed rows from the source, and deleted the removed ones on the target, with one
  statement listing every key of the batch. With a few thousand keys made of several columns that
  statement got too big for the database: Postgres refused it with `stack depth limit exceeded`
  (around 10,000 three-column keys) and MySQL with `Prepared statement contains too many
  placeholders`, so every pass failed on that table. Every statement over a list of keys or tables
  now lists at most 500, on both engines (re-reads, target deletes, the record of applied changes,
  `verify`'s look-ups, size and row estimates), so any `drain_batch` is safe. Tested with
  batches of 40,000 changes on a three-column key.
- **Faster deletes on MySQL with large batches.** Deleting the rows the source had removed
  compared each one with the whole batch, so the time grew with the square of the batch: 30,000
  changes took 94 seconds. The staging table now has an index on the key, and the same batch takes
  2 seconds.
- **One failing group of tables no longer holds up the others.** When a group failed, for a
  value the target refused or a statement the server couldn't run, the pass stopped there and every
  group after it waited too. With both databases up, the other groups now carry on, and the pass's
  `stream.pass_error` names the groups that failed. A database that's down still stops the pass at
  once, as before.
- **The replication lag shows from the start, and while passes fail.** The backlog and lag were
  only published after a pass succeeded, so after a restart the dashboard was blank until the first
  pass finished, and a sync whose passes all failed showed no lag at all. They're now published
  when streaming starts and after every pass, failed or not.
- **Quieter start-up.** Every restart logged a `table.cutover_to_streaming` line for every group
  already streaming, hundreds of identical lines on a large sync. Only a group that actually
  switches is logged now, and the line names it.

## [0.8.2]

- **Large tables whose rows aren't stored in key order are copied in storage order.** A first copy
  reads each table one key range at a time. When the rows of a table are scattered on disk relative
  to its key, every row of a range is a separate disk read on the source: a big table then copies at
  a few hundred rows a second, for days or weeks, and the application's queries wait behind the
  source's disk. On a Postgres 14+ source, a table of 1 GiB or more whose key's first column has a
  correlation below 0.9(from `pg_stats`) is now read a run of disk blocks at a time instead, so the
  source reads it sequentially. It also skips the read through the key column that plans key
  ranges. Rows that move while the copy runs are caught by capture as before, a row seen twice is
  merged, and a `VACUUM FULL`, `CLUSTER`, `pg_repack` or `TRUNCATE` of
  the table starts its copy again(`copy.storage_rewritten`). Each table's choice is logged as
  `copy.storage_order`. The new sync setting `copy_order`(`auto`, the default; `key`, as before;
  `storage`) chooses. A table already part-copied in key order that now qualifies starts again in
  storage order, merging over the rows already copied. State store version 8; going back to an
  earlier version copies a table part-copied in storage order again, in key order, which is safe.

## [0.8.1]

- **Fixed: a sync read as failing for its whole initial copy after a failed start.**
  `replicare_sync_start_failed`(and the dashboard's "Syncs failing" panel) went to `1` when an
  attempt failed, but only went back to `0` once the sync reached streaming. So a sync that started
  on its next attempt still read as failing for as long as its initial copy took, often hours. It
  now reads `0` as soon as the next attempt starts, and `1` only while the sync waits to be tried
  again.
- **Grafana dashboard: a "Syncs, triggers & checks" row**, below "Index rebuild": syncs failing
  to start(`replicare_sync_start_failed`), tables each sync replicates(`replicare_sync_tables`, red
  at 0), the target's own triggers skipped or switched off(`replicare_target_triggers`), and tables
  waiting to be checked or being repaired(`replicare_tables_to_check`, red when a table couldn't be
  repaired). Re-import the dashboard to get it; it works with 0.8.0 as released.

## [0.8.0]

- **Tables copied while the target's own triggers fired are checked and repaired by themselves.**
  The first time `replicare` holds a target's triggers back, any table already copied with them
  firing(by an earlier version, or with `target_triggers` off) may differ from the source. Each
  one is now marked(`check.planned`) and, once the sync has caught up, compared with the source
  row by row; every row that differs is written again from the source, and the table is compared
  again until it matches(`check.repairing`, then `check.ok`). Nothing to run by hand.
  `replicare status` warns while any are waiting, and `replicare_tables_to_check` counts them.
  State store version 7.
- **The check after `skip_initial_copy` repairs what it finds** instead of only reporting it. Each
  row found missing, extra or different is queued on the source as a change, so streaming writes it
  again from the source's current value(or deletes it from the target), and the table is checked
  again a minute later(`copy.skip_check_repairing`). Only a difference that stops shrinking over 3
  rounds is still logged at ERROR(`copy.skip_check_drift`). This also covers the passive layouts in
  the failover guide, where a copy that was behind no longer needs copying again from scratch.
- **Fixed: Redis keyspace notifications could miss the first changes.** The subscription was
  started without waiting for Redis to confirm it, so a change made in the first moments after
  start-up could fire no event. Notifications only speed up delivery and the reconciliation scan
  still found the change, so nothing was lost; it now waits for the confirmation(up to 5 seconds).
- **New page: [cutting over](https://replicare.rudimk.com/cutover.html).** How a cutover works,
  what to check before the day(indexes rebuilt, checks done, tables without a key, data that isn't
  replicated, counters, every writer known), the steps on the day with a way back, falling back,
  and a table of what most often goes wrong. The steps that were on the operations page now live
  there.

## [0.7.5]

- **Fixed: a target could silently miss changes when several targets read the same database.**
  Each target's job cleared a queued change from the source as soon as *its own* target had it,
  so a target that was slower, down, or paused at that moment never received it, with no error.
  This hit a sync with several targets(fan-out), two syncs reading the same database, a paused
  sync whose source another sync kept reading(breaking the promise that pausing loses nothing),
  and active-active clusters of three or more databases. A change is now cleared only once every
  target reading that table from the database has it, paused syncs that have run before
  included; a sync with a narrower table selection only holds back its own tables. The retention
  limit still bounds the queue, but a sync now only gives up on its own target, never another
  sync's, which may be mid-copy or paused. Only targets in the same config are counted: a database
  read by two separate `replicare` configs isn't covered.
- **Fixed: the target's own triggers fired on `replicare`'s writes.** A trigger on a target
  table(one that writes a version or audit row, stamps a column, or refuses changes to an
  append-only table) fired on every row `replicare` copied or applied, although the source's own
  trigger had already done that work and `replicare` copies its result. Version rows clashed with
  the copied ones(`duplicate key value violates unique constraint`), stamped values changed, and
  append-only guards refused `replicare`'s writes. Now, on a Postgres sync, `replicare` keeps them
  from firing on its writes, before anything is written:
  - ordinary triggers are skipped by running `replicare`'s own connections in Postgres' replica
    mode, where the target allows it(superuser, `rds_superuser`, or on Postgres 15+ a
    `GRANT SET ON PARAMETER session_replication_role`). Nothing on the target changes, and the
    triggers fire as usual for everyone else;
  - triggers that fire even then(`ENABLE ALWAYS`), or all of them without replica mode, are
    switched off while the sync runs, their modes saved in the state store first(state store
    version 6). They're switched back on, each in its earlier mode, when the daemon starts with the
    sync paused, or with the new `replicare triggers restore <config>`. `replicare status` warns
    while any are off;
  - a trigger the role doesn't own, so can't switch off, stops the sync from starting, with the
    statements to run.

  It's on by default and changes nothing on a target without triggers.
  `target_triggers: { enabled: false }` lets them fire, as before. Never used on an active-active
  member. New metric `replicare_target_triggers`, and events `trigger.replica_mode`,
  `trigger.switched_off`, `trigger.switched_on` and `trigger.gone`. MySQL targets are unchanged:
  MySQL has no way to do this short of dropping the trigger. See
  [the target's own triggers](https://replicare.rudimk.com/operations.html#the-targets-own-triggers).
- **Copy errors from Postgres now carry the whole error**, including the line saying where it
  happened(for example, which trigger function wrote the row), not just the first line.
- **One sync failing no longer stops the others.** A sync(or one target of it, or one direction
  of an active-active cluster) that couldn't start, or stopped while streaming, used to stop every
  sync and exit the daemon, so under Kubernetes one broken sync kept restarting all the others and
  their copies. Now it's logged at ERROR as `sync.start_failed` with the cause and started again
  after a wait that doubles from 30 seconds up to 10 minutes, while everything else keeps running.
  `replicare_sync_start_failed` and `replicare_sync_start_failures_total` show it. Only state store
  problems still stop the daemon.
- **Much more in the logs while a sync starts and runs.** Each start-up stage is logged with how long
  it took(`sync.starting`), every pre-flight finding at its own level(`preflight.finding`), and how
  many tables the sync replicates(`sync.tables_selected`): a WARN naming the tables skipped for
  having no primary key or unique key, and an ERROR when it replicates nothing, saying whether the
  selection matched no table or none had a key(`replicare_sync_tables` reads `0` then). Installing
  capture logs its progress every 15 seconds(`capture.installing`), the initial copy logs tables
  done, rows copied, share done and ETA every minute(`copy.progress`), and streaming logs rows
  applied, the backlog and failed passes every minute(`stream.progress`). A blocked pre-flight now
  lists every blocking finding in its error.
- **Fixed: the initial copy ETA could read years.** Two causes. A table that kept copying past the
  source's row estimate(statistics that are out of date) stopped counting towards the measured rate,
  so on a copy running at full speed the ETA climbed as it went; and a table the source had never
  analysed counted as already copied. Both now count: rows past the estimate add to the rate, and a
  table with no estimate is weighed at the other tables' average row size. And while under 1% of the
  work is done, no ETA is shown for the first 30 minutes, since the start of a copy is mostly set-up
  on small tables and says little about how long the big ones will take.
- **Faster start-up for a sync with many tables:** the copy connections no longer each read the
  whole table list again before the copy starts.
- **New page: [failover and high availability](https://replicare.rudimk.com/failover.html),**
  which replaces the active-active page. It covers three layouts(one active and one passive; one
  active and several passive, as a fan-out or a chain; several active), and for each: setting it
  up, what to watch, a planned switchover, an unplanned failover, a copy or node failing, finding
  the changes stuck on a failed database(Postgres and MySQL queries), bringing it back, failing
  back, and adding or removing copies. The active-active content(set-up, conflicts, the two rules)
  is now its "several active" section, with what last-write-wins can't settle spelled out. The old
  `active-active.html` address redirects to it, so links in error messages from earlier versions
  still work.

## [0.7.4]

- **A sync can start without its initial copy:** `skip_initial_copy: { enabled: true }`.
  For a target that already holds the source's data, usually the old source after a cutover as
  the target of a sync running the other way, the copy would rewrite every row for nothing. With
  it on, every table that hasn't started copying is marked copied and the sync streams the changes
  queued since capture was installed on the source. Once the target has caught up, each of those
  tables is compared with the source, row by row like `replicare verify`, and a difference is
  logged at ERROR with the keys involved; `replicare status` shows `skipped` until a table passes,
  and `replicare_copy_skipped_tables` counts them. `verify: false` turns the check off. Postgres
  and MySQL. It's set per sync, so with Helm it goes in the sync's entry in the chart's values.
  See [cutting over, with a way back](https://replicare.rudimk.com/operations.html#cutting-over-with-a-way-back).
- **A config can hold a sync and its reverse while one of them is paused.** The check against
  replication loops counted paused syncs too, so `A→B` and `B→A` in one config were refused even
  with one of them off. Now only running syncs count; two running in opposite directions are still
  refused.
- **The "Leftover track rows cleared" dashboard panel shows totals.** It showed a rate, which read 0:
  the clean-up usually clears a table in one round, so each table's count appears already at its total,
  and a rate can't see that first step. It now shows how many leftover rows were removed from each
  table since the daemon started, with the totals in the legend.

## [0.7.3]

- **`replicare verify` checks MySQL tables a key range at a time,** like Postgres: no single
  long read of a whole table, a short pause between ranges, and rows that differ reported by key
  as missing, extra or different after a second look. `--sample` works on MySQL too: for tables
  keyed by a number, text or binary value each sampled range starts with one index lookup; other
  keys(dates, decimals) use a slower random position in key order. Each query also gets a
  server-side time limit.
- **Redis reconciliation passes now end properly.** When a full scan of the keyspace finished,
  the next one started straight away in the same step, so on a keyspace smaller than one batch a
  pass never visibly ended. Now the step after a pass ends is empty, so `replicare` pauses for one
  drain interval between full passes, as intended. This also fixes the active-active Redis tests,
  which waited for a pass to end and never saw one.
- **MySQL and Redis integration tests now run in CI** on every pull request, next to Postgres:
  MySQL 5.7 to 8.4, and standalone Redis 6.2 to 7.4(Redis Cluster stays a local check). The CI
  jobs are now named `integration-postgres`, `integration-mysql` and `integration-redis`.

## [0.7.2]

- **Clearing old track rows no longer slows down replication.** In 0.7.0 and 0.7.1, the clean-up
  of track rows left behind by earlier versions ran inside every streaming pass. On a busy source
  with millions of them, every pass waited on large deletes, and lag climbed until they were gone.
  It now runs in the background on a connection of its own, 10,000 rows at a time with a pause in
  between, so streaming never waits for it. New log events: `track.leftovers_removed` and
  `track.sweep_failed`; a new counter, `replicare_track_leftovers_removed_total`, and a "Leftover
  track rows cleared (per table)" panel on the Grafana dashboard, next to the purge time.
- **Fewer failed passes on a busy source.** Each pass used to read the parent tables' queued
  changes before the child tables'. A parent and child written by the application between those
  two reads left the child in the pass without its parent, so the target rejected it, the pass was
  retried, and on a busy source the same thing could happen on every retry until the pass gave up.
  The child tables' queues are now read first, which rules this out: the source only accepts a
  child row once its parent exists.
- **`verify --sample` checks the number of ranges asked for on small tables too.** Sampling picks
  whole pages at random, and on a small table it sometimes found fewer starting keys than asked
  for; it now widens the sample until it has enough.

## [0.7.1]

- **Restarts never take down the indexes of a table that's already copied.** If `replicare`
  couldn't read a table's copy progress from the state store at startup, it treated the table as
  not yet copied and took its target indexes down, which then meant rebuilding each of them for
  nothing. Now a table keeps its indexes unless the state store says it still has to be copied.

## [0.7.0]

### `replicare verify` works on large databases

- **Postgres tables are checked a key range at a time.** Before, `verify` hashed each table in
  one query per side, which timed out on large tables and put a long scan on the source. Now the
  source picks where each range ends from its key index(10,000 rows by default), both sides hash
  that range with the same query, and there's a short pause between ranges. No query holds a long
  scan or snapshot on the source.
- **Rows that differ are named.** A range that differs is narrowed down to the exact rows, which
  are looked up again by key after a wait(30 seconds by default), so rows that were only on their
  way to the target aren't reported. What still differs is listed by key as missing, extra or
  different.
- **New options:** `--table` to check only some tables, `--sample <n>` for a quick check of `n`
  random ranges per table, and `--chunk-rows`, `--pause` and `--recheck-after` to tune it. A full
  check prints its progress every 30 seconds and no longer gives up after 30 seconds per table.
- The report gains a `MODE` column and the differing keys; `--json` gains `mode`, `chunks`,
  `missing_on_target`, `extra_on_target`, `different`, `keys` and `truncated`.
- MySQL and Redis tables are still checked in one pass each.

### The purge no longer slows the source down over time

- **Track rows are deleted along with their changes.** Earlier versions never deleted rows from
  the track tables(the record of which targets have applied each change), so on a busy source
  they grew forever, and the purge, which reads them, got slower every day until it was taking
  minutes and slowing the application's own queries.
- **Leftover track rows from earlier versions are cleared automatically,** a small batch per
  table on each pass, so a large backlog disappears over the first minutes after upgrading.
- **Each purge statement on Postgres is limited to 30 seconds;** one that takes longer gives up
  and tries again on the next pass.
- Track tables get the same autovacuum settings as the delta tables.
- **New metric** `replicare_delta_purge_seconds{sync,table}`, and a "Delta purge time (per table)"
  panel on the Grafana dashboard.

## [0.6.1]

### Fewer needless restarts on a busy source

- **The liveness check counts progress inside a streaming pass,** not only whole passes. Each
  table applied, each retry attempt and each reseed step now count, so a long but moving pass on
  a busy source no longer fails `/healthz` and gets the pod restarted. A query that's truly stuck
  still does.

## [0.6.0]

### Faster first copies on Postgres: target indexes taken down and built again

- **On by default for Postgres syncs.** Just before each table's initial copy, `replicare` saves
  the definitions of the target's plain indexes in the state store and drops them, so the copy
  doesn't maintain every index row by row. Once every table is streaming, it builds them again
  with `CREATE INDEX CONCURRENTLY`, one at a time and without blocking writes, indexes on linking
  columns first, then refreshes each table's statistics. Primary keys, unique indexes and
  indexes behind constraints are never touched.
- **Safe by design:** a table the target role doesn't own keeps its indexes(with a warning);
  never used on an active-active node; restart-safe at every step; each dropped index's full
  `CREATE INDEX` statement is also logged. A reseed takes them down and builds them again too.
- **Configurable** per sync with `defer_indexes: { enabled, maintenance_work_mem,
  parallel_workers }`; `enabled: false` copies with the indexes in place, as before.
- **Visible:** new metrics(`replicare_deferred_indexes`, `replicare_deferred_indexes_built`,
  `replicare_index_build_info`, `replicare_index_build_phase_progress_ratio`), an "Index
  rebuild" row on the Grafana dashboard, a line in `replicare status`, and `index.*` log events.
- **New command:** `replicare indexes restore <config>` builds any still-missing indexes straight
  away, for a sync that won't run again.

## [0.1.0] — first release

The initial release: a complete, single-engine **Postgres → Postgres** data
replicator. Feature-complete and verified end to end on a Postgres 9.6 → 17
integration matrix with fault-injection, perf, and fidelity gates.

### Replication engine

- **Trigger-based CDC** (Bucardo-style) — no WAL access, no `REPLICATION` role,
  no `wal_level` change. Works on managed and old Postgres.
- **PK-only capture with re-read at sync time** — self-healing dirty-key-set
  model; PK changes enqueue both old and new keys.
- **Resumable, parallel initial copy** — chunked keyset/ctid ranges, text `COPY`
  streamed source→target, parents-first within FK components, components in
  parallel; resumes from checkpoints after a crash.
- **Continuous streaming apply** — FK-ordered per-component transactions
  (parent→child upserts, child→parent deletes) with a bounded retry fallback for
  cycles/self-references; idempotent, at-least-once.
- **Cyclic / self-referential FK initial copy** — NULL-then-fill, deferred
  constraints, or a loud pre-flight block for the genuinely-unloadable case.

### Correctness & safety

- **Faithful transport, never transform** — values move verbatim (text `COPY`,
  both copy and delta paths) with session-GUC canonicalization; a type/constraint
  error halts loudly instead of corrupting. Verified across the 9.6→17 gap,
  including enum and range types, on both copy and delta paths.
- **Bounded source footprint** — batched consumption-gated purge, aggressive
  per-table autovacuum, and bounded retention (age/size) with forced reseed of a
  laggard target. Protects the source even while a target is down, and under a
  stalled `xmin` horizon.
- **Crash-safe checkpointing** — Postgres state store; a restarted or rescheduled
  process resumes cleanly; single-active ownership via `pg_advisory_lock`.

### Operations

- **Daemon** managing multiple named syncs concurrently, with graceful
  (SIGTERM → drain + checkpoint) shutdown.
- **CLI** — `run`, `validate`, `status`, `capture install|remove`, `reseed`,
  `version`.
- **Pre-flight** — classifies every source↔target column pair; blocks on
  incompatible types, warns on risky ones, skips no-key tables.
- **Least-privilege** — documented, verified source and target grants; no
  superuser.

### Observability

- **Prometheus `/metrics`**, a **`/status` + `/healthz` HTTP API**, structured
  `slog` logs, and **OpenTelemetry traces** (OTLP/gRPC export when configured).
- A degrading target (unreachable, or backlog nearing the retention cap) is
  surfaced across metrics, logs, **and** traces simultaneously.

### Packaging

- Single static, CGO-free binary; ~5 MB `scratch`-based Docker image; sample
  systemd unit; grant SQL; a runnable docker-compose demo; and full docs.

### Not in this release (roadmap)

- MySQL and Redis engines (the core is engine-agnostic and designed for them).
- HA / leader election, multi-master conflict resolution, live DDL replication,
  Helm chart, hardened fan-out.

[0.1.0]: https://github.com/rudimk/replicare/releases/tag/v0.1.0
