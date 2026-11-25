# Changelog

All notable changes to replicare are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/), and the project aims to follow
[Semantic Versioning](https://semver.org/).

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
- **New page: [failover and high availability](https://replicare.rudimk.com/failover.html).**
  Three layouts(one active and one passive; one active and several passive, as a fan-out or a
  chain; several active), with how to fail over, recover the changes stuck on a failed database,
  bring it back, and fail back.

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
