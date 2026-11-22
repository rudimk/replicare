# Changelog

All notable changes to replicare are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/), and the project aims to follow
[Semantic Versioning](https://semver.org/).

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
