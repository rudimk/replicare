# Changelog

All notable changes to replicare are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/), and the project aims to follow
[Semantic Versioning](https://semver.org/).

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
  Helm chart, hardened fan-out. See `CLAUDE.md §15`.

[0.1.0]: https://github.com/rudimk/replicare/releases/tag/v0.1.0
