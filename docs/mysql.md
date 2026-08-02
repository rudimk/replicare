# MySQL engine

replicare's second engine replicates **MySQL → MySQL** using the same trigger-based,
no-binlog CDC model as Postgres (CLAUDE.md §3.1) and reuses the engine-neutral
copy/apply/reseed/pipeline/observability layers unchanged. The design and milestone
plan live in [`../.sisyphus/mysql-plan.md`](../.sisyphus/mysql-plan.md).

> **Status: shipped.** The MySQL engine runs end to end — trigger capture,
> chunked initial copy (byte-faithful `LOAD DATA`), FK-ordered streaming apply
> (including cyclic `NOT NULL` components), bloat control (purge/retention/reseed),
> observability, and the full daemon lifecycle — and coexists with Postgres syncs
> in one daemon. This page documents the engine's shape and its two operational
> wrinkles. Try it with the [MySQL demo](../examples/demo-mysql/).

## Supported versions

- **Source floor: MySQL 5.7. Target: 8.4** (the committed CI pair). 5.6 is
  best-effort only. See [`mysql-version-support.md`](mysql-version-support.md).
- **MariaDB is not supported** in v1: its dialect has forked from MySQL's, so the
  engine detects it at connect and refuses it loudly rather than mis-driving it.

## Requirements

- **InnoDB tables only.** Non-transactional engines (MyISAM/MEMORY) break the
  per-component atomic apply and the source consume/track atomicity, so pre-flight
  **blocks** them (mysql-plan §0.5).
- **`local_infile=ON`** on the target for the fast initial-copy transport
  (`LOAD DATA LOCAL INFILE`). When it's off, replicare falls back to a
  byte-faithful batched-`INSERT` path (slower). Set the `local_infile: true` hint
  in the target's `mysql:` block.
- **Every replicated table needs a primary key or a usable unique key.** Tables
  without one are skipped with a loud warning (CLAUDE.md §3.1).
- **No secondary UNIQUE keys on target tables** (beyond the replication key):
  MySQL's `INSERT … ON DUPLICATE KEY UPDATE` fires on *any* unique key and would
  silently mutate the wrong row, so pre-flight **blocks** such target tables
  (mysql-plan §0.4). This is a hard §1.7 (faithful-transport) guarantee.
- **Least privilege (source):** `SELECT` + `TRIGGER` on the replicated tables,
  `CREATE`/owner on the `replicare` capture database — **no `SUPER`, no
  `REPLICATION SLAVE`, no binlog access.** (Exact grant SQL ships with MM8.)

## The two operational wrinkles

Both follow from the v1 architecture and are worth knowing up front:

### 1. A MySQL sync still needs a Postgres for state

replicare's own operational state — which syncs exist, per-table copy progress,
per-target cursors, and the single-active ownership lock — lives in a
**Postgres** state store (`state_store.engine: postgres`), *regardless of the data
engine* (CLAUDE.md §9). This state is small control-plane bookkeeping, entirely
separate from your MySQL data: the correctness-bearing replication state (the
delta/track tables) lives on the **MySQL source**, exactly as Postgres puts them on
a Postgres source.

So to replicate MySQL→MySQL you also point replicare at a small Postgres for its
state. A MySQL-backed state store is a future item behind the pluggable
`StateStore` interface; it is not in v1.

### 2. MySQL syncs are strictly at-least-once

replicare's delivery is at-least-once with idempotent apply (CLAUDE.md §5). For a
**Postgres** target, when the state store *is* that target database, a cursor
advance can ride the same transaction as its apply batch — yielding
effectively-exactly-once apply per batch (CLAUDE.md §9, "bonus property").

That optimization is **not available for MySQL syncs**: the state store is a
different engine/server (Postgres) than the MySQL target, so no single transaction
can span both. MySQL syncs are therefore **strictly at-least-once** — correct and
convergent (applies are idempotent upserts keyed on the PK), but a crash may
re-apply the last batch. This is harmless by design; it's noted only so you don't
expect exactly-once parity with a Postgres-target sync.

## Configuration

A MySQL sync is an engine-neutral envelope with a typed `mysql:` block per
endpoint, plus the Postgres state store:

```yaml
state_store:                       # replicare's own bookkeeping (always Postgres)
  engine: postgres
  postgres: { host: state-db, database: replicare_state, user: replicare, password: "${PW}", sslmode: require }

sources:
  app:
    engine: mysql
    mysql: { host: app-db, port: 3306, database: app, user: replicare, password: "${PW}", tls: verify-full }

targets:
  warehouse:
    engine: mysql
    mysql: { host: wh-db, port: 3306, database: warehouse, user: replicare, password: "${PW}", tls: require, local_infile: true }

syncs:
  - name: app-to-warehouse
    source: app
    targets: [warehouse]
    include: ["app.*"]          # db.table globs (a MySQL schema IS a database)
    exclude: ["*_audit"]
```

- `tls` accepts the full spectrum `disable | allow | prefer | require | verify-ca |
  verify-full` (mapped onto the driver, including a custom `verify-ca`).
- `database` is the DSN default; selection globs may span other databases, and
  cross-database FK components are handled.
- The **single-engine rule** (§6) is enforced: a MySQL source's targets must all be
  MySQL — never cross-engine.

## Validating

```sh
replicare validate config.yml
```

introspects both sides and classifies every column pair (block on incompatible,
warn on risky), reports FK components and giant-component/dangling-FK warnings, and
enforces the InnoDB / secondary-unique / no-key rules above — all before any sync
starts.
