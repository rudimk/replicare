# Postgres engine

Postgres is replicare's **first and reference engine** — the engine the
engine-neutral copy / apply / delta / reseed / pipeline / observability layers were
designed around. It replicates **Postgres → Postgres** with trigger-based,
no-WAL CDC (CLAUDE.md §3.1). MySQL and Redis reuse those same neutral layers; this
page documents the Postgres specifics. The shared docs
([getting-started](getting-started.md), [configuration](configuration.md),
[operations](operations.md), [troubleshooting](troubleshooting.md)) describe the
baseline behavior using Postgres, so treat them as this engine's extended manual.

> **Status: shipped.** Trigger capture, chunked parallel initial copy (text-faithful
> `COPY`), FK-ordered streaming apply (including cyclic components), delta
> bloat/retention/reseed, full observability, and the daemon lifecycle all run end
> to end, and Postgres syncs coexist with MySQL and Redis syncs in one daemon. Try
> it with the [Postgres demo](../examples/demo/).

## Supported versions

- **Source floor: PostgreSQL 9.6 (best-effort). Target: a modern major (12+).** The
  CI matrix runs **source 9.6 → target 17**. See
  [`postgres-version-support.md`](postgres-version-support.md) for the rationale
  (float fidelity on pre-12, no-native-partition path, old PL/pgSQL portability).
- Source SQL — trigger functions, capture DDL, introspection — stays **conservative
  and version-tolerant** so it runs against genuinely old servers (CLAUDE.md §1.6).
  The target is assumed modern, so the apply path uses `INSERT … ON CONFLICT`.

## How CDC works (the relational baseline)

Postgres has a durable change stream (the WAL), but replicare deliberately does
**not** use it — logical replication needs a `REPLICATION` role and a `wal_level`
change that managed and locked-down servers won't grant. Instead (CLAUDE.md §3.1,
§3.3):

1. **Capture** — in a dedicated `replicare` schema on the source, replicare installs
   per-table **delta** and **track** tables and `AFTER INSERT/UPDATE/DELETE`
   triggers. The trigger records **only the changed row's primary key** (PK-only
   capture), not the full row image. A key-changing `UPDATE` enqueues both the old
   and new PK (= delete-old + upsert-new).
2. **Reconcile** — at sync time replicare reads the unconsumed delta rows, **re-reads
   the current values of those PKs from the live source table**, and applies them to
   the target. This is self-healing: it always copies the latest value, so
   overlapping copy/stream windows converge naturally. Consumption is a
   **`delta MINUS track` set-difference** (a dirty-key set, *not* an ordered log),
   deleting by `delta_id` — which sidesteps the commit-order-≠-assignment-order
   hazard that a scalar high-water-mark cursor would fall into (CLAUDE.md §3.3).
3. **Apply** — idempotent upserts keyed on the PK (`INSERT … ON CONFLICT DO UPDATE`)
   and deletes by PK, FK-dependency-ordered within each FK component (upserts
   parent→child, deletes child→parent), with a retry fallback for cycles.
4. **Checkpoint** — progress lives in the Postgres StateStore; a crash safely
   replays from the last confirmed cursor (applies are idempotent).

Capture is enabled **first**, then a chunked parallel initial copy runs while
changes queue — so there is **no single frozen snapshot** and "too big to snapshot"
is not a problem (CLAUDE.md §4).

## Faithful transport (never transform)

Values move **verbatim** as text `COPY` and are validated by the target's own input
functions — replicare never coerces, truncates, casts, or "repairs" data; on a
type/constraint error it **halts loud** rather than corrupting silently (CLAUDE.md
§1.7, §4.2). To keep text round-trippable it pins formatting-only session GUCs on
every connection (no privilege needed): `DateStyle=ISO,YMD`, `TimeZone=UTC`,
**`extra_float_digits=3`** (critical for float fidelity on pre-12 sources),
`IntervalStyle=postgres`, `bytea_output=hex`, `client_encoding=UTF8`. A pre-flight
compatibility check classifies every source↔target column pair: **block** on
incompatible/missing types, **warn** on risky-lossy (e.g. `int8→int4`), ok on
identical/widening.

## Requirements

- **Every replicated table needs a primary key or usable unique key.** Tables
  without one are **skipped with a loud warning** surfaced in logs and telemetry
  (CLAUDE.md §3.1).
- **The target schema must pre-exist.** replicare replicates **data only** — no live
  DDL. You create/migrate target tables with your own tooling (CLAUDE.md §7); it
  introspects them to build correct, name-matched apply statements and never
  installs or alters target types/extensions/enum labels.
- **Cyclic / self-referential FKs** are handled by case (CLAUDE.md §4.1): a nullable
  cycle uses a NULL-then-fill two-pass; a `DEFERRABLE` cycle loads under
  `SET CONSTRAINTS … DEFERRED`; a **`NOT NULL` non-deferrable cycle** is genuinely
  unloadable without disabling constraints (which replicare won't do), so pre-flight
  **fails loud** and asks you to make the FK `DEFERRABLE` or break the cycle.

## The Postgres advantage: effectively-exactly-once

replicare's delivery is **at-least-once with idempotent apply** (CLAUDE.md §5). For
a **Postgres target**, there is a bonus (CLAUDE.md §9): when the StateStore *is* the
target database, a cursor advance can ride the **same transaction** as its apply
batch — yielding **effectively-exactly-once apply per batch**. This is unique to
Postgres-target syncs: MySQL and Redis targets are strictly at-least-once because
their StateStore is a different engine/server, so no single transaction spans both.
(At-least-once is still correct and convergent everywhere — a crash may re-apply the
last batch, which is harmless because applies are idempotent upserts.)

## Delta bloat, retention & reseed

Because capture writes delta tables to a source you may not own, bounding the
footprint is first-class (CLAUDE.md §3.4): **per-table delta tables** (isolated
bloat/contention), a **batched, consumption-gated `DELETE` purge** plus aggressive
per-table autovacuum reloptions set at install time (no superuser, no global
change), and **bounded retention (age/size) with forced reseed** of an over-cap
target to protect the source. The reseed handoff — marking a target needs-reseed,
resetting its track, re-copying, and resuming without gaps — is specified in
[`reseed-state-machine.md`](reseed-state-machine.md). Per-target delta backlog,
oldest-unconsumed age, purge rate, and reseed events are all surfaced as telemetry
(see [operations](operations.md)).

## Least privilege

replicare needs **no superuser, no `REPLICATION` attribute, and no `wal_level`
change** (CLAUDE.md §12). Ready-to-edit grant SQL ships in
[`../deploy/grants-source.sql`](../deploy/grants-source.sql) and
[`../deploy/grants-target.sql`](../deploy/grants-target.sql).

- **Source:** `USAGE` on each replicated schema; `SELECT` + `TRIGGER` on each
  replicated table; and, to hold the capture machinery, either `CREATE` on the
  database **or** a **pre-created `replicare` schema owned by the daemon role** (the
  no-database-grant option). **Teardown asymmetry:** `CREATE TRIGGER` needs only the
  `TRIGGER` privilege, but `DROP TRIGGER` (part of `replicare capture remove`)
  requires **table ownership** — a least-privilege role can install and run capture
  but needs ownership (or the owner's help) to fully uninstall the trigger. Dropping
  replicare's own delta/track tables and functions works with the daemon role since
  it owns them.
- **Target:** just `SELECT, INSERT, UPDATE, DELETE` on the pre-existing target
  tables. If the StateStore lives on the target database, also grant it `CREATE` on
  that database (or pre-create the `replicare_state` schema owned by the role).

## Configuration

A Postgres sync is the engine-neutral envelope with a typed `postgres:` block per
endpoint. The state store is Postgres too (v1's only StateStore backend) and may be
the target, the source, or a separate database:

```yaml
state_store:                       # replicare's own bookkeeping (Postgres)
  engine: postgres
  postgres: { host: state-db, port: 5432, database: replicare_state, user: replicare, password: "${PW}", sslmode: require }

sources:
  app:
    engine: postgres
    postgres: { host: app-db, port: 5432, database: app, user: replicare, password: "${PW}", sslmode: verify-full }

targets:
  warehouse:
    engine: postgres
    postgres: { host: wh-db, port: 5432, database: warehouse, user: replicare, password: "${PW}", sslmode: require }

syncs:
  - name: app-to-warehouse
    source: app
    targets: [warehouse]
    include: ["public.*"]          # schema.table globs
    exclude: ["*_audit"]
```

- The Postgres block uses **`sslmode`** (libpq's `disable | allow | prefer | require
  | verify-ca | verify-full`), matching Postgres's own vocabulary — MySQL and Redis
  use a `tls` field with the same spectrum.
- `database` is the connection default; **selection globs are `schema.table`** and
  may span other schemas. FK components are computed over the selected tables and
  copied/streamed with intra-component ordering and cross-component parallelism
  (CLAUDE.md §8.1); a dominating "giant component" is warned about with a config
  override available.
- Extra libpq parameters can be passed via `params`.
- The **single-engine rule** (§6) is enforced: a Postgres source's targets must all
  be Postgres — never cross-engine.
- All neutral knobs (worker/connection caps, drain interval, batch sizes, retention,
  observability endpoints, logging, env-var overrides) are documented in
  [configuration](configuration.md).

## Validating

```sh
replicare validate config.yml
```

connects to both sides, classifies every column pair (**block** on incompatible,
**warn** on risky-lossy), reports the FK components with giant-component and
dangling-FK (FK→excluded table) warnings, flags tables without a usable key, and
detects unloadable `NOT NULL` cyclic FKs — all before any sync starts. See
[`cli.md`](cli.md) for the full command surface (`run`, `validate`, `status`,
`capture`, `reseed`).
