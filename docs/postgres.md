# Postgres Engine

This page describes the Postgres engine, which replicates Postgres to Postgres using
trigger-based CDC that never touches the WAL. It's `replicare`'s first engine and its reference
one; the engine-neutral copy / apply / delta / reseed / pipeline / observability layers were
designed around it, and MySQL and Redis reuse those same layers. It covers which versions are
supported, why we capture changes with triggers, how values are kept faithful, what the engine
needs from the source and target, as well as how to configure and validate a sync.

The shared docs - [getting-started](getting-started.md), [configuration](configuration.md),
[operations](operations.md) and [troubleshooting](troubleshooting.md) - describe the baseline
behaviour using Postgres, so they double as this engine's extended manual. This page only
covers what's specific to Postgres.

At the moment, the engine is fully shipped. Trigger capture, the chunked parallel initial
copy(text-faithful `COPY`), FK-ordered streaming apply(including cyclic components), delta
bloat / retention / reseed, full observability and the daemon lifecycle all run end to end, and
Postgres syncs coexist with MySQL and Redis syncs in one daemon. The quickest way to try it is
the [Postgres demo](https://github.com/rudimk/replicare/tree/main/examples/demo).

## Supported versions

The source floor is PostgreSQL 9.6, on a best-effort basis, and the target should be a modern
major(12+). The CI matrix runs a 9.6 source against a 17 target. The reasons for that floor -
float fidelity on pre-12 servers, the path with no native partitions, and portability to old
PL/pgSQL - are covered in [`postgres-version-support.md`](postgres-version-support.md).

All of the SQL that runs on the source(trigger functions, capture DDL, introspection) stays
conservative and version-tolerant, so it runs against genuinely old servers. On the other hand,
the target is assumed to be modern, so the apply path uses `INSERT … ON CONFLICT`.

## Why don't we use the WAL?

Postgres already has a durable change stream - the WAL - and logical replication is the usual
way to read it. `replicare` deliberately doesn't use it, since logical replication needs a
`REPLICATION` role and a `wal_level` change, and managed / locked-down servers won't grant
either. So instead, we capture changes with triggers, which only need grants an ordinary role
can be given. This is the relational baseline that MySQL follows too.

## How change capture works

Change capture runs in four steps:

1. Capture. In a dedicated `replicare` schema on the source, `replicare` installs per-table
   delta and track tables, and `AFTER INSERT/UPDATE/DELETE` triggers. The trigger records only
   the changed row's primary key(we call this PK-only capture), not the full row image. An
   `UPDATE` that changes the key enqueues both the old and the new PK, which amounts to a
   delete of the old key plus an upsert of the new one.
2. Reconcile. At sync time, `replicare` reads the unconsumed delta rows, re-reads the current
   values of those PKs from the live source table, and applies them to the target. This is
   self-healing; since it always copies the latest value, overlapping copy / stream windows
   converge naturally. Consumption is a `delta MINUS track` set-difference(a dirty-key set,
   *not* an ordered log), deleting by `delta_id`. That sidesteps the hazard where transactions
   commit in a different order from the one they were assigned in, which a scalar
   high-water-mark cursor would fall straight into.
3. Apply. Changes land as idempotent upserts keyed on the PK(`INSERT … ON CONFLICT DO UPDATE`)
   and deletes by PK, ordered by FK dependency within each FK component - upserts go from
   parent to child, deletes from child to parent - with a retry fallback for cycles. The same
   retry covers a unique value moving between rows within one pass: if a row was deleted and a
   new one created with the same unique value(an account re-created with the same email, say),
   the new row clashes with the old one, which is still on the target because deletes come
   after upserts. `replicare` applies that table's deletes anyway and retries the new row on
   the next pass, once the old one is gone.
4. Checkpoint. Progress lives in the Postgres StateStore, so a crash safely replays from the
   last confirmed cursor; that's harmless, since applies are idempotent.

Capture is enabled first, and the chunked parallel initial copy runs while changes queue up
behind it. So there's no single frozen snapshot, and a database that's "too big to snapshot"
isn't a problem.

## Faithful transport, never transform

Values move verbatim as text `COPY`, and they're validated by the target's own input
functions. `replicare` never coerces, truncates, casts or "repairs" data; on a type / constraint
error it halts loudly rather than corrupting anything silently.

To keep that text round-trippable, `replicare` pins a set of formatting-only session GUCs on
every connection, none of which need a privilege: `DateStyle=ISO,YMD`, `TimeZone=UTC`,
`extra_float_digits=3`(critical for float fidelity on pre-12 sources), `IntervalStyle=postgres`,
`bytea_output=hex` and `client_encoding=UTF8`.

Before anything is copied, a pre-flight compatibility check classifies every source / target
column pair. It blocks on incompatible / missing types, warns on risky-lossy ones(e.g.
`int8→int4`), and lets identical / widening pairs through.

## Requirements

There are a few things the engine needs from the tables it replicates:

1. Every replicated table needs a primary key or a usable unique key. Tables without one are
   skipped with a loud warning, surfaced in logs and telemetry.
2. The target schema must already exist. `replicare` replicates data only, with no live DDL, so
   target tables are created / migrated with the operator's own tooling. `replicare`
   introspects them to build correct, name-matched apply statements, and never installs or
   alters target types / extensions / enum labels.
3. Cyclic / self-referential FKs are handled case by case. A nullable cycle uses a
   NULL-then-fill two-pass load, and a `DEFERRABLE` cycle loads under
   `SET CONSTRAINTS … DEFERRED`. A `NOT NULL` non-deferrable cycle is genuinely unloadable
   without disabling constraints, which `replicare` won't do, so pre-flight stops with an error
   and asks for the FK to be made `DEFERRABLE` or for the cycle to be broken.

There's one exception to the two-pass load: a link column that sits inside a unique index where
empty values count as equal. Examples are `UNIQUE NULLS NOT DISTINCT (email, parent)`, a unique
index on an expression such as `COALESCE(parent, 0)`, or one with a `WHERE` filter. A column like
that can't be emptied during the two-pass load, since rows that differ only by that column would
clash and the copy would stop with a duplicate-key error. `replicare` spots these indexes on both
the source and the target(bare `CREATE UNIQUE INDEX` included) and handles them without any
schema change:

- A table that points at itself(an account tree, a category tree) keeps the column at its real
  value, and the whole table is copied in a single statement; Postgres accepts this because it
  checks this kind of FK only when the statement ends. Live changes keep the real value too.
  The trade-off is that this one table is copied in one piece rather than in parallel chunks,
  and a restart re-copies it from the start.
- A loop across two or more tables has no safe load order, so pre-flight stops the sync before
  anything is copied and asks for the FK to be made `DEFERRABLE`.

Tables linked in a loop are copied as one group, and the group only counts as copied once every
table in it has finished, so a restart(or a pause) copies the whole group again from the start.
Before it does, `replicare` empties the group's tables on the target, so the copy never has to
merge over what the unfinished attempt left behind: by then the source may have moved a unique
value from one row to another(an account deleted and re-created under a new id with the same
email, say), and the old row would block the new one. It uses `TRUNCATE` where the target role is
allowed to, and a single `DELETE` otherwise. It leaves the tables alone, and merges as before,
when a table outside the group points into them, or when the target is an active-active node,
whose rows are its own data.

The group's tables load one at a time, parents first, and each table is copied in chunks of about
50,000 rows. The chunks of a table are shared out over the copy pool's connections(sized by
`pool.max_source_connections` and `max_target_connections`), so several of them load at once, and
the same goes for the pass that fills the link columns in afterwards. A chunk whose rows aren't on
the target yet, which is the usual case once the group has been emptied, goes in with a plain
`COPY` straight into the table. A chunk that already has rows there is merged instead, through a
staging table and `INSERT … ON CONFLICT`.

The source keeps changing while this runs, so a chunk can hold a row that points at a parent row
created after that parent table was copied, which the target refuses. When that happens,
`replicare` copies across just the parent rows that chunk points at, and only the ones the target
doesn't have yet, then retries the chunk. It reads those parent rows and the retried chunk from the
source at the same instant, so a row the app adds in between can't make the retry fail again. If those parent rows in turn point at rows the target
doesn't have, it fetches those too, one level up at a time. It never copies a whole parent table
again for this, so a busy source doesn't slow the copy down much. The one exception is a table
that's loaded in a single statement(see above), which is copied again whole.

On a large target, it's the indexes that slow a copy like this down most. Every row added has to
go into every index on the table, and once those indexes outgrow the target's memory, most of
that becomes waiting on disk reads. When a big copy crawls, dropping the target's non-unique
indexes on the biggest tables for the duration of the copy helps a lot. We'd keep the primary
keys and unique indexes, save each dropped index's definition with `pg_get_indexdef`, and
rebuild them with `CREATE INDEX CONCURRENTLY` once the copy is done, before relying on the target
for reads. Rebuild them promptly too: deleting a parent row is slow while its child table has no
index on the linking column.

## Installing capture on a busy source

Adding a trigger locks the table for an instant, but if something else already holds a
conflicting lock - a long `VACUUM`, a migration, or an application session left *idle in
transaction* - the trigger change has to wait. While it waits, Postgres queues every new query
on that table behind it. So `replicare` does two things to keep out of the application's way:

1. It leaves an already-correct trigger alone on restart, so a restart takes no lock on any
   application table.
2. When it does need to add or replace a trigger, it waits at most 2 seconds for a lock, then
   steps back and retries with a growing pause. After repeated failures, it stops with an error
   that names the table, rather than holding the application up.

If that error appears, find the session holding the table(`pg_stat_activity` / `pg_locks`), let
it finish, then restart `replicare`.

## Effectively-exactly-once on a Postgres target

`replicare`'s delivery is at-least-once, with idempotent apply. A Postgres target gets a bonus on
top of that: when the StateStore *is* the target database, a cursor advance can ride in the same
transaction as its apply batch, which makes apply effectively-exactly-once per batch.

This is unique to Postgres-target syncs. MySQL and Redis targets are strictly at-least-once,
since their StateStore is a different engine / server and no single transaction can span both.
At-least-once is still correct and convergent everywhere; a crash may re-apply the last batch,
which is harmless because applies are idempotent upserts.

## Delta bloat, retention & reseed

Capture writes delta tables to a source we may not own, so we treat bounding that footprint as
seriously as the replication itself. Three things keep it in check. Every table gets its own
delta table, which isolates bloat / contention. Consumed rows are removed with a batched,
consumption-gated `DELETE` purge, backed by aggressive per-table autovacuum reloptions set at
install time(no superuser, no global change). Finally - retention is bounded by age / size,
with a forced reseed of any target that goes over the cap, to protect the source; see
[the source footprint section of operations](operations.md#source-footprint-the-thing-to-watch).

Per-target delta backlog, oldest-unconsumed age, purge rate and reseed events are all surfaced
as telemetry(see [operations](operations.md)).

## Least privilege

`replicare` needs no superuser, no `REPLICATION` attribute and no `wal_level` change. Ready-to-edit
grant SQL ships in
[`../deploy/grants-source.sql`](https://github.com/rudimk/replicare/blob/main/deploy/grants-source.sql) and
[`../deploy/grants-target.sql`](https://github.com/rudimk/replicare/blob/main/deploy/grants-target.sql).

On the source, the role needs `USAGE` on each replicated schema, `SELECT` and `TRIGGER` on each
replicated table, and somewhere to hold the capture machinery: either `CREATE` on the database,
*or* a pre-created `replicare` schema owned by the daemon role(the option that needs no database
grant).

There's an asymmetry on teardown. `CREATE TRIGGER` needs only the `TRIGGER` privilege, but
`DROP TRIGGER`(part of `replicare capture remove`) requires table ownership. So a
least-privilege role can install and run capture, but it needs ownership(or the owner's help) to
fully uninstall the trigger. Dropping `replicare`'s own delta / track tables and functions works
with the daemon role, since it owns them.

On the target, the role needs just `SELECT, INSERT, UPDATE, DELETE` on the pre-existing target
tables. If the StateStore lives on the target database, also grant it `CREATE` on that
database(or pre-create the `replicare_state` schema owned by the role).

## Configuration

A Postgres sync is the engine-neutral envelope, with a typed `postgres:` block for each endpoint.
The state store is Postgres too(it's the only StateStore backend in v1), and it can be the
target, the source, or a separate database:

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

A few notes on the Postgres-specific parts:

- The Postgres block uses `sslmode`(libpq's `disable | allow | prefer | require | verify-ca | verify-full`),
  matching Postgres's own vocabulary; MySQL and Redis use a `tls` field with the same spectrum.
- `database` is the connection default, while selection globs are `schema.table` and may span
  other schemas. FK components are computed over the selected tables, and copied / streamed with
  ordering inside each component and parallelism across components. A dominating "giant
  component" gets a warning, with a config override available.
- Extra libpq parameters can be passed via `params`.
- The single-engine rule is enforced: a Postgres source's targets must all be Postgres, never
  cross-engine.

All the neutral knobs(worker / connection caps, drain interval, batch sizes, retention,
observability endpoints, logging, env-var overrides) are documented in
[configuration](configuration.md).

## Validating

To check a config before any sync starts, run:

```sh
replicare validate config.yml
```

This connects to both sides and classifies every column pair, blocking on incompatible ones and
warning on risky-lossy ones. It also reports the FK components, with giant-component and
dangling-FK warnings(an FK pointing at an excluded table), flags tables without a usable key,
and detects unloadable `NOT NULL` cyclic FKs. See [`cli.md`](cli.md) for the full command
surface(`run`, `validate`, `status`, `verify`, `capture`, `reseed`, `reseed-sequences`).
