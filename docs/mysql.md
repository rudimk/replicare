# MySQL Engine

This page describes the MySQL engine, `replicare`'s second engine, which replicates MySQL to
MySQL using the same trigger-based CDC model as Postgres, without ever reading the binlog. It
reuses the engine-neutral copy / apply / reseed / pipeline / observability layers unchanged. It
covers the supported versions, what the engine needs from the source and target, the two
operational wrinkles worth knowing up front, as well as how to configure and validate a sync.

At the moment, the engine is fully shipped and runs end to end - trigger capture, chunked
initial copy(byte-faithful `LOAD DATA`), FK-ordered streaming apply(including cyclic `NOT NULL`
components), bloat control(purge / retention / reseed), observability and the full daemon
lifecycle - and it coexists with Postgres syncs in one daemon. The quickest way to try it is the
[MySQL demo](https://github.com/rudimk/replicare/tree/main/examples/demo-mysql).

## Supported versions

The source floor is MySQL 5.7 and the target is 8.4, which is the pair CI is committed to; 5.6
is best-effort only. The reasons are covered in
[`mysql-version-support.md`](mysql-version-support.md).

MariaDB isn't supported in v1. Its dialect has forked from MySQL's, so the engine detects it at
connect and refuses it loudly, rather than mis-driving it.

## Requirements

There are five things the engine needs from the source and target:

1. InnoDB tables only. Non-transactional engines(MyISAM / MEMORY) break the transactional
   apply and the source consume / track atomicity, so pre-flight blocks them.
2. `local_infile=ON` is required on the target, since it's the copy / apply
   transport(`LOAD DATA LOCAL INFILE`). `replicare` probes it at connect and, if it's off, halts
   loudly with an actionable error rather than failing cryptically mid-copy. To enable it, run
   `SET GLOBAL local_infile=1`(or set `local-infile=ON` in my.cnf), then set the
   `local_infile: true` hint in the target's `mysql:` block.
3. Every replicated table needs a primary key or a usable unique key. Tables without one are
   skipped with a loud warning.
4. No secondary UNIQUE keys on target tables(beyond the replication key). MySQL's
   `INSERT … ON DUPLICATE KEY UPDATE` fires on *any* unique key and would silently mutate the
   wrong row, so pre-flight blocks such target tables. This is a hard faithful-transport
   guarantee.
5. Least privilege on the source: `SELECT` and `TRIGGER` on the replicated tables, and
   `CREATE` / owner on the `replicare` capture database - no `SUPER`, no `REPLICATION SLAVE`, and
   no binlog access. Ready-to-edit grant SQL is in
   [`grants-source-mysql.sql`](https://github.com/rudimk/replicare/blob/main/deploy/grants-source-mysql.sql) and
   [`grants-target-mysql.sql`](https://github.com/rudimk/replicare/blob/main/deploy/grants-target-mysql.sql).

> ⚠️ At the moment, there's no fallback for targets with `local_infile` off. A batched-`INSERT`
> fallback is deferred to a future release; doing it byte-faithfully across column charsets
> needs careful design, and for a hardening release we've favoured a loud requirement over a
> risky fallback.

## The two operational wrinkles

Both of these follow from the v1 architecture, and they're worth knowing up front.

### 1. A MySQL sync still needs a Postgres for state

`replicare`'s own operational state - which syncs exist, per-table copy progress, per-target
cursors, and the single-active ownership lock - lives in a Postgres state
store(`state_store.engine: postgres`), *regardless* of the data engine. This state is small
control-plane bookkeeping, entirely separate from the MySQL data itself. The replication state
that correctness depends on(the delta / track tables) lives on the MySQL source, exactly as
Postgres puts them on a Postgres source.

So to replicate MySQL to MySQL, `replicare` also has to be pointed at a small Postgres for its
state. A MySQL-backed state store is a future item behind the pluggable `StateStore`
interface; it isn't in v1.

### 2. MySQL syncs are strictly at-least-once

`replicare`'s delivery is at-least-once, with idempotent apply. For a Postgres target, when the
state store *is* that target database, a cursor advance can ride in the same transaction as its
apply batch, which makes apply effectively-exactly-once per batch.

That optimisation isn't available for MySQL syncs, since the state store is a different engine /
server(Postgres) from the MySQL target, and no single transaction can span both. MySQL syncs are
therefore strictly at-least-once. They're still correct and convergent(applies are idempotent
upserts keyed on the PK), but a crash may re-apply the last batch. That's harmless by design;
we only mention it so nobody expects exactly-once parity with a Postgres-target sync.

## Installing capture on a busy source

Adding or replacing a trigger needs an exclusive metadata lock on the table. If an application
transaction that touched the table is still open, the trigger change has to wait, and while it
waits, MySQL queues every new query on that table behind it(`lock_wait_timeout` defaults to a
year). So `replicare` does three things to keep out of the application's way:

1. It leaves a trigger alone on restart when its stored body already matches, so a restart takes
   no lock on any application table.
2. It creates its `replicare` database only when it's missing, since even
   `CREATE DATABASE IF NOT EXISTS` waits behind open application transactions once capture is
   installed.
3. When it does need to change a trigger, it waits at most 2 seconds for a lock, then retries
   with a growing pause, and after repeated failures it stops with an error naming the table.

> ⚠️ MySQL can't replace a trigger in one step(drop and create are separate), so changes to that
> table aren't captured during the instant between them. That only happens when a trigger
> actually changes(an upgrade that alters its body), never on a plain restart.

## Configuration

A MySQL sync is an engine-neutral envelope with a typed `mysql:` block for each endpoint, plus
the Postgres state store:

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

A few notes on the MySQL-specific parts:

- `tls` accepts the full spectrum `disable | allow | prefer | require | verify-ca | verify-full`(mapped
  onto the driver, including a custom `verify-ca`).
- `database` is the DSN default; selection globs may span other databases, and cross-database
  FK components are handled.
- The single-engine rule is enforced: a MySQL source's targets must all be MySQL, never
  cross-engine.

## Validating

To check a config before any sync starts, run:

```sh
replicare validate config.yml
```

This introspects both sides and classifies every column pair, blocking on incompatible ones and
warning on risky ones. It also reports the FK components, with giant-component / dangling-FK
warnings, and enforces the InnoDB / secondary-unique / no-key rules above.
