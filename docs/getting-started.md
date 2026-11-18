# Getting Started

This page goes from nothing to a running replication in a few minutes, using a demo that runs
entirely in Docker, and then walks through pointing `replicare` at real databases - from the
checks worth doing before a migration, through to preparing the target, granting privileges,
writing a config and deploying the daemon.

## What `replicare` does

`replicare` is given a source database, one or more target databases and a list of tables. It
then works in three steps:

1. It installs lightweight capture(triggers) on the source.
2. It copies the selected data to the targets, in chunks and in parallel.
3. It streams every insert / update / delete continuously, to keep the targets converged.

None of this needs WAL access, a `REPLICATION` role or a `wal_level` change - just ordinary,
grantable privileges. [Architecture](architecture.md) explains why.

## Try the demo

The fastest way to see it working end to end takes about 2 minutes:

```sh
task docker                        # build the replicare image
cd examples/demo && docker compose up
```

This starts a source Postgres(seeded with `public.orders`), a target Postgres, and `replicare`
copying and then streaming between them. In another shell, check that the initial copy landed,
make a live change on the source and look at the operator surface:

```sh
# initial copy landed
docker compose exec target psql -U postgres -d warehouse -c "SELECT count(*) FROM public.orders;"

# make a live change on the source and watch it propagate
docker compose exec source psql -U postgres -d app \
  -c "INSERT INTO public.orders VALUES (101,'live'); UPDATE public.orders SET note='changed' WHERE id=1; DELETE FROM public.orders WHERE id=2;"
docker compose exec target psql -U postgres -d warehouse -c "SELECT count(*), max(id) FROM public.orders;"

# operator surface
curl localhost:8080/status | jq
curl localhost:9090/metrics | grep replicare_
```

To clean up, press `Ctrl-C`, then run `docker compose down -v`.

### Verifying the metrics

Once a sync is streaming, `/metrics` shows many `replicare_*` series, not just one. Some of them
only populate at runtime, and this picks those out:

```sh
curl -s localhost:9090/metrics | grep -E \
  'replicare_(rows_copied_total|apply_batch_seconds|throughput_rows_per_second|replication_lag_seconds|table_phase_info|target_up)'
```

`replicare_table_phase_info`(one line per table, `initial_copy` then `streaming`),
`replicare_target_up` and the row estimate `replicare_initial_copy_rows_target` appear as soon as
the sync starts, and `replicare_rows_copied_total` climbs as the initial copy commits rows.
`replicare_apply_batch_seconds` and `replicare_replication_lag_seconds` refresh on every
streaming drain pass.

`replicare_throughput_rows_per_second` is only non-zero on a pass that actually applied deltas.
To see it move, make a change on the source(the `INSERT/UPDATE/DELETE` above) and scrape again;
a sync that's caught up and idle correctly reports `0` here.

The full list of every metric, its type and its labels is in the
[operations metrics reference](operations.md#metrics-reference).

## Point it at real databases

Moving from the demo to real databases takes six steps, starting with a checklist that's worth
going through before anything touches a production source.

### 0. Run the pre-migration checklist

Before pointing `replicare` at a production source, gather the facts below and confirm the
must-haves. The queries for each item, for both Postgres and MySQL, are in the
[before a migration checklist](operations.md#before-a-migration-checklist) in operations.

Start with sizing and traffic:

- [ ] Total database size, and row counts and sizes per table.
- [ ] Write volume over 24 hours(inserts, updates and deletes), since every change adds one row
      to `replicare`'s change tracking on the source.
- [ ] Daily and weekly low-traffic windows, for catching up and for the final cutover.

Then confirm everything that has to be true before starting:

- [ ] Every table has a primary key or a `UNIQUE` constraint(tables without one are skipped).
- [ ] Postgres: every extension the tables use exists on the target. MySQL: every table is
      InnoDB, the source is 5.7 or newer(older sources can't add `replicare`'s triggers next to
      a table's own), and the target has `local_infile` on.
- [ ] Materialised views and views aren't replicated, so there's a plan to create and refresh
      them on the target. Postgres large objects aren't replicated either.
- [ ] Schema changes and migrations are frozen for the whole replication, including online
      schema-change tools such as `pt-online-schema-change` / `gh-ost` on MySQL.
- [ ] Regular vacuum and maintenance jobs are known. Plain `VACUUM` / autovacuum is fine; heavy
      jobs that lock whole tables(`VACUUM FULL`, `CLUSTER`, `pg_repack`, MySQL `OPTIMIZE TABLE`)
      are paused during the initial copy, and kept out of peak hours afterwards.
- [ ] No long-open sessions(`idle in transaction`) on the source.
- [ ] Source and target versions checked(the target on the same major version or newer), grants
      in place, and disk room on the source for the change-tracking tables to grow.
- [ ] A plan for sequence / identity counters at cutover(`replicare reseed-sequences`).

### 1. Prepare the target schema

`replicare` only replicates data; it never creates or migrates target tables. So the target
tables have to be created / migrated first, outside `replicare`, matching the source columns that
should be replicated. The types have to be compatible, and pre-flight reports it if they aren't.

The quickest way to match the source is to dump its structure only(no data) to a file, and then
load that file on each target:

```bash
# Postgres — 1) dump the source structure (no ownership/privilege noise)
pg_dump --schema-only --no-owner --no-privileges -h SRC_HOST -p SRC_PORT -U USER -d DB > schema.sql
# 2) load it into the target
psql -h TARGET_HOST -p TARGET_PORT -U USER -d DB -f schema.sql

# narrow it: -n public (one schema), -t public.orders (one table)
```

```bash
# MySQL — 1) dump  (-P is the PORT; lowercase -p is the password prompt)
mysqldump --no-data -h SRC_HOST -P SRC_PORT -u USER -p DB > schema.sql
# 2) load
mysql -h TARGET_HOST -P TARGET_PORT -u USER -p DB < schema.sql
```

These tools ask for a password when the server requires one; Postgres prompts interactively, and
`mysqldump -p` means "prompt me". We'd recommend never putting the password in the command itself,
since it leaks into the shell history and `ps`. Store it in `~/.pgpass`(Postgres, `chmod 600`) or
`~/.my.cnf`(MySQL) instead. A one-step pipe(`pg_dump … | psql …`) can't prompt at all, because the
password step and the data stream collide; for that, set up `~/.pgpass` / `PGPASSWORD` first, or
stick with the two-step form above.

For active-active, run this against every node before starting, and apply any later schema change
to every node first(see [active-active replication](active-active.md)).

### 2. Grant least-privilege roles

Run the grant scripts, either editing the role / schema / db values in them or passing them with
`-v`:

```sh
psql -v ON_ERROR_STOP=1 -v role=replicare -v db=app -v app_schema=public \
  -f deploy/grants-source.sql "host=SOURCE dbname=app user=admin"
psql -v ON_ERROR_STOP=1 -v role=replicare -v db=warehouse -v app_schema=public \
  -f deploy/grants-target.sql "host=TARGET dbname=warehouse user=admin"
```

The source role only needs `CREATE` on the database(or a pre-created `replicare` schema that it
owns) plus `USAGE` / `TRIGGER` / `SELECT`, and the target role only needs DML. The details are in
[configuration.md](configuration.md) and
[`../deploy/`](https://github.com/rudimk/replicare/tree/main/deploy).

For MySQL, use [`../deploy/grants-source-mysql.sql`](https://github.com/rudimk/replicare/blob/main/deploy/grants-source-mysql.sql)
and [`../deploy/grants-target-mysql.sql`](https://github.com/rudimk/replicare/blob/main/deploy/grants-target-mysql.sql)
instead. There, the source role needs `SELECT` and `TRIGGER` on the replicated tables, as well as
ownership of the `replicare` capture database; the target role needs the four DML verbs plus
`CREATE TEMPORARY TABLES`. The [MySQL engine page](mysql.md) has more.

For Redis, use the ACL presets
[`../deploy/acl-source-redis.txt`](https://github.com/rudimk/replicare/blob/main/deploy/acl-source-redis.txt) and
[`../deploy/acl-target-redis.txt`](https://github.com/rudimk/replicare/blob/main/deploy/acl-target-redis.txt). The
source role is read-only(`+scan +dump +pttl +exists +type +memory|usage`), and the target role
needs `+restore +del +scan +exists +module|list`. The [Redis engine page](redis.md) has more.

> ⚠️ `RESTORE` is the one foot-gun here. It's `@dangerous`, so it has to be granted explicitly
> with `+restore`.

### 3. Write a config

Start from a copy of [`../examples/replicare.yml`](https://github.com/rudimk/replicare/blob/main/examples/replicare.yml),
and edit the connection blocks and the sync's table selection. A minimal config looks like this:

```yaml
state_store:
  engine: postgres
  postgres: { host: db, port: 5432, database: replicare_state, user: replicare, password: "${PW}", sslmode: require }
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
    include: ["public.*"]
    exclude: ["*_audit"]
```

Tables are chosen with `include` and `exclude`, which are both lists of `schema.table` glob
patterns; `exclude` is applied on top of `include`. To drop one specific table, name it in full
under `exclude`:

```yaml
    include: ["public.*"]            # everything in public…
    exclude: ["public.audit_log"]    # …except this one table
```

Patterns work too, so `*_audit` excludes every table whose name ends in `_audit`. Selection is
read at daemon start, so changing it means a restart. Excluding a table only stops replicating
it; rows already on the target are left in place, not deleted. If another replicated table has a
foreign key pointing at the excluded one, `replicare` warns about the dangling reference, since
the target must already satisfy that parent. For Redis, selection uses key patterns instead of
`schema.table`.

Secrets like `${PW}` are read from the environment. Every field is listed in the
[configuration reference](configuration.md).

For a MySQL sync, set `engine: mysql` with `mysql:` blocks(see the
[MySQL connection block](configuration.md#mysql-connection-block)); the state store stays on
Postgres. A ready-to-run example is in
[`../examples/demo-mysql/config.yml`](https://github.com/rudimk/replicare/blob/main/examples/demo-mysql/config.yml).

For a Redis sync, set `engine: redis` with `redis:` blocks(see the
[Redis connection block](configuration.md#redis-connection-block)). The state store still stays
on Postgres; Redis writes nothing to the source, so *all* of its durable state lives there.
Selection uses key-pattern globs(`include: ["app:*"]`) rather than `schema.table`. The
[Redis engine page](redis.md) covers the rest.

### 4. Validate, then run

Validate the config first, and then start replicating:

```sh
export PW=...                          # the replicare role's password
replicare validate config.yml          # pre-flight: no changes, reports problems
replicare run config.yml               # start replicating (Ctrl-C / SIGTERM to stop cleanly)
```

`validate` connects read-only and classifies every column pair; fix any BLOCK
findings(incompatible / missing types) before running. Once it's running, check progress at any
time with `replicare status config.yml`.

There are a few more commands that come up later on. `replicare capture install|remove`
pre-provisions(or tears down) the source-side trigger capture out of band.
`replicare reseed --sync <s> --target <t>` forces a laggard or diverged target to re-copy its
*data*. Finally - when a one-way(DR) target is promoted, `replicare reseed-sequences --sync <s>`
advances its identity / sequence counters to `max(id)+1` before it takes writes; otherwise
`nextval()` collides with the copied rows. That last one is passive / DR only, and it refuses an
active-active mesh. The [CLI reference](cli.md) has every command.

### 5. Deploy

Install the binary and the sample
[systemd unit](https://github.com/rudimk/replicare/blob/main/deploy/replicare.service), run the
[Docker image](https://github.com/rudimk/replicare#install), or deploy on Kubernetes with the Helm
chart, as described in [Kubernetes(Helm)](kubernetes.md). [operations.md](operations.md) covers
tuning, monitoring, retention / reseed and troubleshooting.

## Where to go next

- [Configuration reference](configuration.md) - every config field.
- [CLI reference](cli.md) - every command.
- [Operations](operations.md) - tuning, health signals, retention and restarts.
- [Troubleshooting](troubleshooting.md) - common problems and how to fix them.
- [Architecture](architecture.md) - how the pipeline works end to end(capture, copy, stream,
  apply, then checkpoint).
- [Postgres engine](postgres.md) - the reference engine: trigger CDC, faithful `COPY`, FK
  components and least-privilege grants.
- [MySQL engine](mysql.md) - MySQL to MySQL specifics, plus a
  [runnable MySQL demo](https://github.com/rudimk/replicare/tree/main/examples/demo-mysql).
- [Redis engine](redis.md) - Redis to Redis specifics: SCAN reconciliation, the delete sweep, TTL,
  cluster and ACLs.
