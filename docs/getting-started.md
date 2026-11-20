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

For a big first copy on Postgres, it's worth dropping the target's plain indexes at this point and
rebuilding them once the copy is done; see
[speeding up a big first copy](#speeding-up-a-big-first-copy).

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

## Speeding up a big first copy

On a large Postgres target, the indexes are what slow the first copy down most. Every row copied
has to go into every index on its table, and once those indexes are bigger than the target's
memory, most of the copy's time goes on waiting for disk reads. A copy that starts at thousands
of rows a second can drop to a few hundred.

The way round it is to drop the target's plain(non-unique) indexes before the copy and rebuild
them once it's finished. Primary keys and unique indexes stay, since `replicare` needs them to
apply changes. Nothing here touches the source, and it's all plain SQL, run with `psql` as an
admin on the target. `\gexec` is a `psql` feature that runs each row of a query's result as a
statement.

### 1. Save the index definitions

Do this after the target tables are created and before `replicare` starts. It saves every plain
index's definition in a small table of its own, in a separate schema that `replicare` never
touches:

```sql
CREATE SCHEMA IF NOT EXISTS index_backup;

CREATE TABLE index_backup.dropped AS
SELECT t.relname AS table_name,
       i.relname AS index_name,
       regexp_replace(pg_get_indexdef(x.indexrelid), '^CREATE INDEX', 'CREATE INDEX CONCURRENTLY') AS recreate_sql,
       'DROP INDEX ' || quote_ident(n.nspname) || '.' || quote_ident(i.relname) AS drop_sql
FROM pg_index x
JOIN pg_class i     ON i.oid = x.indexrelid
JOIN pg_class t     ON t.oid = x.indrelid
JOIN pg_namespace n ON n.oid = t.relnamespace
WHERE n.nspname = 'public'
  AND NOT x.indisunique
  AND NOT x.indisprimary
  AND NOT EXISTS (SELECT 1 FROM pg_constraint c WHERE c.conindid = x.indexrelid);
  -- to cover only the biggest tables, add: AND t.relname IN ('orders', 'events')

SELECT table_name, index_name FROM index_backup.dropped ORDER BY table_name, index_name;
```

### 2. Drop them

```sql
SELECT drop_sql FROM index_backup.dropped \gexec
```

On empty tables this is instant. To drop them while a copy is already running, use
`DROP INDEX CONCURRENTLY` instead, so the copy isn't blocked: run
`SELECT replace(drop_sql, 'DROP INDEX', 'DROP INDEX CONCURRENTLY') FROM index_backup.dropped \gexec`.

### 3. Run the copy

Start `replicare` as usual and wait until every table shows streaming in `replicare status`.
For tables linked in a loop, a second pass fills the links in after the rows are copied, and it's
much faster without the indexes, so it's worth waiting for streaming rather than for 100%.

### 4. Rebuild the indexes

`replicare` can keep running: `CREATE INDEX CONCURRENTLY` lets writes carry on while it builds.
Large tables can take a while, so run this somewhere that won't drop, such as a `tmux` session on
a machine or pod that stays up(not the `replicare` pod itself, which a deploy would restart):

```sql
SET statement_timeout = 0;
SET maintenance_work_mem = '2GB';
SET max_parallel_maintenance_workers = 4;

SELECT recreate_sql
FROM index_backup.dropped
ORDER BY (index_name ILIKE '%id_idx') DESC, table_name, index_name
\gexec
```

The two settings only last for this `psql` session, and change nothing else on the database:

- `maintenance_work_mem` is the memory each index build may use for sorting. The indexes build
  one at a time, so this is the most the rebuild uses at once. About an eighth of the instance's
  RAM is a safe size: `2GB` from 16 GB of RAM up, `1GB` on 8 GB, `512MB` on 4 GB. Above `2GB` it
  makes little difference.
- `max_parallel_maintenance_workers` lets each build use several CPU cores, which speeds up the
  big tables most. `4` suits an instance with 4 or more cores; `2` on a smaller one. The memory
  above is shared between the workers, not multiplied.

The order puts the indexes on linking columns(names ending in `id_idx`) first. Deleting a parent
row is slow while the child table has no index on the column that points at it, so those matter
most to streaming. Streaming carries on during the rebuild, a little slower since it shares the
disk.

#### Watching the rebuild

`psql` only prints `CREATE INDEX` once each index is done, so a big table can look stuck. To see
how far the current build has got, open a second `psql` session(in `tmux`, a new window with
Ctrl-b then c, leaving the rebuild's window alone) and run:

```sql
SELECT p.pid,
       c.relname AS table_name,
       i.relname AS index_name,
       p.phase,
       round(100.0 * p.blocks_done / nullif(p.blocks_total, 0), 1) AS blocks_pct,
       round(100.0 * p.tuples_done / nullif(p.tuples_total, 0), 1) AS rows_pct,
       p.current_locker_pid AS waiting_on_pid
FROM pg_stat_progress_create_index p
JOIN pg_class c ON c.oid = p.relid
LEFT JOIN pg_class i ON i.oid = p.index_relid;
```

Each index goes through these phases, in order:

| Phase | What it's doing |
|---|---|
| `building index: scanning table` | Reading the whole table. `blocks_pct` shows how far |
| `building index: sorting live tuples` | Sorting what it read. No percentage here |
| `building index: loading tuples in tree` | Writing the index. `rows_pct` shows how far |
| `index validation: scanning index`, `sorting tuples`, `scanning table` | Reading the table a second time, to pick up rows written while the index was building. About as long as the first read |
| `waiting for old snapshots` | Waiting for transactions that started before the build to finish. Usually short |

The first table read is about a quarter to a third of each index's time, so its pace gives a fair
guess at the rest. A big table with several indexes takes about that long for each one.

A phase starting with `waiting for` that doesn't move on means another session is in the way.
`waiting_on_pid` names it, and this shows what it is:

```sql
SELECT pid,
       state,
       wait_event_type,
       wait_event,
       now() - xact_start AS open_for,
       left(query, 100) AS query
FROM pg_stat_activity
WHERE pid IN (SELECT current_locker_pid FROM pg_stat_progress_create_index);
```

No rows means nothing is in the way: `waiting_on_pid` is `0` whenever the build isn't waiting.
Autovacuum steps aside by itself when a build needs its table(except a run to prevent
transaction ID wraparound, which is rare), and `replicare`'s own transactions are short, so a long
wait is usually an application session left open in a transaction.

#### How many are done, and how many are left

The query above shows only the index being built right now. This counts the whole rebuild:

```sql
SELECT count(*) FILTER (WHERE x.indisvalid)                                                 AS done,
       count(*) FILTER (WHERE i.oid IS NOT NULL AND NOT x.indisvalid AND p.pid IS NOT NULL) AS building_now,
       count(*) FILTER (WHERE i.oid IS NOT NULL AND NOT x.indisvalid AND p.pid IS NULL)     AS broken,
       count(*) FILTER (WHERE i.oid IS NULL)                                                AS not_started,
       count(*)                                                                             AS total
FROM index_backup.dropped d
LEFT JOIN pg_class i ON i.relname = d.index_name AND i.relnamespace = 'public'::regnamespace
LEFT JOIN pg_index x ON x.indexrelid = i.oid
LEFT JOIN pg_stat_progress_create_index p ON p.index_relid = i.oid;
```

| Column | Meaning |
|---|---|
| `done` | Built and in use |
| `building_now` | Being built right now: `1` while the rebuild runs |
| `broken` | A build that was interrupted. It should be `0`; any left over are fixed in step 5, once the rebuild has finished |
| `not_started` | Still waiting their turn |

And this lists the ones still waiting, in the order the rebuild will get to them:

```sql
SELECT d.table_name, d.index_name
FROM index_backup.dropped d
LEFT JOIN pg_class i ON i.relname = d.index_name AND i.relnamespace = 'public'::regnamespace
WHERE i.oid IS NULL
ORDER BY (d.index_name ILIKE '%id_idx') DESC, d.table_name, d.index_name;
```

The indexes on the biggest tables are the slow ones; the rest usually go much faster.

### 5. Check every index is back and usable

An index build that's interrupted leaves the index behind marked invalid: unused by queries but
still slowing writes. This lists any that are missing or broken:

```sql
SELECT d.table_name, d.index_name,
       CASE WHEN i.oid IS NULL THEN 'missing' ELSE 'broken' END AS problem,
       d.recreate_sql
FROM index_backup.dropped d
LEFT JOIN pg_class i ON i.relname = d.index_name AND i.relnamespace = 'public'::regnamespace
LEFT JOIN pg_index x ON x.indexrelid = i.oid
WHERE i.oid IS NULL OR NOT x.indisvalid;
```

For a missing one, run its `recreate_sql`. For a broken one, drop it first with
`DROP INDEX CONCURRENTLY`, then run its `recreate_sql`. Repeat until the query returns nothing.

### 6. Refresh statistics and clean up

After a load this size, Postgres's statistics for these tables are out of date and queries can
pick poor plans until they catch up. `ANALYZE` only samples each table, so it's quick:

```sql
SELECT DISTINCT 'ANALYZE public.' || quote_ident(table_name) FROM index_backup.dropped \gexec

DROP SCHEMA index_backup CASCADE;
```

Only drop `index_backup` once step 5 comes back empty; until then, it's the only record of the
indexes.

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
