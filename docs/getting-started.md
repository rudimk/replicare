# Getting started

This walks you from zero to a running replication in a few minutes, then explains
how to point replicare at your own databases.

## What replicare does

You give replicare a **source** database and one or more **target** databases and
a list of tables. It:

1. installs lightweight capture (triggers) on the source,
2. copies the selected data to the targets (chunked and parallel), then
3. streams every insert/update/delete continuously to keep the targets converged.

No WAL access, no `REPLICATION` role, no `wal_level` change — just ordinary,
grantable privileges. See [`../CLAUDE.md`](../CLAUDE.md) for the why.

## Try the demo (2 minutes)

The fastest way to see it work end to end:

```sh
task docker                        # build the replicare image
cd examples/demo && docker compose up
```

This starts a source Postgres (seeded with `public.orders`), a target Postgres,
and replicare copying then streaming between them. In another shell:

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

`Ctrl-C` then `docker compose down -v` to clean up.

### Verifying the metrics

`/metrics` shows many `replicare_*` series once a sync is streaming, not just
one. To eyeball the series that only populate at runtime:

```sh
curl -s localhost:9090/metrics | grep -E \
  'replicare_(rows_copied_total|apply_batch_seconds|throughput_rows_per_second|replication_lag_seconds|table_phase_info|target_up)'
```

- `replicare_rows_copied_total` is set once the initial copy runs.
- `replicare_apply_batch_seconds`, `replicare_replication_lag_seconds`, and
  `replicare_table_phase_info` refresh on every streaming drain pass.
- `replicare_throughput_rows_per_second` is only non-zero on a pass that actually
  applied deltas — make a change on the source (the `INSERT/UPDATE/DELETE` above)
  and re-scrape; a caught-up idle sync correctly reports `0` here.

The full list of every metric, its type, and its labels is in
[operations.md](operations.md#metrics-reference).

## Point it at your own databases

### 1. Prepare the target schema

replicare replicates **data only** — it never creates or migrates target tables.
Create/migrate the target tables yourself first, matching the source columns you
want to replicate. (Types must be compatible; pre-flight will tell you if not.)

### 2. Grant least-privilege roles

Run the grant scripts (edit the role/schema/db values, or pass them with `-v`):

```sh
psql -v ON_ERROR_STOP=1 -v role=replicare -v db=app -v app_schema=public \
  -f deploy/grants-source.sql "host=SOURCE dbname=app user=admin"
psql -v ON_ERROR_STOP=1 -v role=replicare -v db=warehouse -v app_schema=public \
  -f deploy/grants-target.sql "host=TARGET dbname=warehouse user=admin"
```

The source role needs only `CREATE`-on-db (or a pre-created owned `replicare`
schema) plus `USAGE`/`TRIGGER`/`SELECT`; the target role needs only DML. See
[configuration.md](configuration.md) and
[`../deploy/`](../deploy/) for details.

For **MySQL**, use [`../deploy/grants-source-mysql.sql`](../deploy/grants-source-mysql.sql)
and [`../deploy/grants-target-mysql.sql`](../deploy/grants-target-mysql.sql)
instead: the source role needs `SELECT`+`TRIGGER` on the replicated tables and
ownership of the `replicare` capture database; the target role needs the four DML
verbs plus `CREATE TEMPORARY TABLES`. See [the MySQL engine page](mysql.md).

For **Redis**, use the ACL presets
[`../deploy/acl-source-redis.txt`](../deploy/acl-source-redis.txt) and
[`../deploy/acl-target-redis.txt`](../deploy/acl-target-redis.txt): the source role
is read-only (`+scan +dump +pttl +exists +type +memory|usage`); the target role
needs `+restore +del +scan +exists +module|list`. Note the foot-gun — `RESTORE` is
`@dangerous` and must be granted explicitly with `+restore`. See
[the Redis engine page](redis.md).

### 3. Write a config

Copy [`../examples/replicare.yml`](../examples/replicare.yml) and edit the
connection blocks and the sync's table selection. Minimal example:

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

Secrets like `${PW}` are read from the environment. Full field reference:
[configuration.md](configuration.md).

For a **MySQL** sync, set `engine: mysql` with `mysql:` blocks (see the
[MySQL connection block](configuration.md#mysql-connection-block)); the state
store stays on Postgres. A ready-to-run example is in
[`../examples/demo-mysql/config.yml`](../examples/demo-mysql/config.yml).

For a **Redis** sync, set `engine: redis` with `redis:` blocks (see the
[Redis connection block](configuration.md#redis-connection-block)); the state store
still stays on Postgres (Redis writes nothing to the source, so *all* its durable
state lives there). Selection uses **key-pattern globs** (`include: ["app:*"]`)
rather than `schema.table`. See [the Redis engine page](redis.md).

### 4. Validate, then run

```sh
export PW=...                          # the replicare role's password
replicare validate config.yml          # pre-flight: no changes, reports problems
replicare run config.yml               # start replicating (Ctrl-C / SIGTERM to stop cleanly)
```

`validate` connects read-only and classifies every column pair — fix any **BLOCK**
findings (incompatible/missing types) before running. Once running, check
progress any time with `replicare status config.yml`.

Two more commands you'll reach for later: **`replicare capture install|remove`** to
pre-provision (or tear down) the source-side trigger capture out of band, and
**`replicare reseed --sync <s> --target <t>`** to force a laggard or diverged target
to re-copy. See the [CLI reference](cli.md).

### 5. Deploy

Install the binary and the sample [systemd unit](../deploy/replicare.service), or
run the [Docker image](../README.md#install). See [operations.md](operations.md)
for tuning, monitoring, retention/reseed, and troubleshooting.

## Next

- [Configuration reference](configuration.md) — every config field.
- [CLI reference](cli.md) — every command.
- [Operations](operations.md) — tuning, health signals, retention, restarts.
- [Troubleshooting](troubleshooting.md) — common problems and fixes.
- [Architecture](architecture.md) — how the pipeline works end to end (capture → copy → stream → apply → checkpoint).
- [Postgres engine](postgres.md) — the reference engine: trigger CDC, faithful `COPY`, FK components, least-privilege grants.
- [MySQL engine](mysql.md) — MySQL→MySQL specifics, plus a [runnable MySQL demo](../examples/demo-mysql/).
- [Redis engine](redis.md) — Redis→Redis specifics: SCAN reconciliation, the delete sweep, TTL, cluster, and ACLs.
