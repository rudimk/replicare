# replicare

**replicare** is an open-source, high-performance data replication daemon. Point it at a
**source** database and one or more **target** databases; it performs an **initial copy** of the
selected data, then continuously replicates **inserts, updates, and deletes** to keep the targets
converged with the source.

It is written in Go and ships as a single static binary. **Postgres**, **MySQL**, and **Redis** are
all supported today.

## Why it's different

- **No privileged change stream required.** replicare does **not** need the Postgres WAL, the MySQL
  binlog, or a Redis change feed. On Postgres/MySQL it captures changes with ordinary, grantable
  privileges using trigger-based CDC (Bucardo-style); on Redis it uses capture-less `SCAN`
  reconciliation (writing nothing to the source) with optional keyspace-notification acceleration.
  So it works on managed databases and old servers where logical replication isn't available.
- **Least privilege.** On the source it needs only `CREATE`-on-database (or a pre-created owned
  schema) plus `USAGE`, `TRIGGER`, and `SELECT`. On the target, just `SELECT/INSERT/UPDATE/DELETE`
  on the pre-existing tables. See [`deploy/grants-source.sql`](deploy/grants-source.sql) and
  [`deploy/grants-target.sql`](deploy/grants-target.sql).
- **Faithful transport, never transform.** Values move verbatim (text `COPY`) and are validated by
  the target's own input functions. replicare never coerces, truncates, or "repairs" data — on a
  type/constraint error it fails loudly rather than corrupting silently.
- **Handles very large and very old sources.** Capture starts first, then a chunked parallel copy
  proceeds while changes queue and reconcile — no single frozen snapshot, so "too big to snapshot"
  isn't a problem. Source SQL stays conservative back to Postgres 9.x.
- **Deeply observable.** Prometheus `/metrics`, a `/status` HTTP API, structured `slog` logs, and
  OpenTelemetry spans — including a cross-channel signal when a target degrades.

> The full design rationale and decision log live in [`CLAUDE.md`](CLAUDE.md).

## Quickstart (docker-compose demo)

```sh
task docker                      # build the replicare image
cd examples/demo && docker compose up
```

This starts a source Postgres (seeded with `public.orders`), a target Postgres, and replicare
copying then streaming between them. While it runs:

```sh
curl localhost:8080/status                 # phase, lag, progress per sync
curl localhost:9090/metrics | grep replicare_
docker compose exec source psql -U postgres -d app \
  -c "INSERT INTO public.orders VALUES (101,'live'); DELETE FROM public.orders WHERE id=1;"
docker compose exec target psql -U postgres -d warehouse -c "SELECT count(*) FROM public.orders;"
```

## Install

**Binary** (single static, CGO-free):

```sh
task dist            # -> dist/replicare
```

**Docker image** (~5 MB, `scratch`-based):

```sh
task docker          # -> replicare:latest
```

**systemd**: see [`deploy/replicare.service`](deploy/replicare.service) — a hardened sample unit
that runs `replicare run` unprivileged and gives SIGTERM time to drain and checkpoint.

## Configure

Configuration is a YAML file: an engine-neutral envelope (logging, observability, state store,
syncs, tuning) plus a typed per-engine connection block. Any string may reference the environment
as `${VAR}` or `${VAR:-default}`. See [`examples/replicare.yml`](examples/replicare.yml) for the
full annotated surface.

```yaml
state_store:                     # replicare's own progress/ownership (Postgres)
  engine: postgres
  postgres: { host: db, port: 5432, database: replicare_state, user: replicare, password: "${PW}", sslmode: require }
sources:
  app: { engine: postgres, postgres: { host: app-db, port: 5432, database: app, user: replicare, password: "${PW}", sslmode: verify-full } }
targets:
  warehouse: { engine: postgres, postgres: { host: wh, port: 5432, database: warehouse, user: replicare, password: "${PW}", sslmode: require } }
syncs:
  - name: app-to-warehouse
    source: app
    targets: [warehouse]
    include: ["public.*"]
    exclude: ["*_audit"]
```

The **target schema must already exist** — replicare replicates *data only* (§7); create/migrate
your target tables with your own tooling.

## CLI

```
replicare validate <config>            Introspect and pre-flight a config without starting
replicare run <config>                 Run the daemon (all syncs) until SIGINT/SIGTERM
replicare status <config> [--json]     Report phase, lag, progress, recent events
replicare capture install|remove <config> [--sync <name>]
replicare reseed <config> --sync <name> --target <name>
replicare version
```

`run` shuts down gracefully on SIGTERM: it finishes the in-flight drain pass, checkpoints, and
exits 0. `reseed` flags a target for a full re-copy on the running daemon's next pass.

## Observability

- **`/metrics`** — throughput, replication lag, delta backlog and oldest-unconsumed age, per-target
  reachability, purge rate, reseed count, apply-batch latency.
- **`/status`** — per sync/target/table phase, lag, initial-copy progress, needs-reseed, recent
  events (JSON).
- **`/healthz`** — liveness.
- **Logs** — structured `slog` (JSON or text).
- **Traces** — OpenTelemetry spans, exported to an OTLP/gRPC collector when
  `observability.otlp_endpoint` is set.

A degrading target — unreachable, or its delta backlog climbing toward the retention cap — is
surfaced across metrics, logs, *and* traces simultaneously, never just one channel.

See [`docs/operations.md`](docs/operations.md) for tuning, retention/reseed, and the headline
health signals.

## Documentation

- [Getting started](docs/getting-started.md) — demo to your own databases.
- [Configuration reference](docs/configuration.md) — every config field.
- [CLI reference](docs/cli.md) — every command.
- [Operations](docs/operations.md) — tuning, health, retention, restarts.
- [Troubleshooting](docs/troubleshooting.md) — common problems and fixes.
- [MySQL engine](docs/mysql.md) — the MySQL→MySQL engine.

## Status

v1 ships **Postgres → Postgres** and **MySQL → MySQL** (single source → one or more targets,
one-way; a sync never crosses engines, and both can run in one daemon). **Redis** is planned; the
engine core is designed to accommodate it. Multi-master and HA/leader-election are on the roadmap.

## License

MIT — see [`LICENSE`](LICENSE).
