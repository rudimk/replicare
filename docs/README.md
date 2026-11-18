# replicare

**replicare** is an open-source data replication daemon. Point it at a **source** database and
one or more **target** databases: it makes an **initial copy** of the selected data, then keeps
replicating **inserts, updates and deletes** so the targets stay in step with the source.

It is written in Go and ships as a single static binary. **Postgres**, **MySQL** and **Redis**
are supported, each replicating to the same engine. The source code is on
[GitHub](https://github.com/rudimk/replicare).

## Why it's different

- **No privileged change stream required.** replicare does not need the Postgres WAL, the MySQL
  binlog or a Redis change feed. On Postgres and MySQL it captures changes with ordinary,
  grantable privileges using triggers; on Redis it uses `SCAN` reconciliation and writes nothing
  to the source. So it works on managed databases and old servers where logical replication
  isn't available.
- **Least privilege.** On the source it needs `CREATE` on the database (or a pre-created schema
  it owns) plus `USAGE`, `TRIGGER` and `SELECT`. On the target, just `SELECT`, `INSERT`, `UPDATE`
  and `DELETE` on tables that already exist. See the
  [grant scripts](https://github.com/rudimk/replicare/tree/main/deploy).
- **Faithful transport, never transform.** Values move verbatim and are validated by the target
  database itself. replicare never coerces, truncates or "repairs" data: on a type or constraint
  error it stops loudly instead of corrupting silently.
- **Handles very large and very old sources.** Change capture starts first, then a chunked,
  parallel copy runs while changes queue up and reconcile, so there is no single frozen snapshot
  and "too big to snapshot" isn't a problem. Source-side SQL stays compatible back to Postgres 9.x.
- **Observable.** Prometheus metrics, a status HTTP API, structured logs and OpenTelemetry traces,
  plus a `status` command that works without any monitoring stack.

## Where to start

- **[Getting started](getting-started.md)** — the pre-migration checklist, then from the demo to
  your own databases.
- **[Architecture](architecture.md)** — how the pipeline works end to end, and the four ideas that
  make it non-obvious.
- **[Configuration reference](configuration.md)** — every config field, with types and defaults.
- **[CLI reference](cli.md)** — every command and flag, with examples.
- **[Operations](operations.md)** — the migration checklist, tuning, health signals, vacuum and
  maintenance, retention and reseed, restarts.
- **[Kubernetes (Helm)](kubernetes.md)** and the [Helm chart reference](helm-chart.md).
- **Monitoring** — the [Grafana dashboard](grafana-dashboard.md) and
  [scraping metrics with an OpenTelemetry Collector or Prometheus](otel-collector.md).
- **Verifying data integrity** — standalone source↔target checks for when the `verify` command
  can't be run: [Postgres](integrity-checks-postgres.md) ·
  [MySQL](integrity-checks-mysql.md) · [Redis](integrity-checks-redis.md).
- **[Troubleshooting](troubleshooting.md)** — common problems and fixes.

### Engines

The shared pages describe the relational baseline using Postgres; each engine page covers that
engine's specifics.

- **[Postgres](postgres.md)** — the reference engine: trigger capture, faithful `COPY`, foreign-key
  groups, cyclic and self-referencing tables, least-privilege grants.
- **[MySQL](mysql.md)** — MySQL→MySQL: InnoDB and `local_infile` requirements and the two
  operational wrinkles.
- **[Redis](redis.md)** — Redis→Redis: capture-less `SCAN` reconciliation, the delete sweep,
  value-faithful `DUMP`/`RESTORE`, TTL, cluster and ACLs.

### Active-active

- **[Active-active: operating rules](active-active.md)** — the rules a multi-node setup must
  follow, in plain terms.
- **[Multi-master design](multi-master.md)** — the `clusters:` config, loop suppression and
  last-write-wins conflict resolution, shipped for Postgres, MySQL and Redis.

### Elsewhere in the repository

Deployment files live in
[`deploy/`](https://github.com/rudimk/replicare/tree/main/deploy): a sample systemd unit, the
least-privilege grant SQL and the Redis ACL presets. Runnable end-to-end demos are in
[`examples/`](https://github.com/rudimk/replicare/tree/main/examples)
([Postgres](https://github.com/rudimk/replicare/tree/main/examples/demo),
[MySQL](https://github.com/rudimk/replicare/tree/main/examples/demo-mysql),
[Redis](https://github.com/rudimk/replicare/tree/main/examples/demo-redis)); one config that runs
a Postgres and a Redis pipeline together is
[`examples/postgres-and-redis.yml`](https://github.com/rudimk/replicare/blob/main/examples/postgres-and-redis.yml).
Load-and-verify harnesses that drive heavy write traffic and check the target converges are
documented for [Postgres](load-testing-postgres.md), [MySQL](load-testing-mysql.md) and
[Redis](load-testing-redis.md); see
[Operations → Load & convergence testing](operations.md#load--convergence-testing).
