# `replicare`

`replicare` is an open-source data replication daemon. It's pointed at a source database and
one or more target databases; it makes an initial copy of the selected data, then keeps
replicating inserts, updates and deletes so the targets stay in step with the source. This page
covers what sets it apart, and where to find everything else in these docs.

It's written in Go and ships as a single static binary. Postgres, MySQL and Redis are supported,
each replicating to the same engine. The source code is on
[GitHub](https://github.com/rudimk/replicare).

## Why is it different?

Most replication tools start from the database's own change stream, and that's exactly what
isn't available on a lot of managed services and older servers. `replicare` is built around a
handful of choices that follow from not having one:

1. It doesn't need a privileged change stream. `replicare` doesn't use the Postgres WAL, the
   MySQL binlog or a Redis change feed. On Postgres and MySQL it captures changes with triggers,
   using ordinary, grantable privileges; on Redis it uses `SCAN` reconciliation and writes
   nothing to the source. This means it works on managed databases and old servers where logical
   replication isn't available.
2. It runs with the least privilege it can. On the source, it needs `CREATE` on the
   database(or a pre-created schema it owns) plus `USAGE`, `TRIGGER` and `SELECT`. On the
   target, it only needs `SELECT`, `INSERT`, `UPDATE` and `DELETE` on tables that already exist.
   The [grant scripts](https://github.com/rudimk/replicare/tree/main/deploy) set all of this up.
3. It moves values faithfully, and never transforms them. Values move verbatim and are validated
   by the target database itself. `replicare` never coerces, truncates or "repairs" data; on a type
   or constraint error it stops loudly, rather than corrupting anything silently.
4. It handles very large and very old sources. Change capture starts first, and then a chunked,
   parallel copy runs while changes queue up and reconcile. There's no single frozen snapshot,
   so a source that's too big to snapshot isn't a problem. Source-side SQL stays compatible back
   to Postgres 9.x.
5. Finally - it's observable. There are Prometheus metrics, a status HTTP API, structured logs
   and OpenTelemetry traces, as well as a `status` command that works without any monitoring
   stack at all.

## Where to start

The pages below cover everything from a first run through to running `replicare` in production:

- [Getting started](getting-started.md) - the pre-migration checklist, then a walk from the demo
  through to replicating real databases.
- [Architecture](architecture.md) - how the pipeline works end to end, and the four ideas the
  design is built on.
- [Configuration reference](configuration.md) - every config field, with types and defaults.
- [CLI reference](cli.md) - every command and flag, with examples.
- [Operations](operations.md) - the migration checklist, tuning, health signals, vacuum &
  maintenance, retention & reseed, and restarts.
- [Kubernetes(Helm)](kubernetes.md) and the [Helm chart reference](helm-chart.md).
- Monitoring - the [Grafana dashboard](grafana-dashboard.md), and
  [scraping metrics with an OpenTelemetry Collector or Prometheus](otel-collector.md).
- Verifying data integrity - standalone checks comparing source and target, for when the `verify`
  command can't be run: [Postgres](integrity-checks-postgres.md),
  [MySQL](integrity-checks-mysql.md) and [Redis](integrity-checks-redis.md).
- [Troubleshooting](troubleshooting.md) - common problems and how to fix them.

### Engines

The shared pages describe the relational baseline using Postgres, and each engine page covers
that engine's specifics:

- [Postgres](postgres.md) - the reference engine. Trigger capture, faithful `COPY`, foreign-key
  groups, cyclic and self-referencing tables, and least-privilege grants.
- [MySQL](mysql.md) - MySQL to MySQL. The InnoDB and `local_infile` requirements, and the two
  operational wrinkles to be aware of.
- [Redis](redis.md) - Redis to Redis. Capture-less `SCAN` reconciliation, the delete sweep,
  value-faithful `DUMP` / `RESTORE`, TTL, cluster and ACLs.

### Active-active

[Active-active replication](active-active.md) covers writes on any node, for Postgres, MySQL and
Redis; it describes setting up a cluster, how conflicting changes are settled, and the two rules a
multi-node setup has to follow.

### Elsewhere in the repository

Deployment files live in
[`deploy/`](https://github.com/rudimk/replicare/tree/main/deploy); there's a sample systemd unit,
the least-privilege grant SQL and the Redis ACL presets. Runnable end-to-end demos are in
[`examples/`](https://github.com/rudimk/replicare/tree/main/examples)(for
[Postgres](https://github.com/rudimk/replicare/tree/main/examples/demo),
[MySQL](https://github.com/rudimk/replicare/tree/main/examples/demo-mysql) and
[Redis](https://github.com/rudimk/replicare/tree/main/examples/demo-redis)), and a single config
that runs a Postgres and a Redis pipeline together is in
[`examples/postgres-and-redis.yml`](https://github.com/rudimk/replicare/blob/main/examples/postgres-and-redis.yml).

We also have load-and-verify harnesses, which drive heavy write traffic and check that the target
converges. They're documented for [Postgres](load-testing-postgres.md),
[MySQL](load-testing-mysql.md) and [Redis](load-testing-redis.md), with an overview in the
[load & convergence testing](operations.md#load--convergence-testing) section of operations.
