# replicare documentation

- **[Getting started](getting-started.md)** — from the demo to your own databases in a few minutes.
- **[Architecture](architecture.md)** — how the pipeline works end to end, and the four ideas that make it non-obvious.
- **[Configuration reference](configuration.md)** — every config field, with types and defaults.
- **[CLI reference](cli.md)** — every command and flag, with examples.
- **[Operations](operations.md)** — tuning, health signals, retention/reseed, restarts, ownership.
- **Verifying data integrity** — standalone source↔target checks for when you can't
  reach the pod (the no-`verify` fallback):
  [Postgres](integrity-checks-postgres.md) ·
  [MySQL](integrity-checks-mysql.md) ·
  [Redis](integrity-checks-redis.md).
- **[Kubernetes (Helm)](kubernetes.md)** — deploy the daemon with the Helm chart.
- **[Troubleshooting](troubleshooting.md)** — common problems and fixes.

### Advanced topologies

- **[Multi-master replication](multi-master.md)** — active-active (multi-master)
  across the engines: the `nodes:`/`clusters:` config surface, loop suppression, and
  HLC last-write-wins conflict resolution with GC'd tombstones. **Shipped for Postgres
  and MySQL** — a full N-node mesh runs on both today. **Redis is not yet supported**
  (a Redis `clusters:` entry is rejected at config load until the Redis mesh lands);
  HA leader election is also still pending. The note also records why naive
  bidirectional wiring breaks and the invariants that keep the one-way path unchanged.

### Engine pages

Postgres, MySQL, and Redis are all shipped. The shared docs above describe the
relational baseline using Postgres; each engine page covers that engine's specifics.

- **[Postgres engine](postgres.md)** — the reference engine: trigger CDC, faithful
  `COPY`, FK components, the effectively-exactly-once bonus, least-privilege grants.
- **[MySQL engine](mysql.md)** — MySQL→MySQL: InnoDB/`local_infile` requirements and
  the two operational wrinkles.
- **[Redis engine](redis.md)** — Redis→Redis: capture-less SCAN reconciliation, the
  durable delete sweep, value-faithful `DUMP`/`RESTORE`, TTL, cluster, and ACLs.

Deployment artifacts live in [`../deploy/`](../deploy/): a sample systemd unit and
the least-privilege grant SQL / Redis ACL presets. Runnable end-to-end demos are in
[`../examples/`](../examples/) ([Postgres](../examples/demo/),
[MySQL](../examples/demo-mysql/), [Redis](../examples/demo-redis/)); one config that
runs a Postgres and a Redis pipeline together is
[`../examples/postgres-and-redis.yml`](../examples/postgres-and-redis.yml).

Load-and-verify harnesses that drive high-volume churn and assert the target
converges live in [`../test/loadgen`](../test/loadgen/README.md) (Postgres) and
[`../test/loadgen-redis`](../test/loadgen-redis/README.md) (Redis); see
[Operations → Load & convergence testing](operations.md#load--convergence-testing).

The design rationale and full decision log are in [`../CLAUDE.md`](../CLAUDE.md).
