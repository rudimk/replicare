# replicare documentation

- **[Getting started](getting-started.md)** — from the demo to your own databases in a few minutes.
- **[Architecture](architecture.md)** — how the pipeline works end to end, and the four ideas that make it non-obvious.
- **[Configuration reference](configuration.md)** — every config field, with types and defaults.
- **[CLI reference](cli.md)** — every command and flag, with examples.
- **[Operations](operations.md)** — tuning, health signals, retention/reseed, restarts, ownership.
- **[Troubleshooting](troubleshooting.md)** — common problems and fixes.

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
[MySQL](../examples/demo-mysql/), [Redis](../examples/demo-redis/)).

The design rationale and full decision log are in [`../CLAUDE.md`](../CLAUDE.md).
