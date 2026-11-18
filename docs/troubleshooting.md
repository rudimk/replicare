# Troubleshooting

This page collects the problems operators most often run into with `replicare`, from a config
that won't load through to engine-specific errors on MySQL and Redis. For each one, it explains
what's going on and why `replicare` behaves the way it does, then what to do about it.

## The config won't load (exit code 2)

Before any pre-flight runs, the config is parsed and structurally validated. Failures at this
stage exit with code 2 and a message naming the problem, and they're usually the first thing a
new user hits:

1. A `${VAR}` is unset and has no default. Set the env var, or give it a
   fallback(`${VAR:-default}`). Env expansion applies to *any* field, not just secrets.
2. An unknown YAML field. Decoding is strict and rejects typos / unknown keys, so check the
   field name against the [configuration reference](configuration.md).
3. An unknown engine, or a missing engine block. `engine: postgres|mysql|redis` has to match a
   typed block that's actually present(`postgres:` / `mysql:` / `redis:`).
4. A sync references a source / target that isn't defined, or crosses engines. A sync is
   single-engine, so its source and all of its targets have to share one engine.

`replicare validate <config>` reports all of these without connecting. Exit `2` means a
config / usage problem, while exit `1` means a clean pre-flight BLOCK or a connection
failure(see the [CLI reference](cli.md)).

## `validate` reports BLOCK findings / the daemon refuses to start

Pre-flight blocks on a target type that's incompatible or missing - for example a source
column whose type doesn't exist on the target, a missing enum / extension, or a narrowing that
would lose data. `replicare` never coerces data to fit, so it stops before starting rather than
risk changing a value.

To fix it, create the missing type / extension on the target, or widen the target column, so
that the pair is compatible, then re-run `validate`. Narrowings that are risky but allowed(for
example `int8→int4`) are only warnings, not blocks, but they're still worth heeding.

## A table isn't being replicated

There are two usual reasons. The first is that the table has no primary key or usable unique
key; it's skipped with a warning, since it can't be captured. Add a PK / unique key to it, or
exclude it deliberately.

The second is that it isn't matched by the selection. Check the sync's `include`/`exclude`
globs(`schema.table`); `replicare validate` lists the tables it can replicate and the ones it
skipped.

## `capture remove` fails to drop a trigger

Dropping a trigger needs table ownership. Postgres allows `CREATE TRIGGER` with just the
`TRIGGER` privilege, but not `DROP TRIGGER`. Either run the removal as the table owner, or have
the owner drop the `replicare_trg_*` trigger. `replicare`'s own delta / track tables and
functions are dropped without any trouble by the daemon role.

## The source's disk is filling up (delta / `replicare` schema growth)

The delta tables on the source are a high-churn queue. If a target is down or slow, its deltas
can't be purged, and the queue grows. `replicare` bounds this with retention: past
`retention.max_age`/`max_bytes`, the lagging target is reseeded and the deltas over the cap are
purged. Check where things stand with:

```sh
replicare status config.yml            # look for needs-reseed / stale cursors
curl :9090/metrics | grep -E 'delta_backlog|oldest_unconsumed|target_up'
```

If the backlog is bounded(retention is firing) but the *physical* delta table still won't
shrink, an idle transaction is pinning the source's `xmin` horizon, so autovacuum can't reclaim
the space. Find it and end it:

```sql
SELECT pid, state, xact_start, query FROM pg_stat_activity
WHERE state = 'idle in transaction' ORDER BY xact_start;
```

`replicare` bounds the *logical* growth regardless, but only ending the idle transaction lets
the source reclaim the space.

## A target is unreachable

A drain against a down target fails, and the failure shows up across every channel at once:
`replicare_target_up=0`, an `ERROR target.unreachable` log, and an errored trace span. Nothing
is lost; the daemon retries on each pass and converges when the target comes back. If it stays
down past the retention cap, it's reseeded once it recovers. The source stays protected in the
meantime, since retention runs even while the target is down.

## A sync halted with a type / constraint error

If the target rejects a value at apply time, `replicare` halts that component loudly rather
than skipping the value or guessing. That's a hard product promise; `replicare` never mangles
data. The offending row stays dirty, and re-applies automatically once the cause is fixed,
which is usually a missing target type / extension or a schema mismatch. Check the ERROR logs /
`replicare_errors_total` for the specific failure.

## "sync owned by another daemon; skipping"

Each sync runs under a single-active lock(in v1), and this means another process already owns
it. That's expected when two daemons run over the same config; only one of them drives each
sync, and the other stands by. If the lock looks stale, make sure the previous daemon actually
exited, since the advisory lock is released on disconnect.

## Traces aren't showing up

Traces are only exported when `observability.otlp_endpoint` is set, and they go to an
OTLP / *gRPC* collector(port 4317 by convention) over plaintext, so point it at a local
collector or sidecar. Metrics(`/metrics`) and the status API work independently of tracing.

## MySQL-specific issues

### The daemon refuses to connect to a MariaDB server

MariaDB isn't supported in v1; it's detected and refused at connect. Use a real MySQL(5.7+)
source and target.

### `validate` blocks on a non-InnoDB table

MyISAM / MEMORY tables turn `BEGIN/COMMIT` into a no-op, which breaks per-component atomicity on
the target and the atomic delta consume on the source. Convert the table to InnoDB.

### `validate` blocks on a target table with a secondary UNIQUE key

MySQL's `INSERT ... ON DUPLICATE KEY UPDATE` has no conflict target, so a secondary unique key
could silently rewrite the wrong row. Pre-flight blocks it rather than risk corruption; remove
the extra unique key, or replicate a table without one.

### A copy halts with "target requires local_infile=ON"

The copy / apply transport is `LOAD DATA LOCAL INFILE`, which needs `local_infile=ON` on the
*target* server. It's a system variable, and not a grant. `replicare` probes it at connect and
halts loudly when it's off, since v1 has no INSERT fallback. Enable it with
`SET GLOBAL local_infile=1` or `local-infile=ON` in my.cnf, and set `local_infile: true` in the
target block.

### The source's `replicare` database keeps growing under a long transaction

InnoDB's history list is the equivalent of Postgres' `xmin` horizon here, so end the
long-running source transaction. See
[Source footprint](operations.md#source-footprint-the-thing-to-watch) for more.

See [the MySQL engine page](mysql.md) for the full requirements and the two operational
wrinkles.

## Redis-specific issues

### `validate` blocks on a newer source and an older target (RDB version)

`RESTORE` rejects a payload whose RDB version is higher than the target's maximum, so
`replicare` blocks a newer source / older target pair up front, with an error that mentions the
version gap. Upgrade the target to at least the source's version, or replicate in the other
direction.

### `validate` blocks because a source module is missing on the target

Module-defined types(RedisJSON, RedisBloom, …) produce opaque `DUMP` payloads that only the
identical module can `RESTORE`. Load the module on the target, or exclude those keys.

### Every apply fails loudly with a `RESTORE` permission error

`RESTORE` is an `@dangerous` ACL command, and it *isn't* in the usual read-write presets. Grant
it explicitly with `+restore` on the target role(see
[`../deploy/acl-target-redis.txt`](https://github.com/rudimk/replicare/blob/main/deploy/acl-target-redis.txt)).

### A copy refuses on a big key

Above `big_key_refuse_bytes`, `replicare` blocks loudly rather than falling back to a torn,
incremental transport, which wouldn't be faithful. Raise the cap if the key genuinely has to
replicate, or exclude it. Below the cap but above `big_key_warn_bytes`, there's a big-key WARN,
but the copy goes ahead.

### A deleted key still exists on the target

Deletes aren't captured on Redis. They're found by the periodic target-vs-source sweep, so a
source `DEL` only converges once the next sweep completes, which is bounded by
`delete_sweep_interval`. Watch `replicare_delete_reconciliation_lag_seconds`, and shorten
`delete_sweep_interval` for faster delete propagation, at the cost of more scan load on the
source and target. Upserts converge faster, through the rolling reconciliation scan.

### Keyspace notifications missed a change

Notifications are *only* a lossy accelerator; they're fire-and-forget, lost on disconnect, and
node-local in cluster mode. A missed event is always caught by the next reconciliation / sweep,
and on a subscription gap `replicare` forces a full pass. They're never relied on for
correctness, and only cut latency.

### Why does a Redis to Redis sync need Postgres?

It does - `replicare`'s own state store is Postgres regardless of the data engine, since it's
v1's only `StateStore` backend. Point it at any small Postgres; it only holds control-plane
bookkeeping, and Redis writes nothing to its own source. See
[the Redis engine page](redis.md).

### The daemon refuses to connect to Dragonfly

Dragonfly is blocked in v1, since its RDB `DUMP`/`RESTORE` compatibility is unverified. Valkey
and(on a best-effort basis) KeyDB are accepted, and a server outside the Redis family is
refused at connect.

See [the Redis engine page](redis.md) for the full requirements, the two pre-flight gates, and
the operational wrinkles.

## Getting more detail

Set `logging.level: debug` and `logging.format: text` for verbose, readable output while
diagnosing a problem, and use `replicare status --json` for the full machine-readable state.
