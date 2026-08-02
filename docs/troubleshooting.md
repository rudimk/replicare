# Troubleshooting

## `validate` reports BLOCK findings / the daemon refuses to start

Pre-flight blocks on an **incompatible or missing** target type — e.g. a source
column whose type doesn't exist on the target, a missing enum/extension, or a
narrowing that would lose data. replicare never coerces data to fit, so it stops
before starting.

**Fix:** create the missing type/extension on the target, or widen the target
column, so the pair is compatible. Re-run `validate`. Risky-but-allowed
narrowings (e.g. `int8→int4`) are **warnings**, not blocks — heed them.

## A table isn't being replicated

- **No primary key / usable unique key** → the table is skipped with a warning
  (it can't be captured). Add a PK/unique key, or exclude it deliberately.
- **Not matched by the selection** → check the sync's `include`/`exclude` globs
  (`schema.table`). `replicare validate` lists the replicable and skipped tables.

## `capture remove` fails to drop a trigger

Dropping a trigger requires **table ownership** (Postgres allows `CREATE TRIGGER`
with just the `TRIGGER` privilege, but not `DROP TRIGGER`). Either run remove as
the table owner, or have the owner drop the `replicare_trg_*` trigger. replicare's
own delta/track tables and functions are dropped fine by the daemon role.

## The source's disk is filling up (delta/replicare schema growth)

The source-side delta tables are a high-churn queue. If a target is down or slow,
deltas can't purge and the queue grows. replicare bounds this with **retention**:
past `retention.max_age`/`max_bytes`, the laggard target is reseeded and the
over-cap deltas are purged. Check:

```sh
replicare status config.yml            # look for needs-reseed / stale cursors
curl :9090/metrics | grep -E 'delta_backlog|oldest_unconsumed|target_up'
```

If the backlog is bounded (retention firing) but the **physical** delta table
still won't shrink, an idle transaction is pinning the source's `xmin` horizon so
autovacuum can't reclaim. Find and end it:

```sql
SELECT pid, state, xact_start, query FROM pg_stat_activity
WHERE state = 'idle in transaction' ORDER BY xact_start;
```

replicare bounds *logical* growth regardless, but only ending the idle
transaction lets the source reclaim the space.

## A target is unreachable

A drain against a down target fails and surfaces across all channels
(`replicare_target_up=0`, an `ERROR target.unreachable` log, an errored trace
span). Nothing is lost — the daemon retries each pass and converges when the
target returns. If it stays down past the retention cap, it's reseeded on
recovery. The source is still protected meanwhile (retention runs even while the
target is down).

## A sync halted with a type/constraint error

If the target rejects a value at apply time, replicare **halts that component
loudly** rather than skipping or guessing (a hard product promise — it never
mangles data). The offending row stays dirty and re-applies automatically once
you fix the cause: usually a missing target type/extension or a schema mismatch.
Check the ERROR logs / `replicare_errors_total` for the specific failure.

## "sync owned by another daemon; skipping"

Each sync runs under a single-active lock (v1). Another process already owns it.
This is expected if you run two daemons over the same config — only one drives
each sync; the other stands by. If it's stale, ensure the previous daemon
actually exited (the advisory lock releases on disconnect).

## Traces aren't showing up

Traces export only when `observability.otlp_endpoint` is set, and to an
OTLP/**gRPC** collector (port 4317 by convention) over plaintext — point it at a
local collector or sidecar. Metrics (`/metrics`) and the status API work
independently of tracing.

## MySQL-specific issues

- **The daemon refuses to connect to a MariaDB server.** MariaDB is not supported
  in v1; it is detected and refused at connect. Use a real MySQL (5.7+) source and
  target.
- **`validate` blocks on a non-InnoDB table.** MyISAM/MEMORY tables make
  `BEGIN/COMMIT` a no-op, breaking per-component atomicity (target) and the atomic
  delta consume (source). Convert the table to InnoDB.
- **`validate` blocks on a target table with a secondary UNIQUE key.** MySQL
  `INSERT ... ON DUPLICATE KEY UPDATE` has no conflict target, so a secondary unique
  could silently rewrite the wrong row — pre-flight blocks it rather than risk
  corruption. Remove the extra unique key or replicate a table without one.
- **A copy halts with "target requires local_infile=ON".** The copy/apply transport
  is `LOAD DATA LOCAL INFILE`, which needs `local_infile=ON` on the **target** server
  (a system variable, not a grant). replicare probes it at connect and halts loud
  when it is off (v1 has no INSERT fallback). Enable it — `SET GLOBAL local_infile=1`
  or `local-infile=ON` in my.cnf — and set `local_infile: true` in the target block.
- **The source's `replicare` database keeps growing under a long transaction.** See
  [Source footprint](operations.md#source-footprint-the-thing-to-watch) — InnoDB's
  history list is the `xmin`-horizon analog; end the long-running source transaction.

See [the MySQL engine page](mysql.md) for the full requirements and the two
operational wrinkles.

## Getting more detail

Set `logging.level: debug` and `logging.format: text` for verbose, readable
output while diagnosing. `replicare status --json` gives the full machine-readable
state.
