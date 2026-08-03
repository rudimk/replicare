# Operating replicare

Practical guidance for running replicare in production. Design rationale lives in
[`../CLAUDE.md`](../CLAUDE.md); this is the operator's view.

## Lifecycle of a sync

1. **Pre-flight.** `replicare validate <config>` (and the daemon at startup) introspects the source
   and target and classifies every column pair: identical/widening → ok, risky/lossy → warn,
   incompatible or missing type → **block** (refuses to start). It also reports FK components, a
   giant-component warning, dangling FK edges, and skipped no-key tables. Fix blocks before running.
2. **Capture-first.** The daemon installs trigger-based CDC on the source's selected tables, so
   changes begin queuing into per-table delta tables immediately.
3. **Initial copy.** A chunked, parallel copy of each FK component (parents first) runs while
   capture is active. It is resumable — a restart continues from the last checkpoint, not the top.
4. **Cutover → streaming.** Once a component is copied, it flips to continuous streaming: each pass
   re-reads the current source values of dirty keys and applies them idempotently, FK-ordered.
5. **Purge / retention / reseed.** Consumed deltas are purged; a target that falls too far behind is
   reseeded (see below).

## Tuning knobs (per sync, under `tuning:`)

| Knob | Meaning | Default |
|---|---|---|
| `drain_interval` | Time between streaming drain passes. Longer = more per-PK coalescing (less source load) but higher lag. | 1s |
| `retention.max_age` | Cap on the oldest unconsumed delta before a laggard target is reseeded. | 24h |
| `retention.max_bytes` | Cap on the delta table's on-disk size before reseed. | off |
| `pool.max_source_connections` / `pool.max_target_connections` | Connection caps; the copy worker pool is sized from these. | conservative |

Defaults favor low source pressure. Raise the pool caps and shorten `drain_interval` for throughput;
lengthen `drain_interval` to reduce re-read load under heavy churn.

## Source footprint (the thing to watch)

replicare writes delta and track tables to the **source** — a database you may not own — so bounding
that footprint is a first-class concern. The headline signals (per target/table):

- `replicare_delta_backlog_rows` / `replicare_delta_backlog_bytes` — unconsumed work.
- `replicare_delta_oldest_unconsumed_age_seconds` — how stale the queue is.
- `replicare_target_up` — reachability (1/0).
- `replicare_reseed_total` — forced reseeds.

**Retention & forced reseed.** A slow or unreachable target pins the delta queue (nothing purges
until every target has consumed it), so replicare caps retention by age and/or size. A target over
the cap is marked *needs-reseed*: its consumption record is reset, the unpinned deltas are purged,
and it is brought back by a fresh full copy — trading a re-copy for a protected source. This is on
by default. Watch `retention.cap_approaching` logs (they escalate INFO→WARN→ERROR toward the cap).

**The `xmin`-horizon caveat.** A long idle-in-transaction session (or other held-back `xmin`) on the
source prevents autovacuum from reclaiming purged delta rows, so the delta table can bloat *even
though replicare purges it*. replicare bounds the *logical* backlog regardless, but physical bloat
under a pinned `xmin` is surfaced, not fixed — hunt down the idle transaction on the source.

**MySQL (InnoDB) has the direct analog.** InnoDB does not shrink the tablespace on `DELETE`, and a
long-running source transaction inflates the **history list**, blocking purge of rows it can still
see — the same shape as the `xmin` horizon. The same headline metrics apply (the delta/track tables
live in a dedicated `replicare` *database* on the MySQL source), retention/reseed protects the source
identically, and the fix is the same: end the long-running source transaction. See
[the MySQL engine page](mysql.md).

**Redis has no source footprint at all.** Its CDC is capture-less — replicare writes nothing to the
source (no delta/track tables, no `replicare` schema). So the delta-backlog metrics, retention, and
`xmin`-style bloat simply don't apply. What you watch instead is **delete-propagation latency**:
deletes are found by the periodic target-vs-source sweep, so a source `DEL` converges only after the
next sweep. `replicare_delete_reconciliation_lag_seconds` (sweep duration) and
`replicare_deletes_reconciled_total` are the Redis-specific signals; tune cadence with
`delete_sweep_interval`. Upserts converge faster via the rolling reconciliation scan. See
[the Redis engine page](redis.md).

## When a target goes down

A drain against an unreachable target fires **all three channels at once**: an errored trace span
with backlog attributes, an `ERROR target.unreachable` log, and `replicare_target_up=0` plus a
climbing backlog series. The daemon keeps retrying each pass; nothing is lost. If the target stays
down long enough to exceed the retention cap, it is reseeded on recovery.

## Metrics reference

Every metric replicare exposes on `/metrics` (Prometheus text format; port `9090` in the demo,
configurable under `observability:`). These names, types, and labels are the single source of truth
locked in the observability contract, so a scrape of a running daemon matches this table exactly.

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `replicare_rows_copied_total` | counter | `sync`, `table` | Rows copied during the initial copy. |
| `replicare_initial_copy_rows_target` | gauge | `sync`, `table` | Estimated total rows to copy (the progress denominator). |
| `replicare_throughput_rows_per_second` | gauge | `sync` | Current apply/copy throughput. `0` on an idle, caught-up pass. |
| `replicare_replication_lag_seconds` | gauge | `sync`, `target`, `table` | Replication lag — age of the oldest unconsumed delta (`0` when caught up). |
| `replicare_delta_backlog_rows` | gauge | `sync`, `target`, `table` | Unconsumed delta rows (the queue depth). |
| `replicare_delta_backlog_bytes` | gauge | `sync`, `target`, `table` | Estimated unconsumed delta bytes. |
| `replicare_delta_oldest_unconsumed_age_seconds` | gauge | `sync`, `target`, `table` | Age of the oldest unconsumed delta (how stale the queue is). |
| `replicare_delta_purged_total` | counter | `sync`, `table` | Delta rows purged after consumption. |
| `replicare_reseed_total` | counter | `sync`, `target` | Forced reseeds triggered by the retention cap. |
| `replicare_target_up` | gauge | `sync`, `target` | Target reachability (`1`=up, `0`=down). |
| `replicare_apply_batch_seconds` | histogram | `sync`, `target` | Apply-batch duration per drain pass. |
| `replicare_errors_total` | counter | `sync`, `category` | Errors by category (e.g. `drain`, `retention`). |
| `replicare_table_phase_info` | gauge | `sync`, `table`, `phase` | Table lifecycle phase as an info gauge — value `1` on the active `phase` label (`initial_copy`/`streaming`). |
| `replicare_delete_reconciliation_lag_seconds` | gauge | `sync`, `target`, `table` | **Redis only** — duration of the last completed delete sweep (staleness of capture-less deletes). Unset for Postgres/MySQL. |
| `replicare_deletes_reconciled_total` | counter | `sync`, `target`, `table` | **Redis only** — keys `DEL`ed by the target-vs-source delete sweep. Unset for Postgres/MySQL. |

The headline signals to watch are `replicare_target_up`, `replicare_delta_backlog_rows`,
`replicare_delta_oldest_unconsumed_age_seconds`, and `replicare_replication_lag_seconds` — together
they answer "is the target healthy, and how far behind is it?" (see [Source footprint](#source-footprint-the-thing-to-watch)
and [When a target goes down](#when-a-target-goes-down)).

### Traces (OpenTelemetry)

Traces are exported via OTLP/gRPC when `observability.otlp_endpoint` is set. The span names are a
fixed registry:

| Span | Covers |
|---|---|
| `replicare.initial_copy.chunk` | one chunk of the initial copy |
| `replicare.capture.install` | installing trigger capture on the source |
| `replicare.delta.drain` | one streaming drain pass for a table/target |
| `replicare.apply.component` | applying one FK component's pass on the target |
| `replicare.reseed` | a forced/retention reseed |
| `replicare.preflight` | the start-up compatibility check |

A drain/apply span against an **unreachable** target is recorded with **error status** plus the
backlog/age attributes, so the stall and its blast radius are visible in the trace timeline (§3.4).

### Structured logs (slog)

Logs are JSON or text (`logging.format`), leveled (`logging.level`), with a stable `event` field you
can grep/alert on:

| Event | Level | Meaning |
|---|---|---|
| `capture.installed` / `capture.removed` | INFO | capture lifecycle |
| `table.cutover_to_streaming` | INFO | a table finished initial copy |
| `retention.cap_approaching` | INFO→WARN | delta backlog nearing the retention cap (escalates with proximity) |
| `target.unreachable` / `target.recovered` | ERROR / INFO | target connectivity, cross-channel with the `target_up` gauge and the errored span |
| `target.needs_reseed` | ERROR | a target was sacrificed to the retention cap and will re-copy |
| `preflight.blocked` | ERROR | a sync refused to start on a blocking incompatibility |

A degrading target surfaces in **metrics, traces, AND logs at once** — never just one channel (§3.4).

## The status HTTP API

The daemon serves an operator HTTP surface (bound to `observability.status_addr`, e.g. `:8080`):

- **`GET /status`** — JSON snapshot per sync/target/table: phase (initial_copy/streaming), rows
  copied vs total, replication lag, and last error. This is what the CLI `replicare status` renders;
  add `--json` to the CLI for the raw body.
- **`GET /healthz`** — a liveness probe (200 when the daemon is up), for Kubernetes/load balancers.
- **`GET /metrics`** — the Prometheus scrape. It is served here too, and additionally on
  `observability.metrics_addr` when that is set to a different address (the conventional split).

## Forcing a reseed

To re-copy a target from scratch (e.g. after out-of-band divergence):

```sh
replicare reseed <config> --sync <name> --target <name>
```

This flags the target in the state store; the running daemon re-copies it on its next pass and
resumes streaming. It signals the owning daemon rather than acting directly, honoring single-active
ownership.

## Runtime type errors

If the target rejects a value (type/constraint), replicare **halts the affected component loudly**
(logs + telemetry) rather than skipping or guessing. The offending row stays dirty and re-applies
automatically once you fix the cause (e.g. a missing target type/extension, or a schema mismatch).

## Restarts & ownership

Each sync runs under a single-active `pg_advisory_lock`; a second daemon pointed at the same sync
stands by. State (copy progress, cursors) is checkpointed in the Postgres state store — which is
always Postgres regardless of the data engine, so MySQL syncs restart and resume through the same
mechanism — so a restarted or rescheduled process resumes cleanly from the last checkpoint; an
ungraceful kill replays at-least-once and converges idempotently. One daemon can run Postgres, MySQL,
and Redis syncs side by side (each sync stays single-engine). A Redis sync checkpoints only coarse
phase (snapshot-complete-per-unit; no resumable SCAN cursor), so a crash mid-copy re-runs the whole
`SCAN` idempotently rather than resuming a chunk — see [the Redis engine page](redis.md).

## Least-privilege grants

See [`../deploy/grants-source.sql`](../deploy/grants-source.sql) and
[`../deploy/grants-target.sql`](../deploy/grants-target.sql). Note the documented asymmetry:
installing capture needs only the `TRIGGER` privilege, but `capture remove` (dropping the trigger)
requires table ownership. For the **true zero-database-grant** path, pre-create the `replicare`
capture schema (and, if the state store lives there, the `replicare_state` schema) owned by the
daemon role — the migration runner then issues no `CREATE SCHEMA` and needs no `CREATE`-on-database
privilege (CLAUDE.md §12). See the [Postgres engine page](postgres.md#least-privilege).

For **MySQL**, use [`../deploy/grants-source-mysql.sql`](../deploy/grants-source-mysql.sql) and
[`../deploy/grants-target-mysql.sql`](../deploy/grants-target-mysql.sql). MySQL has no such asymmetry
— the `TRIGGER` privilege covers both `CREATE TRIGGER` and `DROP TRIGGER`, so the daemon role can
fully uninstall capture on its own.

For **Redis**, use the ACL presets [`../deploy/acl-source-redis.txt`](../deploy/acl-source-redis.txt)
and [`../deploy/acl-target-redis.txt`](../deploy/acl-target-redis.txt). The source role is read-only
(`+ping +info +scan +type +dump +pttl +exists +memory|usage`); the target role needs `+ping +info
+restore +del +scan +exists +module|list`. Both roles need **`+info`** (pre-flight reads `INFO
server`), and in **cluster** mode both also need `+cluster|shards +cluster|slots` and the same user
granted on **every master**. Watch the foot-gun: `RESTORE` is `@dangerous` and must be granted
explicitly with `+restore`, or every apply fails loud. See [the Redis engine page](redis.md).
