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

## When a target goes down

A drain against an unreachable target fires **all three channels at once**: an errored trace span
with backlog attributes, an `ERROR target.unreachable` log, and `replicare_target_up=0` plus a
climbing backlog series. The daemon keeps retrying each pass; nothing is lost. If the target stays
down long enough to exceed the retention cap, it is reseeded on recovery.

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
stands by. State (copy progress, cursors) is checkpointed in the Postgres state store, so a
restarted or rescheduled process resumes cleanly from the last checkpoint — an ungraceful kill
replays at-least-once and converges idempotently.

## Least-privilege grants

See [`../deploy/grants-source.sql`](../deploy/grants-source.sql) and
[`../deploy/grants-target.sql`](../deploy/grants-target.sql). Note the documented asymmetry:
installing capture needs only the `TRIGGER` privilege, but `capture remove` (dropping the trigger)
requires table ownership.
