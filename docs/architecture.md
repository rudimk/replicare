# Architecture

How replicare moves data, end to end. This is the newcomer-and-architect
overview; the exhaustive decision log with rationale lives in
[`../CLAUDE.md`](../CLAUDE.md), which this page distills. Everything here is
**engine-neutral** unless noted — Postgres, MySQL, and Redis all ride the same
pipeline, differing only behind the `Source`/`Sink` driver interfaces.

## The one non-negotiable

replicare assumes it will **never** have a privileged, durable change stream — no
Postgres WAL, no MySQL binlog, no durable Redis change feed (CLAUDE.md §1). Change
capture must work with ordinary, grantable privileges. That single constraint
shapes everything below.

## The pipeline

A **sync** (one source → one or more targets, with a table/key selection) runs this
lifecycle. Copy and streaming are the two phases; capture and checkpointing wrap
them.

```
 install capture ─► chunked parallel COPY ─► cut over ─► stream deltas ─► (repeat)
   (first)             (initial copy)                     (continuous)
        └──────────────── checkpoint to the StateStore throughout ─────────────┘
```

1. **Capture is installed FIRST** (relational engines), so changes begin queuing
   *before and during* the copy. For Redis there is nothing to install — capture is
   replaced by re-reading the live keyspace (see [Engines](#how-the-three-engines-map-on)).
2. **Initial copy** streams each table/unit in **chunks, in parallel**, source →
   target through an `io.Pipe` that never fully buffers a chunk. Because capture is
   already running and apply is idempotent, **any change during the copy window
   reconciles automatically** — there is **no single frozen snapshot**, which is how
   replicare handles sources too large to snapshot (CLAUDE.md §4).
3. **Cutover** flips each table to streaming once its copy completes.
4. **Streaming** drains changes continuously and applies them idempotently, forever,
   until the daemon is stopped.
5. **Checkpointing** records progress in the StateStore at every step, so a crash
   resumes from the last confirmed point (applies are idempotent, so replay is
   harmless).

## The four ideas that make it non-obvious

Most of replicare's design collapses into four decisions. Understand these and the
rest follows.

### 1. Capture is a *dirty-key set*, not an ordered log (§3.3)

Triggers on the source record **only the changed row's primary key** (PK-only
capture) into a per-table **delta** table — never the full row image. At sync time
replicare reads the unconsumed keys, **re-reads the current values from the live
source table**, and applies those. This is **self-healing**: it always copies the
*latest* value, so multiple changes to one key coalesce into one re-read and
transient intermediate states don't matter.

Crucially, consumption is a **set-difference** (`delta MINUS track`, per target),
**not** a `WHERE seq > cursor` scan. A scalar high-water-mark cursor is *broken*
here: a value a trigger assigns inside a transaction (a sequence, a txid) is fixed
when the trigger fires, but the delta row only becomes visible when the transaction
**commits** — and commits happen in a different order than those values were grabbed.
A slow transaction holding a low sequence that commits *after* the daemon's read
would be **skipped forever**. The set-difference model can't skip: an uncommitted or
late-committing delta is simply "not yet in `track`," caught on the next pass. Rows
are deleted by unique `delta_id` (never by PK) to survive the read-and-clear race.

### 2. FK connected components are the unit of ordering, parallelism & consistency (§8.1)

Within a sync, replicare auto-partitions the selected tables into **FK connected
components** (from the source catalogs, over the selected tables only). No FK edge
crosses a component boundary, which gives three properties for free:

- **Ordering is always complete** — upserts apply parent→child, deletes child→parent,
  within a component; no child is ever stranded from its parent. Cycles and
  self-references can't be topo-sorted, so a **retry fallback** handles them.
- **Distinct components are referentially independent** → safe to copy and stream
  **fully in parallel**.
- A component's changes for **one drain pass** apply in a **single target
  transaction**, so the target is always referentially consistent within the group.

A "giant component" (everything wired through a `users`/`tenant` hub table) erases
parallelism, so replicare **warns** and lets you override.

### 3. Faithful transport — never transform (§1.7, §4.2)

replicare is a **pipe, not an interpreter**. Values move **verbatim**: the source
server renders them, the target server's own input function validates them. It
**never** coerces, truncates, casts, or "repairs" data — on a type/constraint error
it **halts loud** rather than corrupting silently. This is a hard product promise:
**no value-transform capability exists, even as an opt-in.**

- Relational engines move values as **text `COPY`** (both initial copy *and* delta
  apply), pinning formatting-only session settings (e.g. `extra_float_digits=3`) so
  text round-trips losslessly across version gaps.
- Redis moves values as **`DUMP` → `RESTORE`** — RDB-serialized bytes, *value*-faithful
  (the target re-encodes), never reconstructed.
- A **pre-flight** check classifies every source↔target type pair up front: **block**
  on incompatible/missing, **warn** on risky-lossy, ok on identical/widening — turning
  mid-stream failures into an actionable report before any sync starts.

### 4. The StateStore is separate from source-side capture (§9)

Two different kinds of state, often confused:

- **Source-side capture state** — the delta and track tables — lives **on the source
  database** (that's where triggers write). It is the correctness-bearing replication
  state.
- **The StateStore** — which syncs exist, per-table copy progress, per-target cursors,
  and the single-active ownership lock — is the daemon's own operational bookkeeping.
  It lives in a dedicated schema on a **Postgres** database (v1's only StateStore
  backend), which may be the target, the source, or a separate server.

They are separate concerns even when both happen to be Postgres. One consequence:
**every** sync needs a Postgres StateStore — including a MySQL→MySQL or Redis→Redis
sync (a [documented wart](redis-statestore.md) for the non-Postgres engines).

## How the three engines map on

The pipeline is identical; only the driver behind `Source`/`Sink` changes.

| | **Postgres** (reference) | **MySQL** | **Redis** |
|---|---|---|---|
| Capture | trigger CDC → delta/track tables | trigger CDC (same model) | **none** — capture-less |
| Upsert CDC | re-read dirty PKs | re-read dirty PKs | **full-keyspace `SCAN` reconciliation** (re-read every pass) |
| Delete CDC | delta row for the deleted PK | delta row | **durable target-vs-source keyspace diff** |
| Transport | text `COPY` | `LOAD DATA LOCAL INFILE` | `DUMP` → `RESTORE REPLACE` |
| Unit | FK component of tables | FK component of tables | one **key-namespace unit** (`redis.db<N>`), single-member |
| Apply atomicity | per-component transaction | per-component transaction | **per-key** (no cross-key atomicity) |
| Delivery | at-least-once; **effectively-exactly-once** when the StateStore *is* the target | at-least-once | at-least-once |

Redis is the outlier: with no change stream *at all*, upserts come from re-reading
the whole keyspace each pass and deletes from diffing the target against the source
(keyspace notifications are an optional, lossy accelerator, never trusted). See the
[Redis engine page](redis.md) for the full story, and the
[Postgres](postgres.md) / [MySQL](mysql.md) pages for the relational specifics.

## Delivery, topology & bloat

- **Delivery:** at-least-once with **idempotent apply** (upserts keyed on PK/key,
  deletes by PK/key), checkpointed cursors. A crash replays from the last cursor;
  replays are harmless.
- **Topology:** user choice — default single→single; **fan-out** (one source → many
  targets) is supported for the relational engines; multi-master and HA/leader
  election are on the roadmap. A sync is always **single-engine** (§6).
- **Source bloat:** the delta tables are a high-churn queue on a source you may not
  own, so replicare bounds its footprint with per-table delta tables, a batched
  consumption-gated purge + aggressive autovacuum, and **bounded retention with forced
  reseed** of an over-cap target (CLAUDE.md §3.4). See
  [operations](operations.md) and the [reseed state machine](reseed-state-machine.md).

## Process & observability

One **daemon** runs many named syncs as goroutine worker pools (one process, central
observability). Everything is visible across **four channels at once** (§10): a
Prometheus `/metrics` endpoint, OpenTelemetry traces, structured `slog` logs, and a
`/status` HTTP API. A degrading target must surface in metrics, logs, **and** traces
simultaneously — see [operations](operations.md) for the metric/span/event
reference.
