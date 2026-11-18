# Architecture

This page describes how `replicare` moves data end to end, as an overview for anyone new to the
project or weighing up its design. It covers the constraint the whole design starts from, the
pipeline every sync runs through, the four ideas the rest of the design follows from, and how
each engine fits in. Everything here is engine-neutral unless noted; Postgres, MySQL and Redis
all ride the same pipeline, differing only behind the `Source` / `Sink` driver interfaces.

## Why does it work this way?

`replicare` assumes it'll *never* have a privileged, durable change stream - no Postgres WAL, no
MySQL binlog and no durable Redis change feed. Change capture has to work with ordinary, grantable
privileges instead, and that single constraint shapes everything below.

## The pipeline

A sync(one source to one or more targets, with a table / key selection) runs through the
lifecycle below. Copy and streaming are the two phases, and capture and checkpointing wrap them:

```
 install capture ─► chunked parallel COPY ─► cut over ─► stream deltas ─► (repeat)
   (first)             (initial copy)                     (continuous)
        └──────────────── checkpoint to the StateStore throughout ─────────────┘
```

1. Capture is installed first(on the relational engines), so changes begin queuing *before* and
   during the copy. For Redis there's nothing to install; capture is replaced by re-reading the
   live keyspace(see [Engines](#how-the-three-engines-map-on)).
2. The initial copy streams each table / unit in chunks, in parallel, from source to target
   through an `io.Pipe` that never fully buffers a chunk. Since capture is already running and
   apply is idempotent, any change made during the copy window reconciles automatically. There's
   no single frozen snapshot, which is how `replicare` handles sources too large to snapshot.
3. Cutover flips each table to streaming once its copy completes.
4. Streaming drains changes continuously and applies them idempotently, forever, until the
   daemon is stopped.
5. Checkpointing records progress in the StateStore at every step, so a crash resumes from the
   last confirmed point. Applies are idempotent, so the replay is harmless.

## Four ideas behind the design

Most of `replicare`'s design comes down to four decisions, and once these make sense, the rest
follows from them.

### 1. Capture is a *dirty-key set*, not an ordered log

Triggers on the source record *only* the changed row's primary key(we call this PK-only capture)
into a per-table delta table, and never the full row image. At sync time, `replicare` reads the
unconsumed keys, re-reads their current values from the live source table, and applies those.
This makes it self-healing; it always copies the *latest* value, so several changes to one key
coalesce into a single re-read, and transient intermediate states don't matter.

Crucially, changes are consumed as a set-difference(`delta MINUS track`, per target), and not with
a `WHERE seq > cursor` scan. A scalar high-water-mark cursor is broken here, for a subtle reason.
A value a trigger assigns inside a transaction(a sequence, a txid) is fixed when the trigger
fires, but the delta row only becomes visible when the transaction commits - and transactions
commit in a different order than they grabbed those values. A slow transaction holding a low
sequence that commits *after* the daemon's read would be skipped forever.

The set-difference model can't skip anything. A delta that's uncommitted or commits late is
simply "not yet in `track`", and it's caught on the next pass. Rows are deleted by their unique
`delta_id`, never by PK, so that they survive the read-and-clear race.

### 2. FK connected components are the unit of ordering, parallelism & consistency

Within a sync, `replicare` automatically splits the selected tables into FK connected
components(worked out from the source catalogs, over the selected tables only). Since no FK edge
crosses a component boundary, this gives three properties for free:

1. Ordering is always complete. Within a component, upserts apply from parent to child and
   deletes from child to parent, so no child is ever stranded from its parent. Cycles and
   self-references can't be topologically sorted, so a retry fallback handles them.
2. Distinct components are referentially independent, which makes them safe to copy and stream
   fully in parallel.
3. Each pass applies a component one table at a time, each table in its own committed
   transaction and in the order above, so a parent can move forward even when a child in the
   same pass has to wait for a later one. By the end of a pass the target is referentially
   consistent within the group. The exception is a cycle that can't be split this way(an
   all-`DEFERRABLE` cycle on Postgres, or a cycle on MySQL), which applies in one transaction.

A "giant component", where everything is wired together through a `users`/`tenant` hub table,
erases that parallelism, so `replicare` warns about it and allows it to be overridden.

### 3. Faithful transport, never transform

`replicare` is a pipe, not an interpreter. Values move verbatim; the source server renders them,
and the target server's own input function validates them. It never coerces, truncates, casts or
"repairs" data, and on a type / constraint error it halts loudly rather than corrupting anything
silently. This is a hard product promise - no value-transform capability exists, even as an
opt-in.

The relational engines move values as text `COPY`, for both the initial copy *and* delta apply.
They pin formatting-only session settings(for example `extra_float_digits=3`), so that text
round-trips losslessly across version gaps. Redis, on the other hand, moves values with `DUMP`
and then `RESTORE`; these are RDB-serialised bytes, which are *value*-faithful(the target
re-encodes them) and never reconstructed.

A pre-flight check classifies every source / target type pair up front. It blocks on an
incompatible or missing type, warns on a risky / lossy one, and is fine with an identical or
widening one; this turns what would have been mid-stream failures into an actionable report
before any sync starts.

### 4. The StateStore is separate from source-side capture

There are two different kinds of state here, and they're often confused.

Source-side capture state(the delta and track tables) lives on the source database, since that's
where the triggers write. It's the replication state that correctness depends on.

The StateStore, on the other hand, is the daemon's own operational bookkeeping: which syncs exist,
per-table copy progress, per-target cursors and the single-active ownership lock. It lives in a
dedicated schema on a Postgres database(v1's only StateStore backend), which may be the target,
the source or a separate server.

They're separate concerns, even when both happen to be Postgres. One consequence of this is that
*every* sync needs a Postgres StateStore, including a MySQL to MySQL or a Redis to Redis sync -
a [documented wart](redis-statestore.md) for the non-Postgres engines.

## How the three engines map on

The pipeline is identical for every engine, and only the driver behind `Source` / `Sink` changes:

| | Postgres(reference) | MySQL | Redis |
|---|---|---|---|
| Capture | trigger CDC into delta / track tables | trigger CDC(same model) | none(capture-less) |
| Upsert CDC | re-read dirty PKs | re-read dirty PKs | full-keyspace `SCAN` reconciliation(re-read every pass) |
| Delete CDC | delta row for the deleted PK | delta row | durable target-vs-source keyspace diff |
| Transport | text `COPY` | `LOAD DATA LOCAL INFILE` | `DUMP`, then `RESTORE REPLACE` |
| Unit | FK component of tables | FK component of tables | one key-namespace unit(`redis.db<N>`), single-member |
| Apply atomicity | per table(one transaction for an all-`DEFERRABLE` cycle) | per table(one transaction for a cycle) | per-key(no cross-key atomicity) |
| Delivery | at-least-once; effectively-exactly-once when the StateStore *is* the target | at-least-once | at-least-once |

Redis is the outlier here. With no change stream at *all*, upserts come from re-reading the whole
keyspace on each pass, and deletes from diffing the target against the source. Keyspace
notifications are an optional, lossy accelerator, and never trusted. The
[Redis engine page](redis.md) has the full story, and the [Postgres](postgres.md) /
[MySQL](mysql.md) pages cover the relational specifics.

## Delivery, topology & bloat

Delivery is at-least-once, with idempotent apply(upserts keyed on PK / key, and deletes by
PK / key) and checkpointed cursors. A crash replays from the last cursor, and since applies are
idempotent, the replays are harmless.

Topology is always the user's choice. The default is a single source to a single target. Fan-out(one
source to many targets) is supported for the relational engines, and active-active(writes on any
node) is supported for Postgres, MySQL and Redis, as described in
[Active-active replication](active-active.md). A sync is always single-engine.

> ⚠️ At the moment, there's no standby daemon that takes over automatically; it's on the roadmap.

Finally - source bloat. The delta tables are a high-churn queue on a source we may not own, so
`replicare` bounds its footprint with per-table delta tables, a batched, consumption-gated purge
alongside aggressive autovacuum, and bounded retention with a forced reseed of any target that goes
over the cap. The details are in [operations](operations.md#source-footprint-the-thing-to-watch).

## Process & observability

A single daemon runs many named syncs as goroutine worker pools, so there's one process and one
central place to observe it from. Everything is visible across four channels at once: a
Prometheus `/metrics` endpoint, OpenTelemetry traces, structured `slog` logs and a `/status` HTTP
API. A degrading target has to surface in metrics, logs *and* traces simultaneously;
[operations](operations.md) has the reference for every metric, span and event.
