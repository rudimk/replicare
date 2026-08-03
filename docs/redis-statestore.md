# Redis on the Postgres StateStore

replicare's operational state — which syncs exist, per-unit initial-copy
progress, per-target streaming cursors, ownership, and reseed flags — lives in a
dedicated schema on a Postgres database (the target, the source, or a separate
Postgres; see [state storage](../CLAUDE.md#9-state-storage)). This backend is
**engine-neutral**: the Redis engine ships **no new StateStore code**. It reuses
the identical Postgres backend by overloading the neutral vocabulary. This
document records those overloads so an operator reading the `replicare_state`
schema knows how to interpret a Redis sync's rows.

> The StateStore is the *daemon's own* operational state. It is **not** where
> Redis change-capture lives — Redis has no source-side delta/track tables; its
> CDC is SCAN reconciliation (see [redis-version-support](redis-version-support.md)).

## The overloads

| Neutral concept | Relational meaning | Redis meaning |
|---|---|---|
| `TableRef` (a `copy_progress` / `cursors` row's table) | `schema.table` | a **replication unit** — the connection's logical DB, rendered `redis.db<N>` (cluster = `db0`). One unit per sync. |
| `SyncDef.Selection` | `schema.table` include/exclude globs | **key-pattern** include/exclude globs (Redis `SCAN … MATCH` semantics). |
| `CopyProgress.Watermark` / `.Completed` | keyset high-water mark + sparse completed PK-ranges | **always nil** — see *coarse checkpointing* below. |
| `CopyProgress.Done` | all chunks of a table copied | the unit's SCAN snapshot completed. |
| `Cursor.LastDelta` (`engine.DeltaID`) | last confirmed source delta-table row id | a **per-unit change-id** (a reconciliation-pass counter used only for lag/observability). |
| `Cursor.NeedsReseed` | retention-cap forced reseed | identical — retention/reseed is engine-neutral. |
| `Acquire` (advisory lock) | single-active per sync | identical — a Redis sync is single-active too. |

### One unit per sync (not one per shard)

A Redis sync replicates a single logical keyspace. Even against a cluster, the
persisted identity is **one** `TableRef` (`redis.db0`); the per-master SCAN
fan-out happens *inside* the Source (`CopyChunk` / `ReadDirtyKeys`), never as
N-per-shard rows. This keeps a single, stable unit ref that round-trips cleanly
through the neutral copy/apply/state layers regardless of topology.

### Coarse checkpointing (nil watermark)

A relational table's initial copy is chunked by PK range, so its progress is a
keyset watermark plus a sparse set of out-of-order completed ranges, letting a
restart skip finished chunks. A Redis unit has **no ordered key space to chunk
against** — its snapshot is a full SCAN pass that is either complete or not. So
its `CopyProgress` is deliberately coarse: `Watermark` and `Completed` stay
**nil**, and `Done` flips `false → true` when the unit's snapshot finishes. A
restart mid-snapshot re-runs the whole SCAN, which is idempotent (RESTORE
REPLACE), so no watermark is needed.

## Why no new code

Because the store never *interprets* these fields — it persists `TableRef` as
two opaque strings, `KeyValues`/`DeltaID` as JSON/bigint, and the advisory lock
by sync name — a Redis unit flows through the exact same read/write path as a
Postgres table. The RM3 tests
(`internal/state/postgres/redis_unit_test.go`) assert this by round-tripping
Redis-shaped `SyncDef`, `CopyProgress`, and `Cursor` values, and by proving the
ownership lock excludes a second daemon for a Redis sync.
