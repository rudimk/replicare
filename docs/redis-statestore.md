# Redis on the Postgres StateStore

This page describes how a Redis sync's progress is recorded in `replicare`'s Postgres state
store, and why the Redis engine didn't need any new state store code to get there. It's meant
for an operator reading the `replicare_state` schema, who needs to know how to interpret the
rows a Redis sync leaves there.

## Where Redis state lives

`replicare`'s operational state - which syncs exist, per-unit initial-copy progress, per-target
streaming cursors, ownership and reseed flags - lives in a dedicated schema on a Postgres
database(the target, the source, or a separate Postgres; see [Architecture](architecture.md)).
This backend is engine-neutral; the Redis engine ships no new StateStore code, and instead
reuses the identical Postgres backend by overloading the neutral vocabulary. The rest of this
page records those overloads.

> ⚠️ The StateStore is the *daemon's own* operational state. It's not where Redis change capture
> lives; Redis has no source-side delta / track tables, since its CDC is SCAN reconciliation(see
> [the Redis engine page](redis.md)).

## The overloads

| Neutral concept | Relational meaning | Redis meaning |
|---|---|---|
| `TableRef`(a `copy_progress` / `cursors` row's table) | `schema.table` | a replication unit - the connection's logical DB, rendered `redis.db<N>`(cluster = `db0`). One unit per sync. |
| `SyncDef.Selection` | `schema.table` include / exclude globs | key-pattern include / exclude globs(Redis `SCAN … MATCH` semantics). |
| `CopyProgress.Watermark` / `.Completed` | keyset high-water mark + sparse completed PK-ranges | always nil - see coarse checkpointing below. |
| `CopyProgress.Done` | all chunks of a table copied | the unit's SCAN snapshot completed. |
| `Cursor.LastDelta`(`engine.DeltaID`) | last confirmed source delta-table row id | a per-unit change-id(a reconciliation-pass counter used only for lag / observability). |
| `Cursor.NeedsReseed` | retention-cap forced reseed | identical - retention / reseed is engine-neutral. |
| `Acquire`(advisory lock) | single-active per sync | identical - a Redis sync is single-active too. |

### One unit per sync, not one per shard

A Redis sync replicates a single logical keyspace. Even against a cluster, the persisted
identity is *one* `TableRef`(`redis.db0`); the per-master SCAN fan-out happens *inside* the
Source(`CopyChunk` / `ReadDirtyKeys`), and never as N rows, one per shard. This keeps a single,
stable unit ref that round-trips cleanly through the neutral copy / apply / state layers,
whatever the topology.

### Coarse checkpointing & a nil watermark

A relational table's initial copy is chunked by PK range, so its progress is a keyset
watermark plus a sparse set of completed ranges that finished out of order, which lets a restart
skip the chunks that are already done. A Redis unit has no ordered key space to chunk against;
its snapshot is one full SCAN pass, which is either complete or it isn't. So its `CopyProgress`
is deliberately coarse. `Watermark` and `Completed` stay nil, and `Done` flips `false → true`
when the unit's snapshot finishes. A restart mid-snapshot re-runs the whole SCAN, which is
idempotent(RESTORE REPLACE), so there's no need for a watermark.

## Why is there no new code?

The store never *interprets* these fields. It persists `TableRef` as two opaque strings,
`KeyValues`/`DeltaID` as JSON / bigint, and the advisory lock by sync name, so a Redis unit
flows through exactly the same read / write path as a Postgres table. The tests in
[`internal/state/postgres/redis_unit_test.go`](https://github.com/rudimk/replicare/blob/main/internal/state/postgres/redis_unit_test.go) check this by round-tripping
Redis-shaped `SyncDef`, `CopyProgress` and `Cursor` values, and by proving the ownership lock
keeps a second daemon out of a Redis sync.
