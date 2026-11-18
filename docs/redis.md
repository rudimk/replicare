# Redis Engine

This page describes the Redis engine, `replicare`'s third engine, which replicates Redis to
Redis. It covers why Redis needs a different approach to the relational engines, how changes
are found and moved, the versions and forks we support, how to configure a sync, the
permissions it needs, as well as the gaps and trade-offs worth knowing before relying on it.

## Why is Redis different?

`replicare` holds Redis to the same rule as Postgres and MySQL - no privileged change stream,
ever - but the mechanism has to be very different. Redis has no WAL / binlog, no triggers,
and no way for us to write capture state onto the source, so there's nothing to tail and
nowhere to record a change as it happens. Instead, change capture(CDC) on Redis is a
full-keyspace `SCAN` reconciliation rather than trigger deltas. The engine reuses the same
engine-neutral copy / apply / state / config / observability layers as the other two,
plus one additive delete-reconciliation seam that the relational engines don't need.

The Redis engine has shipped, and runs end to end: value-faithful `DUMP`/`RESTORE` copy,
reconciliation-driven streaming upserts, a durable target-vs-source delete sweep, the optional
keyspace-notification accelerator, standalone *and* Cluster topologies, TTL fidelity,
least-privilege ACLs and the full daemon lifecycle. It sits alongside Postgres and MySQL syncs
in one daemon.

## How Redis CDC works

Since there's no change stream to tail, `replicare` re-reads the source keyspace instead. That
splits into three parts:

1. Upserts come from a rolling reconciliation `SCAN`. Every streaming pass `SCAN`s the source,
   `DUMP`s the keys that are present, and `RESTORE … REPLACE`s them on the target. This is
   idempotent and convergent; a key that's rewritten rapidly simply converges to its last
   value on the next pass. Nothing is written to the source - no delta tables, no `replicare`
   schema, and no source bloat.
2. Deletes come from a durable target-vs-source keyspace diff. A deleted key just stops
   showing up in the source scan, so there's nothing to "capture". Instead, `replicare` runs a
   separate delete sweep: it enumerates the *target* keyspace, `EXISTS`-checks each key against
   the source master, and `DEL`s the ones the source no longer has. This is the correctness
   mechanism for deletes - durable and self-healing, but its latency is bounded by the sweep
   interval rather than instant(see
   [Delete-propagation latency](#delete-propagation-latency-the-thing-to-know)).
3. Keyspace notifications are an optional accelerator, and nothing more. When they're
   enabled, they shorten latency by flagging changed keys ahead of the rolling scan, but
   they're fire-and-forget and lossy, so correctness *never* depends on them(see
   [Keyspace notifications](#keyspace-notifications-optional-accelerator)).

## Value-faithful transport with `DUMP` & `RESTORE`

`replicare` moves each value as its RDB-serialised `DUMP` payload and applies it with
`RESTORE … REPLACE`. It never interprets, coerces or reconstructs a value; it's the same
faithful-transport promise as the relational text `COPY` path. All six native types(string,
list, set, zset, hash, stream) plus TTLs move this way, and a stream's consumer groups and PEL
are preserved in the payload.

Transport is value-faithful, not byte-faithful. `RESTORE` re-encodes on the target and picks
its own optimal internal representation, so a target `DUMP` needn't byte-match the source's
across versions. That's why fidelity is verified by a type-appropriate logical comparison, and
not by `DUMP`-byte equality across a version gap.

There's no reconstruction fallback. When a payload can't be `RESTORE`d(because of the version
gate, a missing module, or its size), `replicare` stops loudly. It never falls back to
rebuilding the value with `GET`/`HGETALL`/…, since that would be exactly the value transform we
forbid(and it wouldn't be idempotent either). See
[the two pre-flight gates](#the-two-pre-flight-blocks).

## Supported versions & forks

The source floor is Redis 3.0(for `RESTORE … REPLACE`), and the target floor is 5.0. We'd
recommend 6.2+ on both ends. The CI-tested pair is a 6.2 source going to a 7.4 target, plus a
3-node 7.4 cluster.

Valkey is supported, since it shares Redis's RDB format and protocol. KeyDB is best-effort.
Dragonfly is blocked in v1; its RDB `DUMP`/`RESTORE` compatibility is unverified, so we refuse
it loudly rather than risk corruption. A server that isn't from the Redis family is refused at
connect, the same way MariaDB is for MySQL.

The full detail, including the RDB-version directional gate and the module gate, is in
[`redis-version-support.md`](redis-version-support.md).

## The two pre-flight blocks

Redis has no type-coercion axis like the relational engines, but it does have two
value-transport gates, and pre-flight(`validate` and daemon startup) blocks on both:

1. The RDB-version directional gate. `RESTORE` rejects a payload whose embedded RDB version is
   higher than the target's maximum. So an older or equal source going to a newer or equal
   target works, while a newer source going to an older target fails loudly. Pre-flight reads
   `INFO server` on both ends and blocks a newer-to-older pair up front, rather than failing
   mid-copy. The fix is to raise the target to at least the source's version.
2. The module gate. A module-defined type(RedisJSON, RedisBloom, …) produces an opaque `DUMP`
   payload that only the identical module can `RESTORE`. Pre-flight compares `MODULE LIST` on
   both ends and blocks when a source module type is missing on the target, since there's no
   faithful native reconstruction of it.

Managed Redis services that disable `MODULE` are handled gracefully. ElastiCache returns
`ERR unknown command 'MODULE'`, and an ACL may deny it with `NOPERM`; in both cases `replicare`
treats a blocked `MODULE LIST` as "no modules" and carries on, rather than failing to start.
Such servers can't load modules through `MODULE` anyway, so this only softens the pre-flight
gate - a module-typed value, if one is somehow present, still fails loudly at `RESTORE`, and
never silently. That makes `+module|list` in the target ACL preset best-effort; grant it where
the provider supports it, and where it's blocked, `replicare` degrades on its own.

## Configuration

A Redis sync is an engine-neutral envelope with a typed `redis:` block per endpoint, plus the
Postgres state store:

```yaml
state_store:                       # replicare's own bookkeeping (always Postgres)
  engine: postgres
  postgres: { host: state-db, database: replicare_state, user: replicare, password: "${PW}", sslmode: require }

sources:
  cache:
    engine: redis
    redis: { mode: standalone, host: src-redis, port: 6379, db: 0, user: replicare, password: "${PW}", tls: require }

targets:
  replica:
    engine: redis
    redis: { mode: standalone, host: dst-redis, port: 6379, db: 0, user: replicare, password: "${PW}", tls: require }

syncs:
  - name: cache-to-replica
    source: cache
    targets: [replica]           # v1: exactly one Redis target (fan-out is a non-goal)
    include: ["app:*"]           # key-pattern globs (Redis SCAN MATCH semantics)
    exclude: ["app:tmp:*"]       # exclude-wins
```

The full `redis:` block reference is in
[configuration.md](configuration.md#redis-connection-block). These are the fields that get
used most:

| Field | Notes |
|---|---|
| `mode` | `standalone`(default) \| `cluster` \| `sentinel`(parsed but experimental) |
| `host` / `port` | standalone seed; `nodes`(a `host:port` list) for cluster / sentinel |
| `db` | logical DB index(standalone only; forced to 0 in cluster) |
| `tls` | the full `disable`\|`allow`\|`prefer`\|`require`\|`verify-ca`\|`verify-full` spectrum |
| `read_from_replica` | offload value reads to replicas; delete detection still hits the master |
| `types` | optional type filter(`string`/`list`/`set`/`zset`/`hash`/`stream`; needs 6.0+) |
| `scan_count` | `SCAN COUNT` hint(per call; distinct from the neutral `drain_interval`/batch) |
| `reconcile_interval` / `delete_sweep_interval` | cadence of the upsert scan and the delete sweep |
| `big_key_warn_bytes` / `big_key_refuse_bytes` | DUMP-and-warn / block-loudly thresholds(via `MEMORY USAGE`) |
| `notifications` | enable the lossy keyspace-notification accelerator(default off) |
| `ttl_mode` | `relative`(default, skew-safe) \| `absttl`(opt-in, trusted clocks) |

The single-engine rule is enforced here too; every target of a Redis source has to be Redis,
and a sync is never cross-engine.

## Selection with key-pattern globs

`include`/`exclude` are key-pattern globs with Redis `SCAN … MATCH` semantics(`*`, `?`, `[…]`
and hash-tags), applied client-side, with exclude winning over include. There's one full `SCAN`
per pass either way, since the matching happens after the scan - except when a `types` filter
lets `replicare` prune server-side with `SCAN … TYPE`(6.0+). A key that matches no include glob
simply isn't replicated.

## Topology & cluster

In standalone mode, a sync picks one logical DB, from `db: 0` to `15`.

In Cluster mode, `replicare` runs everything per master node; the reconciliation `SCAN`, the
delete sweep, and(optionally) the notification subscription each fan out across the masters.
The topology is discovered via `CLUSTER SHARDS`/`SLOTS` and refreshed on `MOVED`/`ASK`, and the
unit of parallelism is the shard. Only DB 0 exists in cluster mode, and the config is validated
for it. Since `DUMP`/`RESTORE`/`DEL`/`EXISTS` all work on a single key, they sidestep
cross-slot limits.

Sentinel is parsed and the client is constructed, but v1 doesn't harden or test failover, so
it should be treated as experimental.

Delete detection *always* reads the master. `read_from_replica` may offload value reads to
replicas to go easy on the master, but `EXISTS`/`MissingAtSource` are pinned to the master; a
lagging replica could report a just-written key as absent, and cause a false `DEL`. This is
enforced.

## TTL fidelity

TTL is captured with `PTTL` and, by default, applied as a relative value(`RESTORE key
<pttl> …`), so the target works out expiry from *its own* clock; this makes it immune to clock
skew between the daemon and the target. `absttl` mode is opt-in for topologies with trusted
clocks, and is computed from the server's own `TIME`, not the daemon's clock. `PTTL -1`(no
expiry) maps to no expiry, while `PTTL -2` and expired keys are handled by the delete sweep.

## Big keys

`replicare` never moves a large value in increments, since a torn or partial transport would
be unfaithful. Instead, above `big_key_warn_bytes`(measured with `MEMORY USAGE`), it still
`DUMP`s the whole key and logs a loud big-key warning, accepting that the source blocks briefly
while it does. Above `big_key_refuse_bytes`, it blocks loudly. This applies to both the initial
copy and the re-reads during streaming.

## Keyspace notifications: optional accelerator

Setting `notifications: true` turns on a latency accelerator: one `PSUBSCRIBE` per master to
`__keyevent@<db>__:*`, which feeds flagged keys into a priority set that's drained ahead of the
rolling scan. It's *never* trusted for correctness. Events are fire-and-forget, carry no value,
are lost on disconnect, and are node-local in a cluster.

Since an event carries no value, it only marks a key as dirty; the current value is always
re-read, so a `set` and a `del` both go through the same path - a re-read, then a
`RESTORE`-or-`DEL`. On any subscription gap or reconnect, `replicare` forces a full
reconciliation and delete sweep for that shard. With notifications off, the engine falls back
cleanly to pure reconciliation.

Turning notifications on at the server needs `CONFIG SET`(`notify-keyspace-events`), or on a
managed tier like ElastiCache, a parameter group set separately from `replicare`.

## Delete-propagation latency, the thing to know

Because deletes aren't captured, and are instead found by the periodic target-vs-source sweep,
a source `DEL` only reaches the target once the next sweep completes. That's bounded by
`delete_sweep_interval`, plus the time it takes to `SCAN` the target. Upserts converge faster,
through the rolling reconciliation(and the notification accelerator, when it's on).

Two Redis-specific metrics make this observable:

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `replicare_delete_reconciliation_lag_seconds` | gauge | `sync`, `target`, `table` | Duration of the last completed delete sweep - how stale capture-less deletes can be. |
| `replicare_deletes_reconciled_total` | counter | `sync`, `target`, `table` | Keys `DEL`ed by the target-vs-source sweep. |

Both are unset for Postgres and MySQL, whose deletes flow through the delta path. The shared
metrics apply too, with `table` carrying the Redis unit(`redis.db0`). Note that
`replicare_delta_backlog_*` stay close to empty for Redis, since there's no source-side queue to
bound.

## Least-privilege ACLs & the `+restore` foot-gun

The Redis engine uses Redis 6+ ACLs, and the baseline has no `@admin`/`@all`/`DEBUG`/`CONFIG`/`KEYS`/`FLUSH*`.
Ready-to-edit presets ship in the repo:

- Source([`../deploy/acl-source-redis.txt`](https://github.com/rudimk/replicare/blob/main/deploy/acl-source-redis.txt)):
  read-only, with `+scan +type +dump +pttl +exists +memory|usage`(plus `+ping +info`).
  `replicare` writes nothing to the source.
- Target([`../deploy/acl-target-redis.txt`](https://github.com/rudimk/replicare/blob/main/deploy/acl-target-redis.txt)):
  `+restore +del +scan +exists +module|list`(plus `+ping +info`).

> ⚠️ `RESTORE` is an `@dangerous` command, so it's left out of the usual read-write presets and
> has to be granted explicitly(`+restore`). Without it, every apply fails loudly. Granting it
> lets the role overwrite *any* key within the ACL's key pattern, so scope `~…` tightly.

The optional notification accelerator also needs `+psubscribe`/`+subscribe` on the source(and
`CONFIG SET`, where the server isn't already configured for it). In cluster mode, grant the same
user on every master, and add `+cluster|shards`/`|slots`.

## Active-active mesh

Redis can take part in an active-active `clusters:` mesh, where writes are accepted on *any*
node and converge on all of them under HLC last-write-wins. It uses the same config surface as
the relational engines, described in [Active-active replication](active-active.md).

Since Redis is capture-less, the `(hlc, node_id)` version register lives in a parallel metadata
keyspace. For each data key `K`, there's a sibling hash `\x00rc:m:{…}K`(placed in `K`'s cluster
slot) holding its version and a fingerprint of its logical value. The value key itself is never
wrapped, so faithful transport still holds. Everything in this section only applies in a mesh;
a one-way Redis sync is byte-for-byte unchanged.

Convergence and loop suppression both fall out of a single comparison. A re-read only
`RESTORE`s or `DEL`s on a peer when the incoming `(hlc, node)` strictly beats the local register,
so `replicare`'s own applied writes lose(no echo storm), and concurrent writes to the same key
resolve to one value everywhere. The value and its register are written together in one atomic
Lua script.

Deletes are version-guarded tombstones, rather than the stateless sweep. The sweep is turned off
in a mesh, since a key that's absent on one node but present here is a peer's legitimate write,
and never an orphan. Tombstones are cleaned up by age, set with `rc_tombstone_retention`(24h by
default).

A mesh member is both a source and a target, so on top of both presets it needs read / write
access to the metadata keyspace, the logical readers, and `+eval`/`+evalsha`; see the mesh
sections in `deploy/acl-*-redis.txt`.

> ⚠️ The metadata keyspace roughly doubles the key count. Correctness for the Redis mesh is
> tested on the local gate(`REPLICARE_REDIS=1`), and not in the Postgres-only CI.

## Validating a sync

To check a Redis sync before it starts, run:

```sh
replicare validate config.yml
```

This connects to both ends, runs the RDB-version and module gates, reports each replication
unit as a single-member component, and validates each shard separately for a cluster, all
before any sync starts.

## Trying it end to end

[`examples/demo-redis/`](https://github.com/rudimk/replicare/tree/main/examples/demo-redis) is a runnable demo with an old 6.2
source, a modern 7.4 target, a Postgres state-store sidecar and `replicare`, replicating the
`app:*` keys. Build the image(`task docker`), then run
`cd examples/demo-redis && docker compose up` and watch the seeded keys, a live `SET`, and a
`DEL` propagate. The config is
[`examples/demo-redis/config.yml`](https://github.com/rudimk/replicare/blob/main/examples/demo-redis/config.yml).

## The operational wrinkles

There are two things that follow from how the engine is built in v1, and they're worth knowing
up front.

### 1. A Redis sync still needs a Postgres for state

`replicare`'s own operational state - which syncs exist, per-unit copy progress, per-target
cursors and the single-active ownership lock - lives in a Postgres state
store(`state_store.engine: postgres`), *regardless* of the data engine. That's a real wart for a
team that only runs Redis; to replicate Redis to Redis, `replicare` also has to be pointed at a
small Postgres for its bookkeeping.

Unlike the relational engines, though, Redis writes nothing to the source. Its CDC is
capture-less, so there are no source-side delta / track tables, and *all* the durable state a
Redis sync keeps is the coarse control-plane state in that Postgres. A Redis-backed state store
is future work behind the pluggable `StateStore` interface, and it isn't in v1. See
[`redis-statestore.md`](redis-statestore.md) for exactly how a Redis sync's rows are
represented.

### 2. Copy progress is coarse

A `SCAN` cursor is an opaque reverse-binary index, and it isn't durably resumable across
rehashes or restarts. So a Redis unit's copy progress is deliberately coarse: it's tracked as
snapshot-complete per unit, with no watermark. A crash mid-snapshot re-runs the whole `SCAN`,
which is idempotent(`RESTORE REPLACE`), so nothing is lost - but a large copy that's half done
restarts from the top, rather than resuming from a chunk.

## Where things stand

At the moment, these are the limitations and non-goals for v1:

1. Redis fan-out(one source going to several Redis targets) isn't supported. A single source
   `SCAN` cursor can't serve two targets at different rates without per-target scan state, so
   it's a single source to a single target only. A mesh is different, since each edge there is
   its own job.
2. Sentinel failover is wired up, but not hardened or tested.
3. Dragonfly is blocked, since its RDB compatibility is unverified.
4. Rebuilding values with commands, and moving big keys in increments, are both refused; either
   would transform values, and transport is faithful only.
5. There's no cross-key or group atomicity. Apply is per-key and idempotent, and *not* wrapped in
   `MULTI`/`EXEC`, which is a deliberate departure from the per-component atomicity of the
   relational engines. This holds in a mesh too; each key's value and register pair is atomic,
   but there's no cross-key transaction.
6. Cross-engine replication will never be supported, and point-in-time consistency is deferred,
   as for the other engines. Redis converges eventually and heals itself. Multi-master *is*
   supported(see above). When nodes that already hold different data form a mesh, each key
   settles on one value by last-write-wins, but a key deleted *before* the mesh was formed isn't
   remembered as deleted, so a node that still holds it copies it back.
