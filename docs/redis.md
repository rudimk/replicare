# Redis engine

replicare's third engine replicates **Redis → Redis** with the same
no-privileged-change-stream philosophy as Postgres and MySQL (CLAUDE.md §3.2), but
a very different mechanism: Redis has no WAL/binlog, no triggers, and no way for us
to write capture state onto the source, so CDC is **full-keyspace `SCAN`
reconciliation** rather than trigger deltas. It reuses the engine-neutral
copy/apply/state/config/observability layers, plus **one additive delete-
reconciliation seam** that the relational engines don't need. The design and
milestone plan live in [`../.sisyphus/redis-plan.md`](../.sisyphus/redis-plan.md).

> **Status: shipped.** The Redis engine runs end to end — value-faithful
> `DUMP`/`RESTORE` copy, reconciliation-driven streaming upserts, a durable
> target-vs-source delete sweep, the optional keyspace-notification accelerator,
> standalone **and** Cluster topologies, TTL fidelity, least-privilege ACLs, and
> the full daemon lifecycle — and coexists with Postgres and MySQL syncs in one
> daemon.

## How Redis CDC works (it's different)

There is no change stream to tail, so replicare **re-reads the source keyspace**:

- **Upserts — rolling reconciliation `SCAN`.** Every streaming pass `SCAN`s the
  source, `DUMP`s the present keys, and `RESTORE … REPLACE`s them on the target.
  This is idempotent and convergent: a rapid rewrite simply converges to the last
  value on the next pass. Nothing is written to the source — no delta tables, no
  `replicare` schema, no source bloat.
- **Deletes — a durable target-vs-source keyspace diff.** A deleted key just stops
  appearing in the source scan; there is nothing to "capture." So replicare runs a
  separate **delete sweep**: enumerate the *target* keyspace, `EXISTS`-check each
  key against the source **master**, and `DEL` the ones the source no longer has.
  This is the correctness mechanism for deletes — durable and self-healing, but its
  latency is bounded by the **sweep interval**, not instant (see
  [Delete-propagation latency](#delete-propagation-latency-the-thing-to-know)).
- **Keyspace notifications — an optional accelerator only.** When enabled they
  shorten latency by flagging changed keys ahead of the rolling scan, but they are
  fire-and-forget and lossy, so correctness **never** depends on them (see
  [Keyspace notifications](#keyspace-notifications-optional-accelerator)).

## Transport — value-faithful `DUMP` → `RESTORE`

replicare moves each value as its **RDB-serialized `DUMP` payload** and applies it
with `RESTORE … REPLACE`. It never interprets, coerces, or reconstructs a value —
the same faithful-transport promise as the relational text `COPY` path (CLAUDE.md
§1.7). All six native types (string, list, set, zset, hash, stream) plus TTLs move
this way; a stream's consumer **groups and PEL are preserved** in the payload.

Transport is **value-faithful, not byte-faithful**: `RESTORE` re-encodes on the
target and picks its own optimal internal representation, so a target `DUMP` need
not byte-match the source's across versions. Fidelity is verified by
type-appropriate logical comparison, not `DUMP`-byte equality across a version gap.

There is **no reconstruction fallback**. When a payload can't be `RESTORE`d
(version gate, missing module, oversize), replicare **fails loud** — it never falls
back to `GET`/`HGETALL`/… rebuilding, which would be the forbidden value transform
(and non-idempotent besides). See [the two pre-flight gates](#the-two-pre-flight-blocks).

## Supported versions & forks

- **Source floor: Redis 3.0** (`RESTORE … REPLACE`). **Target floor: 5.0.**
  Recommended **6.2+** both ends. CI-tested pair: **6.2 source → 7.4 target** plus
  a 3-node 7.4 cluster.
- **Valkey** is supported (shares Redis's RDB format + protocol); **KeyDB** is
  best-effort; **Dragonfly is blocked** in v1 (its RDB `DUMP`/`RESTORE` compat is
  unverified — refused loudly rather than risk corruption). A non-Redis-family
  server is refused at connect, the same way MariaDB is for MySQL.

Full detail — including the RDB-version directional gate and the module gate — is
in [`redis-version-support.md`](redis-version-support.md).

## The two pre-flight BLOCKS

Redis has no type-coercion axis like the relational engines, but it has two
value-transport gates that pre-flight (`validate` and daemon startup) **blocks** on:

1. **RDB-version directional gate.** `RESTORE` rejects a payload whose embedded RDB
   version exceeds the target's maximum. So **older/equal source → newer/equal
   target works; newer source → older target fails loud.** Pre-flight reads `INFO
   server` on both ends and blocks a newer→older pair up front rather than failing
   mid-copy. Fix: raise the target to at least the source's version.
2. **Module gate.** A module-defined type (RedisJSON, RedisBloom, …) produces an
   opaque `DUMP` payload only the identical module can `RESTORE`. Pre-flight
   compares `MODULE LIST` on both ends and blocks when a source module type is
   absent on the target — there is no faithful native reconstruction.

## The operational wrinkles

Both follow from the v1 architecture and are worth knowing up front.

### 1. A Redis sync still needs a Postgres for state

replicare's own operational state — which syncs exist, per-unit copy progress,
per-target cursors, and the single-active ownership lock — lives in a **Postgres**
state store (`state_store.engine: postgres`), *regardless of the data engine*
(CLAUDE.md §9). This is a real wart for an all-Redis shop: to replicate Redis→Redis
you also point replicare at a small Postgres for its bookkeeping.

Unlike the relational engines, though, Redis writes **nothing** to the source — its
CDC is capture-less, so there are no source-side delta/track tables. *All* durable
state a Redis sync keeps is the coarse control-plane state in that Postgres. A
Redis-backed state store is a future item behind the pluggable `StateStore`
interface; it is not in v1. See [`redis-statestore.md`](redis-statestore.md) for
exactly how a Redis sync's rows are represented.

### 2. Copy progress is coarse (no resumable SCAN cursor)

A `SCAN` cursor is an opaque reverse-binary index, **not durably resumable** across
rehashes or restarts. So a Redis unit's copy progress is deliberately coarse:
snapshot-complete-per-unit, with no watermark. A crash mid-snapshot re-runs the
whole `SCAN`, which is idempotent (`RESTORE REPLACE`), so nothing is lost — but a
half-finished large copy restarts from the top rather than resuming a chunk.

## Configuration

A Redis sync is an engine-neutral envelope with a typed `redis:` block per
endpoint, plus the Postgres state store:

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
[configuration.md](configuration.md#redis-connection-block). The most-used fields:

| Field | Notes |
|---|---|
| `mode` | `standalone` (default) \| `cluster` \| `sentinel` (parsed but experimental) |
| `host` / `port` | standalone seed; `nodes` (a `host:port` list) for cluster/sentinel |
| `db` | logical DB index (standalone only; **forced 0 in cluster**) |
| `tls` | full `disable`\|`allow`\|`prefer`\|`require`\|`verify-ca`\|`verify-full` spectrum |
| `read_from_replica` | offload **value** reads to replicas; delete detection still hits the master |
| `types` | optional type filter (`string`/`list`/`set`/`zset`/`hash`/`stream`; needs 6.0+) |
| `scan_count` | `SCAN COUNT` hint (per-call; distinct from the neutral `drain_interval`/batch) |
| `reconcile_interval` / `delete_sweep_interval` | cadence of the upsert scan and the delete sweep |
| `big_key_warn_bytes` / `big_key_refuse_bytes` | DUMP-and-warn / block-loud thresholds (via `MEMORY USAGE`) |
| `notifications` | enable the lossy keyspace-notification accelerator (default off) |
| `ttl_mode` | `relative` (default, skew-safe) \| `absttl` (opt-in, trusted clocks) |

The **single-engine rule** (§6) is enforced: a Redis source's targets must all be
Redis — never cross-engine.

## Selection — key-pattern globs

`include`/`exclude` are **key-pattern globs** with Redis `SCAN … MATCH` semantics
(`*`, `?`, `[…]`, hash-tags), applied client-side, **exclude-wins**. There is one
full `SCAN` per pass regardless (matching is post-scan), except when a `types`
filter lets replicare prune server-side with `SCAN … TYPE` (6.0+). A key that
matches no include glob is simply not replicated.

## Topology & Cluster

- **Standalone** picks one logical DB (`db: 0`–`15`).
- **Cluster** runs **everything per master node**: the reconciliation `SCAN`, the
  delete sweep, and (optionally) the notification subscription each fan out across
  masters; topology is discovered via `CLUSTER SHARDS`/`SLOTS` and refreshed on
  `MOVED`/`ASK`. The parallelism unit is the **shard**. Only **DB 0** exists in
  cluster mode (validated). Per-key `DUMP`/`RESTORE`/`DEL`/`EXISTS` sidestep
  cross-slot limits.
- **Sentinel** is parsed and the client is constructed, but v1 does **not** harden
  or test failover — treat it as experimental.

**Delete detection always reads the master.** `read_from_replica` may offload value
reads to replicas for politeness, but `EXISTS`/`MissingAtSource` are pinned to the
master: a lagging replica could report a just-written key absent and cause a false
`DEL`. This is enforced.

## TTL fidelity

TTL is captured with `PTTL` and, by default, applied **relative** (`RESTORE key
<pttl> …`) so the target evaluates expiry from *its own* clock — immune to
daemon/target clock skew. `absttl` mode is opt-in for trusted-clock topologies and
is computed from the server's own `TIME`, not the daemon clock. `PTTL -1` (no
expiry) maps to no expiry; `PTTL -2`/expired keys are handled by the delete sweep.

## Big keys

replicare never does incremental/torn transport of a large value (that would be
unfaithful). Instead, above `big_key_warn_bytes` (measured with `MEMORY USAGE`) it
**still `DUMP`s the whole key** and logs a loud big-key warning (accepting the
transient source blocking); above `big_key_refuse_bytes` it **blocks loud**. This
applies to both initial copy and streaming re-reads.

## Keyspace notifications (optional accelerator)

Setting `notifications: true` turns on a latency accelerator: one `PSUBSCRIBE` per
master to `__keyevent@<db>__:*`, feeding flagged keys into a priority set drained
ahead of the rolling scan. It is **never trusted for correctness** — events are
fire-and-forget, value-less, lost on disconnect, and node-local in cluster. A
value-less event only marks a key dirty; the current value is always re-read, so a
`set` and a `del` both flow through the same re-read → `RESTORE`-or-`DEL` path. On
any subscription gap/reconnect, replicare forces a full reconciliation + delete
sweep for that shard. With notifications off, the engine falls back cleanly to pure
reconciliation. Enabling notifications on the server needs `CONFIG SET`
(`notify-keyspace-events`) or, on a managed tier like ElastiCache, a parameter
group set out-of-band.

## Delete-propagation latency (the thing to know)

Because deletes are **not captured** — they're found by the periodic target-vs-
source sweep — a source `DEL` converges on the target only after the next sweep
completes, bounded by `delete_sweep_interval` (and the time to `SCAN` the target).
Upserts converge faster (the rolling reconciliation, accelerated by notifications
when on). Two Redis-specific metrics make this observable:

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `replicare_delete_reconciliation_lag_seconds` | gauge | `sync`, `target`, `table` | Duration of the last completed delete sweep — staleness of capture-less deletes. |
| `replicare_deletes_reconciled_total` | counter | `sync`, `target`, `table` | Keys `DEL`ed by the target-vs-source sweep. |

These are unset for Postgres/MySQL (whose deletes flow through the delta path). The
reused F2 signals apply too, with `table` carrying the Redis **unit** (`redis.db0`);
note that `replicare_delta_backlog_*` stay ~empty for Redis (there is no source-side
queue to bound).

## Least-privilege ACLs (and the `+restore` foot-gun)

Redis 6+ ACLs; no `@admin`/`@all`/`DEBUG`/`CONFIG`/`KEYS`/`FLUSH*` in the baseline.
Ready-to-edit presets ship in the repo:

- **Source** ([`../deploy/acl-source-redis.txt`](../deploy/acl-source-redis.txt)):
  read-only — `+scan +type +dump +pttl +exists +memory|usage` (plus `+ping +info`).
  replicare writes **nothing** to the source.
- **Target** ([`../deploy/acl-target-redis.txt`](../deploy/acl-target-redis.txt)):
  `+restore +del +scan +exists +module|list` (plus `+ping +info`).

> **Foot-gun:** `RESTORE` is an `@dangerous` command, so it is **excluded from the
> usual read-write presets and must be granted explicitly** (`+restore`). Without
> it, every apply fails loud. Granting it lets the role overwrite any key in the
> ACL's key pattern, so scope `~…` tightly.

The optional notification accelerator additionally needs `+psubscribe`/`+subscribe`
(and `CONFIG SET` where the server isn't preconfigured) on the source. In cluster
mode, grant the same user on **every master** and add `+cluster|shards`/`|slots`.

## Limitations / non-goals (v1)

- **Redis fan-out** (one source → multiple Redis targets) — a single source `SCAN`
  cursor can't serve two targets at different rates without per-target scan state.
  Single source → single target only.
- **Sentinel-hardened failover** — wired but not hardened/tested.
- **Dragonfly** — blocked (RDB compat unverified).
- **Command-reconstruction / big-key incremental transport** — refused (§1.7).
- **Cross-key/group atomicity** — apply is per-key idempotent, **not** wrapped in
  `MULTI`/`EXEC` (a deliberate departure from the relational per-component
  atomicity in §8.1).
- **Cross-engine, multi-master, point-in-time consistency** — never / deferred, as
  for the other engines. Redis converges eventually and self-heals.

## Validating

```sh
replicare validate config.yml
```

connects to both ends, runs the RDB-version and module gates, reports each
replication unit as a single-member component, and (for cluster) validates
per-shard — all before any sync starts.

## Try it end-to-end

[`examples/demo-redis/`](../examples/demo-redis/) is a runnable demo: an old 6.2
source, a modern 7.4 target, a Postgres state-store sidecar, and replicare —
replicating `app:*` keys. Build the image (`task docker`), then
`cd examples/demo-redis && docker compose up` and watch the seeded keys, a live
`SET`, and a `DEL` propagate. The config is
[`examples/demo-redis/config.yml`](../examples/demo-redis/config.yml).
