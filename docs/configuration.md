# Configuration reference

replicare is configured by a single YAML file (passed to every command). The
schema is an engine-neutral envelope plus a typed per-engine connection block.

**Any** string value may reference the environment as `${VAR}` (required — errors if
unset) or `${VAR:-default}` (falls back to `default`) — not just secrets. Expanded
scalars are re-typed on decode, so it works for ports, hosts, addresses, and
booleans too (e.g. `port: ${PGPORT:-5432}`), matching the "env-var overrides for any
field" goal (CLAUDE.md §11). Keeping passwords out of the file is the common case.
An unset `${VAR}` with no default is a hard config-load error (exit 2).

Validate a config without connecting-and-running via `replicare validate <config>`.

## Top-level structure

```yaml
logging:        { ... }   # log level/format
observability:  { ... }   # metrics/status/OTLP endpoints
state_store:    { ... }   # replicare's own progress store (an endpoint)
sources:        { ... }   # named source endpoints
targets:        { ... }   # named target endpoints
syncs:          [ ... ]   # what replicates where (one-way)
nodes:          { ... }   # peer endpoints for active-active clusters (optional)
clusters:       [ ... ]   # active-active (multi-master) groups (optional)
```

`nodes` and `clusters` are **optional and additive** — a config with neither behaves
exactly like a one-way daemon. See [Active-active clusters](#active-active-clusters-nodes-and-clusters)
and the [multi-master design note](multi-master.md).

## `logging`

| Field | Type | Default | Notes |
|---|---|---|---|
| `level` | `debug`\|`info`\|`warn`\|`error` | `info` | |
| `format` | `json`\|`text` | `json` | `text` is friendlier for local runs |

## `observability`

| Field | Type | Default | Notes |
|---|---|---|---|
| `metrics_addr` | listen addr, e.g. `:9090` | off | serves Prometheus `/metrics` |
| `status_addr` | listen addr, e.g. `:8080` | off | serves `/status`, `/healthz`, and `/metrics` |
| `otlp_endpoint` | host:port, e.g. `otel:4317` | off | exports OTel traces to an OTLP/gRPC collector |
| `stall_timeout` | duration (`2m`, `0`, negative) | `2m` | how long a streaming loop may go without completing a pass before `/healthz` reports unhealthy (so Kubernetes restarts a wedged pod). Must exceed your slowest drain pass. Unset/`0` = `2m`; a negative value disables the check |

Leaving an address empty disables that endpoint. When `metrics_addr` and
`status_addr` differ, `/metrics` is served on both.

`/healthz` reflects **streaming liveness**, not target reachability: a loop that
is cycling — even one retrying against a down target — stays healthy; only a loop
that stops completing passes (a hung query on a dropped socket) trips
`stall_timeout` and fails the probe. The daemon also reconnects a dropped
source/target connection on its own between passes, so a transient blip recovers
without a restart; the probe is the backstop for a true wedge.

## Endpoints: `state_store`, `sources`, `targets`

An endpoint is an engine name plus a typed connection block. `sources` and
`targets` are maps of name → endpoint; `state_store` is a single endpoint.

```yaml
sources:
  app:                       # the name referenced by a sync
    engine: postgres
    postgres:                # the engine-specific block
      host: app-db
      port: 5432
      database: app
      user: replicare
      password: "${APP_PW}"
      sslmode: verify-full
      params:                # optional extra connection params
        application_name: replicare
```

### Postgres connection block

Used when an endpoint's `engine` is `postgres`. See [the Postgres engine page](postgres.md)
for the CDC model, FK-component handling, faithful `COPY` transport, and the
effectively-exactly-once bonus when the state store *is* the target.

| Field | Type | Notes |
|---|---|---|
| `host` | string | |
| `port` | int | usually 5432 |
| `database` | string | |
| `user` | string | the least-privilege replicare role |
| `password` | string | use `${VAR}` |
| `sslmode` | `disable`\|`allow`\|`prefer`\|`require`\|`verify-ca`\|`verify-full` | TLS mode (libpq semantics) |
| `params` | map | extra key=value connection parameters |

### MySQL connection block

Used when an endpoint's `engine` is `mysql`. See [the MySQL engine page](mysql.md)
for the two operational wrinkles (a MySQL sync still keeps its state store on
Postgres; MySQL syncs are strictly at-least-once).

| Field | Type | Notes |
|---|---|---|
| `host` | string | |
| `port` | int | default 3306 |
| `database` | string | the DSN default database only — selection may reference other databases as `db.table` (a MySQL schema *is* a database) |
| `user` | string | the least-privilege replicare role |
| `password` | string | use `${VAR}` |
| `tls` | `disable`\|`allow`\|`prefer`\|`require`\|`verify-ca`\|`verify-full` | TLS mode (same spectrum as Postgres `sslmode`); default `prefer` |
| `local_infile` | bool | hint that the target permits `LOAD DATA LOCAL INFILE`, the copy/apply transport (**required** in v1). replicare probes it at connect and halts loud if off — no INSERT fallback yet. A server system variable, not a grant |
| `params` | map | extra key=value DSN parameters |

### Redis connection block

Used when an endpoint's `engine` is `redis`. See [the Redis engine page](redis.md)
for how Redis CDC works (SCAN reconciliation + a target-vs-source delete sweep, not
triggers) and its two wrinkles (a Redis sync still keeps its state store on
Postgres; copy progress is coarse). Redis is topology-shaped rather than a single
host, and carries engine-specific selection and CDC tuning.

| Field | Type | Notes |
|---|---|---|
| `mode` | `standalone`\|`cluster`\|`sentinel` | default `standalone`; `sentinel` is parsed but experimental (failover not hardened in v1) |
| `host` / `port` | string / int | standalone seed (port default 6379) |
| `nodes` | list of `host:port` | seed nodes for `cluster`/`sentinel` (also allowed for standalone) |
| `db` | int | logical DB index (standalone only; **must be 0 in cluster**) |
| `user` / `password` | string | ACL user (Redis 6+); use `${VAR}` for the password |
| `tls` | `disable`\|`allow`\|`prefer`\|`require`\|`verify-ca`\|`verify-full` | same spectrum as Postgres `sslmode`; **default `disable`** — note this diverges from Postgres/MySQL (`prefer`), so set it explicitly to avoid plaintext |
| `sentinel_master` | string | required when `mode: sentinel` |
| `read_from_replica` | bool | offload **value** reads to replicas; delete detection still reads the master (a lagging replica would cause false deletes) |
| `types` | list | optional type filter: `string`/`list`/`set`/`zset`/`hash`/`stream` (needs Redis 6.0+ for server-side `SCAN … TYPE`) |
| `scan_count` | int | `SCAN COUNT` hint per call (default 512); distinct from the neutral drain batch |
| `reconcile_interval` | duration | cadence of the rolling upsert reconciliation scan |
| `delete_sweep_interval` | duration | cadence of the target-vs-source delete sweep (bounds delete-propagation latency) |
| `big_key_warn_bytes` | size | DUMP-and-warn above this (`MEMORY USAGE`); `0` = off |
| `big_key_refuse_bytes` | size | block loud above this; must be ≥ `big_key_warn_bytes` |
| `notifications` | bool | enable the lossy keyspace-notification latency accelerator (default off; never trusted for correctness) |
| `ttl_mode` | `relative`\|`absttl` | `relative` (default) is clock-skew-safe; `absttl` is opt-in for trusted clocks |
| `params` | map | extra engine-internal params |

Selection for a Redis sync is **key-pattern globs** (Redis `SCAN … MATCH`
semantics), not `schema.table` — see [Selection](#selection) below.

**`state_store`** is where replicare keeps its *own* operational state (sync
progress, cursors, the ownership lock) — a dedicated `replicare_state` schema it
creates and owns. It may point at the target DB, the source DB, or a separate
Postgres. (This is distinct from the source-side delta/track tables, which always
live on the source. Redis writes nothing to the source at all, so a Redis sync's
*only* durable state is here — see [the Redis engine page](redis.md).)

## `syncs`

A sync is one replication job: a source, one or more targets, and a table
selection. All targets of a sync must use the same engine as the source
(never cross-engine).

**Fan-out** (a sync with several `targets`) copies and streams each target
**independently**: initial-copy progress is checkpointed per `(sync, target, table)`,
so one slow or restarted target never disturbs another's watermark, and each
converges on its own. (This is also the per-node building block of an
active-active [cluster](#active-active-clusters-nodes-and-clusters).)

```yaml
syncs:
  - name: app-to-warehouse    # unique; also the ownership-lock key
    source: app               # a key in `sources`
    targets: [warehouse]      # keys in `targets` (one or more; fan-out)
    include: ["public.*"]     # selection globs (schema.table)
    exclude: ["*_audit"]      # excluded from the include set
    tuning: { ... }           # optional (see below)
```

### Selection

`include`/`exclude` are `schema.table` globs (`*` matches within a name segment).
For MySQL, a schema *is* a database, so these are `db.table` globs and a single
sync may span several databases. Tables without a primary key or usable unique key
are **skipped with a warning** (they can't be captured). An FK pointing from a
selected table to an *excluded* one triggers a dangling-FK warning — the target
must already satisfy that parent.

For **Redis**, `include`/`exclude` are **key-pattern globs** using Redis `SCAN …
MATCH` semantics (`*`, `?`, `[…]`, hash-tags), matched **exclude-wins**. There are
no tables, keys, or FKs — the "unit" is the logical keyspace (one DB, or DB 0 in
cluster). An optional `types` filter in the `redis:` block narrows selection to
specific value types.

### `tuning` (per sync)

| Field | Type | Default | Meaning |
|---|---|---|---|
| `drain_interval` | duration (`1s`, `500ms`) | `1s` | time between streaming drain passes; longer = more coalescing (less source load) but higher lag |
| `drain_batch` | int | `1000` | max dirty deltas applied **per table per pass**; with `drain_interval` this is the per-table streaming ceiling (~`drain_batch/drain_interval` rows/s). Raise for a high-volume table that lags |
| `apply_concurrency` | int (≥1) | `1` | how many of a component's tables apply **concurrently** during streaming. `1` is strictly sequential; higher fans the per-table apply across the copy-worker pool (bounded by `pool.max_*_connections`). Helps when **several** tables are backlogged at once |
| `retention.max_age` | duration (`24h`, `0` = off) | `24h` | oldest unconsumed delta before a laggard target is reseeded |
| `retention.max_bytes` | size (`512MB`, `0` = off) | off | delta-table on-disk size before reseed |
| `pool.max_source_connections` | int | 4 | source connection cap (copy worker pool is sized from it) |
| `pool.max_target_connections` | int | 4 | target connection cap |

Durations use Go syntax (`ms`, `s`, `m`, `h`); sizes accept `KB`/`MB`/`GB`/`TB`
(×1000) or `KiB`/`MiB`/`GiB` (×1024), or a bare byte count. Defaults favor low
source pressure — see [operations.md](operations.md) for tuning guidance.

For **Redis**, the neutral `retention.*` knobs are inert (there is no source-side
delta queue to bound); Redis pacing lives in the `redis:` block instead
(`scan_count`, `reconcile_interval`, `delete_sweep_interval`).

## Active-active clusters (`nodes` and `clusters`)

> **Status: in progress.** For **Postgres**, a `clusters:` block **runs today** with full
> conflict resolution: the daemon brings up a full-mesh active-active cluster with loop
> suppression (MM3) and HLC last-write-wins (MM4), so writes accepted on any node — including
> concurrent writes to the **same key** — converge to the same value on every node, with no
> config and no user schema change; deletes resolve via GC'd tombstones. **MySQL/Redis**
> clusters are still parse-and-validate only. See the
> [design note](multi-master.md#8-implementation-status) for the full status. These keys are
> **optional and additive**: omit them and replicare behaves exactly as a one-way daemon. Do
> not confuse a replicare **`clusters:`** entry (a group of active-active peer databases) with
> a Redis endpoint's `redis.mode: cluster` (one *sharded* Redis) — different scopes.

An **active-active cluster** keeps N peer databases converged with writes accepted on
**any** node. Conflict resolution is **zero-config** — replicare manages a hidden
per-row version itself (HLC last-write-wins; no column to add, nothing to declare); see
the [design note](multi-master.md). A cluster is **single-engine**, like a sync.

### `nodes`

A map of peer endpoints. A node has the **same shape as a source/target endpoint** (an
`engine` + that engine's connection block) plus an optional `node_id`. Unlike a
source/target, a node is both **read from and written to**.

| Field | Type | Default | Notes |
|---|---|---|---|
| `engine` | string | — | `postgres` \| `mysql` \| `redis`; must match the cluster's engine |
| `node_id` | string | the map key | stable replication-origin identity, unique within a cluster; stamped into each change's version. Usually leave it to default to the key |
| `<engine>:` | block | — | the engine connection block, exactly as for a source/target |

### `clusters`

A list of active-active groups.

| Field | Type | Default | Notes |
|---|---|---|---|
| `name` | string | — | unique across all syncs **and** clusters |
| `engine` | string | — | the one engine all members share (single-engine rule) |
| `members` | list of strings | — | keys into `nodes:`; **≥ 2**, each a distinct node with a distinct `node_id` |
| `topology` | string | `mesh` | v1 supports only `mesh` (full mesh, any N ≥ 2); `ring`/partial are deferred |
| `include` / `exclude` | list of globs | — | table/key selection, interpreted per engine exactly as for a sync |
| `tuning` | block | sync defaults | same knobs as a sync's `tuning` |

```yaml
nodes:
  us: { engine: postgres, postgres: { host: pg-us, port: 5432, database: app, user: replicare, password: ${PW}, sslmode: require } }
  eu: { engine: postgres, postgres: { host: pg-eu, port: 5432, database: app, user: replicare, password: ${PW}, sslmode: require } }
  ap: { engine: postgres, postgres: { host: pg-ap, port: 5432, database: app, user: replicare, password: ${PW}, sslmode: require } }

clusters:
  - name: global-app
    engine: postgres
    members: [us, eu, ap]     # any N >= 2; node_id defaults to us/eu/ap
    include: ["public.*"]
    exclude: ["*_audit"]
    # No conflict block: HLC last-write-wins is automatic. Nothing to declare.
```

### Cycle safety for one-way syncs

Independently of clusters, `replicare` now **rejects an un-declared replication cycle**
among plain one-way `syncs` (e.g. `A→B` *and* `B→A`, or a longer ring) at config load —
that topology silently corrupted data before. A genuine active-active setup must be
declared as a `clusters:` entry, which is exempt.

## Full example

See [`../examples/replicare.yml`](../examples/replicare.yml) for a fully annotated
config.
