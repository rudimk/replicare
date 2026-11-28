# Configuration Reference

This page is the reference for `replicare`'s configuration file. `replicare` is configured by a
single YAML file, which is passed to every command; it's made up of an engine-neutral
envelope(logging, observability, the state store and the syncs themselves) plus a typed
connection block for each engine. The page goes through every section of that file in turn, lists each
field with its type and default, and explains the behaviour behind the fields that need more
than a line in a table.

## Environment variables & validation

*Any* string value in the file can reference the environment, not just secrets. `${VAR}` is
required and errors if it's unset, while `${VAR:-default}` falls back to `default`. Expanded
scalars are re-typed on decode, so this works for ports, hosts, addresses and booleans too(for
example `port: ${PGPORT:-5432}`); we built it this way so that env-var overrides work for any
field, though keeping passwords out of the file is still the common case. An unset `${VAR}`
with no default is a hard config-load error(exit 2).

To check a config before running it, use `replicare validate <config>`. It loads the config first,
then connects to every source and target, read-only, to run the pre-flight checks; it never starts
replicating.

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

`nodes` and `clusters` are optional and purely additive; a config with neither of them behaves
exactly like a one-way daemon. See [Active-active clusters](#active-active-clusters-nodes-and-clusters)
and [Active-active replication](failover.md#several-active).

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
| `stall_timeout` | duration(`2m`, `0`, negative) | `2m` | how long a streaming loop may go without making progress before `/healthz` reports unhealthy(so Kubernetes restarts a wedged pod). Progress is any step of work finishing, not only a whole pass, so it has to be longer than the slowest single step(see below). Unset / `0` means `2m`; a negative value disables the check |

Leaving an address empty disables that endpoint. When `metrics_addr` and `status_addr` differ,
`/metrics` is served on both.

`/healthz` reflects streaming liveness, and not target reachability. A loop that's still
cycling - even one that's retrying against a down target - stays healthy; only a loop that
stops making progress(say, a query hung on a dropped socket) trips `stall_timeout` and fails
the probe. Progress is counted as work finishes inside a pass, not only at its end: each table's
changes committing on the target, each attempt at a group of linked tables(even one that hit a
row whose parent hasn't arrived yet and will be retried), and, during a reseed, each table
emptied and each chunk copied. So a long pass on a busy source stays healthy as long as it keeps
moving, and `stall_timeout` only has to be longer than the slowest single step: applying one
batch to one table, or emptying the largest table during a reseed. The daemon also reconnects a dropped source / target connection on its own between
passes, so a transient blip recovers without a restart, and the probe is only there as the
backstop for a true wedge.

## Endpoints: `state_store`, `sources`, `targets`

An endpoint is an engine name plus a typed connection block. `sources` and `targets` are maps
from a name to an endpoint, while `state_store` is a single endpoint.

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

This block is used when an endpoint's `engine` is `postgres`. See
[the Postgres engine page](postgres.md) for the capture model, how FK components are handled,
the faithful `COPY` transport, and the effectively-exactly-once bonus when the state store *is*
the target.

| Field | Type | Notes |
|---|---|---|
| `host` | string | |
| `port` | int | usually 5432 |
| `database` | string | |
| `user` | string | the least-privilege `replicare` role |
| `password` | string | use `${VAR}` |
| `sslmode` | `disable`\|`allow`\|`prefer`\|`require`\|`verify-ca`\|`verify-full` | TLS mode(libpq semantics); default `prefer` |
| `params` | map | extra key=value connection parameters |

### MySQL connection block

This block is used when an endpoint's `engine` is `mysql`. See [the MySQL engine page](mysql.md)
for the two operational wrinkles: a MySQL sync still keeps its state store on Postgres, and
MySQL syncs are strictly at-least-once.

| Field | Type | Notes |
|---|---|---|
| `host` | string | |
| `port` | int | default 3306 |
| `database` | string | the DSN default database only - selection may reference other databases as `db.table`(a MySQL schema *is* a database) |
| `user` | string | the least-privilege `replicare` role |
| `password` | string | use `${VAR}` |
| `tls` | `disable`\|`allow`\|`prefer`\|`require`\|`verify-ca`\|`verify-full` | TLS mode(the same spectrum as Postgres `sslmode`); default `prefer` |
| `local_infile` | bool | a hint that the target permits `LOAD DATA LOCAL INFILE`, which is the copy / apply transport(required in v1). `replicare` probes it at connect and halts loudly if it's off; there's no INSERT fallback yet. It's a server system variable, not a grant |
| `params` | map | extra key=value DSN parameters |

### Redis connection block

This block is used when an endpoint's `engine` is `redis`. Redis is shaped by its topology
rather than by a single host, and its block also carries engine-specific selection and capture
tuning. See [the Redis engine page](redis.md) for how Redis change capture works(SCAN
reconciliation plus a target-vs-source delete sweep, rather than triggers) and for its two
wrinkles: a Redis sync still keeps its state store on Postgres, and copy progress is coarse.

| Field | Type | Notes |
|---|---|---|
| `mode` | `standalone`\|`cluster`\|`sentinel` | default `standalone`; `sentinel` is parsed but experimental(failover isn't hardened in v1) |
| `host` / `port` | string / int | the standalone seed(port defaults to 6379) |
| `nodes` | list of `host:port` | seed nodes for `cluster` / `sentinel`(also allowed for standalone) |
| `db` | int | logical DB index(standalone only; it has to be 0 in cluster) |
| `user` / `password` | string | ACL user(Redis 6+); use `${VAR}` for the password |
| `tls` | `disable`\|`allow`\|`prefer`\|`require`\|`verify-ca`\|`verify-full` | the same spectrum as Postgres `sslmode`. The default is `disable`, which differs from Postgres / MySQL(`prefer`), so set it explicitly to avoid plaintext |
| `sentinel_master` | string | required when `mode: sentinel` |
| `read_from_replica` | bool | offloads *value* reads to replicas; delete detection still reads the master, since a lagging replica would cause false deletes |
| `types` | list | an optional type filter: `string`/`list`/`set`/`zset`/`hash`/`stream`(needs Redis 6.0+ for server-side `SCAN … TYPE`) |
| `scan_count` | int | the `SCAN COUNT` hint per call(default 512); it's separate from the neutral drain batch |
| `reconcile_interval` | duration | how often the rolling upsert reconciliation scan runs |
| `delete_sweep_interval` | duration | how often the target-vs-source delete sweep runs(this bounds how long a delete takes to propagate) |
| `big_key_warn_bytes` | size | DUMP and warn above this size(`MEMORY USAGE`); `0` turns it off |
| `big_key_refuse_bytes` | size | block loudly above this size; it has to be at least `big_key_warn_bytes` |
| `notifications` | bool | enables the lossy keyspace-notification latency accelerator(off by default, and never trusted for correctness) |
| `ttl_mode` | `relative`\|`absttl` | `relative`(the default) is safe under clock skew; `absttl` is opt-in, for trusted clocks |
| `params` | map | extra engine-internal params |

Selection for a Redis sync uses key-pattern globs(with Redis `SCAN … MATCH` semantics), and not
`schema.table`; see [Selection](#selection) below.

### The state store

`state_store` is where `replicare` keeps its *own* operational state(sync progress, cursors and
the ownership lock), in a dedicated `replicare_state` schema that it creates and owns. It can
point at the target database, the source database or a separate Postgres. This is a different
thing from the source-side delta / track tables, which always live on the source. Redis writes
nothing to the source at all, so a Redis sync's *only* durable state lives here; see
[the Redis engine page](redis.md).

## `syncs`

A sync is one replication job: a source, one or more targets, and a table selection. Every
target of a sync has to use the same engine as its source, since `replicare` never replicates
across engines.

A sync with several `targets`(fan-out) copies and streams to each target independently.
Initial-copy progress is checkpointed per `(sync, target, table)`, so one slow or restarted
target never disturbs another's watermark, and each target converges on its own. A change queued
on the source is only cleared once every target reading that table from the same database has it:
every target of the sync, the targets of other syncs reading the same database, and paused syncs
that have run before. So a target that's slow, down or paused misses nothing. The retention limit
still bounds the queue, but a sync only ever gives up on its own target. This is also
the per-node building block of an active-active [cluster](#active-active-clusters-nodes-and-clusters).

```yaml
syncs:
  - name: app-to-warehouse    # unique; also the ownership-lock key
    source: app               # a key in `sources`
    targets: [warehouse]      # keys in `targets` (one or more; fan-out)
    enabled: true             # optional; false PAUSES just this sync (see below)
    sync_sequences: false     # optional; true keeps DR counters warm (see below)
    defer_indexes:            # optional; Postgres only, on unless set false (see below)
      enabled: true
    target_triggers:          # optional; Postgres only, on unless set false (see below)
      enabled: true
    skip_initial_copy:        # optional; Postgres and MySQL, off unless set true (see below)
      enabled: false
    copy_order: auto          # optional; Postgres, auto unless set (see below)
    include: ["public.*"]     # selection globs (schema.table)
    exclude: ["*_audit"]      # excluded from the include set
    tuning: { ... }           # optional (see below)
```

### Keeping DR counters warm: `sync_sequences`

`sync_sequences` is an opt-in switch for one-way / DR setups only, and it's off by default.
With it on, the daemon advances each replicated table's owned identity / serial /
`AUTO_INCREMENT` counter on the target to `max(id)+1`, on a lazy interval during streaming.
It's the same mechanism as the
[`reseed-sequences`](cli.md#reseed-sequences-config---sync-name---target-name---dry-run)
command, just kept running, so a promoted DR node has nothing sequence-related left to do at
cutover.

Turning it on adds a steady-state grant on the *target*(Postgres `UPDATE` on each sequence,
MySQL `ALTER` on each table). A missing grant is logged and retried, and never fatal. The
setting is refused at config load when the target is also a member of an active-active
cluster, since advancing a counter across a mesh is silent data loss; use UUID / ULID keys
there instead. See
[sequences & identity counters in operations](operations.md#keeping-counters-warm-continuously-sync_sequences).

### Faster first copies: `defer_indexes`

On a Postgres sync, `replicare` takes the target's plain indexes down just before each table's
initial copy and builds them again once every table is streaming. A large copy goes much faster
this way, since otherwise every copied row has to go into every index on its table. It's on by
default:

```yaml
syncs:
  - name: app-to-warehouse
    # ...
    defer_indexes:
      enabled: true               # default; false copies with the indexes in place
      maintenance_work_mem: 1GB   # optional; memory per index build (default: the server's)
      parallel_workers: 4         # optional; extra workers per build (default: the server's)
```

| Field | Default | Meaning |
|---|---|---|
| `enabled` | on | `false` leaves the target's indexes alone and copies with them in place, as before. |
| `maintenance_work_mem` | the target server's setting | Memory each index build may sort in, as Postgres accepts it(`512MB`, `2GB`). About an eighth of the target's RAM, at most `2GB`, is a good size. |
| `parallel_workers` | the target server's setting | How many extra workers each build may use(Postgres `max_parallel_maintenance_workers`). `4` suits a target with 4 or more cores. |

Only plain indexes are taken down: primary keys, unique indexes and indexes behind a constraint
always stay, since `replicare` needs them to apply changes. A table the target role doesn't own
keeps its indexes too, since only the owner may drop and create them; `replicare` logs a
warning naming each such table. The indexes of partitioned tables are left alone as well.

It's never used on a target that is also a member of an active-active cluster, since that node
serves live traffic: unset, it's simply off there, and `enabled: true` is refused at config load.
Setting `enabled: true` on a MySQL or Redis sync is refused too. How it runs, how to watch it, and
how to put the indexes back by hand are in
[operations](operations.md#target-indexes-during-the-first-copy).

With Helm, it's set in the sync's entry under `config.syncs` in the chart's values, like any other
sync setting.

### The order a first copy reads in: `copy_order`

`replicare` normally reads each table in order of its key, a range of keys at a time. That's fast
when the rows are stored on disk roughly in key order. When they aren't, for example in a table
keyed on `(customer, day)` whose rows arrive day by day for every customer, each row in a key range
sits somewhere else on disk, and the source has to fetch them one by one. On a large table that
turns a copy of hours into one of weeks, and it keeps the source's disk busy for the application
too.

So on a Postgres source, `replicare` reads a table like that in the order its rows are stored
instead, a run of disk blocks at a time, which the source reads in one go:

```yaml
syncs:
  - name: app-to-warehouse
    # ...
    copy_order: auto      # default
```

| Value | Meaning |
|---|---|
| `auto` | The default. A table of 1 GiB or more whose rows are stored out of key order is read in storage order; every other table in key order. |
| `key` | Every table is read in key order, as before version 0.8.2. |
| `storage` | Every table that can be is read in storage order, whatever its size. |

How `auto` decides, and what it logs, is in
[operations](operations.md#reading-in-storage-order). Storage order needs Postgres 14 or later on
the source, and is never used for a partitioned table or for a table in a loop of foreign keys,
which are read in key order whatever is set. `storage` on a MySQL or Redis sync is refused at
config load; `auto` and `key` change nothing there. An active-active cluster always reads in key
order.

With Helm, it's set in the sync's entry under `config.syncs` in the chart's values, like any other
sync setting.

### The target's own triggers: `target_triggers`

On a Postgres sync, `replicare` keeps the target's own triggers from firing on the changes it
writes. On the source those triggers have already done their work(a version row written, a column
stamped), and `replicare` copies the result; firing them again on the target would write
duplicates, change values, or refuse `replicare`'s writes outright. It's on by default:

```yaml
syncs:
  - name: app-to-warehouse
    # ...
    target_triggers:
      enabled: true               # default; false lets the target's triggers fire, as before
```

| Field | Default | Meaning |
|---|---|---|
| `enabled` | on | `false` lets the target's triggers fire on `replicare`'s writes. On the next start, any trigger `replicare` had switched off is switched back on. |

Where the target allows it, ordinary triggers are skipped by running `replicare`'s own connections
in Postgres' replica mode, which changes nothing on the target. Triggers that would still fire are
switched off while the sync runs, their modes saved first, and switched back on when the sync is
paused or by `replicare triggers restore`. Replica mode needs a superuser, a managed service's
admin role(`rds_superuser` on Amazon RDS) or, on Postgres 15 and newer, a
`GRANT SET ON PARAMETER session_replication_role` to the role `replicare` connects as; switching a
trigger off needs ownership of its table. A target without triggers on its replicated tables runs
exactly as before.

It's never used on a target that is also a member of an active-active cluster, since the
application writes there too: unset, it's simply off there, and `enabled: true` is refused at
config load. Setting `enabled: true` on a MySQL or Redis sync is refused too. How it decides, how
to watch it, and what to check at a cutover are in
[operations](operations.md#the-targets-own-triggers).

With Helm, it's set in the sync's entry under `config.syncs` in the chart's values, like any other
sync setting.

### Starting without a copy: `skip_initial_copy`

A new sync normally copies every table before it starts streaming changes. When the target
already holds the same data as the source, that copy rewrites every row for nothing. The usual
case is the way back after a cutover: the old source becomes the target of a sync running the
other way, and at the moment of the cutover it holds exactly what the new source does.
`skip_initial_copy` starts such a sync streaming straight away:

```yaml
syncs:
  - name: back-to-old
    # ...
    skip_initial_copy:
      enabled: true   # default false: copy as usual
      verify: true    # default; false skips the check described below
```

| Field | Default | Meaning |
|---|---|---|
| `enabled` | off | `true` marks every table that hasn't started copying as copied, without copying it, and streams from there. |
| `verify` | on | Once the target has caught up, compare each of those tables with the source, row by row, and report any difference. `false` leaves them unchecked. |

Skipping the copy is only right if the target matched the source **when capture was installed
on the source**. From that moment every insert, update and delete on the source is queued, so the
sync only has to work through the queue. A change made before capture went on is never sent. So
the order matters:

1. Stop writes to the source, and wait until the target has every change(for the way back after a
   cutover: the forward sync's backlog is 0).
2. Install capture on the source with
   [`replicare capture install <config> --sync <name>`](cli.md#capture-installremove-config---sync-name), before any write reaches it.
3. Let writes resume. They're captured from now on.
4. Start the sync with `skip_initial_copy` on, whenever suits; the queue holds everything until then.

The check is what makes a mistake visible. Once every table's oldest queued change is under a
minute old, the daemon compares each skipped table with the source, exactly as
[`replicare verify`](cli.md#verify-config-options) does: one key range at a time, on
connections of its own, with rows still on their way checked again before they're reported. It
reads every row on both sides, so on a large database it can take hours; changes keep streaming
meanwhile. Each table that matches is logged(`copy.skip_check_ok`) and counts as an ordinary
finished copy from then on. A table that differs is repaired: every row found missing, extra or
different is queued on the source as a change, so streaming writes it again from the source's
current value(or deletes it from the target), and the table is compared again a minute later,
until it matches. Each round is logged as `copy.skip_check_repairing`, with how many rows differ
and a few of their keys. Only a difference that stops shrinking over 3 rounds in a row(usually
because the target refuses the rows, which the streaming logs then say) is logged at ERROR as
`copy.skip_check_drift`; copy that table with
[`replicare reseed`](operations.md#forcing-a-reseed). Until a table has passed,
`replicare status` shows `skipped` in its `COPY` column, and the
`replicare_copy_skipped_tables` metric counts tables by where their check stands. A restart
checks again any table that hasn't passed yet; one that has isn't checked again.

A few more details:

- Only tables that haven't started copying are skipped. A table part-way through a copy finishes
  it, and a reseed always copies for real. So turn it on for a sync that has never run, not one
  that's half-way through its copy.
- Nothing is dropped or rewritten on the target: no index is taken down, and no table is emptied.
- With the option turned off again later, tables skipped earlier are still checked, unless
  `verify` is `false`.
- It's for Postgres and MySQL syncs; on a Redis sync, or a target that's also a member of an
  active-active cluster, it's refused at config load.

With Helm, it's set in the sync's entry under `config.syncs` in the chart's values, like any other
sync setting.

### Pausing a sync: `enabled`

`enabled` is a per-pipeline pause switch, and it's a pure opt-out. Leave it out(or set it to
`true`) and the sync runs, so a config written before this field existed behaves exactly as it
did. Setting `enabled: false` pauses that one sync, while other syncs(and clusters) in the
same config keep running.

| `enabled` | Behaviour |
|---|---|
| unset / `true` | The daemon brings the sync up and streams it(the default). |
| `false` | The daemon skips it at startup, and doesn't take its ownership lock. There's no copy, no streaming, and no capture install for a sync that's never been started. |

Pausing is data-loss-free by design. If a sync was already running when it's paused, its
source capture triggers are left in place, so change deltas keep queuing on the source the
whole time it's paused. Setting `enabled: true` again(and restarting) resumes the sync and
drains the accumulated backlog, so the target catches up with nothing lost.

That comes with a trade-off. While a sync is paused, its source-side delta tables keep growing,
and the [retention / reseed source protection](operations.md) *doesn't* run, since it lives in
the streaming loop, which is skipped. A long pause on a busy source can therefore grow the
source's `replicare` schema without any bound. We'd recommend pausing for maintenance /
cutover windows, and not indefinitely; to stop for a long time, it's better to remove the sync
and tear it down(so capture is uninstalled) than to leave an open-ended pause.

The setting takes effect when the daemon starts, so pausing or resuming is a config change plus
a restart, and not a live toggle. On Kubernetes / Helm, change the value and run `helm upgrade`;
the pod rollout that follows applies it. Set `enabled` on the sync by name in the config. Helm
`--set` addresses syncs by array index(`config.syncs[0]` is the first sync), so with several
pipelines it's easy to pause the wrong one; see [kubernetes.md](kubernetes.md#single-active-rollouts--state).
Check which sync actually paused in the daemon logs -
`sync disabled (paused); skipping sync=<name>` for a paused sync,
`sync streaming sync=<name>` for a running one - or with `replicare status`, which shows a
`[PAUSED]` banner. Clusters have the same `enabled` flag,
and setting it to `false` pauses all of that cluster's mesh edges.

Pausing every pipeline is fine, too; the daemon stays up. With all syncs(and clusters) paused,
the daemon *doesn't* exit. It idles, keeps serving `/metrics`, `/status` and `/healthz`, and
stays a healthy pod, so a Kubernetes Deployment doesn't restart-loop it. `replicare status` then
shows each paused sync with a `[PAUSED]` banner(and, when live, its still-growing delta backlog)
rather than a stale "streaming" phase.

### Selection

`include` and `exclude` are `schema.table` globs, where `*` matches within a single name
segment. On MySQL a schema *is* a database, so these are `db.table` globs there, and a single
sync can span several databases. Tables without a primary key or usable unique key are skipped
with a warning, since they can't be captured. An FK pointing from a selected table to an
*excluded* one triggers a dangling-FK warning, because the target has to already satisfy that
parent on its own.

For Redis, `include` and `exclude` are key-pattern globs using Redis
`SCAN … MATCH` semantics(`*`, `?`, `[…]`, hash-tags), matched so that an exclude always wins. There are
no tables, keys or FKs to deal with; the "unit" is the logical keyspace(one DB, or DB 0 in
cluster). An optional `types` filter in the `redis:` block narrows the selection down to
specific value types.

### `tuning`, per sync

| Field | Type | Default | Meaning |
|---|---|---|---|
| `drain_interval` | duration(`1s`, `500ms`) | `1s` | the time between streaming drain passes; a longer interval coalesces more(less source load) but raises lag |
| `drain_batch` | int | `1000` | the most dirty deltas applied per table, per pass. Together with `drain_interval` this sets the per-table streaming ceiling(roughly `drain_batch/drain_interval` rows/s). Raise it for a high-volume table that lags; any value is safe, since the rows are read and deleted at most 500 keys per statement |
| `apply_concurrency` | int(at least 1) | `1` | how many of a component's tables apply concurrently during streaming. `1` is strictly sequential; a higher value fans the per-table apply across the copy-worker pool(bounded by `pool.max_*_connections`). It helps when *several* tables are backlogged at once |
| `retention.max_age` | duration(`24h`, `0` = off) | `24h` | the oldest unconsumed delta allowed before a lagging target is reseeded |
| `retention.max_bytes` | size(`512MB`, `0` = off) | off | the delta table's on-disk size allowed before a reseed |
| `pool.max_source_connections` | int | 4 | the source connection cap(the copy worker pool is sized from it) |
| `pool.max_target_connections` | int | 4 | the target connection cap |

Durations use Go syntax(`ms`, `s`, `m`, `h`). Sizes accept `KB`/`MB`/`GB`/`TB`(multiples of
1000), `KiB`/`MiB`/`GiB`(multiples of 1024), or a bare byte count. The defaults favour low
pressure on the source; see [operations.md](operations.md) for tuning guidance.

For Redis, the neutral `retention.*` knobs do nothing, since there's no source-side delta queue
to bound. Redis pacing lives in the `redis:` block instead(`scan_count`, `reconcile_interval`,
`delete_sweep_interval`).

## Active-active clusters: `nodes` and `clusters`

A `clusters:` block runs a full active-active cluster on Postgres, MySQL and Redis. Writes
accepted on any node - including changes to the *same* row on two nodes at once - end up with
the same value on every node, with nothing to configure and no change to the application's
tables. These keys are optional and purely additive; leave them out and `replicare` behaves
exactly like a one-way daemon.

An active-active cluster keeps N peer databases in step, with writes accepted on *any* node.
Conflicting changes are settled automatically: the later change wins, using a timestamp
`replicare` keeps for each row, with no column to add and nothing to declare. See
[Active-active replication](failover.md#several-active) for how this works. Like a sync, a cluster sticks
to a single engine.

> ⚠️ A `replicare` `clusters:` entry(a group of active-active peer databases) and a Redis
> endpoint's `redis.mode: cluster`(one *sharded* Redis) are two entirely different things, and
> it's easy to mix them up.

### `nodes`

`nodes` is a map of peer endpoints. A node has the same shape as a source / target
endpoint(an `engine` plus that engine's connection block), with an optional `node_id` on top.
Unlike a source or a target, though, a node is both read from *and* written to.

| Field | Type | Default | Notes |
|---|---|---|---|
| `engine` | string | none | `postgres` \| `mysql` \| `redis`; has to match the cluster's engine |
| `node_id` | string | the map key | the stable replication-origin identity, unique within a cluster and stamped into each change's version. It's usually best left to default to the key |
| `<engine>:` | block | none | the engine connection block, exactly as for a source / target |

### `clusters`

`clusters` is a list of active-active groups.

| Field | Type | Default | Notes |
|---|---|---|---|
| `name` | string | none | unique across all syncs *and* clusters |
| `engine` | string | none | the one engine all members share(the single-engine rule) |
| `members` | list of strings | none | keys into `nodes:`; 2 or more, each a distinct node with a distinct `node_id` |
| `topology` | string | `mesh` | v1 only supports `mesh`(full mesh, any N of 2 or more); `ring` / partial layouts are deferred |
| `include` / `exclude` | list of globs | none | the table / key selection, interpreted per engine exactly as for a sync |
| `tuning` | block | sync defaults | the same knobs as a sync's `tuning` |

```yaml
nodes:
  us: { engine: postgres, postgres: { host: pg-us, port: 5432, database: app, user: replicare, password: ${PW}, sslmode: require } }
  eu: { engine: postgres, postgres: { host: pg-eu, port: 5432, database: app, user: replicare, password: ${PW}, sslmode: require } }
  ap: { engine: postgres, postgres: { host: pg-ap, port: 5432, database: app, user: replicare, password: ${PW}, sslmode: require } }

clusters:
  - name: global-app
    engine: postgres
    members: [us, eu, ap]     # any N >= 2; node_id defaults to us/eu/ap
    enabled: true             # optional; false pauses ALL of this cluster's mesh edges
    include: ["public.*"]
    exclude: ["*_audit"]
    # No conflict block: HLC last-write-wins is automatic. Nothing to declare.
```

A cluster has the same `enabled` opt-out as a sync([above](#pausing-a-sync-enabled)). Unset /
`true` runs every mesh edge, while `false` pauses the whole cluster, with all of its edges
skipped at startup.

> ⚠️ At the moment, there's no way to pause a single edge of a cluster; `enabled` pauses all of
> them or none.

### Cycle safety for one-way syncs

Separately from clusters, `replicare` rejects an undeclared replication cycle among plain
one-way `syncs` at config load - for example `A→B` *and* `B→A`, or a longer ring. Before this
check existed, that topology silently corrupted data. A genuine active-active setup has to be
declared as a `clusters:` entry, which is exempt from the check.

A paused sync(`enabled: false`) moves no data, so it doesn't count. That lets one config hold
a sync and its way back, `A→B` and `B→A`, as long as only one of them runs at a time; switching
over is a config change plus a restart, as with any pause. See
[cutting over](cutover.md#on-the-day).

## Full example

See [`../examples/replicare.yml`](https://github.com/rudimk/replicare/blob/main/examples/replicare.yml)
for a fully annotated config. For a single config that runs a Postgres pipeline and a Redis
pipeline side by side(which is how the per-sync single-engine model is meant to be used), see
[`../examples/postgres-and-redis.yml`](https://github.com/rudimk/replicare/blob/main/examples/postgres-and-redis.yml).
