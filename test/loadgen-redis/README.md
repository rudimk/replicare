# Redis Load Testing with `loadgen-redis`

This page describes `loadgen-redis`, the harness we use to load-test `replicare`'s Redis engine,
and the Redis sibling of [`test/loadgen`](https://replicare.rudimk.com/load-testing-postgres.html).
It's a manual, idempotent tool that exercises `replicare` against broad, multi-type,
high-volume data; the first run against a source Redis seeds a rich keyspace, and every run
after that applies random mutations. With `replicare` pointed at the source and a target,
`loadgen-redis verify` then checks that the target has converged. The page covers what the
harness generates, how to run it(on its own, against an active-active mesh, as a complete
end-to-end rig and against a Redis Cluster), as well as a few gotchas.

`loadgen-redis` is a developer / test tool, and isn't part of the shipped binary. Run it from a
checkout with `go run ./test/loadgen-redis`(or `task loadgen-redis:*`).

Unlike the Postgres harness, there's no `ddl` step. Redis has no schema, and `replicare`
`RESTORE`s keys straight into the target keyspace, which doesn't need to exist beforehand.

## What it generates

Keys are laid out as `lg:{<bucket>}:<type>:<n>`, and every cohort is chosen to exercise one of
`replicare`'s Redis paths rather than just to hold data:

| Cohort | What it stresses |
|---|---|
| `str` | plain strings, about 20% of which carry a TTL(volatile keys, which exercise TTL replication) |
| `hash` / `list` / `set` / `zset` | every collection type, going through `DUMP` and then `RESTORE` |
| `stream` | streams with consumer groups, the trickiest `DUMP`/`RESTORE` payload |
| `big` | deliberately large keys(hashes of about 100 KB), which take the big-key path / `MEMORY USAGE` gate |
| `lgskip:*` | written to the source only, and `replicare`'s `include: ["lg:*"]` has to exclude it; it's the Redis analogue of `loadgen`'s keyless `audit_log`. `verify` checks that the target holds none of these. |

The `{bucket}` hash tag is what makes the harness work on a cluster. On a standalone / sentinel
server the braces are inert literal bytes, but on a Redis Cluster they spread the keyspace across
slots / shards(exercising `replicare`'s per-master SCAN fan-out) *and* keep a `RENAME`'s old and
new key in one slot, since a cross-slot `RENAME` is an error.

The churn mix includes the two cases `replicare`'s Redis engine handles specially:

1. `RENAME`, which is a delete of the old key plus a create of the new one. It's the analogue of
   a PK change, and puts stress on the delete-reconciliation sweep.
2. Heavy `DEL`. Redis has no delete capture, so deletes are only caught by `replicare`'s
   target-vs-source diff, which makes this the genuinely hard path.

At `--scale 1.0`(the default), the seed writes about 430k keys. For quick loops, scale it down;
`--scale 0.02` writes about 8.6k keys in well under a second.

## Using the harness

The basic loop is to seed the source, start `replicare`, generate some change, then check the
target has caught up:

```sh
# 1. Seed the SOURCE (first run seeds; later runs churn).
go run ./test/loadgen-redis run --dsn "$SOURCE"                 # ~430k keys
go run ./test/loadgen-redis run --dsn "$SOURCE" --scale 0.05    # smaller, faster

# 2. Start replicare source -> target (your own config), then generate change:
go run ./test/loadgen-redis run --dsn "$SOURCE" --ops 500       # a burst of mutations

# 3. Assert the target converged (retries for replication lag):
go run ./test/loadgen-redis verify --source "$SOURCE" --target "$TARGET" --wait 60s
```

### Connection specs

For a standalone / sentinel server, pass a `redis://` URL, for example
`redis://:pw@localhost:6390/0`; left empty, it defaults to `redis://localhost:6379/0`.

For a cluster, pass `--cluster` and a comma-separated seed list, for example
`--dsn "n1:6379,n2:6379,n3:6379"`, and use `--password` for AUTH. It's one tool for both
topologies - see the hash-tag note above.

### Commands & flags

`run --dsn <src> [--scale F] [--ops N] [--seed S] [--cluster] [--node-id K]` seeds the source if
it's empty, or churns `N` ops if it isn't. `--seed` makes a run reproducible, covering both op
selection and the generated values. `--node-id K`(0 by default) selects this writer's key
namespace for active-active testing - see
[Active-active](#active-active-multi-master-load-testing). The default of 0 is the
single-writer(active-passive) behaviour, unchanged.

`verify --source <src> --target <tgt> [--wait D] [--interval D] [--cluster] [--node-id K]`
compares every `lg:*` key version-independently, using a type-aware canonical content
hash(`GET` / sorted `HGETALL` / `LRANGE` / sorted `SMEMBERS` / `ZRANGE WITHSCORES` / `XRANGE`
plus group last-ids) together with TTL presence. It also checks that no `lgskip:*` key leaked
to the target. Since it's a pure equality check over the whole `lg:*` keyspace, it doubles as
the active-active convergence oracle; run it between any two mesh nodes, which both hold the
union. `--node-id K` is the source's node index, and scopes the skip-leak check to that source's
own `lgskip:*` namespace(0 is the classic single-writer check). `--wait` keeps polling until the
keys converge or the timeout runs out, and the command exits non-zero on any drift.

`reset --dsn <db> --yes [--cluster]` runs `DEL` on every `lg:*` and `lgskip:*` key, plus the
`lgmeta:*` marker key, across all nodes' namespaces. It's scoped by prefix, and never runs
`FLUSHDB`.

### Why not compare `DUMP` bytes directly?

The harness runs an old source(6.2) against a modern target(7.4), and their RDB serialisations
differ even for identical data; `replicare` deliberately preserves each side's own encoding. So
`verify` compares a canonical *rendering* of each value, never its serialised bytes. TTL is
compared as presence only(has a TTL or doesn't), since the exact remaining seconds legitimately
differ by the replication delay.

## Active-active, multi-master load testing

The same harness drives a Redis `clusters:` mesh, using disjoint key namespaces. Each writer node
gets `--node-id K`, which moves every key it writes into a private segment right after the
prefix: `lg:n<K>:{<bucket>}:<type>:<n>`(and `lgskip:n<K>:…` for the source-only cohort). Node
segments never overlap, so nodes never collide on a key, and the mesh converges to the clean
union of all the segments. `--node-id 0` is the default, and is byte-for-byte the single-writer
layout described above(`lg:{<bucket>}:…`, with no segment), so active-passive testing isn't
affected.

The segment sits *after* the `lg:`/`lgskip:` prefix, so a `replicare` `include: ["lg:*"]`
selection still matches every node's data keys and still excludes every node's `lgskip:*` keys;
there's no per-node config change needed.

On `run`, `--node-id K` changes two things:

1. Seeding writes only this node's namespace(`lg:n<K>:…`), and the seeded / empty check scans
   that namespace alone. A node therefore re-seeds only when *its own* slice is empty, and keys
   replicated in from its peers don't count as already seeded.
2. Churn sets / updates / deletes only keys in this node's namespace, and a `RENAME` keeps the
   new key in the same segment(and hash-tag bucket). A node only ever changes what it wrote, so
   there's no accidental cross-node conflict. Resolving concurrent *same-key* writes is a
   separate, planned conflict-storm mode(see below).

On `verify`, `--node-id K` is the source's node index. It scopes the skip-leak check to that
source's own `lgskip:n<K>:*` namespace, so a mesh target, which is itself a source with its own
`lgskip:*` keys, never false-positives on the peer's excluded cohort. The content comparison
itself is a pure equality check over the whole `lg:*` keyspace, so it doubles as the convergence
oracle; run it between any two mesh nodes, and both have to hold the identical union.

Always pass the same `--node-id` for a given node's database. Keep `--scale` modest per node,
since Redis holds everything in RAM and the mesh target holds every node's union.

### Three-node mesh example

Take a `replicare` `clusters:` mesh running over three Redis nodes, `A`, `B` and `C`(configured
as described in
[Active-active replication](https://replicare.rudimk.com/active-active.html#setting-up-a-cluster)),
with `$A`/`$B`/`$C` as their connection specs. There's no `ddl` step, since Redis has no schema:

```sh
# 1. Seed each node into its own disjoint namespace.
go run ./test/loadgen-redis run --dsn "$A" --node-id 0 --scale 0.1
go run ./test/loadgen-redis run --dsn "$B" --node-id 1 --scale 0.1
go run ./test/loadgen-redis run --dsn "$C" --node-id 2 --scale 0.1

# 2. Churn every node concurrently (each in its own namespace).
go run ./test/loadgen-redis run --dsn "$A" --node-id 0 --ops 800 --seed 1 &
go run ./test/loadgen-redis run --dsn "$B" --node-id 1 --ops 800 --seed 2 &
go run ./test/loadgen-redis run --dsn "$C" --node-id 2 --ops 800 --seed 3 &
wait

# 3. Convergence oracle: every pair must be identical (all hold the union).
#    --node-id names the SOURCE so the skip-leak check scopes to its lgskip namespace.
go run ./test/loadgen-redis verify --source "$A" --target "$B" --node-id 0 --wait 300s
go run ./test/loadgen-redis verify --source "$A" --target "$C" --node-id 0 --wait 300s
```

Convergence is transitive, so `A==B` and `A==C` mean all three agree. A run that ends with
`CONVERGED: … no skip leak` on every pair, with no `HALTED` / `stream pass error` in the daemon
logs, is a passing active-active load test.

> ⚠️ There's no conflict-storm mode yet. The disjoint model above never has two nodes write the
> same key, so it only tests convergence of the *union*. A follow-up `--overlap` mode will
> deliberately overlap node namespaces to exercise HLC last-write-wins under concurrent same-key
> writes; there, `verify` will still check convergence(all nodes equal), but not which value
> wins, since the last-write-wins winner is nondeterministic.

> ⚠️ When re-seeding, reset the state store per node(see [Gotchas](#gotchas)). In a mesh this
> applies to every node: clear each node's state-store entries alongside
> `loadgen-redis reset`(which already removes every node's `lg:`/`lgskip:` namespace).

## Running a full load test end-to-end

The commands above assume `replicare` is already running with an existing config. Here's the
complete rig this harness is meant to drive - two Redis instances, a Postgres state store, a
config, the daemon and a churn-and-verify loop. It's the exact flow we use to validate
`replicare`'s Redis copy and streaming under load.

To start with, set up two Redis instances and a Postgres state store. The bundled harness is the
quickest source / target pair, with an old 6.2 source and a modern 7.4 target. The state store
is Postgres, since that's v1's only `StateStore` backend; the *replicated data* is Redis, while
the daemon's own progress / cursors live in Postgres:

```sh
task harness:redis:up          # source on :6390, target on :6391
docker run -d --name lg-state -e POSTGRES_PASSWORD=pw -p 55440:5432 postgres:16
docker exec lg-state psql -U postgres -c 'CREATE DATABASE replicare_state'

export SOURCE="redis://localhost:6390/0"
export TARGET="redis://localhost:6391/0"
```

Next, write a minimal `replicare` config(`loadtest-redis.yaml`). The `drain_interval` is kept
short so the reconciliation SCAN and the delete sweep keep up under churn:

```yaml
logging: { level: info, format: text }
observability: { metrics_addr: ":19091", status_addr: ":18081" }
state_store:
  engine: postgres
  postgres: { host: localhost, port: 55440, database: replicare_state, user: postgres, password: pw, sslmode: disable }
sources:
  src: { engine: redis, redis: { host: localhost, port: 6390, db: 0 } }
targets:
  tgt: { engine: redis, redis: { host: localhost, port: 6391, db: 0 } }
syncs:
  - name: redisload
    source: src
    targets: [tgt]
    include: ["lg:*"]          # excludes lgskip:* by construction
    tuning: { drain_interval: 1s }
```

Then seed the source and start `replicare`. Redis CDC is capture-less, and the initial copy
SCANs the source, so seeding before the daemon starts is fine:

```sh
go run ./test/loadgen-redis run --dsn "$SOURCE"          # seed the source
go run ./cmd/replicare run loadtest-redis.yaml &         # start the daemon
```

Once it's running, check that the initial copy converged:

```sh
go run ./test/loadgen-redis verify --source "$SOURCE" --target "$TARGET" --wait 120s
# => CONVERGED: N replicated keys match, no skip leak
```

Finally - the churn-and-verify loop, which is the actual load test. Each round applies a random
burst of mutations(including `RENAME`s and heavy `DEL`s), and checks that the target converges
again while the daemon streams:

```sh
for r in 1 2 3 4; do
  go run ./test/loadgen-redis run --dsn "$SOURCE" --ops 800 --seed $r
  go run ./test/loadgen-redis verify --source "$SOURCE" --target "$TARGET" --wait 120s
done
```

To stress the delete-reconciliation sweep and the streaming backlog, churn several times
back-to-back *before* verifying. Deletes and renames then pile up while the sweep runs, and
converge afterwards:

```sh
for r in $(seq 1 8); do go run ./test/loadgen-redis run --dsn "$SOURCE" --ops 800 --seed $r; done
go run ./test/loadgen-redis verify --source "$SOURCE" --target "$TARGET" --wait 300s
```

A run that finishes with `CONVERGED: … no skip leak`, with no `HALTED` / `stream pass error` in
the daemon log, is a passing load test. While it runs, watch `replicare_deletes_reconciled_total`
on `:19091/metrics` climb as the `DEL`/`RENAME` churn is reconciled.

## Cluster mode

The same tool works against Redis Cluster. `--cluster` switches *every* connection spec in a
command to a comma-separated seed list, and `--password` is shared by both sides too, so a
`verify` compares a cluster with a cluster; it can't compare a standalone server with a cluster in
one command. The cluster harness brings up one 3-node cluster, which `run` seeds and churns as the
source; point `verify` at it and at a second cluster that `replicare` replicates into:

```sh
task harness:redis:cluster:up
go run ./test/loadgen-redis run    --dsn "127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002" --cluster --scale 0.1
go run ./test/loadgen-redis verify --source "127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002" --target "…target cluster seeds…" --cluster --wait 120s
```

The `{bucket}` hash tags spread keys across the cluster's slots, so the per-master SCAN fan-out
is genuinely exercised, while keeping each `RENAME` within a single slot.

## Gotchas

There are a few things that commonly trip up a load test:

- Reset the state store when re-seeding from scratch. `replicare` records per-unit copy
  progress in its state store; if the source is wiped and re-seeded without clearing that
  state, it may treat the unit as already copied. Use a fresh `replicare_state` database(or
  clear the `replicare` schema in it) alongside `loadgen-redis reset`.
- `RENAME` needs an existing source key. The tool renames within the seeded string range, and
  silently skips a key that an earlier op already renamed / deleted, which is harmless for the
  harness.
- `lgmeta:counts` is a small bookkeeping key, holding per-type seeded counts so churn can
  address existing keys without scanning. It's neither `lg:*` nor `lgskip:*`, so `replicare`
  and `verify` both ignore it, and `reset` removes it.

## Task shortcuts

The same steps are also available as `task` targets:

```sh
task loadgen-redis:seed   DSN="$SOURCE" SCALE=0.1          # seed / scale
task loadgen-redis:churn  DSN="$SOURCE" OPS=500            # a churn burst
task loadgen-redis:verify SRC="$SOURCE" TGT="$TARGET" WAIT=60s
task loadgen-redis:reset  DSN="$SOURCE"                    # add CLUSTER=1 for a cluster
```
