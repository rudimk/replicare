# loadgen-redis — a Redis load + verification harness for replicare

`loadgen-redis` is the Redis sibling of [`test/loadgen`](../loadgen/README.md). It
is a manual, idempotent tool for exercising replicare's Redis engine against
**broad, multi-type, high-volume** data. You run it against a **source** Redis; it
seeds a rich keyspace the first time and applies random mutations on every
subsequent run. Point replicare at the source and a target, and use
`loadgen-redis verify` to assert the target converged.

It is a developer/test tool — **not** part of the shipped binary. Run it from a
checkout with `go run ./test/loadgen-redis` (or `task loadgen-redis:*`).

Unlike the Postgres harness there is **no `ddl` step**: Redis has no schema, and
replicare `RESTORE`s keys straight into the target keyspace, which need not
pre-exist.

## What it generates

Keys are laid out as **`lg:{<bucket>}:<type>:<n>`**. Everything is chosen to
exercise replicare's Redis paths, not just to hold data:

| Cohort | What it stresses |
|---|---|
| `str` | plain strings; ~20% carry a **TTL** (volatile keys → TTL replication) |
| `hash` / `list` / `set` / `zset` | every collection type, through `DUMP`→`RESTORE` |
| `stream` | streams **with consumer groups** — the trickiest `DUMP`/`RESTORE` payload |
| `big` | deliberately **large keys** (~100 KB hashes) → the big-key path / `MEMORY USAGE` gate |
| `lgskip:*` | written to the **source only**; replicare's `include: ["lg:*"]` must exclude it — the Redis analog of loadgen's keyless `audit_log`. `verify` asserts the target holds **none** of these. |

The **`{bucket}` hash tag** is the cluster story: on a standalone/sentinel server
the braces are inert literal bytes, but on a Redis Cluster they spread the keyspace
across slots/shards (exercising replicare's per-master SCAN fan-out) **and** keep a
`RENAME`'s old+new key in one slot (a cross-slot `RENAME` is an error).

The churn mix includes the two cases replicare's Redis engine handles specially:
- **`RENAME`** = delete-old + create-new — the analog of a PK change, and a stress
  on the delete-reconciliation sweep.
- **heavy `DEL`** — Redis has **no delete capture**, so deletes are only caught by
  replicare's target-vs-source diff; this is the genuinely hard path.

At `--scale 1.0` (default) the seed writes ~430k keys. Scale down for quick loops:
`--scale 0.02` ≈ 8.6k keys in well under a second.

## Usage

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

- **standalone / sentinel:** a `redis://` URL, e.g. `redis://:pw@localhost:6390/0`
  (empty = `redis://localhost:6379/0`).
- **cluster:** pass `--cluster` and a comma-separated seed list, e.g.
  `--dsn "n1:6379,n2:6379,n3:6379"` (use `--password` for AUTH). One tool, both
  topologies — see the hash-tag note above.

### Commands & flags

- `run --dsn <src> [--scale F] [--ops N] [--seed S] [--cluster]` — **seed if empty**
  or **churn** `N` ops. `--seed` makes a run reproducible (op selection + generated
  values).
- `verify --source <src> --target <tgt> [--wait D] [--interval D] [--cluster]` —
  compares every `lg:*` key **version-independently**: a type-aware canonical
  content hash (`GET` / sorted `HGETALL` / `LRANGE` / sorted `SMEMBERS` /
  `ZRANGE WITHSCORES` / `XRANGE` + group last-ids) plus **TTL presence**. Also
  asserts no `lgskip:*` leaked to the target. `--wait` polls until converged or the
  timeout. Exits non-zero on drift.
- `reset --dsn <db> --yes [--cluster]` — `DEL` every `lg:*`, `lgskip:*`, and the
  `lgmeta:*` marker key. Scoped by prefix — it never runs `FLUSHDB`.

> **Why not compare `DUMP` bytes directly?** The harness runs an **old source
> (6.2)** against a **modern target (7.4)**, whose RDB serializations differ even
> for identical data (and replicare deliberately preserves each side's own
> encoding). So `verify` compares a canonical *rendering* of each value, not its
> serialized bytes. **TTL** is compared as presence only (has-TTL vs no-TTL) — the
> exact remaining seconds legitimately differ by the replication delay.

## Running a full load test end-to-end

The commands above assume replicare is already running with "your own config". Here
is the complete rig — two Redis instances, a Postgres state store, a config, the
daemon, and a churn-and-verify loop — that this harness is meant to drive. This is
the exact flow used to validate replicare's Redis copy + streaming under load.

**1. Two Redis instances + a Postgres state store.** The bundled harness is the
quickest source/target pair (an old 6.2 source and a modern 7.4 target); the state
store is Postgres (v1's only `StateStore` backend — the *replicated data* is Redis,
the daemon's own progress/cursors live in Postgres):

```sh
task harness:redis:up          # source on :6390, target on :6391
docker run -d --name lg-state -e POSTGRES_PASSWORD=pw -p 55440:5432 postgres:16
docker exec lg-state psql -U postgres -c 'CREATE DATABASE replicare_state'

export SOURCE="redis://localhost:6390/0"
export TARGET="redis://localhost:6391/0"
```

**2. A minimal replicare config** (`loadtest-redis.yaml`). `drain_interval` is short
so the reconciliation SCAN + delete sweep keep up under churn:

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

**3. Seed the source, start replicare.** Redis CDC is capture-less; the initial
copy SCANs the source, so seeding before the daemon starts is fine:

```sh
go run ./test/loadgen-redis run --dsn "$SOURCE"          # seed the source
go run ./cmd/replicare run loadtest-redis.yaml &         # start the daemon
```

**4. Verify the initial copy converged:**

```sh
go run ./test/loadgen-redis verify --source "$SOURCE" --target "$TARGET" --wait 120s
# => CONVERGED: N replicated keys match, no skip leak
```

**5. Churn-and-verify loop — this is the actual load test.** Each round applies a
random burst of mutations (including `RENAME`s and heavy `DEL`s) and asserts the
target re-converges while the daemon streams:

```sh
for r in 1 2 3 4; do
  go run ./test/loadgen-redis run --dsn "$SOURCE" --ops 800 --seed $r
  go run ./test/loadgen-redis verify --source "$SOURCE" --target "$TARGET" --wait 120s
done
```

To stress the **delete-reconciliation sweep** and streaming backlog, churn several
times back-to-back *before* verifying, so deletes and renames pile up while the
sweep runs, then converge:

```sh
for r in $(seq 1 8); do go run ./test/loadgen-redis run --dsn "$SOURCE" --ops 800 --seed $r; done
go run ./test/loadgen-redis verify --source "$SOURCE" --target "$TARGET" --wait 300s
```

A run finishing with `CONVERGED: … no skip leak` — and the daemon log showing no
`HALTED` / `stream pass error` — is a passing load test. Watch
`replicare_deletes_reconciled_total` on `:19091/metrics` climb as `DEL`/`RENAME`
churn is reconciled.

## Cluster mode

The same tool drives a Redis Cluster target. Bring up the cluster harness and pass
`--cluster` plus a seed list:

```sh
task harness:redis:cluster:up
go run ./test/loadgen-redis run    --dsn "127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002" --cluster --scale 0.1
go run ./test/loadgen-redis verify --source "…standalone source…" --target "127.0.0.1:7000,…" --cluster --wait 120s
```

The `{bucket}` hash tags spread keys across the cluster's slots (so the per-master
SCAN fan-out is genuinely exercised) while keeping each `RENAME` intra-slot.

## Gotchas

- **Reset the state store when re-seeding from scratch.** replicare records
  per-unit copy-progress in its state store; if you wipe and re-seed the source
  without clearing that state, it may treat the unit as already copied. Use a fresh
  `replicare_state` database (or clear the `replicare` schema in it) alongside
  `loadgen-redis reset`.
- **`RENAME` needs an existing source key.** The tool renames within the seeded
  string range and silently skips a key that a prior op already renamed/deleted —
  benign for the harness.
- **`lgmeta:counts`** is a small bookkeeping key (per-type seeded counts, so churn
  can address existing keys without scanning). It is neither `lg:*` nor `lgskip:*`,
  so replicare and `verify` both ignore it; `reset` removes it.

## Task shortcuts

```sh
task loadgen-redis:seed   DSN="$SOURCE" SCALE=0.1          # seed / scale
task loadgen-redis:churn  DSN="$SOURCE" OPS=500            # a churn burst
task loadgen-redis:verify SRC="$SOURCE" TGT="$TARGET" WAIT=60s
task loadgen-redis:reset  DSN="$SOURCE"                    # add CLUSTER=1 for a cluster
```
