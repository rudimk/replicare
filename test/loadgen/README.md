# Postgres Load Testing with `loadgen`

This page describes `loadgen`, the harness we use to load-test `replicare` against Postgres. It's
a manual, idempotent tool that exercises `replicare` against broad, high-volume, realistic data;
the first run against a source database seeds a rich schema, and every run after that applies
random inserts / updates / deletes. With `replicare` pointed at the source and a target,
`loadgen verify` then checks that the target has converged. The page covers what the harness
generates, how to run it(on its own, against an active-active mesh and as a complete end-to-end
rig), as well as a few gotchas.

`loadgen` is a developer / test tool, and isn't part of the shipped binary. Run it from a
checkout with `go run ./test/loadgen`(or `task loadgen:*`).

## What it generates

Everything lives in one schema(`loadgen`) of 10 tables, each chosen to exercise one of
`replicare`'s hard paths rather than just to hold rows:

| Table | What it stresses |
|---|---|
| `tenants` | a hub table many others FK to(giant-component pressure); identity PK, `jsonb`, `text[]`, `timestamptz`, `bool` |
| `categories` | a self-referential hierarchy column(a self-referencing FK under `--cyclic`) |
| `users` | `bytea`, `inet`, `numeric(20,4)`, `date`, `jsonb`, many nullable columns; unique `(tenant_id,email)` |
| `products` | a composite text PK `(tenant_id, sku)`; `float8[]`; `timestamp`(no time zone); the target of PK-changing updates |
| `orders` | a UUID PK; `char(3)`; a nullable FK to `users` |
| `order_items` | a composite PK; a composite FK to `products` with `ON UPDATE CASCADE`(a SKU rename is a PK change that cascades) |
| `product_categories` | an M:N join, with a composite PK and two FKs |
| `events` | the high-volume table(about 1M rows at the default scale), with wide type coverage |
| `generated_metrics` | a `GENERATED … STORED` column and an identity PK |
| `audit_log` | no primary / unique key, so `replicare` skips it with a loud warning(and `verify` excludes it too) |

The data is generated server-side(`INSERT … SELECT generate_series`, `random()` made
reproducible with `setseed`, `md5(…)::uuid`). That makes it fast to reach a million rows, keeps
it working on old Postgres versions, and means there's no client-side type encoding involved.
The churn mix includes PK-changing SKU renames and changes to the nullable FK-cycle edge.

At `--scale 1.0`(the default), the seed writes about 1.2M rows, most of them in `events`. For
quick loops, scale it down; `--scale 0.01` writes about 12k rows in around 150ms.

## Using the harness

The basic loop is to create the schema on the target, seed the source, start `replicare`,
generate some change, then check the target has caught up:

```sh
# 1. Create the schema on the TARGET (replicare replicates data only — target
#    tables must pre-exist).
go run ./test/loadgen ddl --dsn "$TARGET"

# 2. Seed the SOURCE (first run seeds; later runs churn).
go run ./test/loadgen run --dsn "$SOURCE"                 # ~1M+ rows
go run ./test/loadgen run --dsn "$SOURCE" --scale 0.05    # smaller, faster

# 3. Start replicare source -> target (your own config), then generate change:
go run ./test/loadgen run --dsn "$SOURCE" --ops 500       # a burst of I/U/D

# 4. Assert the target converged (retries for replication lag):
go run ./test/loadgen verify --source "$SOURCE" --target "$TARGET" --wait 60s
```

`--dsn`/`--source`/`--target` each take a libpq / pgx URL(`postgres://user:pw@host:port/db`) or
a keyword string. Left empty, they fall back to the standard `PG*` environment variables.

### Commands & flags

`run --dsn <src> [--scale F] [--ops N] [--seed S] [--cyclic] [--duration D] [--node-id K]` makes
sure the schema exists, then seeds it if it's empty, or churns `N` statements if it isn't.
`--seed` makes a run reproducible. `--duration D` churns continuously for `D`(repeated `--ops`
bursts); run it while `replicare` is doing its initial copy, paired with `--cyclic`, to exercise
the live-source cyclic-copy skew. Parent rows(tenants / users) inserted mid-copy are then
referenced by children copied from a later snapshot, which is a transient FK failure the cyclic
copy has to recover from rather than crash-loop on. `--node-id K`(0 by default) selects this
writer's key slice for active-active testing - see
[Active-active](#active-active-multi-master-load-testing). The default of 0 is the
single-writer(active-passive) behaviour, unchanged.

`ddl --dsn <db> [--cyclic]` applies the schema only, to prepare the target. One shared schema
serves every node; `--node-id` is a `run`-only concept, since it partitions the *data* and not
the schema.

`verify --source <src> --target <tgt> [--wait D] [--interval D]` compares the row count and an
ordered content checksum of each table, and exits non-zero on any drift. `--wait` keeps polling
until the tables converge or the timeout runs out. GUCs are pinned to match `replicare`'s
transport, so `row::text` renders identically on both ends. Since it's a pure equality check,
it doubles as the active-active convergence oracle; run it between any two mesh nodes, which
have to hold the identical union.

`reset --dsn <db> --yes` runs `DROP SCHEMA loadgen CASCADE`.

`verify` compares only the 9 keyed tables, skipping `audit_log` just as `replicare` does. The
`GENERATED … STORED` column is included, and matches because both servers recompute it.

## Active-active, multi-master load testing

The same harness drives a `clusters:` mesh, using disjoint partitioning. Each writer node gets
`--node-id K`, which shifts every generated key and every FK reference into a private slice,
`[K·1e9+1, K·1e9+1e9)`. That way nodes never collide on a key, and the mesh converges to the clean
union of all the slices, with predictable counts and matching checksums. `--node-id 0` is the
default, and is byte-for-byte the single-writer behaviour described above, so active-passive
testing isn't affected.

On `run`, `--node-id K` changes two things:

1. Seeding writes ids `base+g`, order UUIDs `md5('order:'||(base+g))` and SKUs `SKU-(base+g)`,
   and offsets every FK reference by `base`, so a node's children only ever point at that
   node's own parents. Identity sequences are then restarted past the node's seeded rows, so
   churn inserts stay inside the slice.
2. Churn inserts into the node's slice and scopes every UPDATE / DELETE to rows the node owns,
   so a node only ever changes what it wrote, and there's no accidental cross-node conflict.
   Resolving concurrent *same-key* writes is a separate, planned conflict-storm mode(see
   below).

`--node-id 0` and any `--node-id K` seeded / churned against the *same* node's database have to
be consistent, so always pass the same `--node-id` for a given node. Keep `--scale` well under
about 500M rows per node; the harness refuses a scale that approaches the size of the slice.

### Three-node mesh example

Take a `replicare` `clusters:` mesh running over three Postgres nodes, `A`, `B` and
`C`(configured as described in
[Active-active replication](https://replicare.rudimk.com/active-active.html#setting-up-a-cluster)),
with `$A`/`$B`/`$C` as their DSNs:

```sh
# 1. Schema on every node (data-only replication; targets must pre-exist).
for n in "$A" "$B" "$C"; do go run ./test/loadgen ddl --dsn "$n"; done

# 2. Seed each node into its own disjoint slice.
go run ./test/loadgen run --dsn "$A" --node-id 0
go run ./test/loadgen run --dsn "$B" --node-id 1
go run ./test/loadgen run --dsn "$C" --node-id 2

# 3. Churn every node concurrently (each in its own slice).
go run ./test/loadgen run --dsn "$A" --node-id 0 --ops 800 --seed 1 &
go run ./test/loadgen run --dsn "$B" --node-id 1 --ops 800 --seed 2 &
go run ./test/loadgen run --dsn "$C" --node-id 2 --ops 800 --seed 3 &
wait

# 4. Convergence oracle: every pair must be identical (all hold the union).
go run ./test/loadgen verify --source "$A" --target "$B" --wait 300s
go run ./test/loadgen verify --source "$A" --target "$C" --wait 300s
```

Convergence is transitive, so `A==B` and `A==C` mean all three agree. A run that ends with
`CONVERGED: all 9 replicated tables match` on every pair, with no `HALTED` / `stream pass error`
in the daemon logs, is a passing active-active load test.

> ⚠️ There's no conflict-storm mode yet. The disjoint model above never has two nodes write the
> same key, so it only tests convergence of the *union*. A follow-up `--overlap` mode will
> deliberately overlap node slices to exercise HLC last-write-wins under concurrent same-key
> writes; there, `verify` will still check convergence(all nodes equal), but not which value
> wins, since the last-write-wins winner is nondeterministic.

> ⚠️ When re-seeding, reset the state store per node(see [Gotchas](#gotchas)). In a mesh this
> applies to every node: clear each node's `replicare` schema and its state-store entries
> alongside `loadgen reset`.

## Running a full load test end-to-end

The commands above assume `replicare` is already running with an existing config. Here's the
complete rig this harness is meant to drive - two Postgres instances, a state store, a config,
the daemon and a heavy-churn convergence loop. It's the exact flow we use to validate
`replicare`'s copy and streaming under load.

To start with, set up two Postgres instances and a state-store database. Any two reachable
Postgres servers work, and the quickest option is two containers. `replicare`'s state store is
a third logical database, here on the target server:

```sh
docker run -d --name lg-src -e POSTGRES_PASSWORD=pw -p 55432:5432 postgres:16
docker run -d --name lg-tgt -e POSTGRES_PASSWORD=pw -p 55433:5432 postgres:16
# state store (replicare's own progress/cursors — NOT your data):
docker exec lg-tgt psql -U postgres -c 'CREATE DATABASE replicare_state'

export SOURCE="postgres://postgres:pw@localhost:55432/postgres"
export TARGET="postgres://postgres:pw@localhost:55433/postgres"
```

Next, write a minimal `replicare` config(`loadtest.yaml`). The `drain_interval` is kept short so
streaming keeps up under churn:

```yaml
logging: { level: info, format: text }
observability: { metrics_addr: ":19090", status_addr: ":18080" }
state_store:
  engine: postgres
  postgres: { host: localhost, port: 55433, database: replicare_state, user: postgres, password: pw, sslmode: disable }
sources:
  src: { engine: postgres, postgres: { host: localhost, port: 55432, database: postgres, user: postgres, password: pw, sslmode: disable } }
targets:
  tgt: { engine: postgres, postgres: { host: localhost, port: 55433, database: postgres, user: postgres, password: pw, sslmode: disable } }
syncs:
  - name: loadtest
    source: src
    targets: [tgt]
    include: ["loadgen.*"]
    tuning: { drain_interval: 1s }
```

Then seed the source, create the target schema and start `replicare`. Capture is enabled first
and the chunked copy runs after it, so seeding before the daemon starts is fine; the initial
copy reproduces it:

```sh
go run ./test/loadgen run --dsn "$SOURCE"      # seed ~1M+ rows on the source
go run ./test/loadgen ddl --dsn "$TARGET"      # create the (empty) target schema
go run ./cmd/replicare run loadtest.yaml &     # start the daemon
```

Once it's running, check that the initial copy converged:

```sh
go run ./test/loadgen verify --source "$SOURCE" --target "$TARGET" --wait 120s
# => CONVERGED: all 9 replicated tables match
```

Finally - the churn-and-verify loop, which is the actual load test. Each round applies a random
burst of inserts / updates / deletes(including PK-changing SKU renames), and checks that the
target converges again while the daemon streams deltas:

```sh
for r in 1 2 3 4; do
  go run ./test/loadgen run --dsn "$SOURCE" --ops 800 --seed $r
  go run ./test/loadgen verify --source "$SOURCE" --target "$TARGET" --wait 120s
done
```

To stress cross-batch dependencies and the delta backlog(which is what shook out the FK
livelocks), churn several times back-to-back *before* verifying. A large backlog then builds up
while streaming lags behind, and converges afterwards:

```sh
for r in $(seq 1 8); do go run ./test/loadgen run --dsn "$SOURCE" --ops 800 --seed $r; done
go run ./test/loadgen verify --source "$SOURCE" --target "$TARGET" --wait 300s
```

A run that finishes with `CONVERGED: all 9 replicated tables match`, with no `HALTED` /
`stream pass error` in the daemon log, is a passing load test. For the cyclic variant, add
`--cyclic` to every `ddl` and `run`(see below).

> ⚠️ This is a manual test, and it's separate from `task test:integration`(the automated Go
> suite). Those integration tests have to run serially(`go test -p 1 ...`) because they share
> schemas; the load test above is driven by hand and has no such constraint.

## The `--cyclic` flag

By default the schema is acyclic. `--cyclic` adds two nullable FK cycles - the `categories`
self-reference and a `users ↔ orders` 2-table cycle - to exercise `replicare`'s
cyclic-copy(`null_then_fill`) path. Apply it consistently to the source *and* the
target(`ddl --cyclic`, `run --cyclic`).

Under `--cyclic`, both the initial copy and the streaming of a cyclic component converge, with
standard non-DEFERRABLE target FKs(the common real-world case). The initial copy loads every
table with its cyclic FK columns set to NULL, in an order that respects the non-cyclic edges,
then fills those columns in. Streaming uses the same NULL-then-fill idea on each pass, but per
table: each table's upsert, delete and cyclic-column fill commit independently, so a parent
advances through its own delta queue even when a cross-batch child is transiently blocked(the
same progress guarantee the acyclic drain gives), and a final fill phase closes the cycle
itself. This harness's `--cyclic` schema drains cleanly under heavy churn, with no `DEFERRABLE`
requirement.

## Gotchas

There are a few things that commonly trip up a load test:

- Reset the state store when re-seeding from scratch. `replicare` records per-table copy
  progress in its state store; if the source is wiped and re-seeded without clearing that
  state, `replicare` treats tables already marked `Done` as copied and skips them, so their
  parents go missing and the children hit FK errors. Clear the `replicare` schema in the
  state-store database(or use a fresh one) alongside `loadgen reset`.
- The target schema has to exist first, so run `ddl` against the target before starting
  `replicare`.
- Deletes cascade(`orders → order_items`, `tenants → everything`), so a
  `delete_orders`/`delete_users` op can remove more rows than it names.

## Task shortcuts

The same steps are also available as `task` targets:

```sh
task loadgen:ddl    DSN="$TARGET"                    # schema on the target
task loadgen:seed   DSN="$SOURCE" SCALE=0.1          # seed / scale
task loadgen:churn  DSN="$SOURCE" OPS=500            # a churn burst
task loadgen:verify SRC="$SOURCE" TGT="$TARGET"      # convergence check
```
