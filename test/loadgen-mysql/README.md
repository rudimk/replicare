# MySQL Load Testing with `loadgen-mysql`

This page describes `loadgen-mysql`, the harness we use to load-test `replicare`'s MySQL engine,
and the MySQL sibling of [`test/loadgen`](https://replicare.rudimk.com/load-testing-postgres.html).
It's a manual, idempotent tool that exercises `replicare` against broad, high-volume, realistic
data; the first run against a source MySQL seeds a rich schema, and every run after that applies
random inserts / updates / deletes. With `replicare` pointed at the source and a target,
`loadgen-mysql verify` then checks that the target has converged. The page covers what the
harness generates, how to run it(on its own, against an active-active mesh and as a complete
end-to-end rig), as well as a few gotchas.

`loadgen-mysql` is a developer / test tool, and isn't part of the shipped binary. Run it from a
checkout with `go run ./test/loadgen-mysql`(or `task loadgen-mysql:*`).

Everything lives in a dedicated `loadgen` database(MySQL's analogue of the Postgres `loadgen`
schema), created by `ddl`/`run`. That way `replicare` can select it wholesale(`include: ["loadgen.*"]`),
and the harness never collides with anything else on the server. Every statement is
schema-qualified, so the DSN's default database doesn't matter and doesn't need to exist.

## What it generates

The `loadgen` database holds 10 tables, each chosen to exercise one of `replicare`'s hard MySQL
paths rather than just to hold rows. They're the same 10 as the Postgres harness, in MySQL
types:

| Table | What it stresses |
|---|---|
| `tenants` | a hub table many others FK to(giant-component pressure); `AUTO_INCREMENT` PK, `JSON`, `DATETIME`, `TINYINT(1)` bool |
| `categories` | a self-referential hierarchy column(a self-referencing FK under `--cyclic`) |
| `users` | `VARBINARY`(avatar), IP text, `DECIMAL(20,4)`, `DATE`, `JSON`, many nullable columns; unique `(tenant_id,email)` |
| `products` | a composite VARCHAR PK `(tenant_id, sku)`; `JSON` dims; the target of PK-changing updates |
| `orders` | a CHAR(32) id PK(MD5 hex, the UUID analogue); `CHAR(3)`; a nullable FK to `users` |
| `order_items` | a composite PK; a composite FK to `products` with `ON UPDATE CASCADE`(a SKU rename is a PK change that cascades) |
| `product_categories` | an M:N join, with a composite PK and two FKs |
| `events` | the high-volume table(about 1M rows at the default scale), with wide type coverage including `DOUBLE` |
| `generated_metrics` | a `GENERATED … STORED` column and an `AUTO_INCREMENT` PK |
| `audit_log` | no primary / unique key, so `replicare` skips it with a loud warning(and `verify` excludes it too) |

The data is generated server-side(`INSERT … SELECT` over a cross-joined digit-table row
generator, `RAND()` seeded with `RAND(seed)`, `MD5(…)` ids). That makes it fast to reach a
million rows and, crucially, keeps it safe on MySQL 5.7. 5.7 is the oldest supported source, and
it has no `generate_series`, no CTEs and no window functions, so the harness uses none of them.
The churn mix includes PK-changing SKU renames(cascaded) and changes to the nullable FK-cycle
edge.

At `--scale 1.0`(the default), the seed writes about 1.2M rows, most of them in `events`. For
quick loops, scale it down; `--scale 0.01` writes about 12k rows.

## Using the harness

The basic loop is to create the schema on the target, seed the source, start `replicare`,
generate some change, then check the target has caught up:

```sh
# 1. Create the schema on the TARGET (replicare replicates data only — target tables
#    must pre-exist).
go run ./test/loadgen-mysql ddl --dsn "$TARGET"

# 2. Seed the SOURCE (first run seeds; later runs churn).
go run ./test/loadgen-mysql run --dsn "$SOURCE"                 # ~1M+ rows
go run ./test/loadgen-mysql run --dsn "$SOURCE" --scale 0.05    # smaller, faster

# 3. Start replicare source -> target (your own config), then generate change:
go run ./test/loadgen-mysql run --dsn "$SOURCE" --ops 500       # a burst of I/U/D

# 4. Assert the target converged (retries for replication lag):
go run ./test/loadgen-mysql verify --source "$SOURCE" --target "$TARGET" --wait 60s
```

`--dsn`/`--source`/`--target` each take a go-sql-driver DSN, for example
`root:replicare@tcp(127.0.0.1:3340)/` for the source and `…@tcp(127.0.0.1:3341)/` for the target
in the bundled harness. The harness pins `time_zone='+00:00'` on every connection, so
`NOW()`/`DATETIME` values render deterministically in UTC; this matches `replicare`'s own MySQL
session canonicalisation.

### Commands & flags

`run --dsn <src> [--scale F] [--ops N] [--seed S] [--cyclic] [--duration D] [--node-id K]` makes
sure the schema exists, then seeds it if it's empty, or churns `N` statements if it isn't.
`--seed` seeds both the client's op selection and the server's `RAND()`; op selection is exact,
while server-side value reproducibility is best-effort on MySQL. `--duration D` churns
continuously for `D`(repeated `--ops` bursts); run it while `replicare` is doing its initial
copy, paired with `--cyclic`, to exercise the live-source skew path. Parent rows inserted
mid-copy are then referenced by children copied from a later snapshot, which is a transient FK
failure the copy has to recover from. `--node-id K`(0 by default) selects this writer's key
slice for active-active testing - see
[Active-active](#active-active-multi-master-load-testing). The default of 0 is the
single-writer(active-passive) behaviour, unchanged.

`ddl --dsn <db> [--cyclic]` applies the schema only, to prepare the target. One shared schema
serves every node; `--node-id` is a `run`-only concept, since it partitions the *data* and not
the schema.

`verify --source <src> --target <tgt> [--wait D] [--interval D]` compares the row count and an
order-independent content checksum of each table, and exits non-zero on any drift. The checksum
is an MD5 per row, over a NULL-safe, type-aware rendering of every column, folded together with
`BIT_XOR`. `--wait` keeps polling until the tables converge or the timeout runs out. Since it's
a pure equality check, it doubles as the active-active convergence oracle; run it between any
two mesh nodes, which have to hold the identical union.

`reset --dsn <db> --yes` runs `DROP DATABASE loadgen`.

`verify` compares only the 9 keyed tables, skipping `audit_log` just as `replicare` does. The
`GENERATED … STORED` column is included, and matches because both servers recompute it.

### Why not compare a table checksum directly?

The harness runs an old source(5.7) against a modern target(8.4), and raw storage bytes differ
across those versions even for identical data. So `verify` renders each value server-side into
a version-stable form before hashing it: binary / blob values through `HEX`, `FLOAT`/`DOUBLE`
normalised through `CAST … AS DECIMAL(65,15)`, and everything else through `CAST … AS CHAR`
under a pinned `time_zone`. The float normalisation is there because `replicare` moves the value
bit-faithfully, so both ends hold the identical double, but the two server versions can still
format a raw double as a string differently; normalising removes that difference. It's this
canonical rendering that gets compared, never the raw storage bytes.

## Active-active, multi-master load testing

The same harness drives a MySQL `clusters:` mesh, using disjoint partitioning. Each writer node
gets `--node-id K`, which shifts every generated key and every FK reference into a private
slice, `[K·1e9+1, K·1e9+1e9)`. That way nodes never collide on a key, and the mesh converges to
the clean union of all the slices, with predictable counts and matching checksums.
`--node-id 0` is the default, and is byte-for-byte the single-writer behaviour described above,
so active-passive testing isn't affected.

On `run`, `--node-id K` changes two things:

1. Seeding writes ids `base+g`, order ids `MD5(CONCAT('order:', base+g))` and SKUs
   `SKU-(base+g)`, and offsets every FK reference by `base`, so a node's children only ever
   point at that node's own parents. `AUTO_INCREMENT` counters are then restarted past the
   node's seeded rows, so churn inserts stay inside the slice.
2. Churn inserts into the node's slice and scopes every UPDATE / DELETE to rows the node owns,
   so a node only ever changes what it wrote, and there's no accidental cross-node conflict.
   Resolving concurrent *same-key* writes is a separate, planned conflict-storm mode(see
   below).

`--node-id 0` and any `--node-id K` seeded / churned against the *same* node's database have to
be consistent, so always pass the same `--node-id` for a given node. Keep `--scale` well under
about 500M rows per node; the harness refuses a scale that approaches the size of the slice.

### Three-node mesh example

Take a `replicare` `clusters:` mesh running over three MySQL nodes, `A`, `B` and `C`(configured
as described in
[Active-active replication](https://replicare.rudimk.com/failover.html#setting-up-a-cluster)),
with `$A`/`$B`/`$C` as their DSNs:

```sh
# 1. Schema on every node (data-only replication; targets must pre-exist).
for n in "$A" "$B" "$C"; do go run ./test/loadgen-mysql ddl --dsn "$n"; done

# 2. Seed each node into its own disjoint slice.
go run ./test/loadgen-mysql run --dsn "$A" --node-id 0
go run ./test/loadgen-mysql run --dsn "$B" --node-id 1
go run ./test/loadgen-mysql run --dsn "$C" --node-id 2

# 3. Churn every node concurrently (each in its own slice).
go run ./test/loadgen-mysql run --dsn "$A" --node-id 0 --ops 800 --seed 1 &
go run ./test/loadgen-mysql run --dsn "$B" --node-id 1 --ops 800 --seed 2 &
go run ./test/loadgen-mysql run --dsn "$C" --node-id 2 --ops 800 --seed 3 &
wait

# 4. Convergence oracle: every pair must be identical (all hold the union).
go run ./test/loadgen-mysql verify --source "$A" --target "$B" --wait 300s
go run ./test/loadgen-mysql verify --source "$A" --target "$C" --wait 300s
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
> applies to every node: clear each node's source-side `replicare` schema and its state-store
> entries alongside `loadgen-mysql reset`.

## Running a full load test end-to-end

The commands above assume `replicare` is already running with an existing config. Here's the
complete rig this harness is meant to drive - an old 5.7 source, a modern 8.4 target, a Postgres
state store, a config, the daemon and a heavy-churn convergence loop. It's the exact flow we use
to validate `replicare`'s MySQL copy and streaming under load.

To start with, set up two MySQL instances and a Postgres state store. The bundled harness is the
quickest source / target pair, with an old 5.7 source on `:3340` and a modern 8.4 target on
`:3341`. The state store is Postgres, since that's v1's only `StateStore` backend; the
*replicated data* is MySQL, while the daemon's own progress / cursors live in Postgres:

```sh
task harness:mysql:up          # source on :3340, target on :3341
docker run -d --name lg-state -e POSTGRES_PASSWORD=pw -p 55450:5432 postgres:16
docker exec lg-state psql -U postgres -c 'CREATE DATABASE replicare_state'

export SOURCE="root:replicare@tcp(127.0.0.1:3340)/"
export TARGET="root:replicare@tcp(127.0.0.1:3341)/"
```

Next, write a minimal `replicare` config(`loadtest-mysql.yaml`). The `drain_interval` is kept
short so streaming keeps up under churn:

```yaml
logging: { level: info, format: text }
observability: { metrics_addr: ":19092", status_addr: ":18082" }
state_store:
  engine: postgres
  postgres: { host: localhost, port: 55450, database: replicare_state, user: postgres, password: pw, sslmode: disable }
sources:
  src: { engine: mysql, mysql: { host: 127.0.0.1, port: 3340, database: loadgen, user: root, password: replicare, tls: disable } }
targets:
  # local_infile: true opts into the LOAD DATA fast path (the target server must also
  # permit it — the bundled harness sets --local-infile=ON).
  tgt: { engine: mysql, mysql: { host: 127.0.0.1, port: 3341, database: loadgen, user: root, password: replicare, tls: disable, local_infile: true } }
syncs:
  - name: loadtest-mysql
    source: src
    targets: [tgt]
    include: ["loadgen.*"]
    tuning: { drain_interval: 1s }
```

Then seed the source, create the target schema and start `replicare`. Capture(the triggers) is
installed first and the chunked copy runs after it, so seeding before the daemon starts is fine;
the initial copy reproduces it:

```sh
go run ./test/loadgen-mysql run --dsn "$SOURCE"      # seed ~1M+ rows on the source
go run ./test/loadgen-mysql ddl --dsn "$TARGET"      # create the (empty) target schema
go run ./cmd/replicare run loadtest-mysql.yaml &     # start the daemon
```

Once it's running, check that the initial copy converged:

```sh
go run ./test/loadgen-mysql verify --source "$SOURCE" --target "$TARGET" --wait 120s
# => CONVERGED: all 9 replicated tables match
```

Finally - the churn-and-verify loop, which is the actual load test. Each round applies a random
burst of inserts / updates / deletes(including PK-changing SKU renames), and checks that the
target converges again while the daemon streams deltas:

```sh
for r in 1 2 3 4; do
  go run ./test/loadgen-mysql run --dsn "$SOURCE" --ops 800 --seed $r
  go run ./test/loadgen-mysql verify --source "$SOURCE" --target "$TARGET" --wait 120s
done
```

To stress cross-batch dependencies and the delta backlog, churn several times back-to-back
*before* verifying. A large backlog then builds up while streaming lags behind, and converges
afterwards:

```sh
for r in $(seq 1 8); do go run ./test/loadgen-mysql run --dsn "$SOURCE" --ops 800 --seed $r; done
go run ./test/loadgen-mysql verify --source "$SOURCE" --target "$TARGET" --wait 300s
```

A run that finishes with `CONVERGED: all 9 replicated tables match`, with no `HALTED` /
`stream pass error` in the daemon log, is a passing load test. For the cyclic variant, add
`--cyclic` to every `ddl` and `run`(see below).

> ⚠️ This is a manual test, and it's separate from the automated Go integration suite. MySQL
> correctness is a local gate(`REPLICARE_INTEGRATION=1` with the MySQL harness up), since CI is
> Postgres-only.

## The `--cyclic` flag

By default the schema is acyclic. `--cyclic` adds two nullable FK cycles - the `categories`
self-reference and a `users ↔ orders` 2-table cycle - to exercise `replicare`'s
cyclic-copy(`null_then_fill`) path. Apply it consistently to the source *and* the
target(`ddl --cyclic`, `run --cyclic`).

MySQL has no `ADD CONSTRAINT IF NOT EXISTS`, so the harness only adds each cyclic FK when it's
absent, which keeps `--cyclic` idempotent.

## Gotchas

There are a few things that commonly trip up a load test:

- Reset the state store when re-seeding from scratch. `replicare` records per-table copy
  progress in its state store; if the source is wiped and re-seeded without clearing that
  state, `replicare` treats tables already marked `Done` as copied and skips them. Clear the
  `replicare` schema in the state-store database(or use a fresh one) alongside
  `loadgen-mysql reset`.
- The target schema has to exist first, so run `ddl` against the target before starting
  `replicare`.
- The target has to allow `local_infile`. `replicare`'s MySQL initial copy uses
  `LOAD DATA LOCAL INFILE`, so the target server has to permit it(the bundled `harness-mysql`
  sets `--local-infile=ON`). The harness itself doesn't need it, since it seeds through ordinary
  `INSERT … SELECT`.
- Deletes cascade(`orders → order_items`, `tenants → everything`), so a
  `delete_orders`/`delete_users` op can remove more rows than it names.

## Task shortcuts

The same steps are also available as `task` targets:

```sh
task loadgen-mysql:ddl    DSN="$TARGET"                    # schema on the target
task loadgen-mysql:seed   DSN="$SOURCE" SCALE=0.1          # seed / scale
task loadgen-mysql:churn  DSN="$SOURCE" OPS=500            # a churn burst
task loadgen-mysql:verify SRC="$SOURCE" TGT="$TARGET" WAIT=60s
task loadgen-mysql:reset  DSN="$SOURCE"                    # DROP DATABASE loadgen
```
