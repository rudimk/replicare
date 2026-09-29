# loadgen-mysql — a MySQL load + verification harness for replicare

`loadgen-mysql` is the MySQL sibling of [`test/loadgen`](../loadgen/README.md). It is a
manual, idempotent tool for exercising replicare's MySQL engine against **broad,
high-volume, realistic** data. You run it whenever you like against a **source** MySQL;
it seeds a rich schema the first time and applies random inserts/updates/deletes on
every subsequent run. Point replicare at the source and a target, and use
`loadgen-mysql verify` to assert the target converged.

It is a developer/test tool — **not** part of the shipped binary. Run it from a checkout
with `go run ./test/loadgen-mysql` (or `task loadgen-mysql:*`).

Everything lives in a dedicated **`loadgen` database** (MySQL's analog of the Postgres
`loadgen` schema), created by `ddl`/`run`, so replicare can select it wholesale
(`include: ["loadgen.*"]`) and the harness never collides with anything else on the
server. Every statement is schema-qualified, so the DSN's default database is irrelevant
and need not exist.

## What it generates

One database (`loadgen`) of **10 tables**, chosen to exercise replicare's hard MySQL
paths, not just to hold rows (the same 10 as the Postgres harness, in MySQL types):

| Table | What it stresses |
|---|---|
| `tenants` | hub table many others FK to (giant-component pressure); `AUTO_INCREMENT` PK, `JSON`, `DATETIME`, `TINYINT(1)` bool |
| `categories` | self-referential hierarchy column (self-ref FK under `--cyclic`) |
| `users` | `VARBINARY` (avatar), IP text, `DECIMAL(20,4)`, `DATE`, `JSON`, many nullable columns; unique `(tenant_id,email)` |
| `products` | **composite VARCHAR PK** `(tenant_id, sku)`; `JSON` dims; target of PK-changing updates |
| `orders` | **CHAR(32) id PK** (MD5 hex, the UUID analog); `CHAR(3)`; nullable FK to `users` |
| `order_items` | composite PK; **composite FK** to `products` `ON UPDATE CASCADE` (a SKU rename is a PK change that cascades) |
| `product_categories` | M:N join, composite PK, two FKs |
| `events` | the **high-volume** table (~1M rows at default scale); wide type coverage incl. `DOUBLE` |
| `generated_metrics` | `GENERATED … STORED` column + `AUTO_INCREMENT` PK |
| `audit_log` | **no primary/unique key** → replicare skips it with a loud warning (`verify` excludes it too) |

Data is generated **server-side** (`INSERT … SELECT` over a cross-joined digit-table row
generator, `RAND()` seeded with `RAND(seed)`, `MD5(…)` ids) — fast to a million rows and,
crucially, **MySQL-5.7 safe**: 5.7 is the oldest supported source and has **no
`generate_series`, no CTEs, and no window functions**, so the harness uses none of them.
The churn mix includes PK-changing SKU renames (cascaded) and mutation of the nullable
FK-cycle edge.

At `--scale 1.0` (default) the seed writes ~1.2M rows (events dominate). Scale it down
for quick loops: `--scale 0.01` ≈ 12k rows.

## Usage

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

`--dsn`/`--source`/`--target` take a **go-sql-driver DSN**, e.g.
`root:replicare@tcp(127.0.0.1:3340)/` (source) and `…@tcp(127.0.0.1:3341)/` (target) for
the bundled harness. The harness pins `time_zone='+00:00'` on every connection so
`NOW()`/`DATETIME` values render deterministically in UTC, matching replicare's own MySQL
session canonicalization.

### Commands & flags

- `run --dsn <src> [--scale F] [--ops N] [--seed S] [--cyclic] [--duration D] [--node-id K]` —
  ensure schema, then **seed if empty** or **churn** `N` statements. `--seed` seeds both
  client op-selection and server `RAND()` (server-side value reproducibility is
  best-effort on MySQL; op selection is exact). `--duration D` churns **continuously**
  for `D` (repeated `--ops` bursts) — run it **while replicare does its initial copy** to
  exercise the live-source skew path: parent rows inserted mid-copy referenced by
  children copied from a later snapshot (a transient FK the copy must recover from). Pair
  with `--cyclic`. `--node-id K` (default 0) selects this writer's **key slice** for
  active-active testing — see [Active-active](#active-active-multi-master-load-testing); 0
  is the default single-writer (active-passive) behaviour, unchanged.
- `ddl --dsn <db> [--cyclic]` — apply the schema only (prep the target). One shared
  schema serves every node; `--node-id` is a `run`-only concept (it partitions the
  *data*, not the schema).
- `verify --source <src> --target <tgt> [--wait D] [--interval D]` — per-table row-count +
  order-independent content-checksum comparison (per row, MD5 over a NULL-safe, type-aware
  rendering of every column; folded with `BIT_XOR`). Exits non-zero on drift. `--wait`
  polls until converged or the timeout. It is a pure **equality** check, so it doubles as
  the active-active convergence oracle — run it between any two mesh nodes (they must hold
  the identical union).
- `reset --dsn <db> --yes` — `DROP DATABASE loadgen`.

`verify` compares only the 9 keyed tables (it skips `audit_log`, which replicare skips).
The `GENERATED … STORED` column is included and matches because both servers recompute
it.

> **Why not compare a table checksum directly?** The harness runs an **old source (5.7)**
> against a **modern target (8.4)**. `verify` renders each value server-side into a
> version-stable form before hashing: binary/blob via `HEX`, `FLOAT`/`DOUBLE` normalized
> via `CAST … AS DECIMAL(65,15)` (replicare moves the value bit-faithfully, so both ends
> hold the identical double; normalizing removes any raw double-to-string formatting
> difference between server versions), everything else via `CAST … AS CHAR` under a pinned
> `time_zone`. It compares that canonical rendering, not raw storage bytes (which differ
> across versions for identical data).

## Active-active (multi-master) load testing

The same harness drives a MySQL `clusters:` mesh. The model is **disjoint partitioning**:
each writer node gets `--node-id K`, which shifts every generated key and every FK
reference into a private slice `[K·1e9+1, K·1e9+1e9)`, so nodes never collide on a key and
the mesh converges to the clean **union** of all slices (predictable counts, matching
checksums). `--node-id 0` is the default and is byte-for-byte the single-writer behaviour
above, so active-passive is unaffected.

What `--node-id K` changes on `run`:
- **Seed** writes ids `base+g`, order ids `MD5(CONCAT('order:', base+g))`, SKUs
  `SKU-(base+g)`, and offsets every FK reference by `base`, so a node's children point only
  at that node's own parents. `AUTO_INCREMENT` counters are then restarted past the node's
  seeded rows so churn inserts stay in the slice.
- **Churn** inserts into the node's slice and scopes every UPDATE/DELETE to node-owned
  rows, so a node only ever mutates what it wrote — no accidental cross-node conflict
  (resolving concurrent *same-key* writes is the separate, planned **conflict-storm**
  mode; see below).

`--node-id 0` and any `--node-id K` seeded/churned against the **same** node's DB must be
consistent — always pass the same `--node-id` for a given node. Keep `--scale` well under
~500M rows per node (the harness refuses a scale that approaches the slice).

### Three-node mesh example

Assume a running replicare `clusters:` mesh over three MySQL nodes `A`, `B`, `C` (config
per [multi-master.md](../../docs/multi-master.md)), with `$A`/`$B`/`$C` their DSNs.

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

Convergence is transitive, so `A==B` and `A==C` imply all three agree. A pass ending
`CONVERGED: all 9 replicated tables match` on every pair — with the daemon logs clean of
`HALTED` / `stream pass error` — is a passing active-active load test.

> **Not yet: conflict-storm mode.** The disjoint model above never has two nodes write the
> same key, so it tests convergence of the *union*. A follow-up `--overlap` mode will
> deliberately overlap node slices to exercise HLC last-write-wins under concurrent
> same-key writes; there `verify` still asserts convergence (all nodes equal), but not
> *which* value wins (the LWW winner is nondeterministic).

> **Reset the state store per node when re-seeding** (see [Gotchas](#gotchas)) — this
> applies per node in a mesh: clear each node's source-side `replicare` schema and its
> state-store entries alongside `loadgen-mysql reset`.

## Running a full load test end-to-end

The commands above assume replicare is already running with "your own config". Here is the
complete rig — an old 5.7 source, a modern 8.4 target, a Postgres state store, a config,
the daemon, and a heavy-churn convergence loop — that this harness is meant to drive. It
is the exact flow used to validate replicare's MySQL copy + streaming under load.

**1. Two MySQL instances + a Postgres state store.** The bundled harness is the quickest
source/target pair (an old 5.7 source on `:3340` and a modern 8.4 target on `:3341`); the
state store is Postgres (v1's only `StateStore` backend — the *replicated data* is MySQL,
the daemon's own progress/cursors live in Postgres):

```sh
task harness:mysql:up          # source on :3340, target on :3341
docker run -d --name lg-state -e POSTGRES_PASSWORD=pw -p 55450:5432 postgres:16
docker exec lg-state psql -U postgres -c 'CREATE DATABASE replicare_state'

export SOURCE="root:replicare@tcp(127.0.0.1:3340)/"
export TARGET="root:replicare@tcp(127.0.0.1:3341)/"
```

**2. A minimal replicare config** (`loadtest-mysql.yaml`). `drain_interval` is short so
streaming keeps up under churn:

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

**3. Seed the source, create the target schema, start replicare.** Capture (triggers) is
installed first, then the chunked copy runs — so seeding before the daemon starts is fine
(the initial copy reproduces it):

```sh
go run ./test/loadgen-mysql run --dsn "$SOURCE"      # seed ~1M+ rows on the source
go run ./test/loadgen-mysql ddl --dsn "$TARGET"      # create the (empty) target schema
go run ./cmd/replicare run loadtest-mysql.yaml &     # start the daemon
```

**4. Verify the initial copy converged:**

```sh
go run ./test/loadgen-mysql verify --source "$SOURCE" --target "$TARGET" --wait 120s
# => CONVERGED: all 9 replicated tables match
```

**5. Churn-and-verify loop — this is the actual load test.** Each round applies a random
burst of inserts/updates/deletes (including PK-changing SKU renames) and asserts the
target re-converges while the daemon streams deltas:

```sh
for r in 1 2 3 4; do
  go run ./test/loadgen-mysql run --dsn "$SOURCE" --ops 800 --seed $r
  go run ./test/loadgen-mysql verify --source "$SOURCE" --target "$TARGET" --wait 120s
done
```

To stress **cross-batch dependencies and delta backlog**, churn several times
back-to-back *before* verifying, so a large backlog builds while streaming lags, then
converges:

```sh
for r in $(seq 1 8); do go run ./test/loadgen-mysql run --dsn "$SOURCE" --ops 800 --seed $r; done
go run ./test/loadgen-mysql verify --source "$SOURCE" --target "$TARGET" --wait 300s
```

A run finishing with `CONVERGED: all 9 replicated tables match` — and the daemon log
showing no `HALTED` / `stream pass error` — is a passing load test. For the cyclic
variant, add `--cyclic` to **every** `ddl` and `run` (see below).

> This is a manual test, distinct from the automated Go integration suite. MySQL
> correctness is a **local gate** (`REPLICARE_INTEGRATION=1` with the MySQL harness up);
> CI is Postgres-only.

## The `--cyclic` flag

By default the schema is **acyclic**. `--cyclic` adds two nullable FK **cycles** — the
`categories` self-reference and a `users ↔ orders` 2-table cycle — to exercise replicare's
cyclic-copy (`null_then_fill`) path. Apply it consistently to source **and** target
(`ddl --cyclic`, `run --cyclic`). MySQL has no `ADD CONSTRAINT IF NOT EXISTS`, so the
harness adds each cyclic FK only when it is absent, keeping `--cyclic` idempotent.

## Gotchas

- **Reset the state store when re-seeding from scratch.** replicare records per-table
  copy-progress in its state store; if you wipe and re-seed the source without clearing
  that state, replicare treats already-`Done` tables as copied and skips them. Clear the
  `replicare` schema in your state-store database (or use a fresh one) alongside
  `loadgen-mysql reset`.
- **The target schema must exist first** — run `ddl` against the target before starting
  replicare.
- **`local_infile` on the target.** replicare's MySQL initial copy uses
  `LOAD DATA LOCAL INFILE`, so the target server must permit it (the bundled
  `harness-mysql` sets `--local-infile=ON`). The harness itself does not need it (it seeds
  via ordinary `INSERT … SELECT`).
- Deletes cascade (`orders → order_items`, `tenants → everything`), so a
  `delete_orders`/`delete_users` op can remove more rows than it names.

## Task shortcuts

```sh
task loadgen-mysql:ddl    DSN="$TARGET"                    # schema on the target
task loadgen-mysql:seed   DSN="$SOURCE" SCALE=0.1          # seed / scale
task loadgen-mysql:churn  DSN="$SOURCE" OPS=500            # a churn burst
task loadgen-mysql:verify SRC="$SOURCE" TGT="$TARGET" WAIT=60s
task loadgen-mysql:reset  DSN="$SOURCE"                    # DROP DATABASE loadgen
```
