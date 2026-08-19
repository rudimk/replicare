# loadgen — a Postgres load + verification harness for replicare

`loadgen` is a manual, idempotent tool for exercising replicare against **broad,
high-volume, realistic** Postgres data. You run it whenever you like against a
**source** database; it seeds a rich schema the first time and applies random
inserts/updates/deletes on every subsequent run. Point replicare at the source and
a target, and use `loadgen verify` to assert the target converged.

It is a developer/test tool — **not** part of the shipped binary. Run it from a
checkout with `go run ./test/loadgen` (or `task loadgen:*`).

## What it generates

One schema (`loadgen`) of **10 tables**, chosen to exercise replicare's hard
paths, not just to hold rows:

| Table | What it stresses |
|---|---|
| `tenants` | hub table many others FK to (giant-component pressure); identity PK, `jsonb`, `text[]`, `timestamptz`, `bool` |
| `categories` | self-referential hierarchy column (self-ref FK under `--cyclic`) |
| `users` | `bytea`, `inet`, `numeric(20,4)`, `date`, `jsonb`, many nullable columns; unique `(tenant_id,email)` |
| `products` | **composite text PK** `(tenant_id, sku)`; `float8[]`; `timestamp` (no tz); target of PK-changing updates |
| `orders` | **UUID PK**; `char(3)`; nullable FK to `users` |
| `order_items` | composite PK; **composite FK** to `products` `ON UPDATE CASCADE` (a SKU rename is a PK change that cascades) |
| `product_categories` | M:N join, composite PK, two FKs |
| `events` | the **high-volume** table (~1M rows at default scale); wide type coverage |
| `generated_metrics` | `GENERATED … STORED` column + identity PK |
| `audit_log` | **no primary/unique key** → replicare skips it with a loud warning (`verify` excludes it too) |

Data is generated **server-side** (`INSERT … SELECT generate_series`, `random()`
made reproducible with `setseed`, `md5(…)::uuid`) — fast to a million rows, robust
across old Postgres, no client-side type encoding. The churn mix includes
PK-changing SKU renames and mutation of the nullable FK-cycle edge.

At `--scale 1.0` (default) the seed writes ~1.2M rows (events dominate). Scale it
down for quick loops: `--scale 0.01` ≈ 12k rows in ~150ms.

## Usage

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

`--dsn`/`--source`/`--target` take a libpq/pgx URL (`postgres://user:pw@host:port/db`)
or keyword string; empty uses the standard `PG*` environment variables.

### Commands & flags

- `run --dsn <src> [--scale F] [--ops N] [--seed S] [--cyclic]` — ensure schema,
  then **seed if empty** or **churn** `N` statements. `--seed` makes a run
  reproducible.
- `ddl --dsn <db> [--cyclic]` — apply the schema only (prep the target).
- `verify --source <src> --target <tgt> [--wait D] [--interval D]` — per-table
  row-count + ordered content-checksum comparison. Exits non-zero on drift.
  `--wait` polls until converged or the timeout. GUCs are pinned to match
  replicare's transport so `row::text` renders identically on both ends.
- `reset --dsn <db> --yes` — `DROP SCHEMA loadgen CASCADE`.

`verify` compares only the 9 keyed tables (it skips `audit_log`, which replicare
skips). The `GENERATED … STORED` column is included and matches because both
servers recompute it.

## The `--cyclic` flag

By default the schema is **acyclic**. `--cyclic` adds two nullable FK **cycles** —
the `categories` self-reference and a `users ↔ orders` 2-table cycle — to exercise
replicare's cyclic-copy (`null_then_fill`) path. Apply it consistently to source
**and** target (`ddl --cyclic`, `run --cyclic`).

> **Both initial copy and streaming of a cyclic component converge** under
> `--cyclic`, with **standard non-DEFERRABLE** target FKs (the common real-world
> case). Initial copy loads every table with its cyclic FK columns NULL, in an
> order that respects the non-cyclic edges, then fills those columns. Streaming
> uses the same NULL-then-fill idea per pass, but **per table**: each table's
> upsert, delete, and cyclic-column fill commit independently, so a parent
> advances through its own delta queue even when a cross-batch child is
> transiently blocked (the same progress guarantee the acyclic drain gives),
> and the cycle itself is closed by a final fill phase. This harness's
> `--cyclic` schema drains cleanly under heavy churn with no `DEFERRABLE`
> requirement.

## Gotchas

- **Reset the state store when re-seeding from scratch.** replicare records
  per-table copy-progress in its state store; if you wipe and re-seed the source
  without clearing that state, replicare treats already-`Done` tables as copied
  and skips them (their parents go missing → FK errors on children). Clear the
  `replicare` schema in your state-store database (or use a fresh one) alongside
  `loadgen reset`.
- **The target schema must exist first** — run `ddl` against the target before
  starting replicare.
- Deletes cascade (`orders → order_items`, `tenants → everything`), so a
  `delete_orders`/`delete_users` op can remove more rows than it names.

## Task shortcuts

```sh
task loadgen:ddl    DSN="$TARGET"                    # schema on the target
task loadgen:seed   DSN="$SOURCE" SCALE=0.1          # seed / scale
task loadgen:churn  DSN="$SOURCE" OPS=500            # a churn burst
task loadgen:verify SRC="$SOURCE" TGT="$TARGET"      # convergence check
```
