# MySQL version support (the MF3 floor decision)

This records replicare's committed MySQL version floor and the MariaDB scope
decision for the v1 MySQL engine. It is the MySQL counterpart to
`docs/postgres-version-support.md`, and it fixes the CI test pair the milestone
plan (`.sisyphus/mysql-plan.md`, MF3) commits every acceptance test to.

## The committed pair: **MySQL 5.7 source → MySQL 8.4 target**

CI runs a single concrete pair — `mysql:5.7` as the old source, `mysql:8.4` as
the modern target — not a broad matrix. This mirrors the Postgres engine's single
`9.6 → 17` pair: one `integration-mysql` job, amd64 (5.7 has no arm64 image, so it
runs under emulation locally on Apple Silicon; CI is amd64 natively).

### Why 5.7 is the source floor

5.7 is the oldest source we support, chosen because it is the oldest release that
still has the features replicare's introspection and capture rely on, while being
old enough to genuinely exercise the "old source → modern target" path (§1.6):

| Capability | Introduced | Consequence if the floor were older (5.6) |
|---|---|---|
| **Generated columns** (`VIRTUAL`/`STORED` in `information_schema.COLUMNS.EXTRA`) | 5.7 | 5.6 has none; the generated-column exclusion path (MM4) would be untestable there. |
| **JSON type** | 5.7 | 5.6 lacks it; a headline fidelity-corpus type would be missing. |
| **Usable `information_schema`** | 5.7 (materially faster than 5.6) | 5.6's `information_schema` is notoriously slow, degrading introspection. |
| **`sys` schema, spatial indexes, etc.** | 5.7 | Not load-bearing, but part of why 5.7 is the practical floor. |

5.6 is **best-effort only**: the engine may work against it, but no acceptance
test is committed to it, and the features above are unverified there. If a future
need arises, 5.6 support is additive.

### Why 8.4 is the target

8.4 is the current LTS. It exercises the modern-target concerns the plan calls
out: 8.0+ multi-trigger-per-event, `utf8mb4` defaults, `ROW_NUMBER()` window
functions for boundary discovery (MM4a), and the strict-`sql_mode`/zero-date
interaction the faithful-transport path depends on (`mysql-plan.md` §0.4). The
5.7→8.4 gap is where the old-source fidelity work earns its keep.

## MariaDB: **out of scope for v1**

replicare v1 does **not** support MariaDB. Although MariaDB began as a MySQL fork,
its dialect, system catalogs, `sql_mode` semantics, and JSON/sequence behavior have
diverged enough that silently driving it as MySQL would risk exactly the value
corruption §1.7 forbids. The engine therefore **detects MariaDB at connect time**
(the server advertises `MariaDB` in `VERSION()`) and **refuses it with a loud
error** rather than mis-driving it. MariaDB is a candidate for a future engine
variant behind the same interface, not a v1 goal.

## What this pins

- The CI job builds and tests only against `mysql:5.7` and `mysql:8.4`.
- Every MySQL milestone's acceptance is verified on this pair (`mysql-plan.md`,
  "Testing strategy").
- If CI cannot run 5.7 (e.g. image unavailability), the plan requires listing which
  fidelity claims become unverified rather than silently raising the floor.
