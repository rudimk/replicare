# MySQL Version Support

This page covers the oldest MySQL version `replicare` supports as a source, the target version
it's tested against, and why MariaDB isn't supported. It's the MySQL counterpart to
[Postgres version support](postgres-version-support.md), and it names the pair of versions every
MySQL test runs against.

## The committed pair: MySQL 5.7 source to MySQL 8.4 target

CI runs a single concrete pair - `mysql:5.7` as the old source and `mysql:8.4` as the modern
target - rather than a broad matrix. This mirrors the Postgres engine's single `9.6 → 17` pair,
with one `integration-mysql` job on amd64. 5.7 has no arm64 image, so it runs under emulation
locally on Apple Silicon; CI is amd64 natively.

### Why 5.7 is the source floor

5.7 is the oldest source we support. We chose it because it's the oldest release that still has
the features `replicare`'s introspection and capture rely on, while being old enough to genuinely
exercise the path from an old source to a modern target:

| Capability | Introduced | Consequence if the floor were older(5.6) |
|---|---|---|
| Generated columns(`VIRTUAL`/`STORED` in `information_schema.COLUMNS.EXTRA`) | 5.7 | 5.6 has none; the handling of generated columns(they're left for the target to compute) couldn't be tested there. |
| JSON type | 5.7 | 5.6 lacks it; a headline fidelity-corpus type would be missing. |
| Usable `information_schema` | 5.7(materially faster than 5.6) | 5.6's `information_schema` is notoriously slow, degrading introspection. |
| `sys` schema, spatial indexes, etc. | 5.7 | Not load-bearing, but part of why 5.7 is the practical floor. |

> ⚠️ 5.6 is best-effort only. The engine may work against it, but no test runs against it, and
> the features above are unverified there. If a future need arises, 5.6 support is additive.

### Why 8.4 is the target

8.4 is the current long-term-support release, and it covers the concerns of a modern target:
`utf8mb4` defaults, `ROW_NUMBER()` window functions(used to split tables into chunks for the
initial copy), and the strict-`sql_mode` / zero-date interaction the faithful-transport path
depends on. The gap between 5.7 and 8.4 is where the old-source fidelity work earns its keep.

## MariaDB: out of scope for v1

`replicare` v1 doesn't support MariaDB. Although MariaDB began as a MySQL fork, its dialect,
system catalogues, `sql_mode` semantics and JSON / sequence behaviour have diverged enough that
silently driving it as MySQL would risk exactly the value corruption faithful transport forbids.
So the engine detects MariaDB at connect time(the server advertises `MariaDB` in `VERSION()`)
and refuses it with a loud error, rather than mis-driving it. MariaDB is a candidate for a
future engine variant behind the same interface, but it isn't a v1 goal.

## What this pins

The CI job builds and tests only against `mysql:5.7` and `mysql:8.4`, and every MySQL test runs
on this pair. If CI ever can't run 5.7(for example, if the image becomes unavailable), we'll
list the value checks that become unverified, rather than quietly raising the floor.
