# Postgres version support (F3 floor decision)

This records replicare's committed Postgres version support floor and the
rationale, per the M0 deliverable F3. It is load-bearing: several CLAUDE.md
decisions hinge on genuinely old source behavior, so the floor is a decision, not
an afterthought.

## Decision

| Role | Version in the test harness | Support statement |
|------|------|-------------------|
| **Source** | **PostgreSQL 9.6** | Best-effort support back to **9.6**. replicare reads from old sources using conservative, version-tolerant SQL (CLAUDE.md §1.6). |
| **Target** | **PostgreSQL 17** | Target should be a modern major (12+); apply uses modern features such as `INSERT ... ON CONFLICT`. |

The CI integration matrix runs **source 9.6 → target 17**.

## Why 9.6 as the source floor

- **Float fidelity (CLAUDE.md §4.2).** `extra_float_digits = 3` is *critical* for
  round-trippable `float4`/`float8` text on servers **older than 12**. A floor of
  9.6 actually exercises that regime; a floor of 12+ would leave the most
  important float-fidelity claim untested.
- **No declarative partitioning.** Native partitioning arrived in PG 10, so 9.6
  exercises the "no native partitions" path and the ctid/keyset chunking
  fallbacks (CLAUDE.md §4.1).
- **Old PL/pgSQL.** Trigger functions and capture DDL must run on old servers
  (CLAUDE.md §3.1); 9.6 is a realistic old target for that portability.
- **Availability.** `postgres:9.6` is still pullable from Docker Hub for CI.

## Caveats

- **Apple Silicon (arm64):** the `postgres:9.6` image is amd64-only, so it runs
  under emulation locally (`platform: linux/amd64` in the harness). CI runners are
  amd64, so this only affects local dev speed.
- **Even older sources (9.4 / 9.5 and earlier):** not covered by the CI matrix.
  If we later need them, add matrix entries; until then, behavior on pre-9.6
  servers is **unverified**. (Notably, pre-9.0 `bytea` defaults to `escape`
  output, which the modern target still parses — but this is not exercised.)

## What is verified vs unverified

- **Verified by CI:** everything against source 9.6 → target 17.
- **Unverified (flagged):** sources older than 9.6, and target majors other than
  the one(s) in the matrix. Revisit if a real deployment needs them.
