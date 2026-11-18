# Postgres Version Support

This page covers the oldest Postgres version `replicare` supports as a source, the target version
it's tested against, and the reasons behind both. Much of `replicare`'s source-side SQL is written
to work on genuinely old servers, so the oldest supported version matters.

## Supported versions

| Role | Version in the test harness | Support statement |
|------|------|-------------------|
| Source | PostgreSQL 9.6 | Best-effort support back to 9.6. `replicare` reads from old sources using conservative, version-tolerant SQL. |
| Target | PostgreSQL 17 | The target should be a modern major(12+); apply uses modern features such as `INSERT ... ON CONFLICT`. |

The CI integration matrix runs a 9.6 source against a 17 target.

## Why 9.6 as the source floor

We picked 9.6 because it's old enough to exercise the parts of `replicare` that only matter on
old servers, while still being practical to run in CI:

1. Float fidelity. `extra_float_digits = 3` is *critical* for round-trippable `float4` / `float8`
   text on servers older than 12. A floor of 9.6 actually exercises that regime, whereas a floor
   of 12+ would leave the most important float-fidelity claim untested.
2. No declarative partitioning. Native partitioning arrived in PG 10, so 9.6 exercises the "no
   native partitions" path and the ctid / keyset chunking fallbacks.
3. Old PL/pgSQL. Trigger functions and capture DDL have to run on old servers, and 9.6 is a
   realistic old target for that portability.
4. Availability. `postgres:9.6` is still pullable from Docker Hub for CI.

## Caveats

> ⚠️ On Apple Silicon(arm64), the `postgres:9.6` image is amd64-only, so it runs under emulation
> locally(`platform: linux/amd64` in the harness). CI runners are amd64, so this only affects
> local dev speed.

> ⚠️ Even older sources(9.4 / 9.5 and earlier) aren't covered by the CI matrix. If we need them
> later, we'll add matrix entries; until then, behaviour on pre-9.6 servers is unverified.
> Notably, pre-9.0 `bytea` defaults to `escape` output, which the modern target still parses -
> but this isn't exercised.

## What is verified vs unverified

Everything against a 9.6 source and a 17 target is verified by CI. Sources older than 9.6, and
target majors other than the one(s) in the matrix, are unverified, and flagged as such; we'll
revisit them if a real deployment needs them.
