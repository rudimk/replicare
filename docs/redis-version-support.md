# Redis version & fork support

replicare's Redis engine replicates **Redis → Redis** using ordinary, grantable
commands (`SCAN`, `DUMP`, `PTTL`, `RESTORE`, `DEL`) — no replication link, no
RDB/AOF access, no `PSYNC` (CLAUDE.md §3.2). Transport is **`DUMP` → `RESTORE`**
(value-faithful; the value's RDB-serialized bytes are moved and never
transformed, §1.7). This page records the supported version range and fork stance
(see `.sisyphus/redis-plan.md` for the full design).

## Version range

| Role | Best-effort floor | Recommended | CI-tested |
|---|---|---|---|
| Source | **3.0** (`RESTORE … REPLACE`) | 6.2+ | 6.2 |
| Target | **5.0** (`RESTORE … ABSTTL` opt-in) | 7.x | 7.4 |

The engine keeps source-side commands conservative (`SCAN` 2.8+, `DUMP`/`RESTORE`
2.6+, `PTTL` 2.6+); `SCAN … TYPE` (6.0+), ACLs (6.0+), and `RESTORE … ABSTTL`
(5.0+) are modern-only niceties with older fallbacks.

### The RDB-version directional gate (important)

`RESTORE` **rejects a payload whose embedded RDB version exceeds the target's
maximum**. Therefore:

- **older/equal source → newer/equal target: works** (backward compatible).
- **newer source → older target: fails loud** (`DUMP payload version or checksum
  are wrong`).

replicare's pre-flight (RM2) reads `INFO server` on both ends and **blocks a
newer-source → older-target pair** with an actionable error, rather than failing
mid-copy. Upgrade the target to at least the source's version, or replicate the
other direction. This is the Redis analog of the relational "block on
incompatible type."

## Forks

Detected from `INFO server`:

| Server | Status | Rationale |
|---|---|---|
| **Redis** | Supported | The reference implementation. |
| **Valkey** | Supported | Shares Redis's RDB format and RESP protocol, so `DUMP`/`RESTORE` transport is compatible. |
| **KeyDB** | Best-effort | Redis-protocol compatible; RDB compatibility generally holds but is not CI-verified. |
| **Dragonfly** | **Blocked (v1)** | Its `DUMP`/`RESTORE` RDB-format compatibility with Redis is unverified; since transport is `DUMP`/`RESTORE`, replicare refuses it loudly rather than risk silent corruption (redis-plan RQ-1). |

A non-Redis-family server is refused at connect, the same way MariaDB is refused
for the MySQL engine.

## Modules

Module-defined types (RedisJSON `ReJSON-RL`, RedisBloom, RedisTimeSeries, …)
produce **module-specific opaque DUMP payloads** that only `RESTORE` on a target
loading the identical module. Pre-flight (RM2) compares `MODULE LIST` on both ends
and **blocks** when a source module type is absent on the target — there is no
faithful native reconstruction of an opaque module value.
