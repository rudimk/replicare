# Redis Version & Fork Support

This page records the Redis versions and forks the Redis engine supports, and the reasons
behind where we've drawn each line. `replicare` replicates Redis to Redis using ordinary,
grantable commands(`SCAN`, `DUMP`, `PTTL`, `RESTORE`, `DEL`), with no replication link, no
RDB / AOF access and no `PSYNC`. Transport is `DUMP` then `RESTORE`, which is value-faithful;
each value's RDB-serialised bytes are moved, and never transformed.

## Version range

| Role | Best-effort floor | Recommended | CI-tested |
|---|---|---|---|
| Source | 3.0(`RESTORE … REPLACE`) | 6.2+ | 6.2 |
| Target | 5.0(`RESTORE … ABSTTL` opt-in) | 7.x | 7.4 |

The engine keeps the commands it runs on the source conservative(`SCAN` 2.8+, `DUMP`/`RESTORE`
2.6+, `PTTL` 2.6+). `SCAN … TYPE`(6.0+), ACLs(6.0+) and `RESTORE … ABSTTL`(5.0+) are niceties
that only exist on modern servers, and each of them has a fallback for older ones.

### The RDB-version directional gate

This is the one to pay attention to. `RESTORE` rejects a payload whose embedded RDB version is
higher than the target's maximum, which means:

- an older or equal source going to a newer or equal target works, since the format is
  backward compatible;
- a newer source going to an older target fails loudly(`DUMP payload version or checksum
  are wrong`).

Rather than let that happen mid-copy, `replicare`'s pre-flight check reads `INFO server` on both
ends and blocks a pair where the source is newer than the target, with an error that says what
to do. The fix is to upgrade the target to at least the source's version, or to replicate in
the other direction. This is the Redis analogue of the relational engines blocking on an
incompatible type.

## Forks

The server type is detected from `INFO server`:

| Server | Status | Rationale |
|---|---|---|
| Redis | Supported | The reference implementation. |
| Valkey | Supported | Shares Redis's RDB format and RESP protocol, so `DUMP`/`RESTORE` transport is compatible. |
| KeyDB | Best-effort | Redis-protocol compatible; RDB compatibility generally holds, but isn't CI-verified. |
| Dragonfly | Blocked(v1) | Its `DUMP`/`RESTORE` RDB-format compatibility with Redis is unverified; since transport is `DUMP`/`RESTORE`, `replicare` refuses it loudly rather than risk silent corruption. |

A server that isn't from the Redis family is refused at connect, the same way MariaDB is
refused by the MySQL engine.

## Modules

Module-defined types(RedisJSON `ReJSON-RL`, RedisBloom, RedisTimeSeries, …) produce
module-specific, opaque `DUMP` payloads, which only `RESTORE` on a target that loads the
identical module. There's no faithful native way to rebuild an opaque module value, so the
pre-flight check compares `MODULE LIST` on both ends, and blocks when a source module type is
missing on the target.
