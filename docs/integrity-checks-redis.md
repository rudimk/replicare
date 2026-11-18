# Redis Integrity Checks

This page describes a set of standalone `redis-cli` checks for confirming that a Redis source
and target have converged, for the times the `replicare` pod can't be reached to run
[`replicare status`](cli.md) / [`replicare verify`](cli.md). Redis has no query language, so
they're shell one-liners and a small script rather than SQL.

They mirror what `verify` does: hash the key, its type and its canonical logical value, using
type-aware logical reads(`GET` / `LRANGE` / `SMEMBERS` / `ZRANGE WITHSCORES` / `HGETALL` /
`XRANGE`). They *never* hash raw `DUMP` bytes, since those aren't byte-identical across Redis
versions for equal values. TTL is left out as well; it's replicated as a relative value, which
keeps it safe from clock skew, so the exact remaining time differs by design.

> ⚠️ The authoritative check is still `replicare verify <config>`. What's on this page is the
> fallback for when there's no pod to run it from.

## 1. Key count on both ends

Run this against both the source and the target:

```sh
redis-cli -h "$SRC_HOST" -p "$SRC_PORT" -n 0 DBSIZE
redis-cli -h "$TGT_HOST" -p "$TGT_PORT" -n 0 DBSIZE
```

The source may be a few keys ahead, by whatever the current reconciliation sweep hasn't
applied yet. A gap that shrinks towards 0 is normal, and not drift. (Deletes on the target lag
by the sweep interval, by design.)

## 2. A keyspace content digest on both ends

Save the following as `rc-redis-digest.sh`. It SCANs the keyspace, computes a type-aware
canonical value for each key, and XOR-folds each key's MD5, so the order keys come back in
doesn't matter. Run it against the source and the target; `keys` and `digest` have to match.

```sh
#!/usr/bin/env bash
# rc-redis-digest.sh <host> <port> [db]
# Order-independent content digest of a Redis DB, mirroring `replicare verify`:
# key + type + canonical logical value (NOT DUMP bytes); TTL excluded.
set -euo pipefail
host=${1:?host}; port=${2:?port}; db=${3:-0}
r(){ redis-cli -h "$host" -p "$port" -n "$db" "$@"; }

acc=0 count=0
while IFS= read -r key; do
  [ -z "$key" ] && continue
  t=$(r type "$key")
  case "$t" in
    string) v=$(r get "$key") ;;
    list)   v=$(r lrange "$key" 0 -1) ;;                 # order is significant, preserved
    set)    v=$(r smembers "$key" | LC_ALL=C sort) ;;    # unordered → sort for canonical form
    zset)   v=$(r zrange "$key" 0 -1 withscores) ;;      # ordered by score, deterministic
    hash)   v=$(r hgetall "$key" | paste - - | LC_ALL=C sort) ;;  # field/value pairs, sorted
    stream) v=$(r xrange "$key" - +) ;;                  # ordered by entry id
    *)      v="" ;;
  esac
  h=$(printf '%s\x00%s\x00%s' "$key" "$t" "$v" | md5sum | cut -c1-16)
  acc=$(( acc ^ 0x$h ))
  count=$(( count + 1 ))
done < <(r --scan)
printf 'keys=%d digest=%016x\n' "$count" "$acc"
```

Then run it against both ends:

```sh
chmod +x rc-redis-digest.sh
./rc-redis-digest.sh "$SRC_HOST" "$SRC_PORT" 0
./rc-redis-digest.sh "$TGT_HOST" "$TGT_PORT" 0
```

Use the *same* `redis-cli` binary for both runs, so values are formatted identically. Point the
source read at the master, and not a replica, since a lagging replica reads stale values.

## 3. Per-key spot checks

To eyeball a specific key on both sides, read it with its type's logical command, and `diff`
the two:

```sh
# string
diff <(redis-cli -h "$SRC_HOST" -p "$SRC_PORT" GET  mykey) \
     <(redis-cli -h "$TGT_HOST" -p "$TGT_PORT" GET  mykey)
# hash (sort so field order doesn't matter)
diff <(redis-cli -h "$SRC_HOST" -p "$SRC_PORT" HGETALL myhash | paste - - | sort) \
     <(redis-cli -h "$TGT_HOST" -p "$TGT_PORT" HGETALL myhash | paste - - | sort)
# zset (with scores)
diff <(redis-cli -h "$SRC_HOST" -p "$SRC_PORT" ZRANGE myzset 0 -1 WITHSCORES) \
     <(redis-cli -h "$TGT_HOST" -p "$TGT_PORT" ZRANGE myzset 0 -1 WITHSCORES)
```

## 4. Clusters

`--scan` and `DBSIZE` work per node. On a cluster, run steps 1 and 2 against each master, and
add up / compare the results shard by shard; `replicare` itself scans, subscribes and runs its
delete sweep per master, and its delete detection is pinned to the master. Only DB 0 is
replicated.

## Caveats

There are a few things to keep in mind with these checks:

1. Never hash `DUMP` bytes. They differ across Redis versions for equal values, so a hash of
   the bytes would report drift that isn't there across a version gap. The script above uses
   logical reads, which match because transport is *value*-faithful.
2. TTL is left out by design. It's replicated as a relative value, which keeps it safe from
   clock skew, so the exact remaining time differs. Don't compare `TTL`/`PTTL`.
3. The XOR fold is safe for distinct keys, since the key is part of each hash. It's an
   indicator of drift, though, with `verify` as the authority; it's the same fold `replicare`'s
   own Redis verify uses.
4. Big keys are slow. Serialising a value with millions of elements through `redis-cli` takes a
   while, so either spot-check those individually rather than in the full-keyspace script, or
   accept the runtime.
5. Streams only have their entries compared. Consumer groups / PEL aren't deep-verified here,
   and nor are they by `verify` in v1.
6. Value reads may use a replica, but any comparison of deletes or of the keyspace has to use
   the master; a lagging replica would show keys missing or extra that aren't.

See also the [Postgres integrity checks](integrity-checks-postgres.md) and the
[MySQL integrity checks](integrity-checks-mysql.md).
