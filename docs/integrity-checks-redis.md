# Verifying data integrity — Redis

Standalone `redis-cli` checks to confirm a Redis **source** and **target** are
converged, for when you can't reach the replicare pod to run
[`replicare status`](cli.md) / [`replicare verify`](cli.md).

Redis has no query language, so these are shell one-liners and a small script. They
mirror what `verify` does: hash **key + type + canonical logical value**, using
type-aware logical reads (`GET` / `LRANGE` / `SMEMBERS` / `ZRANGE WITHSCORES` /
`HGETALL` / `XRANGE`) — **never** raw `DUMP` bytes, which are **not** byte-identical
across Redis versions for equal values. **TTL is excluded** (replicated relative →
skew-safe, so the exact remaining time differs by design).

> The authoritative check is still `replicare verify <config>`. This is the no-pod
> fallback.

---

## 1. Key count (run on both)

```sh
redis-cli -h "$SRC_HOST" -p "$SRC_PORT" -n 0 DBSIZE
redis-cli -h "$TGT_HOST" -p "$TGT_PORT" -n 0 DBSIZE
```

The source may read a few keys ahead by whatever the current reconciliation sweep
hasn't applied yet — a gap that **shrinks toward 0** is normal, not drift. (Deletes on
the target lag by the sweep interval by design.)

## 2. Keyspace content digest (run on both — digests must match)

Save this as `rc-redis-digest.sh`. It SCANs the keyspace, computes a type-aware
canonical value per key, and XOR-folds each key's MD5 so **iteration order doesn't
matter**. Run it against source and target; `keys` and `digest` must match.

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

```sh
chmod +x rc-redis-digest.sh
./rc-redis-digest.sh "$SRC_HOST" "$SRC_PORT" 0
./rc-redis-digest.sh "$TGT_HOST" "$TGT_PORT" 0
```

Use the **same `redis-cli` binary** for both runs so value formatting is identical.
Point the source read at the **master**, not a replica (a lagging replica reads stale).

## 3. Per-key spot checks

To eyeball a specific key on both sides, read it with its type's logical command and
`diff`:

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

## 4. Cluster

`--scan` and `DBSIZE` are **per-node**. On a cluster, run steps 1–2 against **each
master** and sum/compare per shard (replicare scans, subscribes, and delete-sweeps
per master; delete detection is master-pinned). Only DB 0 is replicated.

---

## Caveats

- **Never hash `DUMP` bytes.** They differ across Redis versions for equal values, so
  a byte hash would false-positive across a version gap. The script above uses logical
  reads, which match because transport is *value*-faithful.
- **TTL is excluded** by design (relative replication → skew-safe; exact remaining
  time differs). Don't compare `TTL`/`PTTL`.
- **XOR folding.** Distinct keys fold safely (key is part of each hash); the fold is a
  drift *indicator*, with `verify` the authority. It's the same fold replicare's own
  Redis verify uses.
- **Big keys.** A multi-million-element value serialized through `redis-cli` is slow;
  spot-check those individually rather than in the full-keyspace script, or accept the
  runtime.
- **Streams.** Entries are compared; consumer groups / PEL are not deep-verified here
  (nor by `verify` in v1).
- **Value reads may use a replica; delete/keyspace comparisons must use the master** —
  a lagging replica would show phantom missing/extra keys.

See also: [Postgres integrity checks](integrity-checks-postgres.md) ·
[MySQL integrity checks](integrity-checks-mysql.md).
