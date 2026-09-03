package main

import (
	"context"
	"fmt"

	goredis "github.com/redis/go-redis/v9"
)

// Key layout. Replicated keys are lg:{<bucket>}:<type>:<n>; the {bucket} hash tag
// pins a key to one Redis Cluster slot, so it (a) spreads the keyspace across
// slots/shards (exercising replicare's per-master SCAN fan-out) and (b) keeps a
// RENAME's old and new key in the same slot (a cross-slot RENAME is an error). On
// a standalone server the braces are literal, inert bytes.
//
// lgskip:* keys are written to the SOURCE only. replicare's selection is expected
// to be include: ["lg:*"], which does not match lgskip:*, so those keys must never
// reach the target -- the Redis analog of loadgen's keyless audit_log. verify
// asserts the target holds none of them.
const (
	keyPrefix  = "lg:"
	skipPrefix = "lgskip:"
	buckets    = 16 // number of hash-tag buckets; enough to spread a small cluster
)

// keyTypes are the value types the harness seeds, churns, and verifies. Every one
// is exercised through replicare's DUMP -> RESTORE transport, which is value-
// faithful and type-agnostic (CLAUDE.md §1.7). "big" is a large hash used to
// exercise the big-key path; it verifies as a normal hash.
var keyTypes = []string{"str", "hash", "list", "set", "zset", "stream", "big"}

// bucketOf spreads index i deterministically across the hash-tag buckets.
func bucketOf(i int) int { return i % buckets }

// dataKey builds a replicated key: lg:{<bucket>}:<type>:<n>.
func dataKey(typ string, i int) string {
	return fmt.Sprintf("%s{%d}:%s:%d", keyPrefix, bucketOf(i), typ, i)
}

// skipKey builds a source-only key: lgskip:{<bucket>}:<n>.
func skipKey(i int) string {
	return fmt.Sprintf("%s{%d}:%d", skipPrefix, bucketOf(i), i)
}

// scale controls how many keys of each type the initial seed writes. Kept small
// per-type-relative so the default lands near a few hundred thousand keys (Redis
// holds everything in RAM); scale up/down with --scale.
type scale struct {
	str    int
	hash   int
	list   int
	set    int
	zset   int
	stream int
	big    int
	skip   int
}

func defaultScale() scale {
	return scale{
		str:    200000,
		hash:   50000,
		list:   50000,
		set:    50000,
		zset:   50000,
		stream: 10000,
		big:    200, // few, but large (see seedBig)
		skip:   20000,
	}
}

func (s scale) mul(f float64) scale {
	one := func(n int) int {
		v := int(float64(n) * f)
		if v < 1 {
			v = 1
		}
		return v
	}
	return scale{
		str:    one(s.str),
		hash:   one(s.hash),
		list:   one(s.list),
		set:    one(s.set),
		zset:   one(s.zset),
		stream: one(s.stream),
		big:    one(s.big),
		skip:   one(s.skip),
	}
}

func (s scale) total() int {
	return s.str + s.hash + s.list + s.set + s.zset + s.stream + s.big + s.skip
}

// countFor returns the seeded count for a data type (used by churn to pick an
// existing index). Streams/big excluded from some churn ops handled by name.
func (s scale) countFor(typ string) int {
	switch typ {
	case "str":
		return s.str
	case "hash":
		return s.hash
	case "list":
		return s.list
	case "set":
		return s.set
	case "zset":
		return s.zset
	case "stream":
		return s.stream
	case "big":
		return s.big
	}
	return 0
}

// anyKey reports whether at least one key matches pattern anywhere in the
// keyspace (across masters in cluster mode). Used as the seeded/empty check.
func (r *rdb) anyKey(ctx context.Context, pattern string) (bool, error) {
	found := false
	err := r.forEachMaster(ctx, func(ctx context.Context, c goredis.Cmdable) error {
		var cursor uint64
		for {
			keys, next, err := c.Scan(ctx, cursor, pattern, 256).Result()
			if err != nil {
				return err
			}
			if len(keys) > 0 {
				found = true
				return nil
			}
			if next == 0 {
				return nil
			}
			cursor = next
		}
	})
	return found, err
}

// scanKeys collects every key matching pattern across all masters.
func scanKeys(ctx context.Context, r *rdb, pattern string) ([]string, error) {
	var keys []string
	err := r.forEachMaster(ctx, func(ctx context.Context, c goredis.Cmdable) error {
		var cursor uint64
		for {
			batch, next, err := c.Scan(ctx, cursor, pattern, 512).Result()
			if err != nil {
				return err
			}
			keys = append(keys, batch...)
			if next == 0 {
				return nil
			}
			cursor = next
		}
	})
	return keys, err
}

// deleteMatching DELs every key matching pattern, in pipelined batches, and
// returns the count removed. Each DEL names a single key so the pipeline is
// cluster-safe (a multi-key DEL across slots would be CROSSSLOT); go-redis routes
// each command to its own slot. The harness datasets are ephemeral.
func deleteMatching(ctx context.Context, r *rdb, pattern string) (int64, error) {
	keys, err := scanKeys(ctx, r, pattern)
	if err != nil {
		return 0, err
	}
	var deleted int64
	const batch = 512
	for lo := 0; lo < len(keys); lo += batch {
		hi := lo + batch
		if hi > len(keys) {
			hi = len(keys)
		}
		pipe := r.uc.Pipeline()
		cmds := make([]*goredis.IntCmd, 0, hi-lo)
		for _, k := range keys[lo:hi] {
			cmds = append(cmds, pipe.Del(ctx, k))
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return deleted, fmt.Errorf("del: %w", err)
		}
		for _, c := range cmds {
			deleted += c.Val()
		}
	}
	return deleted, nil
}
