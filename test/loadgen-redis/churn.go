package main

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// A churn op mutates a small slice of the keyspace: one execution touches k keys
// (kMin..kMax). Each run of `loadgen-redis run` on a seeded keyspace executes
// --ops of these, weighted, so a run is a realistic burst of change for replicare
// to reconcile -- inserts, value mutations, TTL changes, and the two cases the
// Redis engine handles specially: RENAME (delete-old + create-new, the analog of a
// PK change, and a stress on the delete-reconciliation sweep) and heavy DEL (Redis
// has no delete capture, so deletes are only caught by the target-vs-source diff).
type churnOp struct {
	name       string
	weight     int
	kMin, kMax int
	fn         func(ctx context.Context, r *rdb, s scale, rng *rand.Rand, k int) (int64, error)
}

func churnOps() []churnOp {
	return []churnOp{
		{"set_string", 18, 10, 100, func(ctx context.Context, r *rdb, s scale, rng *rand.Rand, k int) (int64, error) {
			return pipeK(ctx, r, k, func(p goredis.Pipeliner) {
				p.Set(ctx, dataKey("str", existing(rng, s.str)), randVal(rng, 8, 64), 0)
			})
		}},
		{"insert_string", 10, 10, 80, func(ctx context.Context, r *rdb, s scale, rng *rand.Rand, k int) (int64, error) {
			// Brand-new keys beyond the seeded range grow the keyspace.
			return pipeK(ctx, r, k, func(p goredis.Pipeliner) {
				i := s.str + 1 + rng.Intn(1_000_000)
				p.Set(ctx, dataKey("str", i), randVal(rng, 8, 64), 0)
			})
		}},
		{"update_hash", 12, 5, 60, func(ctx context.Context, r *rdb, s scale, rng *rand.Rand, k int) (int64, error) {
			return pipeK(ctx, r, k, func(p goredis.Pipeliner) {
				p.HSet(ctx, dataKey("hash", existing(rng, s.hash)), "u"+randVal(rng, 3, 3), randVal(rng, 4, 32))
			})
		}},
		{"append_list", 10, 5, 60, func(ctx context.Context, r *rdb, s scale, rng *rand.Rand, k int) (int64, error) {
			return pipeK(ctx, r, k, func(p goredis.Pipeliner) {
				p.RPush(ctx, dataKey("list", existing(rng, s.list)), randVal(rng, 4, 24))
			})
		}},
		{"add_set", 10, 5, 60, func(ctx context.Context, r *rdb, s scale, rng *rand.Rand, k int) (int64, error) {
			return pipeK(ctx, r, k, func(p goredis.Pipeliner) {
				p.SAdd(ctx, dataKey("set", existing(rng, s.set)), randVal(rng, 4, 24))
			})
		}},
		{"add_zset", 10, 5, 60, func(ctx context.Context, r *rdb, s scale, rng *rand.Rand, k int) (int64, error) {
			return pipeK(ctx, r, k, func(p goredis.Pipeliner) {
				p.ZAdd(ctx, dataKey("zset", existing(rng, s.zset)),
					goredis.Z{Score: float64(rng.Intn(100000)) + rng.Float64(), Member: randVal(rng, 4, 16)})
			})
		}},
		{"xadd_stream", 8, 5, 40, func(ctx context.Context, r *rdb, s scale, rng *rand.Rand, k int) (int64, error) {
			return pipeK(ctx, r, k, func(p goredis.Pipeliner) {
				p.XAdd(ctx, &goredis.XAddArgs{
					Stream: dataKey("stream", existing(rng, s.stream)),
					Values: map[string]any{"v": randVal(rng, 6, 24)},
				})
			})
		}},
		{"set_ttl", 8, 10, 80, func(ctx context.Context, r *rdb, s scale, rng *rand.Rand, k int) (int64, error) {
			return pipeK(ctx, r, k, func(p goredis.Pipeliner) {
				key := dataKey("str", existing(rng, s.str))
				if rng.Float64() < 0.3 {
					p.Persist(ctx, key)
				} else {
					p.Expire(ctx, key, hoursTTL(rng))
				}
			})
		}},
		// RENAME within the same hash-tag bucket (so old+new stay in one slot in
		// cluster mode). This is a delete(old)+create(new) as far as the target sees.
		{"rename_key", 6, 1, 20, renameChurn},
		// Heavy DEL across types: the delete-reconciliation stress.
		{"delete_keys", 10, 5, 50, func(ctx context.Context, r *rdb, s scale, rng *rand.Rand, k int) (int64, error) {
			return pipeK(ctx, r, k, func(p goredis.Pipeliner) {
				typ := keyTypes[rng.Intn(len(keyTypes))]
				if n := s.countFor(typ); n > 0 {
					p.Del(ctx, dataKey(typ, existing(rng, n)))
				}
			})
		}},
		// lgskip:* churn: source-only writes so the excluded cohort keeps changing
		// and verify keeps confirming it never leaks to the target.
		{"set_skip", 5, 10, 60, func(ctx context.Context, r *rdb, s scale, rng *rand.Rand, k int) (int64, error) {
			return pipeK(ctx, r, k, func(p goredis.Pipeliner) {
				p.Set(ctx, skipKey(existing(rng, s.skip)), randVal(rng, 8, 32), 0)
			})
		}},
	}
}

// renameChurn renames k existing string keys to a fresh suffix in the same bucket.
// A RENAME on a key that was already renamed/deleted returns "no such key"; that is
// a benign no-op for the harness, so it is swallowed.
func renameChurn(ctx context.Context, r *rdb, s scale, rng *rand.Rand, k int) (int64, error) {
	var done int64
	for j := 0; j < k; j++ {
		i := existing(rng, s.str)
		src := dataKey("str", i)
		dst := fmt.Sprintf("%s{%d}:str:%d:r%d", keyPrefix, bucketOf(i), i, rng.Intn(1_000_000_000))
		if err := r.uc.Rename(ctx, src, dst).Err(); err != nil {
			if strings.Contains(err.Error(), "no such key") {
				continue
			}
			return done, fmt.Errorf("rename: %w", err)
		}
		done++
	}
	return done, nil
}

// existing returns a 1-based index into a seeded count (>=1 even if n==0).
func existing(rng *rand.Rand, n int) int {
	if n <= 0 {
		return 1
	}
	return 1 + rng.Intn(n)
}

func hoursTTL(rng *rand.Rand) (d time.Duration) {
	return time.Duration(1+rng.Intn(24)) * time.Hour
}

// pipeK runs emit k times into one pipeline and returns the number of commands
// that reported a positive result (rows/keys affected), for the summary. Every
// emitted command is single-key, so the pipeline is cluster-safe.
func pipeK(ctx context.Context, r *rdb, k int, emit func(p goredis.Pipeliner)) (int64, error) {
	pipe := r.uc.Pipeline()
	for j := 0; j < k; j++ {
		emit(pipe)
	}
	cmds, err := pipe.Exec(ctx)
	if err != nil && !isNil(err) {
		return 0, err
	}
	var affected int64
	for _, c := range cmds {
		if ic, ok := c.(*goredis.IntCmd); ok && ic.Val() > 0 {
			affected += ic.Val()
		} else {
			affected++ // Set/XAdd etc. don't report a count; count the call
		}
	}
	return affected, nil
}

func isNil(err error) bool { return err == goredis.Nil }

// churn executes `ops` weighted-random ops and returns a per-op summary.
func churn(ctx context.Context, r *rdb, ops int, rng *rand.Rand, log logf) (map[string]churnStat, error) {
	s := readMeta(ctx, r)
	table := churnOps()
	total := 0
	for _, op := range table {
		total += op.weight
	}

	stats := map[string]churnStat{}
	for i := 0; i < ops; i++ {
		op := pickOp(table, total, rng)
		k := op.kMin
		if op.kMax > op.kMin {
			k += rng.Intn(op.kMax - op.kMin + 1)
		}
		affected, err := op.fn(ctx, r, s, rng, k)
		if err != nil {
			return stats, fmt.Errorf("churn op %s (k=%d): %w", op.name, k, err)
		}
		st := stats[op.name]
		st.calls++
		st.keys += affected
		stats[op.name] = st
	}
	return stats, nil
}

type churnStat struct {
	calls int
	keys  int64
}

func pickOp(table []churnOp, total int, rng *rand.Rand) churnOp {
	r := rng.Intn(total)
	for _, op := range table {
		if r < op.weight {
			return op
		}
		r -= op.weight
	}
	return table[len(table)-1]
}

func summaryLines(stats map[string]churnStat) []string {
	names := make([]string, 0, len(stats))
	for n := range stats {
		names = append(names, n)
	}
	sort.Strings(names)
	lines := make([]string, 0, len(names))
	for _, n := range names {
		s := stats[n]
		lines = append(lines, fmt.Sprintf("  %-20s %4d calls  %8d keys", n, s.calls, s.keys))
	}
	return lines
}
