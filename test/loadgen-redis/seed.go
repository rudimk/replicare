package main

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// seedBatch bounds how many single-key writes are pipelined per round-trip. Every
// command names one key, so the pipeline is cluster-safe (go-redis routes each to
// its slot).
const seedBatch = 1000

// seed populates an empty keyspace with every value type, a fraction of volatile
// (TTL'd) keys, a handful of deliberately large keys, and a source-only lgskip:*
// cohort. Values come from the seeded rng so a given --seed reproduces the same
// dataset. All writes are single-key and pipelined.
func seed(ctx context.Context, r *rdb, s scale, rng *rand.Rand, log logf) error {
	steps := []struct {
		name string
		n    int
		fn   func(p goredis.Pipeliner, i int)
	}{
		{"str", s.str, func(p goredis.Pipeliner, i int) {
			ttl := time.Duration(0)
			if rng.Float64() < 0.2 { // ~20% volatile
				ttl = time.Duration(1+rng.Intn(24)) * time.Hour
			}
			p.Set(ctx, dataKey("str", i), randVal(rng, 8, 64), ttl)
		}},
		{"hash", s.hash, func(p goredis.Pipeliner, i int) {
			p.HSet(ctx, dataKey("hash", i), hashFields(rng, 3+rng.Intn(5)))
		}},
		{"list", s.list, func(p goredis.Pipeliner, i int) {
			p.RPush(ctx, dataKey("list", i), elems(rng, 2+rng.Intn(6))...)
		}},
		{"set", s.set, func(p goredis.Pipeliner, i int) {
			p.SAdd(ctx, dataKey("set", i), elems(rng, 2+rng.Intn(6))...)
		}},
		{"zset", s.zset, func(p goredis.Pipeliner, i int) {
			p.ZAdd(ctx, dataKey("zset", i), zmembers(rng, 2+rng.Intn(6))...)
		}},
		{"skip", s.skip, func(p goredis.Pipeliner, i int) {
			p.Set(ctx, skipKey(i), randVal(rng, 8, 32), 0)
		}},
	}

	for _, st := range steps {
		if err := pipelineN(ctx, r, st.n, func(p goredis.Pipeliner, i int) { st.fn(p, i) }); err != nil {
			return fmt.Errorf("seed %s: %w", st.name, err)
		}
		log("seeded %s (%d)", st.name, st.n)
	}

	// Streams: a few entries each, and a consumer group on some, so DUMP/RESTORE of
	// stream + group state is exercised. XADD/XGROUP are separate commands, still
	// single-key.
	if err := seedStreams(ctx, r, s.stream, rng, log); err != nil {
		return err
	}
	// Big keys: large hashes to exercise DUMP/RESTORE of big payloads (and the
	// big-key MEMORY USAGE gate when the sync configures it).
	if err := seedBig(ctx, r, s.big, rng, log); err != nil {
		return err
	}
	// Record the per-type seeded counts so churn can address existing keys without
	// scanning. lgmeta:* is neither lg:* (replicated) nor lgskip:* (source-only), so
	// it is invisible to replicare's selection and to verify.
	return writeMeta(ctx, r, s)
}

// metaKey holds the per-type seeded counts. Not under lg:/lgskip:, so it is
// ignored by both replicare's include: ["lg:*"] and by verify.
const metaKey = "lgmeta:counts"

func writeMeta(ctx context.Context, r *rdb, s scale) error {
	return r.uc.HSet(ctx, metaKey,
		"str", s.str, "hash", s.hash, "list", s.list, "set", s.set,
		"zset", s.zset, "stream", s.stream, "big", s.big, "skip", s.skip,
	).Err()
}

// readMeta returns the seeded counts, falling back to the default scale when the
// meta key is absent (e.g. a keyspace seeded by an older tool).
func readMeta(ctx context.Context, r *rdb) scale {
	m, err := r.uc.HGetAll(ctx, metaKey).Result()
	if err != nil || len(m) == 0 {
		return defaultScale()
	}
	atoi := func(k string) int { n, _ := strconv.Atoi(m[k]); return n }
	return scale{
		str: atoi("str"), hash: atoi("hash"), list: atoi("list"), set: atoi("set"),
		zset: atoi("zset"), stream: atoi("stream"), big: atoi("big"), skip: atoi("skip"),
	}
}

// pipelineN calls fn for i in [1,n], flushing the pipeline every seedBatch.
func pipelineN(ctx context.Context, r *rdb, n int, fn func(p goredis.Pipeliner, i int)) error {
	pipe := r.uc.Pipeline()
	for i := 1; i <= n; i++ {
		fn(pipe, i)
		if i%seedBatch == 0 {
			if _, err := pipe.Exec(ctx); err != nil {
				return err
			}
			pipe = r.uc.Pipeline()
		}
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}
	return nil
}

func seedStreams(ctx context.Context, r *rdb, n int, rng *rand.Rand, log logf) error {
	if err := pipelineN(ctx, r, n, func(p goredis.Pipeliner, i int) {
		key := dataKey("stream", i)
		for j := 0; j < 2+rng.Intn(4); j++ {
			p.XAdd(ctx, &goredis.XAddArgs{
				Stream: key,
				Values: map[string]any{"seq": j, "v": randVal(rng, 6, 24)},
			})
		}
		if rng.Float64() < 0.5 {
			p.XGroupCreateMkStream(ctx, key, "g1", "0")
		}
	}); err != nil {
		return fmt.Errorf("seed stream: %w", err)
	}
	log("seeded stream (%d)", n)
	return nil
}

func seedBig(ctx context.Context, r *rdb, n int, rng *rand.Rand, log logf) error {
	if err := pipelineN(ctx, r, n, func(p goredis.Pipeliner, i int) {
		// ~100 KB hash: 20 fields x ~5 KB.
		vals := make([]any, 0, 40)
		for f := 0; f < 20; f++ {
			vals = append(vals, "f"+strconv.Itoa(f), randVal(rng, 5000, 5000))
		}
		p.HSet(ctx, dataKey("big", i), vals...)
	}); err != nil {
		return fmt.Errorf("seed big: %w", err)
	}
	log("seeded big (%d)", n)
	return nil
}

const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// randVal returns a random string whose length is in [lo,hi].
func randVal(rng *rand.Rand, lo, hi int) string {
	n := lo
	if hi > lo {
		n += rng.Intn(hi - lo + 1)
	}
	b := make([]byte, n)
	for i := range b {
		b[i] = alphabet[rng.Intn(len(alphabet))]
	}
	return string(b)
}

// hashFields returns a []any of alternating field/value for HSET.
func hashFields(rng *rand.Rand, n int) []any {
	out := make([]any, 0, n*2)
	for i := 0; i < n; i++ {
		out = append(out, "f"+strconv.Itoa(i), randVal(rng, 4, 32))
	}
	return out
}

// elems returns n random element strings for RPUSH/SADD.
func elems(rng *rand.Rand, n int) []any {
	out := make([]any, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, randVal(rng, 4, 24))
	}
	return out
}

// zmembers returns n scored members for ZADD.
func zmembers(rng *rand.Rand, n int) []goredis.Z {
	out := make([]goredis.Z, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, goredis.Z{Score: float64(rng.Intn(100000)) + rng.Float64(), Member: randVal(rng, 4, 16)})
	}
	return out
}
