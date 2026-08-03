package redis

import (
	"context"
	"fmt"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/rudimk/replicare/internal/apply"
	"github.com/rudimk/replicare/internal/engine"
)

// streamToConvergence drives the neutral apply layer (DrainComponent — the exact
// daemon path) over the Redis unit until a full rolling reconciliation pass
// completes (ReadDirtyKeys returns empty -> DrainComponent returns 0). Returns the
// total keys applied. A guard bounds the loop so a bug can't spin forever.
func streamToConvergence(t *testing.T, ctx context.Context, src *Source, sink *Sink) int {
	t.Helper()
	ref := unitRef(src.cfg)
	total := 0
	for i := 0; i < 10000; i++ {
		n, err := apply.DrainComponent(ctx, src, sink, []engine.TableRef{ref}, "dst", 128, false)
		if err != nil {
			t.Fatalf("DrainComponent: %v", err)
		}
		total += n
		if n == 0 {
			return total
		}
	}
	t.Fatal("streaming did not converge within the pass guard")
	return total
}

// TestStreamingConvergence is the RM5 keystone (6.2 -> 7.4 standalone): after the
// initial copy, source SET/overwrite/new-key/EXPIRE/type-change all CONVERGE on the
// target through rolling reconciliation, verified by logical compare. A binary key
// round-trips verbatim; a second pass is idempotent (crash-before-confirm model).
func TestStreamingConvergence(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	src, sink := mustConnectPair(t, ctx, srcCfg(), tgtCfg())
	sc, tc := src.db.uc, sink.db.uc
	must(t, sc.FlushDB(ctx).Err())
	must(t, tc.FlushDB(ctx).Err())

	// Initial state, copied.
	binKey := string([]byte{0x00, 0xff, 0x41, 0x00})
	binVal := string([]byte{0xde, 0xad, 0x00, 0xbe, 0xef})
	must(t, sc.Set(ctx, "over", "v1", 0).Err())
	must(t, sc.Set(ctx, "typ", "iam-a-string", 0).Err())
	must(t, sc.Set(ctx, "exp", "will-expire", 0).Err())
	must(t, sc.Set(ctx, binKey, "pre", 0).Err())
	copyUnit(t, ctx, src, sink)

	// Post-copy mutations on the source.
	must(t, sc.Set(ctx, "over", "v2", 0).Err())           // overwrite
	must(t, sc.Set(ctx, "fresh", "new", 0).Err())         // brand-new key
	must(t, sc.Del(ctx, "typ").Err())                     // type change: string -> list
	must(t, sc.RPush(ctx, "typ", "a", "b", "c").Err())    //
	must(t, sc.PExpire(ctx, "exp", 40*time.Second).Err()) // gains a TTL
	must(t, sc.Set(ctx, binKey, binVal, 0).Err())         // binary value under a binary key
	for i := 0; i < 5; i++ {                              // rapid rewrite -> last wins
		must(t, sc.Set(ctx, "rapid", string(rune('a'+i)), 0).Err())
	}

	streamToConvergence(t, ctx, src, sink)

	// Converged?
	if got, _ := tc.Get(ctx, "over").Result(); got != "v2" {
		t.Errorf("over = %q, want v2 (overwrite)", got)
	}
	if got, _ := tc.Get(ctx, "fresh").Result(); got != "new" {
		t.Errorf("fresh = %q, want new (new key streamed)", got)
	}
	if got, _ := tc.LRange(ctx, "typ", 0, -1).Result(); len(got) != 3 || got[0] != "a" || got[2] != "c" {
		t.Errorf("typ = %v, want [a b c] (type change converged)", got)
	}
	if pttl := tc.PTTL(ctx, "exp").Val(); pttl <= 0 || pttl > 40*time.Second {
		t.Errorf("exp PTTL = %v, want (0, 40s] (EXPIRE converged)", pttl)
	}
	if got, _ := tc.Get(ctx, binKey).Result(); got != binVal {
		t.Errorf("binary key value = %q, want %q", got, binVal)
	}
	if got, _ := tc.Get(ctx, "rapid").Result(); got != "e" {
		t.Errorf("rapid = %q, want e (last of rapid rewrite)", got)
	}

	// Idempotent: a second full pass changes nothing (crash-before-confirm just
	// reprocesses; RESTORE REPLACE is idempotent).
	streamToConvergence(t, ctx, src, sink)
	if got, _ := tc.Get(ctx, "over").Result(); got != "v2" {
		t.Errorf("after 2nd pass over = %q, want v2 (idempotent)", got)
	}
}

// TestStreamingReconcileCoversAllShards is the RM5 cluster acceptance: the bounded
// reconciliation SCAN state machine, driven across many small ReadDirtyKeys calls,
// eventually returns EVERY key across ALL shard masters in one rolling pass — the
// coverage convergence depends on (redis-plan §0.5). A missed master would drop keys.
func TestStreamingReconcileCoversAllShards(t *testing.T) {
	if !clusterIntegration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	src := &Source{cfg: clusterCfg()}
	if err := src.Connect(ctx); err != nil {
		t.Fatalf("connect cluster: %v", err)
	}
	defer src.Close(context.Background())

	if err := src.db.cluster.ForEachMaster(ctx, func(ctx context.Context, c *goredis.Client) error {
		return c.FlushAll(ctx).Err()
	}); err != nil {
		t.Fatalf("flush cluster: %v", err)
	}
	const n = 90
	for i := 0; i < n; i++ {
		if err := src.db.uc.Set(ctx, fmt.Sprintf("k:%c:%d", 'A'+i%26, i), "v", 0).Err(); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	// Drive small bounded batches; collect distinct keys until a pass completes.
	ref := unitRef(src.cfg)
	seen := map[string]bool{}
	for i := 0; i < 100000; i++ {
		batch, err := src.ReadDirtyKeys(ctx, ref, "dst", 7) // small max -> many ticks
		if err != nil {
			t.Fatalf("ReadDirtyKeys: %v", err)
		}
		if len(batch) == 0 {
			break // pass complete
		}
		for _, d := range batch {
			seen[redisKey(d.Key)] = true
		}
	}
	if len(seen) != n {
		t.Fatalf("reconciliation saw %d distinct keys across the cluster, want %d — a shard master was missed", len(seen), n)
	}
}
