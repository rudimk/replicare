package redis

import (
	"context"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/pipeline"
)

// sweepToCompletion drives the neutral delete-reconciliation step (the exact daemon
// path) over the unit until the rolling target scan finishes one full pass
// (next==0). Returns the total keys deleted. A guard bounds the loop.
func sweepToCompletion(t *testing.T, ctx context.Context, src *Source, sink *Sink) int {
	t.Helper()
	ref := unitRef(src.cfg)
	total, cursor := 0, uint64(0)
	for i := 0; i < 100000; i++ {
		n, next, err := pipeline.DeleteSweepStep(ctx, sink, src, sink, ref, cursor, 64)
		if err != nil {
			t.Fatalf("DeleteSweepStep: %v", err)
		}
		total += n
		cursor = next
		if next == 0 {
			return total
		}
	}
	t.Fatal("delete sweep did not complete within the guard")
	return total
}

// TestDeleteConvergence is the RM6 keystone (6.2 -> 7.4 standalone): a key deleted
// (or expired) at the source is DELETEd on the target within one sweep — the durable
// target-vs-source diff. Every sweep is a full diff, so it subsumes the copy-window
// orphan and restart cases with no special-casing. Present keys are untouched.
func TestDeleteConvergence(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	src, sink := mustConnectPair(t, ctx, srcCfg(), tgtCfg())
	sc, tc := src.db.uc, sink.db.uc
	must(t, sc.FlushDB(ctx).Err())
	must(t, tc.FlushDB(ctx).Err())

	must(t, sc.Set(ctx, "keep", "v", 0).Err())
	must(t, sc.Set(ctx, "goner", "v", 0).Err())
	must(t, sc.Set(ctx, "expiry", "v", 0).Err())
	copyUnit(t, ctx, src, sink) // target now has keep, goner, expiry

	// Source-side deletes AFTER the copy: a plain DEL and an expiry.
	must(t, sc.Del(ctx, "goner").Err())
	must(t, sc.PExpire(ctx, "expiry", 20*time.Millisecond).Err())
	time.Sleep(80 * time.Millisecond) // let "expiry" lapse out of the source

	deleted := sweepToCompletion(t, ctx, src, sink)
	if deleted != 2 {
		t.Errorf("swept %d deletes, want 2 (goner + expiry)", deleted)
	}
	if n, _ := tc.Exists(ctx, "goner").Result(); n != 0 {
		t.Errorf("deleted-at-source key still on target")
	}
	if n, _ := tc.Exists(ctx, "expiry").Result(); n != 0 {
		t.Errorf("expired-at-source key still on target")
	}
	if got, _ := tc.Get(ctx, "keep").Result(); got != "v" {
		t.Errorf("present key wrongly deleted: keep = %q", got)
	}

	// Idempotent: a second full sweep deletes nothing more.
	if again := sweepToCompletion(t, ctx, src, sink); again != 0 {
		t.Errorf("second sweep deleted %d, want 0 (idempotent full diff)", again)
	}
}

// TestDeleteSweepRespectsSelection: a target key OUTSIDE the sync selection is NEVER
// deleted, even though it is absent at the source — the sweep only manages keys the
// user selected for replication.
func TestDeleteSweepRespectsSelection(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	src, sink := mustConnectPair(t, ctx, srcCfg(), tgtCfg())
	sc, tc := src.db.uc, sink.db.uc
	must(t, sc.FlushDB(ctx).Err())
	must(t, tc.FlushDB(ctx).Err())

	// Compile a selection into both sides (as the daemon does via Introspect).
	sel := engine.Selection{Include: []string{"keep:*"}}
	if _, err := src.Introspect(ctx, sel); err != nil {
		t.Fatalf("source introspect: %v", err)
	}
	if _, err := sink.Introspect(ctx, sel); err != nil {
		t.Fatalf("sink introspect: %v", err)
	}

	// Target holds a selected key absent at source (should be deleted) and an
	// unselected key absent at source (must be left alone).
	must(t, tc.Set(ctx, "keep:gone", "v", 0).Err())
	must(t, tc.Set(ctx, "other:mine", "v", 0).Err())

	deleted := sweepToCompletion(t, ctx, src, sink)
	if deleted != 1 {
		t.Errorf("swept %d, want 1 (only the selected key)", deleted)
	}
	if n, _ := tc.Exists(ctx, "keep:gone").Result(); n != 0 {
		t.Errorf("selected absent key not deleted")
	}
	if n, _ := tc.Exists(ctx, "other:mine").Result(); n != 1 {
		t.Errorf("UNSELECTED key was deleted — the sweep must never touch keys outside the selection")
	}
}

// TestDeleteSweepClusterPerMaster is the RM6 cluster acceptance: both halves of the
// delete diff cover ALL shard masters. The harness has a single cluster, so rather
// than source!=target deletion (which would delete on both), it verifies the two
// building blocks per-master: ScanTargetKeys enumerates every key across all
// masters, and MissingAtSource (master-pinned EXISTS, routed per slot) correctly
// separates present from absent keys spanning all shards. A missed master in either
// would corrupt the diff.
func TestDeleteSweepClusterPerMaster(t *testing.T) {
	if !clusterIntegration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	src := &Source{cfg: clusterCfg()}
	if err := src.Connect(ctx); err != nil {
		t.Fatalf("connect cluster source: %v", err)
	}
	defer src.Close(context.Background())
	sink := &Sink{cfg: clusterCfg()}
	if err := sink.Connect(ctx); err != nil {
		t.Fatalf("connect cluster sink: %v", err)
	}
	defer sink.Close(context.Background())

	if err := src.db.cluster.ForEachMaster(ctx, func(ctx context.Context, c *goredis.Client) error {
		return c.FlushAll(ctx).Err()
	}); err != nil {
		t.Fatalf("flush cluster: %v", err)
	}
	const n = 60
	present := make([]engine.KeyValues, 0, n)
	for i := 0; i < n; i++ {
		k := slotKey(i)
		must(t, src.db.uc.Set(ctx, k, "v", 0).Err())
		present = append(present, engine.KeyValues{k})
	}

	// ScanTargetKeys must enumerate every key across all 3 masters.
	ref := unitRef(sink.cfg)
	seen := map[string]bool{}
	cursor := uint64(0)
	for i := 0; i < 100000; i++ {
		keys, next, err := sink.ScanTargetKeys(ctx, ref, cursor, 7)
		if err != nil {
			t.Fatalf("ScanTargetKeys: %v", err)
		}
		for _, kv := range keys {
			seen[redisKey(kv)] = true
		}
		cursor = next
		if next == 0 {
			break
		}
	}
	if len(seen) != n {
		t.Fatalf("ScanTargetKeys saw %d keys across the cluster, want %d — a master was missed", len(seen), n)
	}

	// MissingAtSource over present + fabricated-absent keys spanning slots: only the
	// fabricated ones come back missing (EXISTS routed to the right master per slot).
	absent := make([]engine.KeyValues, 0, n)
	query := append([]engine.KeyValues{}, present...)
	for i := 0; i < n; i++ {
		k := slotKey(i) + ":ghost"
		absent = append(absent, engine.KeyValues{k})
		query = append(query, engine.KeyValues{k})
	}
	missing, err := src.MissingAtSource(ctx, ref, query)
	if err != nil {
		t.Fatalf("MissingAtSource: %v", err)
	}
	if len(missing) != len(absent) {
		t.Fatalf("MissingAtSource found %d missing, want %d (the ghost keys only)", len(missing), len(absent))
	}
	got := map[string]bool{}
	for _, kv := range missing {
		got[redisKey(kv)] = true
	}
	for _, kv := range absent {
		if !got[redisKey(kv)] {
			t.Errorf("absent key %q not reported missing", redisKey(kv))
		}
	}
}

func slotKey(i int) string {
	return "s:" + string(rune('A'+i%13)) + ":" + itoaSimple(i)
}

func itoaSimple(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}
