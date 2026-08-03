package redis

import (
	"context"
	"fmt"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// TestInitialCopyClusterFanout is the RM4 cluster acceptance: CopyChunk fans SCAN
// out across ALL shard masters. Seeding keys that distribute across slots and
// copying the unit (cluster -> itself, RESTORE REPLACE being idempotent) must load
// EVERY key — a count short of the total would mean a master was missed. Verifying
// full coverage + value fidelity across the 3-node cluster is the per-master-parallel
// guarantee (redis-plan §0.5).
func TestInitialCopyClusterFanout(t *testing.T) {
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

	// Fresh slate on every master, then seed keys that spread across slots.
	if err := src.db.cluster.ForEachMaster(ctx, func(ctx context.Context, c *goredis.Client) error {
		return c.FlushAll(ctx).Err()
	}); err != nil {
		t.Fatalf("flush cluster: %v", err)
	}
	const n = 60
	want := map[string]string{}
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("k:%d", i)
		v := fmt.Sprintf("v-%d-%x", i, []byte{0x00, byte(i), 0xff})
		if err := src.db.uc.Set(ctx, k, v, 0).Err(); err != nil {
			t.Fatalf("seed %s: %v", k, err)
		}
		want[k] = v
	}

	loaded := copyUnit(t, ctx, src, sink)
	if loaded != int64(n) {
		t.Fatalf("copied %d keys, want %d — a shard master was likely missed by the SCAN fan-out", loaded, n)
	}
	for k, v := range want {
		if got, _ := sink.db.uc.Get(ctx, k).Result(); got != v {
			t.Errorf("cluster key %s = %q, want %q", k, got, v)
		}
	}
}
