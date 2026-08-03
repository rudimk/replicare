package redis

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/rudimk/replicare/internal/engine"
)

// clusterNode is one master's id + dialable address, from CLUSTER SHARDS.
type clusterNode struct {
	id   string
	addr string
	lo   int
	hi   int
}

// clusterMasters resolves the master nodes (id, addr, slot range) from a live node.
func clusterMasters(t *testing.T, ctx context.Context, cc *goredis.ClusterClient) []clusterNode {
	t.Helper()
	shards, err := cc.ClusterShards(ctx).Result()
	if err != nil {
		t.Fatalf("cluster shards: %v", err)
	}
	var out []clusterNode
	for _, sh := range shards {
		lo, hi := 0, 0
		if len(sh.Slots) >= 1 {
			lo, hi = int(sh.Slots[0].Start), int(sh.Slots[0].End)
		}
		for _, n := range sh.Nodes {
			if n.Role == "master" {
				host := n.Endpoint
				if host == "" {
					host = n.IP
				}
				out = append(out, clusterNode{id: n.ID, addr: fmt.Sprintf("%s:%d", host, n.Port), lo: lo, hi: hi})
			}
		}
	}
	return out
}

// TestClusterReshardingConverges is the RM11 deterministic-resharding hook (a
// best-effort gate, not hard, redis-plan RM11): migrate a single key's slot from
// one master to another, then prove (a) per-key routing follows the MOVE and
// (b) the reconciliation SCAN still covers the moved key — resharding never drops
// a key from replication coverage. On any migration-setup failure it SKIPs rather
// than flaking.
func TestClusterReshardingConverges(t *testing.T) {
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
	cc := src.db.cluster
	if err := cc.ForEachMaster(ctx, func(ctx context.Context, c *goredis.Client) error {
		return c.FlushAll(ctx).Err()
	}); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Seed a background keyspace + the key we will migrate.
	const bg = 40
	for i := 0; i < bg; i++ {
		must(t, cc.Set(ctx, fmt.Sprintf("bg:%d", i), "v", 0).Err())
	}
	const key = "reshard:key"
	must(t, cc.Set(ctx, key, "moved-value", 0).Err())
	slot, err := cc.ClusterKeySlot(ctx, key).Result()
	if err != nil {
		t.Fatalf("keyslot: %v", err)
	}

	masters := clusterMasters(t, ctx, cc)
	if len(masters) < 2 {
		t.Skip("need >=2 masters to reshard")
	}
	var owner, dest clusterNode
	for _, m := range masters {
		if int(slot) >= m.lo && int(slot) <= m.hi {
			owner = m
		}
	}
	for _, m := range masters {
		if m.id != owner.id {
			dest = m
			break
		}
	}
	if owner.id == "" || dest.id == "" {
		t.Skip("could not resolve owner/dest masters for the slot")
	}

	ownerCli := goredis.NewClient(&goredis.Options{Addr: owner.addr})
	destCli := goredis.NewClient(&goredis.Options{Addr: dest.addr})
	defer ownerCli.Close()
	defer destCli.Close()
	destHost, destPort, _ := strings.Cut(dest.addr, ":")

	// The slot-migration dance. Any error here is an environment issue, not a
	// replicare bug → skip.
	steps := []struct {
		cli  *goredis.Client
		args []any
	}{
		{destCli, []any{"CLUSTER", "SETSLOT", slot, "IMPORTING", owner.id}},
		{ownerCli, []any{"CLUSTER", "SETSLOT", slot, "MIGRATING", dest.id}},
		{ownerCli, []any{"MIGRATE", destHost, destPort, key, 0, 5000}},
	}
	for _, s := range steps {
		if err := s.cli.Do(ctx, s.args...).Err(); err != nil {
			t.Skipf("resharding step %v failed (environment): %v", s.args, err)
		}
	}
	// Assign the slot to the destination on every master so the topology agrees.
	for _, m := range masters {
		c := goredis.NewClient(&goredis.Options{Addr: m.addr})
		_ = c.Do(ctx, "CLUSTER", "SETSLOT", slot, "NODE", dest.id).Err()
		_ = c.Close()
	}

	// (a) per-key routing follows the move: the cluster client still reads the key
	// (a MOVED is handled transparently) and it now lives on the destination node.
	if got, err := cc.Get(ctx, key).Result(); err != nil || got != "moved-value" {
		t.Fatalf("post-reshard GET %q = %q err=%v, want moved-value (MOVED not followed)", key, got, err)
	}
	if n, _ := destCli.Exists(ctx, key).Result(); n != 1 {
		t.Errorf("migrated key not on the destination master")
	}

	// (b) reconciliation coverage survives: a fresh Source (re-reads topology) still
	// returns the moved key across a full rolling pass.
	src2 := &Source{cfg: clusterCfg()}
	if err := src2.Connect(ctx); err != nil {
		t.Fatalf("reconnect: %v", err)
	}
	defer src2.Close(context.Background())
	seen := map[string]bool{}
	ref := unitRef(src2.cfg)
	for i := 0; i < 100000; i++ {
		batch, err := src2.ReadDirtyKeys(ctx, ref, "dst", 8)
		if err != nil {
			t.Fatalf("ReadDirtyKeys: %v", err)
		}
		if len(batch) == 0 {
			break
		}
		for _, d := range batch {
			seen[redisKey(d.Key)] = true
		}
	}
	if !seen[key] {
		t.Fatalf("reconciliation dropped the migrated key %q after resharding", key)
	}
	if len(seen) != bg+1 {
		t.Errorf("reconciliation saw %d keys after reshard, want %d", len(seen), bg+1)
	}

	// And the master-pinned delete detector agrees the key is present (not missing).
	missing, err := src2.MissingAtSource(ctx, ref, []engine.KeyValues{{key}})
	if err != nil {
		t.Fatalf("MissingAtSource: %v", err)
	}
	if len(missing) != 0 {
		t.Errorf("moved key wrongly reported missing at source (%v)", missing)
	}
}
