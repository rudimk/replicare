package redis

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

// clusterIntegration gates the cluster tests on REPLICARE_REDIS_CLUSTER=1 (set by
// `task test:integration:redis:cluster`), which brings up the 3-node cluster
// harness. It is separate from the standalone gate because the cluster harness is
// heavier (host networking) and Linux-only.
func clusterIntegration(t *testing.T) bool {
	t.Helper()
	if os.Getenv("REPLICARE_INTEGRATION") != "1" || os.Getenv("REPLICARE_REDIS_CLUSTER") != "1" {
		t.Skip("Redis cluster integration test; run `task test:integration:redis:cluster`")
		return false
	}
	return true
}

// clusterCfg resolves a cluster ConnConfig (3 seed nodes) through the config block,
// exercising the RM0.5 topology threading + the RM1 mode-aware construction.
func clusterCfg() engine.ConnConfig {
	c := &Conn{Mode: "cluster", Nodes: []string{
		env("RC_REDIS_C1", "127.0.0.1:7000"),
		env("RC_REDIS_C2", "127.0.0.1:7001"),
		env("RC_REDIS_C3", "127.0.0.1:7002"),
	}}
	return c.ConnConfig()
}

// TestClusterConnectAndTopology is the RM1 cluster acceptance: a cluster-mode
// Source connects through go-redis, discovers all three shard masters, and the
// version/fork probe works across the cluster.
func TestClusterConnectAndTopology(t *testing.T) {
	if !clusterIntegration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	src := &Source{cfg: clusterCfg()}
	if err := src.Connect(ctx); err != nil {
		t.Fatalf("connect cluster source: %v", err)
	}
	defer src.Close(context.Background())

	// go-redis discovers topology (CLUSTER SHARDS/SLOTS) and handles MOVED/ASK
	// internally; masters() enumerates the shard masters.
	masters, err := src.db.masters(ctx)
	if err != nil {
		t.Fatalf("discover masters: %v", err)
	}
	if len(masters) != 3 {
		t.Fatalf("discovered %d masters %v, want 3", len(masters), masters)
	}

	sv, err := src.ServerVersion(ctx)
	if err != nil {
		t.Fatalf("cluster server version: %v", err)
	}
	if sv < 70400 || sv >= 70500 {
		t.Errorf("cluster version = %d, want 7.4.x", sv)
	}

	// A key write + read exercises MOVED/ASK resolution across slots transparently.
	if err := src.db.Do(ctx, "SET", "rm1:probe:{tag}", "ok").Err(); err != nil {
		t.Fatalf("SET across cluster: %v", err)
	}
	got, err := src.db.Do(ctx, "GET", "rm1:probe:{tag}").Text()
	if err != nil || got != "ok" {
		t.Errorf("GET across cluster = %q, %v; want ok", got, err)
	}
	_ = src.db.Do(ctx, "DEL", "rm1:probe:{tag}")
}
