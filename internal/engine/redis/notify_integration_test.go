package redis

import (
	"context"
	"fmt"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/rudimk/replicare/internal/engine"
)

// notifySrcCfg is the standalone source with the keyspace-notification accelerator
// enabled (rc_notifications=1), as the config block would thread it.
func notifySrcCfg() engine.ConnConfig {
	c := srcCfg()
	c.Params = map[string]string{paramNotifications: "1"}
	return c
}

// flagged reports whether the notifier has recorded key (same-package test access).
func flagged(n *notifier, key string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	_, ok := n.priority[key]
	return ok
}

// eventually polls cond up to d, returning true once it holds.
func eventually(d time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return cond()
}

// TestNotificationPriority is the RM7 ordering acceptance (standalone): with
// notifications on, a key changed after subscription is drained AHEAD of the rolling
// scan — it appears in the very first ReadDirtyKeys batch even though a full scan of
// the (large) keyspace would take many batches to reach it.
func TestNotificationPriority(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	src := &Source{cfg: notifySrcCfg()}
	if err := src.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer src.Close(context.Background())
	sc := src.db.uc
	must(t, sc.FlushDB(ctx).Err())

	// A large pre-existing keyspace: a pure scan would need ~bulk/scanCount batches.
	for i := 0; i < 400; i++ {
		must(t, sc.Set(ctx, fmt.Sprintf("bulk:%d", i), "v", 0).Err())
	}
	if err := src.InstallCapture(ctx, nil); err != nil {
		t.Fatalf("InstallCapture: %v", err)
	}
	if src.notify == nil {
		t.Fatal("notifications on, but notifier not installed")
	}

	// A change made AFTER subscribing fires an event.
	must(t, sc.Set(ctx, "hot:key", "v", 0).Err())
	if !eventually(3*time.Second, func() bool { return flagged(src.notify, "hot:key") }) {
		t.Fatal("keyspace event for hot:key never arrived")
	}

	// The very first drained batch contains the flagged key (priority ahead of scan).
	batch, err := src.ReadDirtyKeys(ctx, unitRef(src.cfg), "dst", 10)
	if err != nil {
		t.Fatalf("ReadDirtyKeys: %v", err)
	}
	found := false
	for _, d := range batch {
		if redisKey(d.Key) == "hot:key" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("hot:key not in the first batch — notification priority not honored")
	}
}

// TestNotificationOffFallback: with notifications disabled, InstallCapture is a
// no-op and streaming falls back cleanly to pure reconciliation (the scan still
// surfaces every present key).
func TestNotificationOffFallback(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	src := &Source{cfg: srcCfg()} // no rc_notifications
	if err := src.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer src.Close(context.Background())
	sc := src.db.uc
	must(t, sc.FlushDB(ctx).Err())
	must(t, sc.Set(ctx, "a", "v", 0).Err())
	must(t, sc.Set(ctx, "b", "v", 0).Err())

	if err := src.InstallCapture(ctx, nil); err != nil {
		t.Fatalf("InstallCapture: %v", err)
	}
	if src.notify != nil {
		t.Fatal("notifications off, but a notifier was installed")
	}
	seen := map[string]bool{}
	ref := unitRef(src.cfg)
	for i := 0; i < 1000; i++ {
		batch, err := src.ReadDirtyKeys(ctx, ref, "dst", 8)
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
	if !seen["a"] || !seen["b"] {
		t.Errorf("pure-reconciliation fallback missed keys: %v", seen)
	}
}

// TestNotificationDropStillConverges: notifications are lossy, so correctness must
// NOT depend on them. Even with the accelerator installed, a full reconciliation
// pass surfaces every present key regardless of which events were delivered —
// modelling a subscriber that dropped events.
func TestNotificationDropStillConverges(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	src := &Source{cfg: notifySrcCfg()}
	if err := src.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer src.Close(context.Background())
	sc := src.db.uc
	must(t, sc.FlushDB(ctx).Err())
	if err := src.InstallCapture(ctx, nil); err != nil {
		t.Fatalf("InstallCapture: %v", err)
	}

	// Write keys, then immediately DROP all notifications by clearing the priority
	// set (simulating a disconnect that lost the events).
	want := map[string]bool{}
	for i := 0; i < 50; i++ {
		k := fmt.Sprintf("k:%d", i)
		must(t, sc.Set(ctx, k, "v", 0).Err())
		want[k] = true
	}
	src.notify.mu.Lock()
	src.notify.priority = map[string]struct{}{} // drop every pending event
	src.notify.mu.Unlock()

	// A full rolling pass must still return every key (scan is the source of truth).
	seen := map[string]bool{}
	ref := unitRef(src.cfg)
	for i := 0; i < 10000; i++ {
		batch, err := src.ReadDirtyKeys(ctx, ref, "dst", 8)
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
	for k := range want {
		if !seen[k] {
			t.Fatalf("dropped-notification key %q not recovered by the scan", k)
		}
	}
}

// TestNotificationClusterPerMaster (cluster): each master is subscribed, so events
// for keys on ALL shards reach the priority set.
func TestNotificationClusterPerMaster(t *testing.T) {
	if !clusterIntegration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cfg := clusterCfg()
	cfg.Params[paramNotifications] = "1"
	src := &Source{cfg: cfg}
	if err := src.Connect(ctx); err != nil {
		t.Fatalf("connect cluster: %v", err)
	}
	defer src.Close(context.Background())
	if err := src.db.cluster.ForEachMaster(ctx, func(ctx context.Context, c *goredis.Client) error {
		return c.FlushAll(ctx).Err()
	}); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := src.InstallCapture(ctx, nil); err != nil {
		t.Fatalf("InstallCapture: %v", err)
	}

	// Keys that distribute across all masters; every one must be flagged by the
	// per-master subscribers.
	keys := make([]string, 0, 60)
	for i := 0; i < 60; i++ {
		k := fmt.Sprintf("k:%c:%d", 'A'+i%13, i)
		must(t, src.db.uc.Set(ctx, k, "v", 0).Err())
		keys = append(keys, k)
	}
	ok := eventually(5*time.Second, func() bool {
		for _, k := range keys {
			if !flagged(src.notify, k) {
				return false
			}
		}
		return true
	})
	if !ok {
		src.notify.mu.Lock()
		got := len(src.notify.priority)
		src.notify.mu.Unlock()
		t.Fatalf("only %d/%d keys flagged — a master's subscription was missed", got, len(keys))
	}
}
