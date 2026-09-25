package daemon

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/config"
)

// TestDaemonPausedSyncDoesNotRun proves the per-sync `enabled: false` pause flag is
// per-pipeline: with two syncs sharing one source/target pair, the enabled sync is
// brought up and copies its schema to the target while the paused sync is skipped
// entirely — its target schema stays empty and its source capture is never installed.
func TestDaemonPausedSyncDoesNotRun(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	src := dial(t, ctx, envd("RC_SRC_HOST", "127.0.0.1"), envd("RC_SRC_PORT", "5440"), envd("RC_SRC_DB", "replicare_src"))
	defer src.Close(context.Background())
	tgt := dial(t, ctx, envd("RC_DST_HOST", "127.0.0.1"), envd("RC_DST_PORT", "5441"), envd("RC_DST_DB", "replicare_dst"))
	defer tgt.Close(context.Background())

	// Two independent schemas: rc_on is replicated by the enabled sync, rc_off by the
	// paused one. Both are seeded on the source; only rc_on should reach the target.
	for _, exec := range []func(string){
		func(s string) { mustExec(t, ctx, src, s) },
		func(s string) { mustExec(t, ctx, tgt, s) },
	} {
		for _, sc := range []string{"rc_on", "rc_off"} {
			exec("DROP SCHEMA IF EXISTS " + sc + " CASCADE")
			exec("CREATE SCHEMA " + sc)
			exec("CREATE TABLE " + sc + ".orders (id int PRIMARY KEY, note text)")
		}
	}
	mustExec(t, ctx, src, "DROP SCHEMA IF EXISTS replicare CASCADE") // source capture
	clearPGState(t, ctx)
	mustExec(t, ctx, src, "INSERT INTO rc_on.orders SELECT g, 'v'||g FROM generate_series(1,30) g")
	mustExec(t, ctx, src, "INSERT INTO rc_off.orders SELECT g, 'v'||g FROM generate_series(1,30) g")
	t.Cleanup(func() {
		bg := context.Background()
		for _, sc := range []string{"rc_on", "rc_off"} {
			_, _ = src.Exec(bg, "DROP SCHEMA IF EXISTS "+sc+" CASCADE")
			_, _ = tgt.Exec(bg, "DROP SCHEMA IF EXISTS "+sc+" CASCADE")
		}
		_, _ = src.Exec(bg, "DROP SCHEMA IF EXISTS replicare CASCADE")
	})

	cfg, err := config.Load(writeConfig(t, harnessConfigYAML(`
syncs:
  - name: on
    source: src
    targets: [dst]
    include: ["rc_on.*"]
    tuning: { drain_interval: 100ms }
  - name: off
    source: src
    targets: [dst]
    include: ["rc_off.*"]
    enabled: false
    tuning: { drain_interval: 100ms }
`)))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	stop, done := failFastRun(t, ctx, cfg)
	defer stop()

	// The enabled sync copies rc_on to the target.
	ok := pollUntil(t, 60*time.Second, func() bool {
		select {
		case err := <-done:
			t.Fatalf("daemon exited unexpectedly: %v", err)
		default:
		}
		return count(t, ctx, tgt, "rc_on.orders") == 30
	})
	if !ok {
		t.Fatalf("enabled sync did not copy: target rc_on.orders = %d, want 30", count(t, ctx, tgt, "rc_on.orders"))
	}

	// The paused sync must never have run: its target schema is untouched, and it
	// installed no capture on the source. Give it a beat to be sure it's not just slow.
	time.Sleep(2 * time.Second)
	if got := count(t, ctx, tgt, "rc_off.orders"); got != 0 {
		t.Errorf("paused sync wrote to target: rc_off.orders = %d, want 0", got)
	}
	// Capture for the paused sync's table would live in the source `replicare` schema;
	// the enabled sync creates that schema, so assert specifically that no delta/track
	// table exists for rc_off.orders.
	if n := count(t, ctx, src,
		"information_schema.tables WHERE table_schema = 'replicare' AND table_name LIKE '%rc_off%'"); n != 0 {
		t.Errorf("paused sync installed capture: %d replicare tables match rc_off", n)
	}
}

// TestDaemonPausedRedisSyncDoesNotRun proves the enabled:false pause gate works for a
// REDIS sync too (not just Postgres): with the sole redis sync paused, the daemon must
// not copy any keys to the target. Gated on REPLICARE_REDIS=1.
func TestDaemonPausedRedisSyncDoesNotRun(t *testing.T) {
	if !redisDaemon(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	src := rDial(t, envd("RC_REDIS_SRC_HOST", "127.0.0.1"), envd("RC_REDIS_SRC_PORT", "6390"))
	tgt := rDial(t, envd("RC_REDIS_DST_HOST", "127.0.0.1"), envd("RC_REDIS_DST_PORT", "6391"))
	if err := src.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flush src: %v", err)
	}
	if err := tgt.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flush tgt: %v", err)
	}
	clearPGState(t, ctx)
	for i := 0; i < 30; i++ {
		if err := src.Set(ctx, fmt.Sprintf("rc:%d", i), fmt.Sprintf("v%d", i), 0).Err(); err != nil {
			t.Fatalf("seed src: %v", err)
		}
	}

	cfg, err := config.Load(writeConfig(t, redisDaemonConfigYAML(`
syncs:
  - name: s1
    source: rsrc
    targets: [rdst]
    include: ["rc:*"]
    enabled: false
    tuning: { drain_interval: 100ms }
`)))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	d, err := New(cfg, nil)
	if err != nil {
		t.Fatalf("new daemon: %v", err)
	}
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- d.Run(runCtx) }()

	// Give the daemon time to (not) do anything, then assert the target is untouched.
	time.Sleep(3 * time.Second)
	if n := rDBSize(t, ctx, tgt); n != 0 {
		t.Errorf("paused redis sync copied to target: dbsize=%d, want 0", n)
	}
	stop()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("daemon did not shut down")
	}
}

// TestDaemonAllSyncsPausedStaysUp is the regression for the CrashLoop bug: when every
// sync is paused, Run must NOT return (which would exit the process and, under a
// Kubernetes Deployment, restart the container in a crash loop). It must idle until
// ctx is cancelled so the pod stays up serving observability, then exit cleanly.
func TestDaemonAllSyncsPausedStaysUp(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	clearPGState(t, ctx)

	cfg, err := config.Load(writeConfig(t, harnessConfigYAML(`
syncs:
  - name: a
    source: src
    targets: [dst]
    include: ["rc_it.*"]
    enabled: false
  - name: b
    source: src
    targets: [dst]
    include: ["rc_it.*"]
    enabled: false
`)))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	d, err := New(cfg, nil)
	if err != nil {
		t.Fatalf("new daemon: %v", err)
	}
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- d.Run(runCtx) }()

	// It must still be running after a beat — an idle daemon, not an exited one.
	select {
	case err := <-done:
		t.Fatalf("daemon exited with all syncs paused (want it to idle, not crash-loop): %v", err)
	case <-time.After(2 * time.Second):
	}

	// Cancelling ctx (the SIGTERM analogue) must return cleanly.
	stop()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned error on shutdown: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("daemon did not shut down within 10s of ctx cancel")
	}
}
