package daemon

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/config"
)

// These tests reproduce the restart/reschedule scenario that previously broke the
// initial copy: the target ALREADY holds rows/keys replicated on an earlier run,
// and the state store has been reset/lost (a fresh node with no copy-progress), so
// the daemon re-copies from scratch INTO a populated target. Two failure modes have
// to stay fixed:
//
//  1. Empty-target direct COPY into a populated table collided on the primary key:
//     copy ...: chunk 0: write side: bulk load ...:
//     ERROR: duplicate key value violates unique constraint "..._pkey" (23505)
//
//  2. A per-chunk DELETE-range (the first, wrong fix) HANGS forever on a populated
//     FK target: copy is parents-first, so deleting a still-referenced parent row
//     either violates the child FK or blocks on locks — the sync never leaves the
//     copy phase, with no error logged (it never returns from Bringup).
//
// The fix is neither: a non-empty target is copied via the idempotent, FK-safe merge
// path (INSERT ... ON CONFLICT DO UPDATE, no deletes). The copy must converge AND
// correct the pre-existing "stale" values to the source's. The Postgres case uses a
// parent+child FK schema specifically to catch mode (2).

// failFastRun starts the daemon and returns a stop func; if Run exits during the
// test (e.g. a bring-up copy error), the test fails immediately with that error
// rather than waiting out the convergence timeout.
func failFastRun(t *testing.T, ctx context.Context, cfg *config.Config) (context.CancelFunc, chan error) {
	t.Helper()
	d, err := New(cfg, nil)
	if err != nil {
		t.Fatalf("new daemon: %v", err)
	}
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- d.Run(runCtx) }()
	return stop, done
}

// TestDaemonCopyIdempotentPrepopulatedTargetPostgres — Postgres, parent+child FK
// schema (the exact shape that hung under the per-chunk-delete fix): fresh state
// store, target already holds customers 1..20 and their orders (with a "stale"
// marker) from a prior run; the copy of source customers 1..30 + orders must
// converge and overwrite the stale rows WITHOUT hanging (the daemon must reach
// streaming). Copy is parents-first, so a delete-based idempotence would try to
// delete a still-referenced customer and stall — this proves the merge path avoids
// that.
func TestDaemonCopyIdempotentPrepopulatedTargetPostgres(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	src := dial(t, ctx, envd("RC_SRC_HOST", "127.0.0.1"), envd("RC_SRC_PORT", "5440"), envd("RC_SRC_DB", "replicare_src"))
	defer src.Close(context.Background())
	tgt := dial(t, ctx, envd("RC_DST_HOST", "127.0.0.1"), envd("RC_DST_PORT", "5441"), envd("RC_DST_DB", "replicare_dst"))
	defer tgt.Close(context.Background())

	ddl := []string{
		"CREATE TABLE rc_it.customers (id int PRIMARY KEY, note text)",
		"CREATE TABLE rc_it.orders (id int PRIMARY KEY, customer_id int NOT NULL REFERENCES rc_it.customers(id), note text)",
	}
	for _, exec := range []func(string){
		func(s string) { mustExec(t, ctx, src, s) },
		func(s string) { mustExec(t, ctx, tgt, s) },
	} {
		exec("DROP SCHEMA IF EXISTS rc_it CASCADE")
		exec("CREATE SCHEMA rc_it")
		for _, d := range ddl {
			exec(d)
		}
	}
	mustExec(t, ctx, src, "DROP SCHEMA IF EXISTS replicare CASCADE") // source capture
	clearPGState(t, ctx)                                             // simulate lost/reset state store
	mustExec(t, ctx, src, "INSERT INTO rc_it.customers SELECT g, 'v'||g FROM generate_series(1,30) g")
	mustExec(t, ctx, src, "INSERT INTO rc_it.orders SELECT g, ((g-1)%30)+1, 'o'||g FROM generate_series(1,30) g")
	// Pre-existing, previously-replicated rows on the target — parent AND child, with a
	// marker value so we can prove the idempotent copy REPLACES them, and so that a
	// parent-delete would be blocked by the referencing child rows (customer_id stays
	// within the 1..20 the target already holds).
	mustExec(t, ctx, tgt, "INSERT INTO rc_it.customers SELECT g, 'stale' FROM generate_series(1,20) g")
	mustExec(t, ctx, tgt, "INSERT INTO rc_it.orders SELECT g, ((g-1)%20)+1, 'stale' FROM generate_series(1,20) g")
	t.Cleanup(func() {
		bg := context.Background()
		_, _ = src.Exec(bg, "DROP SCHEMA IF EXISTS rc_it CASCADE")
		_, _ = src.Exec(bg, "DROP SCHEMA IF EXISTS replicare CASCADE")
		_, _ = tgt.Exec(bg, "DROP SCHEMA IF EXISTS rc_it CASCADE")
	})

	cfg, err := config.Load(writeConfig(t, harnessConfigYAML(`
syncs:
  - name: s1
    source: src
    targets: [dst]
    include: ["rc_it.*"]
    tuning: { drain_interval: 100ms }
`)))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	stop, done := failFastRun(t, ctx, cfg)
	defer stop()

	ok := pollUntil(t, 60*time.Second, func() bool {
		select {
		case err := <-done:
			t.Fatalf("daemon exited during copy into a pre-populated target (want idempotent copy): %v", err)
		default:
		}
		return count(t, ctx, tgt, "rc_it.customers") == 30 &&
			count(t, ctx, tgt, "rc_it.orders") == 30 &&
			count(t, ctx, tgt, "rc_it.customers WHERE note = 'stale'") == 0 &&
			count(t, ctx, tgt, "rc_it.orders WHERE note = 'stale'") == 0
	})
	if !ok {
		t.Fatalf("did not converge: customers=%d (stale %d), orders=%d (stale %d)",
			count(t, ctx, tgt, "rc_it.customers"), count(t, ctx, tgt, "rc_it.customers WHERE note = 'stale'"),
			count(t, ctx, tgt, "rc_it.orders"), count(t, ctx, tgt, "rc_it.orders WHERE note = 'stale'"))
	}
	if got := count(t, ctx, tgt, "rc_it.customers WHERE id = 1 AND note = 'v1'"); got != 1 {
		t.Errorf("customer 1 not corrected to source value: matches=%d, want 1", got)
	}
}

// TestDaemonCopyIdempotentPrepopulatedTargetMySQL — same scenario on MySQL (LOAD
// DATA collides on a duplicate PK just like COPY). Covered by the SAME neutral
// per-chunk delete-then-load fix.
func TestDaemonCopyIdempotentPrepopulatedTargetMySQL(t *testing.T) {
	if !mysqlIntegration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()

	src, tgt := resetMySQLPair(t, ctx, "rc_it",
		"CREATE TABLE rc_it.orders (id INT PRIMARY KEY, note VARCHAR(50)) ENGINE=InnoDB")
	clearPGState(t, ctx)
	myExec(t, ctx, src, "INSERT INTO rc_it.orders SELECT n, CONCAT('v', n) FROM "+seq(1, 30))
	myExec(t, ctx, tgt, "INSERT INTO rc_it.orders SELECT n, 'stale' FROM "+seq(1, 20))

	cfg, err := config.Load(writeConfig(t, mysqlDaemonConfigYAML(`
syncs:
  - name: s1
    source: mysrc
    targets: [mydst]
    include: ["rc_it.*"]
    tuning: { drain_interval: 100ms }
`)))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	stop, done := failFastRun(t, ctx, cfg)
	defer stop()

	ok := pollUntil(t, 60*time.Second, func() bool {
		select {
		case err := <-done:
			t.Fatalf("daemon exited during copy into a pre-populated target (want idempotent copy): %v", err)
		default:
		}
		return myCount(t, ctx, tgt, "rc_it.orders") == 30 &&
			myCount(t, ctx, tgt, "rc_it.orders WHERE note = 'stale'") == 0
	})
	if !ok {
		t.Fatalf("did not converge: target has %d rows, %d still stale",
			myCount(t, ctx, tgt, "rc_it.orders"), myCount(t, ctx, tgt, "rc_it.orders WHERE note = 'stale'"))
	}
	if got := myCount(t, ctx, tgt, "rc_it.orders WHERE id = 1 AND note = 'v1'"); got != 1 {
		t.Errorf("row 1 not corrected to source value: matches=%d, want 1", got)
	}
}

// TestDaemonCopyIdempotentPrepopulatedTargetRedis — Redis is idempotent by
// construction (RESTORE ... REPLACE overwrites), so a restart over a populated
// target already converges. This guards that property: target holds keys 0..19 with
// stale values; the copy of source keys 0..29 must reach 30 and overwrite the stale.
func TestDaemonCopyIdempotentPrepopulatedTargetRedis(t *testing.T) {
	if !redisDaemon(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
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
	for i := 0; i < 20; i++ { // pre-existing, previously-replicated keys with a stale value
		if err := tgt.Set(ctx, fmt.Sprintf("rc:%d", i), "stale", 0).Err(); err != nil {
			t.Fatalf("seed tgt: %v", err)
		}
	}

	cfg, err := config.Load(writeConfig(t, redisDaemonConfigYAML(`
syncs:
  - name: s1
    source: rsrc
    targets: [rdst]
    include: ["rc:*"]
    tuning: { drain_interval: 100ms }
`)))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	stop, done := failFastRun(t, ctx, cfg)
	defer stop()

	ok := pollUntil(t, 60*time.Second, func() bool {
		select {
		case err := <-done:
			t.Fatalf("daemon exited during copy into a pre-populated target (want idempotent copy): %v", err)
		default:
		}
		got, _ := tgt.Get(ctx, "rc:0").Result()
		return rDBSize(t, ctx, tgt) == 30 && got == "v0"
	})
	if !ok {
		t.Fatalf("did not converge: target has %d keys", rDBSize(t, ctx, tgt))
	}
}
