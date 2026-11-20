package daemon

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/rudimk/replicare/internal/config"
	_ "github.com/rudimk/replicare/internal/engine/postgres" // register the engine
)

func integration(t *testing.T) bool {
	t.Helper()
	if os.Getenv("REPLICARE_INTEGRATION") != "1" {
		t.Skip("integration test; set REPLICARE_INTEGRATION=1 and run `task harness:up`")
		return false
	}
	return true
}

func envd(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// harnessConfigYAML writes a config pointing source, target, and state store at
// the harness (old source :5440, modern target :5441; state store lives on the
// target DB). drainInterval is short so the test converges quickly.
func harnessConfigYAML(syncs string) string {
	return fmt.Sprintf(`
logging: { level: warn, format: text }
state_store:
  engine: postgres
  postgres: { host: %s, port: %s, database: %s, user: %s, password: %s, sslmode: disable }
sources:
  src:
    engine: postgres
    postgres: { host: %s, port: %s, database: %s, user: %s, password: %s, sslmode: disable }
targets:
  dst:
    engine: postgres
    postgres: { host: %s, port: %s, database: %s, user: %s, password: %s, sslmode: disable }
%s`,
		envd("RC_DST_HOST", "127.0.0.1"), envd("RC_DST_PORT", "5441"), envd("RC_DST_DB", "replicare_dst"), envd("RC_USER", "postgres"), envd("RC_PASSWORD", "postgres"),
		envd("RC_SRC_HOST", "127.0.0.1"), envd("RC_SRC_PORT", "5440"), envd("RC_SRC_DB", "replicare_src"), envd("RC_USER", "postgres"), envd("RC_PASSWORD", "postgres"),
		envd("RC_DST_HOST", "127.0.0.1"), envd("RC_DST_PORT", "5441"), envd("RC_DST_DB", "replicare_dst"), envd("RC_USER", "postgres"), envd("RC_PASSWORD", "postgres"),
		syncs)
}

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "replicare.yml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

func dial(t *testing.T, ctx context.Context, host, port, db string) *pgx.Conn {
	t.Helper()
	c, err := pgx.Connect(ctx, fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		envd("RC_USER", "postgres"), envd("RC_PASSWORD", "postgres"), host, port, db))
	if err != nil {
		t.Fatalf("dial %s:%s/%s: %v", host, port, db, err)
	}
	return c
}

func mustExec(t *testing.T, ctx context.Context, c *pgx.Conn, sql string) {
	t.Helper()
	if _, err := c.Exec(ctx, sql); err != nil {
		t.Fatalf("exec: %v\nSQL: %s", err, sql)
	}
}

func count(t *testing.T, ctx context.Context, c *pgx.Conn, q string) int {
	t.Helper()
	var n int
	if err := c.QueryRow(ctx, "SELECT count(*) FROM "+q).Scan(&n); err != nil {
		return -1 // table may not exist yet during bring-up
	}
	return n
}

func pollUntil(t *testing.T, timeout time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

// The initial-copy and streaming-convergence polls in these daemon integration tests
// are ORACLES driven by the daemon's continuous drain loop, so their durations are
// generous CEILINGS, not expected times — convergence normally happens in well under a
// second. Under the serial (-p 1) integration suite on a CPU-shared CI runner a
// late-running test can occasionally need far longer than a local run, which surfaced
// as flaky timeouts (e.g. TestDaemonFanOutTwoTargets "did not converge on both
// targets" at the old 40s ceiling). Set the ceilings high so load-induced slowness is
// tolerated; a genuine non-convergence still fails, just later. Mirrors the mesh-test
// hardening in cluster_test.go (#66).
const (
	copyTimeout      = 60 * time.Second  // initial-copy completion polls
	convergeTimeout  = 90 * time.Second  // streaming-convergence polls
	daemonTestBudget = 300 * time.Second // per-test context budget
)

// TestDaemonRunConvergesAndStopsCleanly exercises the whole process path: load a
// config, run the daemon, and — with the source mutated while it streams —
// converge the target, then a cancellation stops it cleanly (graceful shutdown).
func TestDaemonRunConvergesAndStopsCleanly(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), daemonTestBudget)
	defer cancel()

	src := dial(t, ctx, envd("RC_SRC_HOST", "127.0.0.1"), envd("RC_SRC_PORT", "5440"), envd("RC_SRC_DB", "replicare_src"))
	defer src.Close(context.Background())
	tgt := dial(t, ctx, envd("RC_DST_HOST", "127.0.0.1"), envd("RC_DST_PORT", "5441"), envd("RC_DST_DB", "replicare_dst"))
	defer tgt.Close(context.Background())

	// Clean slate: schema on both, seed the source, clear capture + state.
	ddl := "CREATE TABLE rc_it.orders (id int PRIMARY KEY, note text)"
	for _, c := range []*pgx.Conn{src, tgt} {
		mustExec(t, ctx, c, "DROP SCHEMA IF EXISTS rc_it CASCADE")
		mustExec(t, ctx, c, "CREATE SCHEMA rc_it")
		mustExec(t, ctx, c, ddl)
	}
	mustExec(t, ctx, src, "DROP SCHEMA IF EXISTS replicare CASCADE")
	mustExec(t, ctx, tgt, "DROP SCHEMA IF EXISTS replicare_state CASCADE")
	mustExec(t, ctx, src, "INSERT INTO rc_it.orders SELECT g, 'v'||g FROM generate_series(1,30) g")
	t.Cleanup(func() {
		bg := context.Background()
		_, _ = src.Exec(bg, "DROP SCHEMA IF EXISTS rc_it CASCADE")
		_, _ = src.Exec(bg, "DROP SCHEMA IF EXISTS replicare CASCADE")
		_, _ = tgt.Exec(bg, "DROP SCHEMA IF EXISTS rc_it CASCADE")
		_, _ = tgt.Exec(bg, "DROP SCHEMA IF EXISTS replicare_state CASCADE")
	})

	syncBlock := `
syncs:
  - name: s1
    source: src
    targets: [dst]
    include: ["rc_it.*"]
    tuning: { drain_interval: 100ms }
`
	cfg, err := config.Load(writeConfig(t, harnessConfigYAML(syncBlock)))
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

	// Initial copy converges.
	if !pollUntil(t, copyTimeout, func() bool { return count(t, ctx, tgt, "rc_it.orders") == 30 }) {
		t.Fatalf("initial copy did not converge: target has %d rows", count(t, ctx, tgt, "rc_it.orders"))
	}

	// Streaming reconciles a live mutation.
	mustExec(t, ctx, src, "INSERT INTO rc_it.orders VALUES (31, 'v31')")
	mustExec(t, ctx, src, "UPDATE rc_it.orders SET note = 'v1-updated' WHERE id = 1")
	mustExec(t, ctx, src, "DELETE FROM rc_it.orders WHERE id = 2")
	converged := pollUntil(t, convergeTimeout, func() bool {
		return count(t, ctx, tgt, "rc_it.orders") == 30 &&
			count(t, ctx, tgt, "rc_it.orders WHERE id = 31") == 1 &&
			count(t, ctx, tgt, "rc_it.orders WHERE id = 2") == 0
	})
	if !converged {
		t.Fatalf("streaming did not converge on mutation")
	}

	// Graceful shutdown: cancel and expect a clean return.
	stop()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("daemon Run returned %v, want nil on graceful shutdown", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("daemon did not stop within 15s of cancellation")
	}
}

// TestDaemonFanOutTwoTargets is the MM2 acceptance: one source fans out to TWO
// targets, each with its own independent initial-copy progress (keyed per
// (sync, target, table)), and both converge from cold copy and stay converged
// under a live mutation. Before MM2 the two targets shared one copy_progress row
// and collided; this proves they no longer do.
func TestDaemonFanOutTwoTargets(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), daemonTestBudget)
	defer cancel()

	src := dial(t, ctx, envd("RC_SRC_HOST", "127.0.0.1"), envd("RC_SRC_PORT", "5440"), envd("RC_SRC_DB", "replicare_src"))
	defer src.Close(context.Background())
	tgt1 := dial(t, ctx, envd("RC_DST_HOST", "127.0.0.1"), envd("RC_DST_PORT", "5441"), envd("RC_DST_DB", "replicare_dst"))
	defer tgt1.Close(context.Background())

	// A second target DATABASE on the same target server (fan-out needs two
	// distinct targets). CREATE/DROP DATABASE run outside a txn via autocommit.
	const dst2DB = "replicare_dst2"
	mustExec(t, ctx, tgt1, "DROP DATABASE IF EXISTS "+dst2DB+" WITH (FORCE)")
	mustExec(t, ctx, tgt1, "CREATE DATABASE "+dst2DB)
	tgt2 := dial(t, ctx, envd("RC_DST_HOST", "127.0.0.1"), envd("RC_DST_PORT", "5441"), dst2DB)

	ddl := "CREATE TABLE rc_it.orders (id int PRIMARY KEY, note text)"
	for _, c := range []*pgx.Conn{src, tgt1, tgt2} {
		mustExec(t, ctx, c, "DROP SCHEMA IF EXISTS rc_it CASCADE")
		mustExec(t, ctx, c, "CREATE SCHEMA rc_it")
		mustExec(t, ctx, c, ddl)
	}
	mustExec(t, ctx, src, "DROP SCHEMA IF EXISTS replicare CASCADE")
	mustExec(t, ctx, tgt1, "DROP SCHEMA IF EXISTS replicare_state CASCADE")
	mustExec(t, ctx, src, "INSERT INTO rc_it.orders SELECT g, 'v'||g FROM generate_series(1,30) g")
	t.Cleanup(func() {
		bg := context.Background()
		_, _ = src.Exec(bg, "DROP SCHEMA IF EXISTS rc_it CASCADE")
		_, _ = src.Exec(bg, "DROP SCHEMA IF EXISTS replicare CASCADE")
		_, _ = tgt1.Exec(bg, "DROP SCHEMA IF EXISTS rc_it CASCADE")
		_, _ = tgt1.Exec(bg, "DROP SCHEMA IF EXISTS replicare_state CASCADE")
		_ = tgt2.Close(bg)
		_, _ = tgt1.Exec(bg, "DROP DATABASE IF EXISTS "+dst2DB+" WITH (FORCE)")
	})

	cfgYAML := fmt.Sprintf(`
logging: { level: warn, format: text }
state_store:
  engine: postgres
  postgres: { host: %[1]s, port: %[2]s, database: %[3]s, user: %[4]s, password: %[5]s, sslmode: disable }
sources:
  src:
    engine: postgres
    postgres: { host: %[6]s, port: %[7]s, database: %[8]s, user: %[4]s, password: %[5]s, sslmode: disable }
targets:
  dst1:
    engine: postgres
    postgres: { host: %[1]s, port: %[2]s, database: %[3]s, user: %[4]s, password: %[5]s, sslmode: disable }
  dst2:
    engine: postgres
    postgres: { host: %[1]s, port: %[2]s, database: %[9]s, user: %[4]s, password: %[5]s, sslmode: disable }
syncs:
  - name: fan
    source: src
    targets: [dst1, dst2]
    include: ["rc_it.*"]
    tuning: { drain_interval: 100ms }
`,
		envd("RC_DST_HOST", "127.0.0.1"), envd("RC_DST_PORT", "5441"), envd("RC_DST_DB", "replicare_dst"),
		envd("RC_USER", "postgres"), envd("RC_PASSWORD", "postgres"),
		envd("RC_SRC_HOST", "127.0.0.1"), envd("RC_SRC_PORT", "5440"), envd("RC_SRC_DB", "replicare_src"),
		dst2DB)

	cfg, err := config.Load(writeConfig(t, cfgYAML))
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

	// Both targets converge from cold copy — independently.
	if !pollUntil(t, copyTimeout, func() bool {
		return count(t, ctx, tgt1, "rc_it.orders") == 30 && count(t, ctx, tgt2, "rc_it.orders") == 30
	}) {
		t.Fatalf("fan-out initial copy did not converge: dst1=%d dst2=%d",
			count(t, ctx, tgt1, "rc_it.orders"), count(t, ctx, tgt2, "rc_it.orders"))
	}

	// Per-target copy progress is independent: two rows (dst1, dst2) for the one
	// table, keyed by target (the MM2 fix). State store lives on dst1's DB.
	//
	// Poll rather than instant-check: the convergence poll above waits on the target
	// ROW COUNTS, but each target's copy_progress "done" row commits in a separate
	// transaction from its data, so the second target's progress row can land a beat
	// after both targets show 30 rows. Instant-checking here raced that write and
	// intermittently saw 1. Polling for 2 proves both rows do land (a genuinely lost
	// row never reaches 2, so this still fails loud on a real regression).
	progRows := func() int {
		var n int
		if err := tgt1.QueryRow(ctx,
			"SELECT count(*) FROM replicare_state.copy_progress WHERE sync='fan' AND table_name='orders'").Scan(&n); err != nil {
			return -1
		}
		return n
	}
	if !pollUntil(t, copyTimeout, func() bool { return progRows() == 2 }) {
		t.Errorf("copy_progress rows for orders = %d, want 2 (one per target)", progRows())
	}

	// A live mutation streams to BOTH targets.
	mustExec(t, ctx, src, "INSERT INTO rc_it.orders VALUES (31, 'v31')")
	mustExec(t, ctx, src, "DELETE FROM rc_it.orders WHERE id = 2")
	if !pollUntil(t, convergeTimeout, func() bool {
		return count(t, ctx, tgt1, "rc_it.orders") == 30 && count(t, ctx, tgt2, "rc_it.orders") == 30 &&
			count(t, ctx, tgt1, "rc_it.orders WHERE id = 31") == 1 && count(t, ctx, tgt2, "rc_it.orders WHERE id = 31") == 1 &&
			count(t, ctx, tgt1, "rc_it.orders WHERE id = 2") == 0 && count(t, ctx, tgt2, "rc_it.orders WHERE id = 2") == 0
	}) {
		dumpFanOutState(t, ctx, src, map[string]*pgx.Conn{"dst1": tgt1, "dst2": tgt2})
		t.Fatalf("streaming did not converge on both targets")
	}

	stop()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("daemon Run returned %v, want nil on graceful shutdown", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("daemon did not stop within 15s of cancellation")
	}
}

// dumpFanOutState logs, on a fan-out convergence failure, which target is missing
// which change (id 31 present, id 2 gone) and the source-side delta and track rows
// for rc_it.orders at that moment, so a CI failure is diagnosable from the log
// alone (the cleanup drops the schemas right after).
func dumpFanOutState(t *testing.T, ctx context.Context, src *pgx.Conn, tgts map[string]*pgx.Conn) {
	t.Helper()
	for _, name := range []string{"dst1", "dst2"} {
		c := tgts[name]
		t.Logf("%s: rows=%d id31=%d id2=%d", name,
			count(t, ctx, c, "rc_it.orders"),
			count(t, ctx, c, "rc_it.orders WHERE id = 31"),
			count(t, ctx, c, "rc_it.orders WHERE id = 2"))
	}
	var relID int
	if err := src.QueryRow(ctx,
		"SELECT rel_id FROM replicare.captured WHERE schema_name = 'rc_it' AND table_name = 'orders'").Scan(&relID); err != nil {
		t.Logf("source: no capture row for rc_it.orders: %v", err)
		return
	}
	dump := func(label, q string) {
		rows, err := src.Query(ctx, q)
		if err != nil {
			t.Logf("source %s: %v", label, err)
			return
		}
		defer rows.Close()
		n := 0
		for rows.Next() {
			vals, err := rows.Values()
			if err != nil {
				t.Logf("source %s: %v", label, err)
				return
			}
			t.Logf("source %s: %v", label, vals)
			n++
		}
		t.Logf("source %s: %d rows", label, n)
	}
	dump("delta", fmt.Sprintf("SELECT delta_id, rc_op, k1::text FROM replicare.delta_%d ORDER BY delta_id", relID))
	dump("track", fmt.Sprintf("SELECT target, delta_id FROM replicare.track_%d ORDER BY target, delta_id", relID))
}

// TestDaemonTwoConcurrentSyncs is the M7 concurrency acceptance: one daemon runs
// two independent syncs at once, each converging its own schema.
func TestDaemonTwoConcurrentSyncs(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), daemonTestBudget)
	defer cancel()

	src := dial(t, ctx, envd("RC_SRC_HOST", "127.0.0.1"), envd("RC_SRC_PORT", "5440"), envd("RC_SRC_DB", "replicare_src"))
	defer src.Close(context.Background())
	tgt := dial(t, ctx, envd("RC_DST_HOST", "127.0.0.1"), envd("RC_DST_PORT", "5441"), envd("RC_DST_DB", "replicare_dst"))
	defer tgt.Close(context.Background())

	for _, s := range []string{"rc_a", "rc_b"} {
		for _, c := range []*pgx.Conn{src, tgt} {
			mustExec(t, ctx, c, "DROP SCHEMA IF EXISTS "+s+" CASCADE")
			mustExec(t, ctx, c, "CREATE SCHEMA "+s)
			mustExec(t, ctx, c, "CREATE TABLE "+s+".t (id int primary key, v text)")
		}
	}
	mustExec(t, ctx, src, "DROP SCHEMA IF EXISTS replicare CASCADE")
	mustExec(t, ctx, tgt, "DROP SCHEMA IF EXISTS replicare_state CASCADE")
	mustExec(t, ctx, src, "INSERT INTO rc_a.t SELECT g, 'a'||g FROM generate_series(1,15) g")
	mustExec(t, ctx, src, "INSERT INTO rc_b.t SELECT g, 'b'||g FROM generate_series(1,25) g")
	t.Cleanup(func() {
		bg := context.Background()
		for _, s := range []string{"rc_a", "rc_b"} {
			_, _ = src.Exec(bg, "DROP SCHEMA IF EXISTS "+s+" CASCADE")
			_, _ = tgt.Exec(bg, "DROP SCHEMA IF EXISTS "+s+" CASCADE")
		}
		_, _ = src.Exec(bg, "DROP SCHEMA IF EXISTS replicare CASCADE")
		_, _ = tgt.Exec(bg, "DROP SCHEMA IF EXISTS replicare_state CASCADE")
	})

	syncBlock := `
syncs:
  - { name: sa, source: src, targets: [dst], include: ["rc_a.*"], tuning: { drain_interval: 100ms } }
  - { name: sb, source: src, targets: [dst], include: ["rc_b.*"], tuning: { drain_interval: 100ms } }
`
	cfg, err := config.Load(writeConfig(t, harnessConfigYAML(syncBlock)))
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

	if !pollUntil(t, convergeTimeout, func() bool {
		return count(t, ctx, tgt, "rc_a.t") == 15 && count(t, ctx, tgt, "rc_b.t") == 25
	}) {
		t.Fatalf("two syncs did not both converge: rc_a=%d rc_b=%d", count(t, ctx, tgt, "rc_a.t"), count(t, ctx, tgt, "rc_b.t"))
	}
	stop()
	<-done
}

// TestDaemonCyclicCopyConvergesUnderConcurrentWrites reproduces the production
// crash-loop end-to-end: a CYCLIC FK component (hub with a nullable self-ref, plus a
// child with a NOT NULL FK to hub) copied from a LIVE source that keeps gaining rows.
// A concurrent writer inserts fresh hub rows AND child rows referencing them while the
// initial copy runs, so a child snapshot can reference a hub row not yet in the hub's
// earlier snapshot — a transient FK violation (SQLSTATE 23503). Before the fix the
// cyclic copy aborted on that and the daemon exited (crash-loop); now the copy retries
// (re-copying parents) and converges. The load-harness fixtures never covered this —
// they seed a STATIC source, so the skew window never opens.
func TestDaemonCyclicCopyConvergesUnderConcurrentWrites(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	src := dial(t, ctx, envd("RC_SRC_HOST", "127.0.0.1"), envd("RC_SRC_PORT", "5440"), envd("RC_SRC_DB", "replicare_src"))
	defer src.Close(context.Background())
	tgt := dial(t, ctx, envd("RC_DST_HOST", "127.0.0.1"), envd("RC_DST_PORT", "5441"), envd("RC_DST_DB", "replicare_dst"))
	defer tgt.Close(context.Background())
	writer := dial(t, ctx, envd("RC_SRC_HOST", "127.0.0.1"), envd("RC_SRC_PORT", "5440"), envd("RC_SRC_DB", "replicare_src"))
	defer writer.Close(context.Background())

	// hub is cyclic via a nullable self-ref (routes the component through the cyclic
	// null-then-fill copy); child has a NOT NULL, non-cyclic FK to hub — the edge that
	// skews under live writes and cannot be NULLed.
	hubDDL := "CREATE TABLE rc_it.hub (id int PRIMARY KEY, buddy_id int REFERENCES rc_it.hub(id), name text)"
	childDDL := "CREATE TABLE rc_it.child (id int PRIMARY KEY, hub_id int NOT NULL REFERENCES rc_it.hub(id), note text)"
	for _, c := range []*pgx.Conn{src, tgt} {
		mustExec(t, ctx, c, "DROP SCHEMA IF EXISTS rc_it CASCADE")
		mustExec(t, ctx, c, "CREATE SCHEMA rc_it")
		mustExec(t, ctx, c, hubDDL)
		mustExec(t, ctx, c, childDDL)
	}
	mustExec(t, ctx, src, "DROP SCHEMA IF EXISTS replicare CASCADE")
	mustExec(t, ctx, tgt, "DROP SCHEMA IF EXISTS replicare_state CASCADE")
	// Seed a body large enough that the copy takes long enough for concurrent inserts to
	// land mid-copy (opening the skew window).
	const seed = 4000
	mustExec(t, ctx, src, fmt.Sprintf("INSERT INTO rc_it.hub SELECT g, NULL, 'h'||g FROM generate_series(1,%d) g", seed))
	mustExec(t, ctx, src, fmt.Sprintf("INSERT INTO rc_it.child SELECT g, g, 'c'||g FROM generate_series(1,%d) g", seed))
	t.Cleanup(func() {
		bg := context.Background()
		_, _ = src.Exec(bg, "DROP SCHEMA IF EXISTS rc_it CASCADE")
		_, _ = src.Exec(bg, "DROP SCHEMA IF EXISTS replicare CASCADE")
		_, _ = tgt.Exec(bg, "DROP SCHEMA IF EXISTS rc_it CASCADE")
		_, _ = tgt.Exec(bg, "DROP SCHEMA IF EXISTS replicare_state CASCADE")
	})

	syncBlock := `
syncs:
  - name: s1
    source: src
    targets: [dst]
    include: ["rc_it.*"]
    tuning: { drain_interval: 100ms }
`
	cfg, err := config.Load(writeConfig(t, harnessConfigYAML(syncBlock)))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	d, err := New(cfg, nil)
	if err != nil {
		t.Fatalf("new daemon: %v", err)
	}

	// Concurrent writer: insert a fresh hub + a child referencing it, back-to-back, so a
	// child copied from a later snapshot references a hub the parent snapshot missed.
	stopWrites := make(chan struct{})
	writesDone := make(chan struct{})
	go func() {
		defer close(writesDone)
		id := seed + 1
		for {
			select {
			case <-stopWrites:
				return
			default:
			}
			if _, err := writer.Exec(ctx, "INSERT INTO rc_it.hub (id, buddy_id, name) VALUES ($1, NULL, $2)", id, fmt.Sprintf("h%d", id)); err != nil {
				return // ctx cancelled or shutting down
			}
			if _, err := writer.Exec(ctx, "INSERT INTO rc_it.child (id, hub_id, note) VALUES ($1, $2, $3)", id, id, fmt.Sprintf("c%d", id)); err != nil {
				return
			}
			id++
			time.Sleep(2 * time.Millisecond)
		}
	}()

	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- d.Run(runCtx) }()

	// Let the copy run under sustained concurrent writes, then quiesce the writer so the
	// source stops moving and the target can fully catch up.
	time.Sleep(5 * time.Second)
	close(stopWrites)
	<-writesDone

	// The daemon must be alive (no crash from a transient FK) and converge to the source.
	select {
	case err := <-done:
		t.Fatalf("daemon exited during cyclic copy under concurrent writes: %v", err)
	default:
	}
	converged := pollUntil(t, 60*time.Second, func() bool {
		sh := count(t, ctx, src, "rc_it.hub")
		sc := count(t, ctx, src, "rc_it.child")
		th := count(t, ctx, tgt, "rc_it.hub")
		tc := count(t, ctx, tgt, "rc_it.child")
		return sh > 0 && sh == th && sc == tc
	})
	if !converged {
		t.Fatalf("did not converge under concurrent writes: src hub=%d child=%d, tgt hub=%d child=%d",
			count(t, ctx, src, "rc_it.hub"), count(t, ctx, src, "rc_it.child"),
			count(t, ctx, tgt, "rc_it.hub"), count(t, ctx, tgt, "rc_it.child"))
	}
	// No orphaned children on the target — referential integrity held throughout.
	if orphans := count(t, ctx, tgt, "rc_it.child c WHERE NOT EXISTS (SELECT 1 FROM rc_it.hub h WHERE h.id=c.hub_id)"); orphans != 0 {
		t.Errorf("%d target child rows reference a missing hub", orphans)
	}

	stop()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("daemon Run returned %v, want nil on graceful shutdown", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("daemon did not stop within 15s of cancellation")
	}
}

// TestDaemonSelfRefNullCollidingUniqueCopiesAndStreams is the production failure
// end-to-end: a self-referencing table (account -> parent account) whose TARGET has a
// unique index where empty values collide — UNIQUE NULLS NOT DISTINCT (email, parent) —
// and where the same email legitimately appears under different parents. The old
// two-pass cyclic load emptied `parent` on every row and died with a duplicate key
// (SQLSTATE 23505); streaming would likewise empty it on every NEW row. Both must now keep
// the self-link real: the initial copy converges, and live source changes — a new child
// whose email matches a parentless account, a rename, and a re-parent onto that new row —
// replicate. The source (an old major in the harness) carries an ordinary unique.
func TestDaemonSelfRefNullCollidingUniqueCopiesAndStreams(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), daemonTestBudget)
	defer cancel()

	src := dial(t, ctx, envd("RC_SRC_HOST", "127.0.0.1"), envd("RC_SRC_PORT", "5440"), envd("RC_SRC_DB", "replicare_src"))
	defer src.Close(context.Background())
	tgt := dial(t, ctx, envd("RC_DST_HOST", "127.0.0.1"), envd("RC_DST_PORT", "5441"), envd("RC_DST_DB", "replicare_dst"))
	defer tgt.Close(context.Background())

	for _, c := range []*pgx.Conn{src, tgt} {
		mustExec(t, ctx, c, "DROP SCHEMA IF EXISTS rc_it CASCADE")
		mustExec(t, ctx, c, "CREATE SCHEMA rc_it")
	}
	mustExec(t, ctx, src, "DROP SCHEMA IF EXISTS replicare CASCADE")
	mustExec(t, ctx, tgt, "DROP SCHEMA IF EXISTS replicare_state CASCADE")
	mustExec(t, ctx, src, `CREATE TABLE rc_it.acct (id int PRIMARY KEY, email text NOT NULL,
		parent int REFERENCES rc_it.acct(id), name text, CONSTRAINT acct_email_parent_unique UNIQUE (email, parent))`)
	mustExec(t, ctx, tgt, `CREATE TABLE rc_it.acct (id int PRIMARY KEY, email text NOT NULL,
		parent int REFERENCES rc_it.acct(id), name text, CONSTRAINT acct_email_parent_unique UNIQUE NULLS NOT DISTINCT (email, parent))`)
	// 50 roots (1-3 use emails e1-e3) and 450 children reusing e1..e9 under different
	// parents; stored children-first so the copy stream emits a child before its parent.
	mustExec(t, ctx, src, `INSERT INTO rc_it.acct (id, email, parent, name)
		SELECT id, email, parent, 'n'||id FROM (
		  SELECT g AS id, CASE WHEN g <= 3 THEN 'e'||g ELSE 'r'||g END AS email, NULL::int AS parent FROM generate_series(1,50) g
		  UNION ALL SELECT g, 'e'||(g/50), (g % 50) + 1 FROM generate_series(51,500) g
		) v ORDER BY id DESC`)
	mustExec(t, ctx, src, "ANALYZE rc_it.acct") // gives the copy a row estimate to report
	t.Cleanup(func() {
		bg := context.Background()
		_, _ = src.Exec(bg, "DROP SCHEMA IF EXISTS rc_it CASCADE")
		_, _ = src.Exec(bg, "DROP SCHEMA IF EXISTS replicare CASCADE")
		_, _ = tgt.Exec(bg, "DROP SCHEMA IF EXISTS rc_it CASCADE")
		_, _ = tgt.Exec(bg, "DROP SCHEMA IF EXISTS replicare_state CASCADE")
	})

	// Order-independent content digest of the table (works on old sources too).
	digest := func(c *pgx.Conn) string {
		var d *string
		err := c.QueryRow(ctx, `SELECT md5(string_agg(id||'|'||email||'|'||coalesce(parent::text,'-')||'|'||coalesce(name,'-'), ',' ORDER BY id)) FROM rc_it.acct`).Scan(&d)
		if err != nil || d == nil {
			return ""
		}
		return *d
	}

	syncBlock := `
syncs:
  - name: s1
    source: src
    targets: [dst]
    include: ["rc_it.*"]
    tuning: { drain_interval: 100ms }
`
	cfg, err := config.Load(writeConfig(t, harnessConfigYAML(syncBlock)))
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

	// Initial copy converges (before the fix: duplicate key in the cyclic copy).
	if !pollUntil(t, copyTimeout, func() bool { s := digest(src); return s != "" && s == digest(tgt) }) {
		select {
		case err := <-done:
			t.Fatalf("daemon exited during initial copy: %v", err)
		default:
		}
		t.Fatalf("initial copy did not converge: src rows=%d tgt rows=%d", count(t, ctx, src, "rc_it.acct"), count(t, ctx, tgt, "rc_it.acct"))
	}

	// The dashboard's initial-copy panels: this cyclic copy reports the rows it committed
	// and the source's row estimate, and both ends report up from bring-up.
	for _, want := range []string{
		`replicare_rows_copied_total{sync="s1",table="rc_it.acct"} 500`,
		`replicare_initial_copy_rows_target{sync="s1",table="rc_it.acct"} 500`,
		`replicare_source_up{sync="s1"} 1`,
		`replicare_target_up{sync="s1",target="dst"} 1`,
	} {
		if page := scrapeMetrics(t, d); !strings.Contains(page, want) {
			t.Errorf("/metrics is missing %s", want)
		}
	}

	// Live changes. (e1, 1) is free, but emptied it would be (e1, NULL) — root 1's value.
	mustExec(t, ctx, src, "INSERT INTO rc_it.acct VALUES (9001, 'e1', 1, 'new child')")
	mustExec(t, ctx, src, "UPDATE rc_it.acct SET name = 'renamed' WHERE id = 60")
	mustExec(t, ctx, src, "UPDATE rc_it.acct SET parent = 9001 WHERE id = 199")
	mustExec(t, ctx, src, "INSERT INTO rc_it.acct VALUES (9002, 'zz', NULL, 'new root')")

	if !pollUntil(t, convergeTimeout, func() bool { return digest(src) == digest(tgt) }) {
		var p199 *int
		_ = tgt.QueryRow(ctx, "SELECT parent FROM rc_it.acct WHERE id = 199").Scan(&p199)
		t.Fatalf("streaming did not converge: tgt has 9001=%d rows, 199.parent=%v",
			count(t, ctx, tgt, "rc_it.acct WHERE id = 9001"), p199)
	}

	stop()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("daemon Run returned %v, want nil on graceful shutdown", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("daemon did not stop within 15s of cancellation")
	}
}

// TestDaemonResumesAfterRestart proves checkpoint-resume: after one daemon stops,
// a fresh daemon on the same state resumes without re-copying (copy progress is
// Done) and streams deltas queued in the interim to convergence — no duplicates.
func TestDaemonResumesAfterRestart(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), daemonTestBudget)
	defer cancel()

	src := dial(t, ctx, envd("RC_SRC_HOST", "127.0.0.1"), envd("RC_SRC_PORT", "5440"), envd("RC_SRC_DB", "replicare_src"))
	defer src.Close(context.Background())
	tgt := dial(t, ctx, envd("RC_DST_HOST", "127.0.0.1"), envd("RC_DST_PORT", "5441"), envd("RC_DST_DB", "replicare_dst"))
	defer tgt.Close(context.Background())

	ddl := "CREATE TABLE rc_it.orders (id int PRIMARY KEY, note text)"
	for _, c := range []*pgx.Conn{src, tgt} {
		mustExec(t, ctx, c, "DROP SCHEMA IF EXISTS rc_it CASCADE")
		mustExec(t, ctx, c, "CREATE SCHEMA rc_it")
		mustExec(t, ctx, c, ddl)
	}
	mustExec(t, ctx, src, "DROP SCHEMA IF EXISTS replicare CASCADE")
	mustExec(t, ctx, tgt, "DROP SCHEMA IF EXISTS replicare_state CASCADE")
	mustExec(t, ctx, src, "INSERT INTO rc_it.orders SELECT g, 'v'||g FROM generate_series(1,20) g")
	t.Cleanup(func() {
		bg := context.Background()
		_, _ = src.Exec(bg, "DROP SCHEMA IF EXISTS rc_it CASCADE")
		_, _ = src.Exec(bg, "DROP SCHEMA IF EXISTS replicare CASCADE")
		_, _ = tgt.Exec(bg, "DROP SCHEMA IF EXISTS rc_it CASCADE")
		_, _ = tgt.Exec(bg, "DROP SCHEMA IF EXISTS replicare_state CASCADE")
	})

	syncBlock := `
syncs:
  - { name: s1, source: src, targets: [dst], include: ["rc_it.*"], tuning: { drain_interval: 100ms } }
`
	cfgPath := writeConfig(t, harnessConfigYAML(syncBlock))

	// First daemon: converge the initial copy, then stop.
	cfg1, _ := config.Load(cfgPath)
	d1, err := New(cfg1, nil)
	if err != nil {
		t.Fatalf("new daemon 1: %v", err)
	}
	ctx1, stop1 := context.WithCancel(ctx)
	done1 := make(chan error, 1)
	go func() { done1 <- d1.Run(ctx1) }()
	if !pollUntil(t, copyTimeout, func() bool { return count(t, ctx, tgt, "rc_it.orders") == 20 }) {
		t.Fatalf("daemon 1 did not converge initial copy")
	}
	stop1()
	<-done1

	// While no daemon runs, the source changes (deltas queue in the source).
	mustExec(t, ctx, src, "INSERT INTO rc_it.orders SELECT g, 'v'||g FROM generate_series(21,30) g")
	mustExec(t, ctx, src, "DELETE FROM rc_it.orders WHERE id = 3")

	// Second daemon on the same state must resume (no re-copy) and converge.
	cfg2, _ := config.Load(cfgPath)
	d2, err := New(cfg2, nil)
	if err != nil {
		t.Fatalf("new daemon 2: %v", err)
	}
	ctx2, stop2 := context.WithCancel(ctx)
	done2 := make(chan error, 1)
	go func() { done2 <- d2.Run(ctx2) }()
	converged := pollUntil(t, convergeTimeout, func() bool {
		return count(t, ctx, tgt, "rc_it.orders") == 29 && // 20 + 10 - 1 delete
			count(t, ctx, tgt, "rc_it.orders WHERE id = 3") == 0 &&
			count(t, ctx, tgt, "rc_it.orders WHERE id = 30") == 1
	})
	stop2()
	<-done2
	if !converged {
		t.Fatalf("daemon 2 did not resume+converge: target has %d rows", count(t, ctx, tgt, "rc_it.orders"))
	}
}

// scrapeMetrics renders the daemon's /metrics page.
func scrapeMetrics(t *testing.T, d *Daemon) string {
	t.Helper()
	rec := httptest.NewRecorder()
	d.metrics.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	return rec.Body.String()
}
