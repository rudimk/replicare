package daemon

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/rudimk/replicare/internal/config"
)

// Mesh convergence is eventually-consistent and driven by the daemon's continuous
// drain (both edges at drain_interval), so these are POLL CEILINGS, not expected
// durations — a converged mesh satisfies them in well under a second. They are
// generous because the whole integration suite runs serially (-p 1) against one
// shared 2-node harness on a CPU-shared CI runner, where a late-running mesh test
// occasionally needs far longer than a local isolated run. Correctness is unchanged:
// a poll that never sees convergence still fails (it just waits longer first), so a
// real non-convergence regression is still caught — only slow-but-eventual
// convergence is tolerated. (The 40s ceilings here previously flaked in CI.)
const (
	meshBringupTimeout  = 60 * time.Second  // capture install + mesh state up on both nodes
	meshConvergeTimeout = 90 * time.Second  // both nodes settle on the same value/keyset
	meshTestBudget      = 300 * time.Second // per-test ctx; must exceed the sum of the ceilings above
)

// TestDaemonTwoNodeMeshConverges is the MM3 acceptance: a 2-node Postgres mesh
// (active-active) where writes accepted on EITHER node converge on both, and — the
// heart of loop suppression — an inbound change applied to a node is NOT re-captured
// and echoed back around the mesh, so convergence is stable, not a runaway storm.
//
// The two members seed DISJOINT primary-key ranges (A: 1..15, B: 101..115): MM3 is
// loop suppression only; same-key conflict resolution (HLC-LWW) is MM4, so this test
// deliberately avoids a conflict. Both nodes must end with the union of both ranges.
func TestDaemonTwoNodeMeshConverges(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), meshTestBudget)
	defer cancel()

	// Node A = harness source (9.6), Node B = harness target (17). Both are mesh
	// members (each a source AND a target). The state store lives on B's DB.
	a := dial(t, ctx, envd("RC_SRC_HOST", "127.0.0.1"), envd("RC_SRC_PORT", "5440"), envd("RC_SRC_DB", "replicare_src"))
	defer a.Close(context.Background())
	b := dial(t, ctx, envd("RC_DST_HOST", "127.0.0.1"), envd("RC_DST_PORT", "5441"), envd("RC_DST_DB", "replicare_dst"))
	defer b.Close(context.Background())

	// Data-only: the target schema pre-exists on both members (CLAUDE.md §7).
	ddl := "CREATE TABLE rc_it.orders (id int PRIMARY KEY, note text)"
	for _, c := range []*pgx.Conn{a, b} {
		mustExec(t, ctx, c, "DROP SCHEMA IF EXISTS rc_it CASCADE")
		mustExec(t, ctx, c, "CREATE SCHEMA rc_it")
		mustExec(t, ctx, c, ddl)
		mustExec(t, ctx, c, "DROP SCHEMA IF EXISTS replicare CASCADE") // clear capture on BOTH
	}
	mustExec(t, ctx, b, "DROP SCHEMA IF EXISTS replicare_state CASCADE")
	// Disjoint seed ranges so the two directions never collide on a key.
	mustExec(t, ctx, a, "INSERT INTO rc_it.orders SELECT g, 'a'||g FROM generate_series(1,15) g")
	mustExec(t, ctx, b, "INSERT INTO rc_it.orders SELECT g, 'b'||g FROM generate_series(101,115) g")
	t.Cleanup(func() {
		bg := context.Background()
		for _, c := range []*pgx.Conn{a, b} {
			_, _ = c.Exec(bg, "DROP SCHEMA IF EXISTS rc_it CASCADE")
			_, _ = c.Exec(bg, "DROP SCHEMA IF EXISTS replicare CASCADE")
		}
		_, _ = b.Exec(bg, "DROP SCHEMA IF EXISTS replicare_state CASCADE")
	})

	cfgYAML := fmt.Sprintf(`
logging: { level: warn, format: text }
state_store:
  engine: postgres
  postgres: { host: %[1]s, port: %[2]s, database: %[3]s, user: %[4]s, password: %[5]s, sslmode: disable }
nodes:
  a:
    engine: postgres
    postgres: { host: %[6]s, port: %[7]s, database: %[8]s, user: %[4]s, password: %[5]s, sslmode: disable }
  b:
    engine: postgres
    postgres: { host: %[1]s, port: %[2]s, database: %[3]s, user: %[4]s, password: %[5]s, sslmode: disable }
clusters:
  - name: c1
    engine: postgres
    members: [a, b]
    include: ["rc_it.*"]
    tuning: { drain_interval: 100ms }
`,
		envd("RC_DST_HOST", "127.0.0.1"), envd("RC_DST_PORT", "5441"), envd("RC_DST_DB", "replicare_dst"),
		envd("RC_USER", "postgres"), envd("RC_PASSWORD", "postgres"),
		envd("RC_SRC_HOST", "127.0.0.1"), envd("RC_SRC_PORT", "5440"), envd("RC_SRC_DB", "replicare_src"))

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

	// Bidirectional initial copy: each member ends with the union (30 rows).
	if !pollUntil(t, meshConvergeTimeout, func() bool {
		return count(t, ctx, a, "rc_it.orders") == 30 && count(t, ctx, b, "rc_it.orders") == 30
	}) {
		select {
		case err := <-done:
			t.Fatalf("daemon exited early during initial copy: %v (a=%d b=%d)", err,
				count(t, ctx, a, "rc_it.orders"), count(t, ctx, b, "rc_it.orders"))
		default:
		}
		t.Fatalf("mesh initial copy did not converge: a=%d b=%d",
			count(t, ctx, a, "rc_it.orders"), count(t, ctx, b, "rc_it.orders"))
	}

	// A live write accepted on EITHER node reaches the other (active-active).
	mustExec(t, ctx, a, "INSERT INTO rc_it.orders VALUES (16, 'a16')")
	mustExec(t, ctx, b, "INSERT INTO rc_it.orders VALUES (116, 'b116')")
	if !pollUntil(t, meshConvergeTimeout, func() bool {
		return count(t, ctx, a, "rc_it.orders") == 32 && count(t, ctx, b, "rc_it.orders") == 32 &&
			count(t, ctx, a, "rc_it.orders WHERE id = 116") == 1 && // B's write reached A
			count(t, ctx, b, "rc_it.orders WHERE id = 16") == 1 // A's write reached B
	}) {
		t.Fatalf("mesh streaming did not converge both ways: a=%d b=%d",
			count(t, ctx, a, "rc_it.orders"), count(t, ctx, b, "rc_it.orders"))
	}

	// Loop suppression: with both directions quiescent, the row counts must STAY 32
	// (an unsuppressed mesh would keep re-capturing applied writes and ping-pong
	// them forever — visible here as growth or churn). Hold and re-check.
	time.Sleep(3 * time.Second)
	if got := count(t, ctx, a, "rc_it.orders"); got != 32 {
		t.Fatalf("node A not stable after quiescence: %d rows, want 32 (echo storm?)", got)
	}
	if got := count(t, ctx, b, "rc_it.orders"); got != 32 {
		t.Fatalf("node B not stable after quiescence: %d rows, want 32 (echo storm?)", got)
	}

	// The delta queues on both members drain and stay drained — no re-capture of
	// replicare's own applies (the direct loop-suppression signal).
	if got := unconsumedDeltas(t, ctx, a); got != 0 {
		t.Errorf("node A has %d unconsumed deltas after quiescence, want 0", got)
	}
	if got := unconsumedDeltas(t, ctx, b); got != 0 {
		t.Errorf("node B has %d unconsumed deltas after quiescence, want 0", got)
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

// TestDaemonMeshSameKeyConflictConverges is the MM4 acceptance: concurrent writes to
// the SAME key on two nodes of an active-active mesh converge to the SAME value on
// both, resolved by HLC last-write-wins — and a delete-vs-update on the same key
// resolves under the same total order. This is what MM3 could not do (MM3 converged
// only for non-conflicting keys); MM4's version register + HLC-LWW closes it.
func TestDaemonMeshSameKeyConflictConverges(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), meshTestBudget)
	defer cancel()

	a := dial(t, ctx, envd("RC_SRC_HOST", "127.0.0.1"), envd("RC_SRC_PORT", "5440"), envd("RC_SRC_DB", "replicare_src"))
	defer a.Close(context.Background())
	b := dial(t, ctx, envd("RC_DST_HOST", "127.0.0.1"), envd("RC_DST_PORT", "5441"), envd("RC_DST_DB", "replicare_dst"))
	defer b.Close(context.Background())

	ddl := "CREATE TABLE rc_it.orders (id int PRIMARY KEY, note text)"
	for _, c := range []*pgx.Conn{a, b} {
		mustExec(t, ctx, c, "DROP SCHEMA IF EXISTS rc_it CASCADE")
		mustExec(t, ctx, c, "CREATE SCHEMA rc_it")
		mustExec(t, ctx, c, ddl)
		mustExec(t, ctx, c, "DROP SCHEMA IF EXISTS replicare CASCADE")
	}
	mustExec(t, ctx, b, "DROP SCHEMA IF EXISTS replicare_state CASCADE")
	t.Cleanup(func() {
		bg := context.Background()
		for _, c := range []*pgx.Conn{a, b} {
			_, _ = c.Exec(bg, "DROP SCHEMA IF EXISTS rc_it CASCADE")
			_, _ = c.Exec(bg, "DROP SCHEMA IF EXISTS replicare CASCADE")
		}
		_, _ = b.Exec(bg, "DROP SCHEMA IF EXISTS replicare_state CASCADE")
	})

	cfgYAML := fmt.Sprintf(`
logging: { level: warn, format: text }
state_store:
  engine: postgres
  postgres: { host: %[1]s, port: %[2]s, database: %[3]s, user: %[4]s, password: %[5]s, sslmode: disable }
nodes:
  a:
    engine: postgres
    postgres: { host: %[6]s, port: %[7]s, database: %[8]s, user: %[4]s, password: %[5]s, sslmode: disable }
  b:
    engine: postgres
    postgres: { host: %[1]s, port: %[2]s, database: %[3]s, user: %[4]s, password: %[5]s, sslmode: disable }
clusters:
  - name: c1
    engine: postgres
    members: [a, b]
    include: ["rc_it.*"]
    tuning: { drain_interval: 100ms }
`,
		envd("RC_DST_HOST", "127.0.0.1"), envd("RC_DST_PORT", "5441"), envd("RC_DST_DB", "replicare_dst"),
		envd("RC_USER", "postgres"), envd("RC_PASSWORD", "postgres"),
		envd("RC_SRC_HOST", "127.0.0.1"), envd("RC_SRC_PORT", "5440"), envd("RC_SRC_DB", "replicare_src"))

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
	defer func() { stop(); <-done }()

	// Give both edges a moment to install capture + reach streaming (tables empty, so
	// there is nothing to copy).
	if !pollUntil(t, meshBringupTimeout, func() bool {
		var n int
		_ = b.QueryRow(ctx, "SELECT count(*) FROM pg_tables WHERE schemaname='replicare' AND tablename='hlc_state'").Scan(&n)
		var m int
		_ = a.QueryRow(ctx, "SELECT count(*) FROM pg_tables WHERE schemaname='replicare' AND tablename='hlc_state'").Scan(&m)
		return n == 1 && m == 1
	}) {
		t.Fatalf("cluster did not bring up mesh state on both nodes")
	}

	// Concurrent conflict on the SAME key: different values written on each node.
	mustExec(t, ctx, a, "INSERT INTO rc_it.orders (id, note) VALUES (1, 'from-a')")
	mustExec(t, ctx, b, "INSERT INTO rc_it.orders (id, note) VALUES (1, 'from-b')")

	// Convergence oracle: both nodes settle on the SAME value for key 1 (whichever
	// (hlc, node) is greater — the test asserts agreement, not which one).
	note := func(c *pgx.Conn) string {
		var s string
		if err := c.QueryRow(ctx, "SELECT note FROM rc_it.orders WHERE id=1").Scan(&s); err != nil {
			return ""
		}
		return s
	}
	if !pollUntil(t, meshConvergeTimeout, func() bool {
		na, nb := note(a), note(b)
		return na != "" && na == nb
	}) {
		t.Fatalf("same-key conflict did not converge: a=%q b=%q", note(a), note(b))
	}
	winner := note(a)
	if winner != "from-a" && winner != "from-b" {
		t.Fatalf("converged to an unexpected value %q (want one of the two writes)", winner)
	}

	// Stability: the converged value holds (no flip-flop / echo).
	time.Sleep(2 * time.Second)
	if note(a) != winner || note(b) != winner {
		t.Fatalf("converged value not stable: a=%q b=%q, want %q", note(a), note(b), winner)
	}

	// Delete-vs-update conflict on the same key: delete on A, update on B. Under the
	// total order one wins; both nodes must agree (either the row is gone on both, or
	// present-and-identical on both).
	mustExec(t, ctx, a, "DELETE FROM rc_it.orders WHERE id=1")
	mustExec(t, ctx, b, "UPDATE rc_it.orders SET note='b-updated' WHERE id=1")
	if !pollUntil(t, meshConvergeTimeout, func() bool {
		_, aok := existsRow(ctx, a)
		_, bok := existsRow(ctx, b)
		if aok != bok {
			return false // still diverged on presence
		}
		return note(a) == note(b) // agree on value (both "" when absent)
	}) {
		t.Fatalf("delete-vs-update did not converge: a=%q b=%q", note(a), note(b))
	}
}

// existsRow reports whether id=1 is present.
func existsRow(ctx context.Context, c *pgx.Conn) (string, bool) {
	var s string
	err := c.QueryRow(ctx, "SELECT COALESCE(note,'') FROM rc_it.orders WHERE id=1").Scan(&s)
	if err != nil {
		return "", false
	}
	return s, true
}

// unconsumedDeltas sums the rows across every replicare delta table on a member.
// After quiescence in a correctly loop-suppressed mesh this is 0: local writes were
// consumed by the outbound edge, and replicare's own inbound applies were never
// captured in the first place. A non-zero, non-draining value is the echo-storm
// signature.
func unconsumedDeltas(t *testing.T, ctx context.Context, c *pgx.Conn) int {
	t.Helper()
	rows, err := c.Query(ctx, `
		SELECT format('%I.%I', schemaname, tablename)
		FROM pg_tables
		WHERE schemaname = 'replicare' AND tablename LIKE 'delta_%'`)
	if err != nil {
		t.Fatalf("list delta tables: %v", err)
	}
	var tbls []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan delta table: %v", err)
		}
		tbls = append(tbls, name)
	}
	rows.Close()

	total := 0
	for _, tb := range tbls {
		var n int
		if err := c.QueryRow(ctx, "SELECT count(*) FROM "+tb).Scan(&n); err != nil {
			t.Fatalf("count %s: %v", tb, err)
		}
		total += n
	}
	return total
}
