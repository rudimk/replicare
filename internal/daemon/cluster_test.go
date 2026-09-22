package daemon

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/rudimk/replicare/internal/config"
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
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
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
	if !pollUntil(t, 60*time.Second, func() bool {
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
	if !pollUntil(t, 40*time.Second, func() bool {
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
