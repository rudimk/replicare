package daemon

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/config"
)

// mysqlClusterConfigYAML wires a 2-node MySQL active-active cluster (nodes a=source
// harness :3340, b=target harness :3341), with the state store on the Postgres target
// (v1's only StateStore backend). Both MySQL databases are `rc_it`.
func mysqlClusterConfigYAML() string {
	return fmt.Sprintf(`
logging: { level: warn, format: text }
state_store:
  engine: postgres
  postgres: { host: %[1]s, port: %[2]s, database: %[3]s, user: %[4]s, password: %[5]s, sslmode: disable }
nodes:
  a:
    engine: mysql
    mysql: { host: %[6]s, port: %[7]s, database: rc_it, user: root, password: replicare, tls: disable, local_infile: true }
  b:
    engine: mysql
    mysql: { host: %[8]s, port: %[9]s, database: rc_it, user: root, password: replicare, tls: disable, local_infile: true }
clusters:
  - name: mysql-mesh
    engine: mysql
    members: [a, b]
    include: ["rc_it.*"]
    tuning: { drain_interval: 100ms }
`,
		envd("RC_DST_HOST", "127.0.0.1"), envd("RC_DST_PORT", "5441"), envd("RC_DST_DB", "replicare_dst"),
		envd("RC_USER", "postgres"), envd("RC_PASSWORD", "postgres"),
		myHost(), envd("RC_MYSQL_SRC_PORT", "3340"),
		myTgtHost(), envd("RC_MYSQL_DST_PORT", "3341"))
}

// TestDaemonMySQLMeshSameKeyConflictConverges is the MM5 acceptance: a 2-node MySQL
// active-active mesh where writes on either node converge on both, concurrent writes
// to the SAME key converge to the same value under HLC-LWW, and a delete-vs-update on
// the same key resolves under the same total order. This exercises the full MySQL
// cluster path end to end (loop-suppression marker, version register, version-guarded
// apply) through the real daemon.
func TestDaemonMySQLMeshSameKeyConflictConverges(t *testing.T) {
	if !mysqlIntegration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	a, b := resetMySQLPair(t, ctx, "rc_it",
		"CREATE TABLE rc_it.orders (id INT PRIMARY KEY, note VARCHAR(50)) ENGINE=InnoDB")
	// Both members are also sources, so clear the capture db on BOTH (resetMySQLPair
	// only clears it on the source handle).
	myExec(t, ctx, b, "DROP DATABASE IF EXISTS replicare")
	t.Cleanup(func() { _, _ = b.Exec("DROP DATABASE IF EXISTS replicare") })
	clearPGState(t, ctx)

	cfg, err := config.Load(writeConfig(t, mysqlClusterConfigYAML()))
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

	// Wait for both members to bring up mesh state (tables are empty, nothing to copy).
	if !pollUntil(t, 40*time.Second, func() bool {
		return myHasHLCState(ctx, a) && myHasHLCState(ctx, b)
	}) {
		t.Fatalf("cluster did not bring up mesh state on both nodes")
	}

	// Concurrent same-key conflict: different values on each node.
	myExec(t, ctx, a, "INSERT INTO rc_it.orders (id, note) VALUES (1, 'from-a')")
	myExec(t, ctx, b, "INSERT INTO rc_it.orders (id, note) VALUES (1, 'from-b')")

	if !pollUntil(t, 60*time.Second, func() bool {
		na, nb := myNote(ctx, a, 1), myNote(ctx, b, 1)
		return na != "" && na == nb
	}) {
		select {
		case err := <-done:
			t.Fatalf("daemon exited early: %v (a=%q b=%q)", err, myNote(ctx, a, 1), myNote(ctx, b, 1))
		default:
		}
		// Surface recorded stream errors from the PG state store.
		pg := dial(t, ctx, envd("RC_DST_HOST", "127.0.0.1"), envd("RC_DST_PORT", "5441"), envd("RC_DST_DB", "replicare_dst"))
		defer pg.Close(context.Background())
		rows, _ := pg.Query(ctx, "SELECT sync, event, message FROM replicare_state.events WHERE level IN ('WARN','ERROR') ORDER BY id DESC LIMIT 8")
		if rows != nil {
			for rows.Next() {
				var s, e, m string
				_ = rows.Scan(&s, &e, &m)
				t.Logf("event: sync=%s event=%s msg=%s", s, e, m)
			}
			rows.Close()
		}
		t.Fatalf("same-key conflict did not converge: a=%q b=%q", myNote(ctx, a, 1), myNote(ctx, b, 1))
	}
	winner := myNote(ctx, a, 1)
	if winner != "from-a" && winner != "from-b" {
		t.Fatalf("converged to unexpected value %q (want one of the two writes)", winner)
	}

	// Stability: the converged value holds (no echo/flip-flop).
	time.Sleep(3 * time.Second)
	if myNote(ctx, a, 1) != winner || myNote(ctx, b, 1) != winner {
		t.Fatalf("converged value not stable: a=%q b=%q, want %q", myNote(ctx, a, 1), myNote(ctx, b, 1), winner)
	}

	// Delete-vs-update on the same key: both nodes must agree on the outcome.
	myExec(t, ctx, a, "DELETE FROM rc_it.orders WHERE id=1")
	myExec(t, ctx, b, "UPDATE rc_it.orders SET note='b-updated' WHERE id=1")
	if !pollUntil(t, 60*time.Second, func() bool {
		pa, pb := myPresent(ctx, a, 1), myPresent(ctx, b, 1)
		if pa != pb {
			return false
		}
		return myNote(ctx, a, 1) == myNote(ctx, b, 1)
	}) {
		t.Fatalf("delete-vs-update did not converge: a=%q b=%q", myNote(ctx, a, 1), myNote(ctx, b, 1))
	}
}

func myHasHLCState(ctx context.Context, db *sql.DB) bool {
	var n int
	if err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA='replicare' AND TABLE_NAME='hlc_state'").Scan(&n); err != nil {
		return false
	}
	return n == 1
}

func myNote(ctx context.Context, db *sql.DB, id int) string {
	var s string
	if err := db.QueryRowContext(ctx, "SELECT note FROM rc_it.orders WHERE id=?", id).Scan(&s); err != nil {
		return ""
	}
	return s
}

func myPresent(ctx context.Context, db *sql.DB, id int) bool {
	var n int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM rc_it.orders WHERE id=?", id).Scan(&n); err != nil {
		return false
	}
	return n > 0
}
