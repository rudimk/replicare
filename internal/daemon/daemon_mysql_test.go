package daemon

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"

	"github.com/rudimk/replicare/internal/config"
	_ "github.com/rudimk/replicare/internal/engine/mysql" // register the MySQL engine
)

// MM7 verifies that MySQL syncs run under the REAL neutral daemon and that two
// engines coexist in one process. The daemon (build.go/daemon.go) dispatches
// purely through engine.Get() + the neutral Source/Sink interfaces, so nothing
// here is MySQL-specific in the daemon itself — these tests prove that neutrality
// end to end on the live dual-harness. Gated on REPLICARE_MYSQL=1 (plus a
// Postgres state store + the MySQL pair), so they SKIP in the Postgres-only CI
// integration job; run them with `task test:integration:dual`.

func mysqlIntegration(t *testing.T) bool {
	t.Helper()
	if os.Getenv("REPLICARE_INTEGRATION") != "1" || os.Getenv("REPLICARE_MYSQL") != "1" {
		t.Skip("dual-harness test; run `task test:integration:dual`")
		return false
	}
	return true
}

func myHost() string { return envd("RC_MYSQL_SRC_HOST", "127.0.0.1") }
func myTgtHost() string { return envd("RC_MYSQL_DST_HOST", "127.0.0.1") }

// myDial opens a database/sql handle to a harness MySQL server (default db).
func myDial(t *testing.T, host, port string) *sql.DB {
	t.Helper()
	db, err := sql.Open("mysql", fmt.Sprintf("root:replicare@tcp(%s:%s)/?parseTime=true", host, port))
	if err != nil {
		t.Fatalf("open mysql %s:%s: %v", host, port, err)
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("ping mysql %s:%s: %v", host, port, err)
	}
	return db
}

func myExec(t *testing.T, ctx context.Context, db *sql.DB, stmts ...string) {
	t.Helper()
	for _, s := range stmts {
		if _, err := db.ExecContext(ctx, s); err != nil {
			t.Fatalf("mysql exec %q: %v", s, err)
		}
	}
}

// myCount returns the row count of a table/predicate, or -1 if the table does not
// exist yet (during bring-up).
func myCount(t *testing.T, ctx context.Context, db *sql.DB, q string) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+q).Scan(&n); err != nil {
		return -1
	}
	return n
}

// mysqlDaemonConfigYAML points source+target at the MySQL pair and the state store
// at the Postgres target (v1's only StateStore backend).
func mysqlDaemonConfigYAML(syncs string) string {
	return fmt.Sprintf(`
logging: { level: warn, format: text }
state_store:
  engine: postgres
  postgres: { host: %s, port: %s, database: %s, user: %s, password: %s, sslmode: disable }
sources:
  mysrc:
    engine: mysql
    mysql: { host: %s, port: %s, database: replicare_src, user: root, password: replicare, tls: disable, local_infile: true }
targets:
  mydst:
    engine: mysql
    mysql: { host: %s, port: %s, database: replicare_dst, user: root, password: replicare, tls: disable, local_infile: true }
%s`,
		envd("RC_DST_HOST", "127.0.0.1"), envd("RC_DST_PORT", "5441"), envd("RC_DST_DB", "replicare_dst"), envd("RC_USER", "postgres"), envd("RC_PASSWORD", "postgres"),
		myHost(), envd("RC_MYSQL_SRC_PORT", "3340"),
		myTgtHost(), envd("RC_MYSQL_DST_PORT", "3341"),
		syncs)
}

// resetMySQLData drops+recreates a data database on both MySQL endpoints and drops
// the source capture database, returning connected handles.
func resetMySQLPair(t *testing.T, ctx context.Context, dataDB string, ddl ...string) (src, tgt *sql.DB) {
	t.Helper()
	src = myDial(t, myHost(), envd("RC_MYSQL_SRC_PORT", "3340"))
	tgt = myDial(t, myTgtHost(), envd("RC_MYSQL_DST_PORT", "3341"))
	t.Cleanup(func() { _ = src.Close() })
	t.Cleanup(func() { _ = tgt.Close() })
	for _, db := range []*sql.DB{src, tgt} {
		myExec(t, ctx, db, "DROP DATABASE IF EXISTS "+dataDB, "CREATE DATABASE "+dataDB)
		for _, d := range ddl {
			myExec(t, ctx, db, d)
		}
	}
	myExec(t, ctx, src, "DROP DATABASE IF EXISTS replicare") // source capture db
	t.Cleanup(func() {
		bg := context.Background()
		_, _ = src.Exec("DROP DATABASE IF EXISTS " + dataDB)
		_, _ = src.Exec("DROP DATABASE IF EXISTS replicare")
		_, _ = tgt.Exec("DROP DATABASE IF EXISTS " + dataDB)
		_ = bg
	})
	return src, tgt
}

// clearPGState wipes the Postgres state-store schema so each daemon test starts
// from a clean ownership/cursor/copy-progress slate.
func clearPGState(t *testing.T, ctx context.Context) {
	t.Helper()
	pg := dial(t, ctx, envd("RC_DST_HOST", "127.0.0.1"), envd("RC_DST_PORT", "5441"), envd("RC_DST_DB", "replicare_dst"))
	defer pg.Close(context.Background())
	mustExec(t, ctx, pg, "DROP SCHEMA IF EXISTS replicare_state CASCADE")
}

// TestDaemonMySQLConvergesAndStopsCleanly runs a MySQL sync under the real daemon:
// initial copy converges, a live mutation streams through, and a cancellation
// stops it cleanly (graceful drain+checkpoint).
func TestDaemonMySQLConvergesAndStopsCleanly(t *testing.T) {
	if !mysqlIntegration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()

	src, tgt := resetMySQLPair(t, ctx, "rc_it",
		"CREATE TABLE rc_it.orders (id INT PRIMARY KEY, note VARCHAR(50)) ENGINE=InnoDB")
	clearPGState(t, ctx)
	myExec(t, ctx, src, "INSERT INTO rc_it.orders SELECT n, CONCAT('v', n) FROM "+seq(1, 30))

	syncBlock := `
syncs:
  - name: s1
    source: mysrc
    targets: [mydst]
    include: ["rc_it.*"]
    tuning: { drain_interval: 100ms }
`
	cfg, err := config.Load(writeConfig(t, mysqlDaemonConfigYAML(syncBlock)))
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

	if !pollUntil(t, 40*time.Second, func() bool { return myCount(t, ctx, tgt, "rc_it.orders") == 30 }) {
		t.Fatalf("initial copy did not converge: target has %d rows", myCount(t, ctx, tgt, "rc_it.orders"))
	}

	myExec(t, ctx, src,
		"INSERT INTO rc_it.orders VALUES (31, 'v31')",
		"UPDATE rc_it.orders SET note = 'v1-updated' WHERE id = 1",
		"DELETE FROM rc_it.orders WHERE id = 2")
	converged := pollUntil(t, 40*time.Second, func() bool {
		return myCount(t, ctx, tgt, "rc_it.orders") == 30 &&
			myCount(t, ctx, tgt, "rc_it.orders WHERE id = 31") == 1 &&
			myCount(t, ctx, tgt, "rc_it.orders WHERE id = 2") == 0 &&
			myCount(t, ctx, tgt, "rc_it.orders WHERE id = 1 AND note = 'v1-updated'") == 1
	})
	if !converged {
		t.Fatalf("streaming did not converge on mutation")
	}

	stop()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("daemon Run returned %v, want nil on graceful shutdown", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("daemon did not stop within 20s of cancellation")
	}
}

// TestDaemonTwoConcurrentMySQLSyncs is the MM7 concurrency acceptance: one daemon
// runs two independent MySQL syncs at once, each converging its own database.
func TestDaemonTwoConcurrentMySQLSyncs(t *testing.T) {
	if !mysqlIntegration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()

	srcA, tgtA := resetMySQLPair(t, ctx, "rc_a", "CREATE TABLE rc_a.t (id INT PRIMARY KEY, v VARCHAR(50)) ENGINE=InnoDB")
	srcB, tgtB := resetMySQLPair(t, ctx, "rc_b", "CREATE TABLE rc_b.t (id INT PRIMARY KEY, v VARCHAR(50)) ENGINE=InnoDB")
	clearPGState(t, ctx)
	myExec(t, ctx, srcA, "INSERT INTO rc_a.t SELECT n, CONCAT('a', n) FROM "+seq(1, 15))
	myExec(t, ctx, srcB, "INSERT INTO rc_b.t SELECT n, CONCAT('b', n) FROM "+seq(1, 25))

	syncBlock := `
syncs:
  - { name: sa, source: mysrc, targets: [mydst], include: ["rc_a.*"], tuning: { drain_interval: 100ms } }
  - { name: sb, source: mysrc, targets: [mydst], include: ["rc_b.*"], tuning: { drain_interval: 100ms } }
`
	cfg, err := config.Load(writeConfig(t, mysqlDaemonConfigYAML(syncBlock)))
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

	if !pollUntil(t, 60*time.Second, func() bool {
		return myCount(t, ctx, tgtA, "rc_a.t") == 15 && myCount(t, ctx, tgtB, "rc_b.t") == 25
	}) {
		t.Fatalf("two MySQL syncs did not both converge: rc_a=%d rc_b=%d",
			myCount(t, ctx, tgtA, "rc_a.t"), myCount(t, ctx, tgtB, "rc_b.t"))
	}
	stop()
	<-done
}

// TestDaemonMixedEngines is the MM7 headline acceptance: a MySQL sync and a
// Postgres sync run concurrently in ONE daemon on the dual-harness, each within
// its own engine (the single-engine-per-sync rule holds), both converging.
func TestDaemonMixedEngines(t *testing.T) {
	if !mysqlIntegration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	// MySQL side data.
	mysrc, mytgt := resetMySQLPair(t, ctx, "rc_my", "CREATE TABLE rc_my.t (id INT PRIMARY KEY, v VARCHAR(50)) ENGINE=InnoDB")
	myExec(t, ctx, mysrc, "INSERT INTO rc_my.t SELECT n, CONCAT('m', n) FROM "+seq(1, 20))

	// Postgres side data + shared state store (both on the PG pair).
	pgSrc := dial(t, ctx, envd("RC_SRC_HOST", "127.0.0.1"), envd("RC_SRC_PORT", "5440"), envd("RC_SRC_DB", "replicare_src"))
	defer pgSrc.Close(context.Background())
	pgTgt := dial(t, ctx, envd("RC_DST_HOST", "127.0.0.1"), envd("RC_DST_PORT", "5441"), envd("RC_DST_DB", "replicare_dst"))
	defer pgTgt.Close(context.Background())
	pgDDL := "CREATE TABLE rc_pg.t (id int PRIMARY KEY, v text)"
	for _, c := range []*pgx.Conn{pgSrc, pgTgt} {
		mustExec(t, ctx, c, "DROP SCHEMA IF EXISTS rc_pg CASCADE")
		mustExec(t, ctx, c, "CREATE SCHEMA rc_pg")
		mustExec(t, ctx, c, pgDDL)
	}
	mustExec(t, ctx, pgSrc, "DROP SCHEMA IF EXISTS replicare CASCADE")
	mustExec(t, ctx, pgTgt, "DROP SCHEMA IF EXISTS replicare_state CASCADE")
	mustExec(t, ctx, pgSrc, "INSERT INTO rc_pg.t SELECT g, 'p'||g FROM generate_series(1,20) g")
	t.Cleanup(func() {
		bg := context.Background()
		_, _ = pgSrc.Exec(bg, "DROP SCHEMA IF EXISTS rc_pg CASCADE")
		_, _ = pgSrc.Exec(bg, "DROP SCHEMA IF EXISTS replicare CASCADE")
		_, _ = pgTgt.Exec(bg, "DROP SCHEMA IF EXISTS rc_pg CASCADE")
		_, _ = pgTgt.Exec(bg, "DROP SCHEMA IF EXISTS replicare_state CASCADE")
	})

	// Mixed config: both engines, one Postgres state store.
	body := fmt.Sprintf(`
logging: { level: warn, format: text }
state_store:
  engine: postgres
  postgres: { host: %s, port: %s, database: %s, user: %s, password: %s, sslmode: disable }
sources:
  pgsrc:
    engine: postgres
    postgres: { host: %s, port: %s, database: %s, user: %s, password: %s, sslmode: disable }
  mysrc:
    engine: mysql
    mysql: { host: %s, port: %s, database: replicare_src, user: root, password: replicare, tls: disable, local_infile: true }
targets:
  pgdst:
    engine: postgres
    postgres: { host: %s, port: %s, database: %s, user: %s, password: %s, sslmode: disable }
  mydst:
    engine: mysql
    mysql: { host: %s, port: %s, database: replicare_dst, user: root, password: replicare, tls: disable, local_infile: true }
syncs:
  - { name: pg, source: pgsrc, targets: [pgdst], include: ["rc_pg.*"], tuning: { drain_interval: 100ms } }
  - { name: my, source: mysrc, targets: [mydst], include: ["rc_my.*"], tuning: { drain_interval: 100ms } }
`,
		envd("RC_DST_HOST", "127.0.0.1"), envd("RC_DST_PORT", "5441"), envd("RC_DST_DB", "replicare_dst"), envd("RC_USER", "postgres"), envd("RC_PASSWORD", "postgres"),
		envd("RC_SRC_HOST", "127.0.0.1"), envd("RC_SRC_PORT", "5440"), envd("RC_SRC_DB", "replicare_src"), envd("RC_USER", "postgres"), envd("RC_PASSWORD", "postgres"),
		myHost(), envd("RC_MYSQL_SRC_PORT", "3340"),
		envd("RC_DST_HOST", "127.0.0.1"), envd("RC_DST_PORT", "5441"), envd("RC_DST_DB", "replicare_dst"), envd("RC_USER", "postgres"), envd("RC_PASSWORD", "postgres"),
		myTgtHost(), envd("RC_MYSQL_DST_PORT", "3341"))

	cfg, err := config.Load(writeConfig(t, body))
	if err != nil {
		t.Fatalf("load mixed config: %v", err)
	}
	d, err := New(cfg, nil)
	if err != nil {
		t.Fatalf("new daemon: %v", err)
	}
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- d.Run(runCtx) }()

	// Both engines converge concurrently in one daemon.
	if !pollUntil(t, 60*time.Second, func() bool {
		return myCount(t, ctx, mytgt, "rc_my.t") == 20 && count(t, ctx, pgTgt, "rc_pg.t") == 20
	}) {
		t.Fatalf("mixed syncs did not both converge: mysql=%d postgres=%d",
			myCount(t, ctx, mytgt, "rc_my.t"), count(t, ctx, pgTgt, "rc_pg.t"))
	}

	// A live mutation on each engine also streams through side by side.
	myExec(t, ctx, mysrc, "INSERT INTO rc_my.t VALUES (21, 'm21')")
	mustExec(t, ctx, pgSrc, "INSERT INTO rc_pg.t VALUES (21, 'p21')")
	if !pollUntil(t, 40*time.Second, func() bool {
		return myCount(t, ctx, mytgt, "rc_my.t WHERE id = 21") == 1 &&
			count(t, ctx, pgTgt, "rc_pg.t WHERE id = 21") == 1
	}) {
		t.Fatalf("mixed streaming did not converge on both engines")
	}

	stop()
	<-done
}

// seq builds a MySQL derived-table row generator "(SELECT lo n UNION SELECT lo+1
// ... UNION SELECT hi) s", a version-portable stand-in for generate_series.
func seq(lo, hi int) string {
	s := fmt.Sprintf("(SELECT %d n", lo)
	for i := lo + 1; i <= hi; i++ {
		s += fmt.Sprintf(" UNION SELECT %d", i)
	}
	return s + ") s"
}
