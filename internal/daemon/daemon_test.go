package daemon

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
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

// TestDaemonRunConvergesAndStopsCleanly exercises the whole process path: load a
// config, run the daemon, and — with the source mutated while it streams —
// converge the target, then a cancellation stops it cleanly (graceful shutdown).
func TestDaemonRunConvergesAndStopsCleanly(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
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
	if !pollUntil(t, 30*time.Second, func() bool { return count(t, ctx, tgt, "rc_it.orders") == 30 }) {
		t.Fatalf("initial copy did not converge: target has %d rows", count(t, ctx, tgt, "rc_it.orders"))
	}

	// Streaming reconciles a live mutation.
	mustExec(t, ctx, src, "INSERT INTO rc_it.orders VALUES (31, 'v31')")
	mustExec(t, ctx, src, "UPDATE rc_it.orders SET note = 'v1-updated' WHERE id = 1")
	mustExec(t, ctx, src, "DELETE FROM rc_it.orders WHERE id = 2")
	converged := pollUntil(t, 30*time.Second, func() bool {
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
