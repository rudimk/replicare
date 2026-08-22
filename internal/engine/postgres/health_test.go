package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

// TestHealthCheckReconnectIntegration proves the reconnect mechanism end-to-end:
// a Source whose backend is terminated server-side fails HealthCheck, and a
// Close + Connect restores a working connection (with session GUCs re-applied).
// This is the engine half of the pipeline's reconnect-on-drain-failure recovery.
func TestHealthCheckReconnectIntegration(t *testing.T) {
	cc := harnessConn(t, "source") // skips unless REPLICARE_INTEGRATION=1
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	src := &Source{cfg: cc}
	if err := src.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = src.Close(context.Background()) }()

	if err := src.HealthCheck(ctx); err != nil {
		t.Fatalf("health check on a fresh connection: %v", err)
	}

	// Grab the backend PID, then terminate it from a separate admin connection —
	// simulating an RDS failover / idle reap / network drop of this exact backend.
	var pid int
	if err := src.conn.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid); err != nil {
		t.Fatalf("read backend pid: %v", err)
	}
	admin, err := connect(ctx, cc)
	if err != nil {
		t.Fatalf("admin connect: %v", err)
	}
	defer func() { _ = admin.Close(context.Background()) }()
	if _, err := admin.Exec(ctx, "SELECT pg_terminate_backend($1)", pid); err != nil {
		t.Fatalf("terminate backend: %v", err)
	}

	// HealthCheck must now fail (bounded) — the socket is dead.
	hctx, hcancel := context.WithTimeout(ctx, 10*time.Second)
	defer hcancel()
	if err := src.HealthCheck(hctx); err == nil {
		t.Fatal("health check should fail after the backend was terminated")
	}

	// Reconnect = Close + Connect, and the connection works again.
	_ = src.Close(context.Background())
	if err := src.Connect(ctx); err != nil {
		t.Fatalf("reconnect: %v", err)
	}
	if err := src.HealthCheck(ctx); err != nil {
		t.Fatalf("health check after reconnect: %v", err)
	}
	var one int
	if err := src.conn.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil || one != 1 {
		t.Fatalf("query after reconnect: got %d err=%v", one, err)
	}
}

// TestDatabaseSizeIntegration checks the engine.DBSizer implementation returns a
// plausible (>0) database size for the DB-size metric.
func TestDatabaseSizeIntegration(t *testing.T) {
	cc := harnessConn(t, "source")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	src := &Source{cfg: cc}
	if err := src.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = src.Close(context.Background()) }()

	b, err := src.DatabaseSize(ctx)
	if err != nil {
		t.Fatalf("database size: %v", err)
	}
	if b <= 0 {
		t.Errorf("database size = %d, want > 0", b)
	}
}

// TestReplicatedSizeIntegration checks the engine.DBSizer replicated-size path:
// it sums only the named tables (a real one is > 0, doesn't exceed the whole DB),
// missing tables contribute nothing, and an empty list is 0.
func TestReplicatedSizeIntegration(t *testing.T) {
	cc := harnessConn(t, "source")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	src := &Source{cfg: cc}
	if err := src.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = src.Close(context.Background()) }()

	if _, err := src.conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS rc_repsize (id int PRIMARY KEY, pad text)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	defer func() { _, _ = src.conn.Exec(context.Background(), `DROP TABLE IF EXISTS rc_repsize`) }()
	if _, err := src.conn.Exec(ctx, `INSERT INTO rc_repsize SELECT g, repeat('x', 200) FROM generate_series(1, 500) g ON CONFLICT DO NOTHING`); err != nil {
		t.Fatalf("seed rows: %v", err)
	}

	tbl := engine.TableRef{Schema: "public", Name: "rc_repsize"}
	got, err := src.ReplicatedSize(ctx, []engine.TableRef{tbl})
	if err != nil {
		t.Fatalf("replicated size: %v", err)
	}
	if got <= 0 {
		t.Errorf("replicated size = %d, want > 0", got)
	}
	dbSize, err := src.DatabaseSize(ctx)
	if err != nil {
		t.Fatalf("database size: %v", err)
	}
	if got > dbSize {
		t.Errorf("replicated size %d exceeds whole-db size %d", got, dbSize)
	}

	// A missing table contributes nothing: adding a nonexistent ref must not change
	// the total (no regclass cast that could error on a missing relation).
	withMissing, err := src.ReplicatedSize(ctx, []engine.TableRef{tbl, {Schema: "public", Name: "rc_nope"}})
	if err != nil {
		t.Fatalf("replicated size with missing: %v", err)
	}
	if withMissing != got {
		t.Errorf("missing table changed total: got %d, want %d", withMissing, got)
	}

	// An empty selection is 0, not an error.
	empty, err := src.ReplicatedSize(ctx, nil)
	if err != nil {
		t.Fatalf("replicated size empty: %v", err)
	}
	if empty != 0 {
		t.Errorf("empty replicated size = %d, want 0", empty)
	}
}
