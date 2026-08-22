package postgres

import (
	"context"
	"testing"
	"time"
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
