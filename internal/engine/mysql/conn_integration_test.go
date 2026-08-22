package mysql

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

// integration skips unless the MySQL harness is enabled. It requires the
// MySQL-specific REPLICARE_MYSQL=1 (set by `task test:integration:mysql`), NOT
// just REPLICARE_INTEGRATION, so these tests do NOT run in the Postgres
// integration job (which sets REPLICARE_INTEGRATION but brings up no MySQL).
func integration(t *testing.T) bool {
	t.Helper()
	if os.Getenv("REPLICARE_INTEGRATION") != "1" || os.Getenv("REPLICARE_MYSQL") != "1" {
		t.Skip("MySQL integration test; run `task test:integration:mysql`")
		return false
	}
	return true
}

func srcCfg() engine.ConnConfig {
	return engine.ConnConfig{Host: env("RC_MYSQL_SRC_HOST", "127.0.0.1"), Port: 3340, Database: "replicare_src", User: "root", Password: "replicare", TLS: engine.TLSDisable}
}
func tgtCfg() engine.ConnConfig {
	return engine.ConnConfig{Host: env("RC_MYSQL_DST_HOST", "127.0.0.1"), Port: 3341, Database: "replicare_dst", User: "root", Password: "replicare", TLS: engine.TLSDisable}
}
func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// TestConnectAndVersion is the MM0 acceptance against the live 5.7->8.4 harness:
// both endpoints connect and report a plausible version through the probe (5.7.x
// source, 8.x target), and MariaDB is not falsely detected.
func TestConnectAndVersion(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	src := &Source{cfg: srcCfg()}
	if err := src.Connect(ctx); err != nil {
		t.Fatalf("connect source: %v", err)
	}
	defer src.Close(context.Background())
	sv, err := src.ServerVersion(ctx)
	if err != nil {
		t.Fatalf("source version: %v", err)
	}
	if sv < 50700 || sv >= 50800 {
		t.Errorf("source version = %d, want 5.7.x (50700..50799)", sv)
	}

	sink := &Sink{cfg: tgtCfg()}
	if err := sink.Connect(ctx); err != nil {
		t.Fatalf("connect target: %v", err)
	}
	defer sink.Close(context.Background())
	tv, err := sink.ServerVersion(ctx)
	if err != nil {
		t.Fatalf("target version: %v", err)
	}
	if tv < 80000 {
		t.Errorf("target version = %d, want >= 8.0.0", tv)
	}
	t.Logf("source=%d target=%d", sv, tv)
}

// TestSizeIntegration exercises the engine.DBSizer implementation: the whole-DB
// size is plausible (>0), the replicated size sums only the named tables (>0 and
// not exceeding the whole DB), a missing table contributes nothing, and an empty
// list is 0.
func TestSizeIntegration(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	src := &Source{cfg: srcCfg()}
	if err := src.Connect(ctx); err != nil {
		t.Fatalf("connect source: %v", err)
	}
	defer src.Close(context.Background())

	if _, err := src.db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS rc_repsize (id INT PRIMARY KEY, pad VARCHAR(255))"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	defer func() { _, _ = src.db.ExecContext(context.Background(), "DROP TABLE IF EXISTS rc_repsize") }()
	for i := 0; i < 500; i++ {
		if _, err := src.db.ExecContext(ctx, "INSERT IGNORE INTO rc_repsize (id, pad) VALUES (?, REPEAT('x', 200))", i); err != nil {
			t.Fatalf("seed rows: %v", err)
		}
	}
	// information_schema size stats are refreshed lazily; ANALYZE nudges them.
	_, _ = src.db.ExecContext(ctx, "ANALYZE TABLE rc_repsize")

	dbSize, err := src.DatabaseSize(ctx)
	if err != nil {
		t.Fatalf("database size: %v", err)
	}
	if dbSize <= 0 {
		t.Errorf("database size = %d, want > 0", dbSize)
	}

	tbl := engine.TableRef{Schema: "replicare_src", Name: "rc_repsize"}
	got, err := src.ReplicatedSize(ctx, []engine.TableRef{tbl})
	if err != nil {
		t.Fatalf("replicated size: %v", err)
	}
	if got <= 0 {
		t.Errorf("replicated size = %d, want > 0", got)
	}
	if got > dbSize {
		t.Errorf("replicated size %d exceeds whole-db size %d", got, dbSize)
	}

	withMissing, err := src.ReplicatedSize(ctx, []engine.TableRef{tbl, {Schema: "replicare_src", Name: "rc_nope"}})
	if err != nil {
		t.Fatalf("replicated size with missing: %v", err)
	}
	if withMissing != got {
		t.Errorf("missing table changed total: got %d, want %d", withMissing, got)
	}

	empty, err := src.ReplicatedSize(ctx, nil)
	if err != nil {
		t.Fatalf("replicated size empty: %v", err)
	}
	if empty != 0 {
		t.Errorf("empty replicated size = %d, want 0", empty)
	}
}
