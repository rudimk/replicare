package statepg

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

func TestBuildDSN(t *testing.T) {
	dsn := buildDSN(engine.ConnConfig{
		Host: "db.example.com", Port: 6432, Database: "state", User: "repl", Password: "p w",
		TLS: engine.TLSVerifyFull,
	})
	// Deterministic, sorted keyword/value form.
	for _, want := range []string{"host=db.example.com", "port=6432", "dbname=state", "user=repl", "sslmode=verify-full", `password='p w'`} {
		if !strings.Contains(dsn, want) {
			t.Errorf("DSN %q missing %q", dsn, want)
		}
	}
}

func TestBuildDSNDefaults(t *testing.T) {
	dsn := buildDSN(engine.ConnConfig{Host: "h", Database: "d", User: "u"})
	if !strings.Contains(dsn, "port=5432") {
		t.Errorf("expected default port 5432 in %q", dsn)
	}
	if !strings.Contains(dsn, "sslmode=prefer") {
		t.Errorf("expected default sslmode prefer in %q", dsn)
	}
}

// --- integration (gated on REPLICARE_INTEGRATION=1) ---
//
// These use the harness target (PG17) as the state-store database. Each test
// drops the replicare_state schema before and after so runs are independent.

func stateConn(t *testing.T) engine.ConnConfig {
	t.Helper()
	if os.Getenv("REPLICARE_INTEGRATION") != "1" {
		t.Skip("integration test; set REPLICARE_INTEGRATION=1 and run `task harness:up`")
	}
	env := func(k, def string) string {
		if v := os.Getenv(k); v != "" {
			return v
		}
		return def
	}
	port, err := strconv.Atoi(env("RC_STATE_PORT", "5441"))
	if err != nil {
		t.Fatalf("bad port: %v", err)
	}
	return engine.ConnConfig{
		Host:     env("RC_STATE_HOST", "127.0.0.1"),
		Port:     port,
		Database: env("RC_STATE_DB", "replicare_dst"),
		User:     env("RC_USER", "postgres"),
		Password: env("RC_PASSWORD", "postgres"),
		TLS:      engine.TLSDisable,
	}
}

// openTestStore opens a Store against the harness, dropping any prior state
// schema first, and registers cleanup.
func openTestStore(t *testing.T, ctx context.Context) *Store {
	t.Helper()
	s := New(stateConn(t))
	if err := s.Open(ctx); err != nil {
		t.Fatalf("open: %v", err)
	}
	// Ensure a clean slate: drop and re-open so the schema is freshly migrated.
	if _, err := s.pool.Exec(ctx, "DROP SCHEMA IF EXISTS "+stateSchema+" CASCADE"); err != nil {
		t.Fatalf("drop schema: %v", err)
	}
	_ = s.Close(ctx)
	s = New(stateConn(t))
	if err := s.Open(ctx); err != nil {
		t.Fatalf("re-open: %v", err)
	}
	t.Cleanup(func() {
		_, _ = s.pool.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+stateSchema+" CASCADE")
		_ = s.Close(context.Background())
	})
	return s
}

func TestOpenCreatesSchemaIdempotent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s := openTestStore(t, ctx)

	// The schema_version table records version 1.
	var version int
	if err := s.pool.QueryRow(ctx, "SELECT COALESCE(MAX(version),0) FROM "+versionTable).Scan(&version); err != nil {
		t.Fatalf("read version: %v", err)
	}
	if version != schemaSet.Target() {
		t.Errorf("schema version = %d, want %d", version, schemaSet.Target())
	}

	// Opening a second Store against the same DB is a no-op (idempotent).
	s2 := New(stateConn(t))
	if err := s2.Open(ctx); err != nil {
		t.Fatalf("second open should be idempotent: %v", err)
	}
	defer s2.Close(ctx)

	// All expected tables exist.
	for _, tbl := range []string{"syncs", "copy_progress", "cursors", "events"} {
		var exists bool
		err := s.pool.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema=$1 AND table_name=$2)`,
			stateSchema, tbl).Scan(&exists)
		if err != nil {
			t.Fatalf("check table %s: %v", tbl, err)
		}
		if !exists {
			t.Errorf("expected table %s.%s to exist", stateSchema, tbl)
		}
	}
}
