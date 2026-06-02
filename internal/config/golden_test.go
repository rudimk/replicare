package config_test

import (
	"testing"

	"github.com/rudimk/replicare/internal/config"
	"github.com/rudimk/replicare/internal/engine"

	// Register the Postgres engine so its connection block resolves.
	_ "github.com/rudimk/replicare/internal/engine/postgres"
)

// TestExampleConfigLoads is the golden end-to-end check: the shipped example
// config parses, env-expands (with defaults), resolves the real Postgres engine
// block, and validates.
func TestExampleConfigLoads(t *testing.T) {
	c, err := config.Load("../../examples/replicare.yml")
	if err != nil {
		t.Fatalf("load example config: %v", err)
	}

	if len(c.Syncs) != 1 || c.Syncs[0].Name != "app-to-warehouse" {
		t.Fatalf("unexpected syncs: %+v", c.Syncs)
	}
	if c.StateStore == nil || c.StateStore.Engine != "postgres" {
		t.Fatalf("state_store not resolved: %+v", c.StateStore)
	}

	src := c.Sources["app"]
	if src == nil || src.Conn == nil || src.Conn.EngineName() != "postgres" {
		t.Fatalf("source 'app' not resolved to postgres: %+v", src)
	}
	cc := src.Conn.ConnConfig()
	if cc.Database != "app" {
		t.Errorf("source database = %q, want app", cc.Database)
	}
	if cc.TLS != engine.TLSVerifyFull {
		t.Errorf("source sslmode = %q, want verify-full", cc.TLS)
	}
	// Default applied because APP_DB_PASSWORD is unset in the test env.
	if cc.Password != "postgres" {
		t.Errorf("source password = %q, want defaulted 'postgres'", cc.Password)
	}

	tgt := c.Targets["warehouse"]
	if tgt == nil || tgt.Conn == nil || tgt.Conn.ConnConfig().TLS != engine.TLSRequire {
		t.Errorf("target 'warehouse' not resolved with sslmode=require: %+v", tgt)
	}
}
