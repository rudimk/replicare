package redis

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

// integration skips unless the Redis harness is enabled. It requires the
// Redis-specific REPLICARE_REDIS=1 (set by `task test:integration:redis`), NOT
// just REPLICARE_INTEGRATION, so these tests do NOT run in the Postgres-only CI
// integration job (which brings up no Redis).
func integration(t *testing.T) bool {
	t.Helper()
	if os.Getenv("REPLICARE_INTEGRATION") != "1" || os.Getenv("REPLICARE_REDIS") != "1" {
		t.Skip("Redis integration test; run `task test:integration:redis`")
		return false
	}
	return true
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func srcCfg() engine.ConnConfig {
	return engine.ConnConfig{Host: env("RC_REDIS_SRC_HOST", "127.0.0.1"), Port: 6390, TLS: engine.TLSDisable}
}
func tgtCfg() engine.ConnConfig {
	return engine.ConnConfig{Host: env("RC_REDIS_DST_HOST", "127.0.0.1"), Port: 6391, TLS: engine.TLSDisable}
}

// TestConnectAndVersion is the RM0 acceptance against the live 6.2->7.4 standalone
// harness: both endpoints connect, report a plausible version through the probe
// (6.2.x source, 7.4.x target), and are detected as Redis (not a blocked fork).
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
	if sv < 60200 || sv >= 60300 {
		t.Errorf("source version = %d, want 6.2.x (60200..60299)", sv)
	}

	sink := &Sink{cfg: tgtCfg()}
	if err := sink.Connect(ctx); err != nil {
		t.Fatalf("connect sink: %v", err)
	}
	defer sink.Close(context.Background())
	tv, err := sink.ServerVersion(ctx)
	if err != nil {
		t.Fatalf("target version: %v", err)
	}
	if tv < 70400 || tv >= 70500 {
		t.Errorf("target version = %d, want 7.4.x (70400..70499)", tv)
	}

	// Old-source -> modern-target is the supported RDB-version direction: target
	// version must be >= source (the RM2 pre-flight gate will enforce this).
	if tv < sv {
		t.Errorf("target (%d) older than source (%d) — RDB-version gate would block", tv, sv)
	}
}
