package postgres

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

func TestSslmodeParam(t *testing.T) {
	if got := sslmodeParam(""); got != defaultSSLMode {
		t.Errorf("empty TLS mode: got %q, want default %q", got, defaultSSLMode)
	}
	if got := sslmodeParam(engine.TLSVerifyFull); got != "verify-full" {
		t.Errorf("verify-full: got %q", got)
	}
	if got := sslmodeParam(engine.TLSDisable); got != "disable" {
		t.Errorf("disable: got %q", got)
	}
}

func TestPgxConfigDSN(t *testing.T) {
	cc := engine.ConnConfig{
		Host:     "db.example.com",
		Port:     6432,
		Database: "app",
		User:     "repl",
		Password: "s3cr3t",
		TLS:      engine.TLSVerifyFull,
	}
	cfg, err := pgxConfig(cc)
	if err != nil {
		t.Fatalf("pgxConfig: %v", err)
	}
	if cfg.Host != "db.example.com" {
		t.Errorf("host: got %q", cfg.Host)
	}
	if cfg.Port != 6432 {
		t.Errorf("port: got %d", cfg.Port)
	}
	if cfg.Database != "app" {
		t.Errorf("database: got %q", cfg.Database)
	}
	if cfg.User != "repl" {
		t.Errorf("user: got %q", cfg.User)
	}
	if cfg.Password != "s3cr3t" {
		t.Errorf("password: got %q", cfg.Password)
	}
	if got := cfg.RuntimeParams["sslmode"]; got != "" && got != "verify-full" {
		// pgx may consume sslmode into TLSConfig rather than RuntimeParams; the
		// authoritative check is that a TLSConfig is present for a verify mode.
		t.Logf("sslmode runtime param: %q", got)
	}
	if cfg.TLSConfig == nil {
		t.Errorf("verify-full should produce a non-nil TLSConfig")
	}
}

func TestPgxConfigDisableTLS(t *testing.T) {
	cfg, err := pgxConfig(engine.ConnConfig{
		Host: "h", Port: 5432, Database: "d", User: "u", TLS: engine.TLSDisable,
	})
	if err != nil {
		t.Fatalf("pgxConfig: %v", err)
	}
	if cfg.TLSConfig != nil {
		t.Errorf("disable should produce a nil TLSConfig, got %+v", cfg.TLSConfig)
	}
}

func TestPgxConfigParamsThreaded(t *testing.T) {
	cfg, err := pgxConfig(engine.ConnConfig{
		Host: "h", Port: 5432, Database: "d", User: "u",
		Params: map[string]string{"application_name": "replicare-test"},
	})
	if err != nil {
		t.Fatalf("pgxConfig: %v", err)
	}
	if got := cfg.RuntimeParams["application_name"]; got != "replicare-test" {
		t.Errorf("application_name not threaded through: got %q", got)
	}
}

func TestPgxConfigReservedParamRejected(t *testing.T) {
	_, err := pgxConfig(engine.ConnConfig{
		Host: "h", Port: 5432, Database: "d", User: "u",
		Params: map[string]string{"host": "evil"},
	})
	if err == nil {
		t.Fatal("expected error for reserved param override, got nil")
	}
}

func TestQuoteDSNValue(t *testing.T) {
	cases := map[string]string{
		"":           "''",
		"simple":     "simple",
		"with space": "'with space'",
		`quo'te`:     `'quo\'te'`,
		`back\slash`: `'back\\slash'`,
	}
	for in, want := range cases {
		if got := quoteDSNValue(in); got != want {
			t.Errorf("quoteDSNValue(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestQuoteLiteral(t *testing.T) {
	if got := quoteLiteral("ISO, YMD"); got != "'ISO, YMD'" {
		t.Errorf("got %q", got)
	}
	if got := quoteLiteral("it's"); got != "'it''s'" {
		t.Errorf("got %q", got)
	}
}

// --- Integration tests (gated on REPLICARE_INTEGRATION=1) ---
//
// These require the multi-version harness (`task harness:up`): an old source PG
// on :5440 and a modern target PG on :5441. They exercise the live connect path,
// session-GUC canonicalization, and the version probe against real servers.

// harnessConn returns the ConnConfig for a harness endpoint, or skips the test if
// integration mode is off. role is "source" or "target".
func harnessConn(t *testing.T, role string) engine.ConnConfig {
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
	var host, port, db string
	switch role {
	case "source":
		host = env("RC_SRC_HOST", "127.0.0.1")
		port = env("RC_SRC_PORT", "5440")
		db = env("RC_SRC_DB", "replicare_src")
	case "target":
		host = env("RC_DST_HOST", "127.0.0.1")
		port = env("RC_DST_PORT", "5441")
		db = env("RC_DST_DB", "replicare_dst")
	default:
		t.Fatalf("unknown role %q", role)
	}
	p, err := strconv.Atoi(port)
	if err != nil {
		t.Fatalf("bad port %q: %v", port, err)
	}
	return engine.ConnConfig{
		Host:     host,
		Port:     p,
		Database: db,
		User:     env("RC_USER", "postgres"),
		Password: env("RC_PASSWORD", "postgres"),
		TLS:      engine.TLSDisable,
	}
}

func TestConnectAndCanonicalizeIntegration(t *testing.T) {
	cc := harnessConn(t, "source")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	conn, err := connect(ctx, cc)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)

	// Verify each canonicalization GUC is actually set on the session.
	wantGUCs := map[string]string{
		"DateStyle":          "ISO, YMD",
		"TimeZone":           "UTC",
		"extra_float_digits": "3",
		"IntervalStyle":      "postgres",
		"bytea_output":       "hex",
		"client_encoding":    "UTF8",
	}
	for name, want := range wantGUCs {
		var got string
		if err := conn.QueryRow(ctx, "SHOW "+name).Scan(&got); err != nil {
			t.Errorf("SHOW %s: %v", name, err)
			continue
		}
		if got != want {
			t.Errorf("GUC %s = %q, want %q", name, got, want)
		}
	}
}

func TestServerVersionIntegration(t *testing.T) {
	src := harnessConn(t, "source")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	conn, err := connect(ctx, src)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)

	v, err := serverVersion(ctx, conn)
	if err != nil {
		t.Fatalf("serverVersion: %v", err)
	}
	// The harness source is old (9.x); guard against an obviously wrong parse.
	if v < 80000 || v > 200000 {
		t.Errorf("implausible server version %d", v)
	}
	t.Logf("source server_version_num=%d", v)
}
