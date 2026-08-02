package mysql

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/rudimk/replicare/internal/config"
	_ "github.com/rudimk/replicare/internal/engine/postgres" // register postgres (state_store + cross-engine case)
)

const mysqlConfig = `
state_store:
  engine: postgres
  postgres: {host: localhost, database: replicare_state, user: replicare, password: pw, sslmode: disable}
sources:
  app:
    engine: mysql
    mysql: {host: src, database: app, user: repl, password: pw, tls: verify-full, local_infile: true}
targets:
  warehouse:
    engine: mysql
    mysql: {host: wh, database: warehouse, user: repl, password: pw, tls: require}
syncs:
  - name: app-to-wh
    source: app
    targets: [warehouse]
    include: ["app.*"]
`

const crossEngineConfig = `
state_store:
  engine: postgres
  postgres: {host: localhost, database: replicare_state, user: replicare, password: pw, sslmode: disable}
sources:
  app:
    engine: mysql
    mysql: {host: src, database: app, user: repl, password: pw}
targets:
  warehouse:
    engine: postgres
    postgres: {host: wh, database: warehouse, user: repl, password: pw}
syncs:
  - name: bad
    source: app
    targets: [warehouse]
    include: ["app.*"]
`

func writeTemp(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.yml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatalf("write temp config: %v", err)
	}
	return p
}

// TestMySQLConfigLoadsEndToEnd proves a full MySQL config dispatches through the
// neutral loader to the mysql block parser and resolves correctly (MM0.5).
func TestMySQLConfigLoadsEndToEnd(t *testing.T) {
	c, err := config.Load(writeTemp(t, mysqlConfig))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	src, ok := c.Sources["app"]
	if !ok {
		t.Fatal("source app missing")
	}
	if src.Engine != "mysql" {
		t.Fatalf("source engine = %q, want mysql", src.Engine)
	}
	cc := src.Conn.ConnConfig()
	if cc.Host != "src" || cc.Port != 3306 || cc.TLS != "verify-full" {
		t.Errorf("resolved source conn = %+v, want host=src port=3306 tls=verify-full", cc)
	}
	if cc.Params[paramLocalInfile] != "1" {
		t.Errorf("local_infile hint not threaded: %v", cc.Params)
	}
}

// TestSingleEngineRuleRejectsCrossEngine confirms the neutral single-engine rule
// (§6) rejects a MySQL source with a Postgres target.
func TestSingleEngineRuleRejectsCrossEngine(t *testing.T) {
	if _, err := config.Load(writeTemp(t, crossEngineConfig)); err == nil {
		t.Fatal("expected the single-engine rule to reject a mysql->postgres sync")
	}
}
