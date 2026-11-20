package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/state"
	statepg "github.com/rudimk/replicare/internal/state/postgres"
)

func TestIndexesUsage(t *testing.T) {
	var out, errb bytes.Buffer
	if code := run([]string{"indexes"}, &out, &errb); code != 2 || !strings.Contains(errb.String(), "indexes restore") {
		t.Fatalf("bare `indexes` = %d, %q; want usage", code, errb.String())
	}
	errb.Reset()
	if code := run([]string{"indexes", "restore", "c.yml", "--sync"}, &out, &errb); code != 2 {
		t.Fatalf("--sync without a value = %d, want 2", code)
	}
	out.Reset()
	run([]string{"help"}, &out, &errb)
	if !strings.Contains(out.String(), "indexes restore") {
		t.Fatalf("help does not list `indexes restore`:\n%s", out.String())
	}
}

// TestIndexesRestoreIntegration: an index recorded as taken down (and missing on the
// target) is built by `replicare indexes restore`, and recorded as built; a second
// run has nothing to do.
func TestIndexesRestoreIntegration(t *testing.T) {
	if os.Getenv("REPLICARE_INTEGRATION") != "1" {
		t.Skip("integration test; set REPLICARE_INTEGRATION=1 and run `task harness:up`")
	}
	env := func(k, def string) string {
		if v := os.Getenv(k); v != "" {
			return v
		}
		return def
	}
	host, port, db := env("RC_DST_HOST", "127.0.0.1"), env("RC_DST_PORT", "5441"), env("RC_DST_DB", "replicare_dst")
	user, pass := env("RC_USER", "postgres"), env("RC_PASSWORD", "postgres")
	shost, sport, sdb := env("RC_SRC_HOST", "127.0.0.1"), env("RC_SRC_PORT", "5440"), env("RC_SRC_DB", "replicare_src")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	tgt, err := pgx.Connect(ctx, fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", user, pass, host, port, db))
	if err != nil {
		t.Fatal(err)
	}
	defer tgt.Close(context.Background())
	for _, q := range []string{
		"DROP SCHEMA IF EXISTS rc_ixr CASCADE",
		"DROP SCHEMA IF EXISTS replicare_state CASCADE",
		"CREATE SCHEMA rc_ixr",
		"CREATE TABLE rc_ixr.t (id int PRIMARY KEY, v text)",
		"INSERT INTO rc_ixr.t SELECT g, 'v'||g FROM generate_series(1,100) g",
	} {
		if _, err := tgt.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	t.Cleanup(func() {
		_, _ = tgt.Exec(context.Background(), "DROP SCHEMA IF EXISTS rc_ixr CASCADE")
		_, _ = tgt.Exec(context.Background(), "DROP SCHEMA IF EXISTS replicare_state CASCADE")
	})

	cfgPath := filepath.Join(t.TempDir(), "c.yml")
	conn := func(h, p, d string) string {
		return fmt.Sprintf("{ host: %s, port: %s, database: %s, user: %s, password: %s, sslmode: disable }", h, p, d, user, pass)
	}
	cfgYAML := fmt.Sprintf(`
state_store: { engine: postgres, postgres: %s }
sources: { src: { engine: postgres, postgres: %s } }
targets: { dst: { engine: postgres, postgres: %s } }
syncs:
  - name: ixr
    source: src
    targets: [dst]
    include: ["rc_ixr.*"]
`, conn(host, port, db), conn(shost, sport, sdb), conn(host, port, db))
	if err := os.WriteFile(cfgPath, []byte(cfgYAML), 0o600); err != nil {
		t.Fatal(err)
	}

	// Record an index as taken down, as the daemon does before a copy.
	store := statepg.New(engine.ConnConfig{Host: host, Port: atoiOr(port, 5441), Database: db, User: user, Password: pass, TLS: engine.TLSDisable})
	if err := store.Open(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close(context.Background()) }()
	if err := store.PutSync(ctx, state.SyncDef{Name: "ixr", Source: "src", Targets: []engine.TargetID{"dst"}}); err != nil {
		t.Fatal(err)
	}
	ix := engine.DeferredIndex{Table: engine.TableRef{Schema: "rc_ixr", Name: "t"}, Name: "t_v_idx",
		Definition: "CREATE INDEX t_v_idx ON rc_ixr.t USING btree (v)"}
	if err := store.SaveIndexes(ctx, "ixr", "dst", []engine.DeferredIndex{ix}); err != nil {
		t.Fatal(err)
	}

	var out, errb bytes.Buffer
	if code := run([]string{"indexes", "restore", cfgPath}, &out, &errb); code != 0 {
		t.Fatalf("restore = %d\nstdout: %s\nstderr: %s", code, out.String(), errb.String())
	}
	var valid bool
	if err := tgt.QueryRow(ctx, "SELECT indisvalid FROM pg_index WHERE indexrelid = 'rc_ixr.t_v_idx'::regclass").Scan(&valid); err != nil || !valid {
		t.Fatalf("t_v_idx after restore: valid=%v err=%v\n%s", valid, err, out.String())
	}
	saved, err := store.ListIndexes(ctx, "ixr")
	if err != nil || len(saved) != 1 || !saved[0].Built {
		t.Fatalf("saved after restore = %+v, %v; want one built", saved, err)
	}
	out.Reset()
	if code := run([]string{"indexes", "restore", cfgPath}, &out, &errb); code != 0 || !strings.Contains(out.String(), "no indexes to build") {
		t.Fatalf("second restore = %d, %q; want nothing to do", code, out.String())
	}
}

func atoiOr(s string, def int) int {
	var n int
	if _, err := fmt.Sscanf(s, "%d", &n); err != nil {
		return def
	}
	return n
}
