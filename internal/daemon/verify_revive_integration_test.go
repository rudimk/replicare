package daemon

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/rudimk/replicare/internal/config"
	"github.com/rudimk/replicare/internal/observability/live"
)

// TestVerifyCarriesOnAfterLostConnection: when the source connection is lost during one
// table's check (in production a read past its time limit, which makes pgx close the
// connection), that table reports the error and every table after it is still checked
// on a fresh connection. Before, every later table failed with "conn closed".
func TestVerifyCarriesOnAfterLostConnection(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	src := dial(t, ctx, envd("RC_SRC_HOST", "127.0.0.1"), envd("RC_SRC_PORT", "5440"), envd("RC_SRC_DB", "replicare_src"))
	defer src.Close(context.Background())
	tgt := dial(t, ctx, envd("RC_DST_HOST", "127.0.0.1"), envd("RC_DST_PORT", "5441"), envd("RC_DST_DB", "replicare_dst"))
	defer tgt.Close(context.Background())
	for _, c := range []*pgx.Conn{src, tgt} {
		mustExec(t, ctx, c, "DROP SCHEMA IF EXISTS rc_vr CASCADE")
		mustExec(t, ctx, c, "CREATE SCHEMA rc_vr")
		for _, tbl := range []string{"a", "b", "c"} {
			mustExec(t, ctx, c, "CREATE TABLE rc_vr."+tbl+" (id int PRIMARY KEY, v int)")
			mustExec(t, ctx, c, "INSERT INTO rc_vr."+tbl+" SELECT g, g FROM generate_series(1, 1000) g")
		}
	}
	t.Cleanup(func() {
		_, _ = src.Exec(context.Background(), "DROP SCHEMA IF EXISTS rc_vr CASCADE")
		_, _ = tgt.Exec(context.Background(), "DROP SCHEMA IF EXISTS rc_vr CASCADE")
	})
	cfg, err := config.Load(writeConfig(t, harnessConfigYAML(`
syncs:
  - name: s1
    source: src
    targets: [dst]
    include: ["rc_vr.*"]
`)))
	if err != nil {
		t.Fatal(err)
	}

	// Cut verify's source connection after the first range of the first table checked.
	var once sync.Once
	first := ""
	opts := live.VerifyOptions{ChunkRows: 300, Pause: -1, RecheckAfter: -1, Progress: func(table string, _ int64) {
		once.Do(func() {
			first = table
			mustExec(t, ctx, src, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity
				WHERE datname = current_database() AND pid <> pg_backend_pid() AND backend_type = 'client backend'`)
		})
	}}
	vr := live.New(cfg, 20*time.Second).VerifyWith(ctx, "s1", opts)
	if vr.Error != "" {
		t.Fatalf("verify stopped: %q", vr.Error)
	}
	for _, name := range []string{"rc_vr.a", "rc_vr.b", "rc_vr.c"} {
		row := findVerifyTable(vr, "dst", name)
		if row == nil {
			t.Fatalf("%s not checked: %+v", name, vr)
		}
		if name == first {
			if row.Status != "error" {
				t.Errorf("%s (connection cut while checking it) = %+v, want an error", name, row)
			}
			continue
		}
		if row.Status != "ok" || strings.Contains(row.Error, "conn closed") {
			t.Errorf("%s after the lost connection = %+v, want ok on a fresh connection", name, row)
		}
	}
	if first == "" {
		t.Fatal("progress was never reported")
	}
}
