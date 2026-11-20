package daemon

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/rudimk/replicare/internal/config"
	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/state"
	statepg "github.com/rudimk/replicare/internal/state/postgres"
)

// TestDaemonDefersAndRebuildsTargetIndexes: by default a Postgres sync takes the
// target's plain indexes down for the initial copy (recording each first), and once
// every table streams builds them all again, valid and identical, while keeping
// primary keys and unique indexes throughout. Covers an acyclic pair and a table
// that points at itself (the cyclic copy path).
func TestDaemonDefersAndRebuildsTargetIndexes(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), daemonTestBudget)
	defer cancel()
	src, tgt := indexTestDBs(t, ctx)

	defs := indexDefs(t, ctx, tgt)
	if len(defs) != 4 {
		t.Fatalf("target starts with %d plain indexes, want 4: %v", len(defs), defs)
	}
	uniqueBefore := count(t, ctx, tgt, "pg_indexes WHERE schemaname = 'rc_ix' AND indexdef LIKE 'CREATE UNIQUE%'")

	d := startIndexDaemon(t, ctx, "")
	defer d.stop()

	if !pollUntil(t, copyTimeout, func() bool {
		return count(t, ctx, tgt, "rc_ix.child") == 500 && count(t, ctx, tgt, "rc_ix.node") == 100
	}) {
		t.Fatalf("initial copy did not converge")
	}
	// Every plain index is back, valid and identical, and recorded as built.
	if !pollUntil(t, convergeTimeout, func() bool {
		return count(t, ctx, tgt, "replicare_state.deferred_indexes WHERE sync = 'ix' AND built") == 4
	}) {
		t.Fatalf("indexes not rebuilt: %d of 4 built", count(t, ctx, tgt, "replicare_state.deferred_indexes WHERE sync = 'ix' AND built"))
	}
	after := indexDefs(t, ctx, tgt)
	for name, def := range defs {
		if after[name] != def {
			t.Errorf("index %s rebuilt as %q, want %q", name, after[name], def)
		}
	}
	if n := count(t, ctx, tgt, "pg_index x JOIN pg_class i ON i.oid = x.indexrelid JOIN pg_namespace n ON n.oid = i.relnamespace WHERE n.nspname = 'rc_ix' AND NOT x.indisvalid"); n != 0 {
		t.Errorf("%d invalid indexes left on the target", n)
	}
	if n := count(t, ctx, tgt, "pg_indexes WHERE schemaname = 'rc_ix' AND indexdef LIKE 'CREATE UNIQUE%'"); n != uniqueBefore {
		t.Errorf("unique indexes: %d, want %d untouched", n, uniqueBefore)
	}
	if n := count(t, ctx, tgt, "replicare_state.events WHERE sync = 'ix' AND event = 'index.dropped'"); n != 4 {
		t.Errorf("index.dropped events = %d, want 4", n)
	}
	if n := count(t, ctx, tgt, "replicare_state.events WHERE sync = 'ix' AND event = 'index.rebuild_done'"); n != 1 {
		t.Errorf("index.rebuild_done events = %d, want 1", n)
	}

	// Streaming carries on as normal.
	mustExec(t, ctx, src, "INSERT INTO rc_ix.child VALUES (501, 1, 'late')")
	mustExec(t, ctx, src, "DELETE FROM rc_ix.child WHERE id = 7")
	if !pollUntil(t, convergeTimeout, func() bool {
		return count(t, ctx, tgt, "rc_ix.child WHERE id = 501") == 1 && count(t, ctx, tgt, "rc_ix.child WHERE id = 7") == 0
	}) {
		t.Fatal("streaming did not converge after the rebuild")
	}
	// A reseed re-copies the target: it takes the indexes down again once the tables
	// are emptied, and builds them all again afterwards.
	mustExec(t, ctx, tgt, "UPDATE replicare_state.cursors SET needs_reseed = true WHERE sync = 'ix'")
	if !pollUntil(t, convergeTimeout, func() bool {
		return count(t, ctx, tgt, "replicare_state.events WHERE sync = 'ix' AND event = 'index.rebuild_done'") == 2 &&
			count(t, ctx, tgt, "replicare_state.deferred_indexes WHERE sync = 'ix' AND built") == 4
	}) {
		t.Fatalf("after a reseed: %d index.dropped, %d rebuild_done, %d built; want 8, 2, 4",
			count(t, ctx, tgt, "replicare_state.events WHERE sync = 'ix' AND event = 'index.dropped'"),
			count(t, ctx, tgt, "replicare_state.events WHERE sync = 'ix' AND event = 'index.rebuild_done'"),
			count(t, ctx, tgt, "replicare_state.deferred_indexes WHERE sync = 'ix' AND built"))
	}
	if n := count(t, ctx, tgt, "replicare_state.events WHERE sync = 'ix' AND event = 'index.dropped'"); n != 8 {
		t.Errorf("index.dropped events after a reseed = %d, want 8", n)
	}
	if got := indexDefs(t, ctx, tgt); len(got) != len(defs) {
		t.Errorf("after a reseed the target has %d plain indexes, want %d", len(got), len(defs))
	}
	if !pollUntil(t, convergeTimeout, func() bool {
		return count(t, ctx, tgt, "rc_ix.child") == 500 && count(t, ctx, tgt, "rc_ix.child WHERE id = 501") == 1
	}) {
		t.Fatalf("target not converged after the reseed: %d child rows", count(t, ctx, tgt, "rc_ix.child"))
	}

	if m := scrapeMetrics(t, d.d); !strings.Contains(m, `replicare_deferred_indexes_built{sync="ix",target="dst"} 4`) {
		t.Errorf("metrics do not show 4 indexes built:\n%s", grepLines(m, "deferred_indexes"))
	}
}

// TestDaemonKeepsTargetIndexesWhenDeferralIsOff: defer_indexes.enabled: false copies
// with the target's indexes in place and records nothing.
func TestDaemonKeepsTargetIndexesWhenDeferralIsOff(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), daemonTestBudget)
	defer cancel()
	_, tgt := indexTestDBs(t, ctx)
	var oidBefore uint32
	if err := tgt.QueryRow(ctx, "SELECT 'rc_ix.child_note_idx'::regclass::oid").Scan(&oidBefore); err != nil {
		t.Fatal(err)
	}

	d := startIndexDaemon(t, ctx, "    defer_indexes: { enabled: false }\n")
	defer d.stop()
	if !pollUntil(t, copyTimeout, func() bool {
		return count(t, ctx, tgt, "rc_ix.child") == 500 && count(t, ctx, tgt, "rc_ix.node") == 100
	}) {
		t.Fatalf("initial copy did not converge")
	}
	var oidAfter uint32
	if err := tgt.QueryRow(ctx, "SELECT 'rc_ix.child_note_idx'::regclass::oid").Scan(&oidAfter); err != nil {
		t.Fatal(err)
	}
	if oidAfter != oidBefore {
		t.Errorf("child_note_idx was dropped and recreated with deferral off")
	}
	if n := count(t, ctx, tgt, "replicare_state.deferred_indexes"); n != 0 {
		t.Errorf("%d indexes recorded with deferral off, want 0", n)
	}
}

// indexTestDBs creates the same tables on source and target, with plain, unique and
// constraint indexes on the target, and seeds the source.
func indexTestDBs(t *testing.T, ctx context.Context) (src, tgt *pgx.Conn) {
	t.Helper()
	src = dial(t, ctx, envd("RC_SRC_HOST", "127.0.0.1"), envd("RC_SRC_PORT", "5440"), envd("RC_SRC_DB", "replicare_src"))
	tgt = dial(t, ctx, envd("RC_DST_HOST", "127.0.0.1"), envd("RC_DST_PORT", "5441"), envd("RC_DST_DB", "replicare_dst"))
	t.Cleanup(func() {
		bg := context.Background()
		_, _ = src.Exec(bg, "DROP SCHEMA IF EXISTS rc_ix CASCADE")
		_, _ = src.Exec(bg, "DROP SCHEMA IF EXISTS replicare CASCADE")
		_, _ = tgt.Exec(bg, "DROP SCHEMA IF EXISTS rc_ix CASCADE")
		_, _ = tgt.Exec(bg, "DROP SCHEMA IF EXISTS replicare_state CASCADE")
		_ = src.Close(bg)
		_ = tgt.Close(bg)
	})
	ddl := []string{
		"CREATE TABLE rc_ix.parent (id int PRIMARY KEY, email text UNIQUE, name text)",
		"CREATE TABLE rc_ix.child (id int PRIMARY KEY, parent_id int NOT NULL REFERENCES rc_ix.parent(id), note text)",
		"CREATE TABLE rc_ix.node (id int PRIMARY KEY, up int REFERENCES rc_ix.node(id), label text)",
	}
	for _, c := range []*pgx.Conn{src, tgt} {
		mustExec(t, ctx, c, "DROP SCHEMA IF EXISTS rc_ix CASCADE")
		mustExec(t, ctx, c, "CREATE SCHEMA rc_ix")
		for _, q := range ddl {
			mustExec(t, ctx, c, q)
		}
	}
	for _, q := range []string{
		"CREATE INDEX parent_name_idx ON rc_ix.parent (name)",
		"CREATE INDEX child_parent_id_idx ON rc_ix.child (parent_id)",
		"CREATE INDEX child_note_idx ON rc_ix.child (lower(note)) WHERE note IS NOT NULL",
		"CREATE INDEX node_up_idx ON rc_ix.node (up)",
		"CREATE UNIQUE INDEX node_label_uq ON rc_ix.node (label)",
	} {
		mustExec(t, ctx, tgt, q)
	}
	mustExec(t, ctx, src, "DROP SCHEMA IF EXISTS replicare CASCADE")
	mustExec(t, ctx, tgt, "DROP SCHEMA IF EXISTS replicare_state CASCADE")
	mustExec(t, ctx, src, "INSERT INTO rc_ix.parent SELECT g, 'e'||g, 'n'||g FROM generate_series(1,50) g")
	mustExec(t, ctx, src, "INSERT INTO rc_ix.child SELECT g, 1 + g % 50, 'Note '||g FROM generate_series(1,500) g")
	mustExec(t, ctx, src, "INSERT INTO rc_ix.node SELECT g, CASE WHEN g > 1 THEN g / 2 END, 'l'||g FROM generate_series(1,100) g")
	return src, tgt
}

// indexDefs maps each plain (non-unique) index on the rc_ix tables to its definition.
func indexDefs(t *testing.T, ctx context.Context, c *pgx.Conn) map[string]string {
	t.Helper()
	rows, err := c.Query(ctx, "SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = 'rc_ix' AND indexdef NOT LIKE 'CREATE UNIQUE%'")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var n, d string
		if err := rows.Scan(&n, &d); err != nil {
			t.Fatal(err)
		}
		out[n] = d
	}
	return out
}

type runningDaemon struct {
	d    *Daemon
	stop func()
}

func startIndexDaemon(t *testing.T, ctx context.Context, extra string) runningDaemon {
	t.Helper()
	cfg, err := config.Load(writeConfig(t, harnessConfigYAML(`
syncs:
  - name: ix
    source: src
    targets: [dst]
    include: ["rc_ix.*"]
    tuning: { drain_interval: 100ms }
`+extra)))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	d, err := New(cfg, nil)
	if err != nil {
		t.Fatalf("new daemon: %v", err)
	}
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- d.Run(runCtx) }()
	return runningDaemon{d: d, stop: func() {
		stop()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("daemon Run returned %v, want nil on graceful shutdown", err)
			}
		case <-time.After(15 * time.Second):
			t.Error("daemon did not stop within 15s of cancellation")
		}
	}}
}

func grepLines(s, sub string) string {
	var out []string
	for _, line := range strings.Split(s, "\n") {
		if strings.Contains(line, sub) {
			out = append(out, line)
		}
	}
	return strings.Join(out, "\n")
}

// TestDaemonRebuildsIndexesLeftByAnEarlierRun: an index recorded as taken down by an
// earlier run (which then stopped, and whose setting was since turned off) is still
// built again once the sync streams, so a target is never left without it.
func TestDaemonRebuildsIndexesLeftByAnEarlierRun(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), daemonTestBudget)
	defer cancel()
	_, tgt := indexTestDBs(t, ctx)
	def := indexDefs(t, ctx, tgt)["node_up_idx"]
	mustExec(t, ctx, tgt, "DROP INDEX rc_ix.node_up_idx")

	port, err := strconv.Atoi(envd("RC_DST_PORT", "5441"))
	if err != nil {
		t.Fatal(err)
	}
	store := statepg.New(engine.ConnConfig{Host: envd("RC_DST_HOST", "127.0.0.1"), Port: port,
		Database: envd("RC_DST_DB", "replicare_dst"), User: envd("RC_USER", "postgres"), Password: envd("RC_PASSWORD", "postgres"), TLS: engine.TLSDisable})
	if err := store.Open(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close(context.Background()) }()
	if err := store.PutSync(ctx, state.SyncDef{Name: "ix", Source: "src", Targets: []engine.TargetID{"dst"}}); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveIndexes(ctx, "ix", "dst", []engine.DeferredIndex{{
		Table: engine.TableRef{Schema: "rc_ix", Name: "node"}, Name: "node_up_idx", Definition: def, Link: true}}); err != nil {
		t.Fatal(err)
	}

	d := startIndexDaemon(t, ctx, "    defer_indexes: { enabled: false }\n")
	defer d.stop()
	if !pollUntil(t, convergeTimeout, func() bool {
		return count(t, ctx, tgt, "replicare_state.deferred_indexes WHERE sync = 'ix' AND built") == 1
	}) {
		t.Fatal("the index left by the earlier run was not built again")
	}
	if got := indexDefs(t, ctx, tgt)["node_up_idx"]; got != def {
		t.Errorf("node_up_idx = %q, want %q", got, def)
	}
}
