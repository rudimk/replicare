package daemon

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/rudimk/replicare/internal/config"
	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/observability/live"
)

// TestSkipInitialCopyIntegration follows the cutover order: capture is installed by
// hand while the two sides match, the source keeps changing, then a sync with
// skip_initial_copy starts. It copies nothing (a target-side trigger counts every row
// written), streams both the changes queued before it started and those made after, and
// once caught up checks each table: the ones that match are cleared, the one missing a
// row on the target is reported as drift and stays marked. A restart copies nothing and
// checks only the drifted table again.
func TestSkipInitialCopyIntegration(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), daemonTestBudget)
	defer cancel()
	old := skipCheckOptions
	skipCheckOptions = live.VerifyOptions{ChunkRows: 100, Pause: -1, RecheckAfter: time.Second}
	t.Cleanup(func() { skipCheckOptions = old })

	src := dial(t, ctx, envd("RC_SRC_HOST", "127.0.0.1"), envd("RC_SRC_PORT", "5440"), envd("RC_SRC_DB", "replicare_src"))
	defer src.Close(context.Background())
	tgt := dial(t, ctx, envd("RC_DST_HOST", "127.0.0.1"), envd("RC_DST_PORT", "5441"), envd("RC_DST_DB", "replicare_dst"))
	defer tgt.Close(context.Background())

	for _, c := range []*pgx.Conn{src, tgt} {
		mustExec(t, ctx, c, "DROP SCHEMA IF EXISTS rc_sk CASCADE")
		mustExec(t, ctx, c, "CREATE SCHEMA rc_sk")
		mustExec(t, ctx, c, "CREATE TABLE rc_sk.customers (id int PRIMARY KEY, name text)")
		mustExec(t, ctx, c, "CREATE TABLE rc_sk.orders (id int PRIMARY KEY, customer_id int NOT NULL REFERENCES rc_sk.customers(id), note text)")
		mustExec(t, ctx, c, "CREATE TABLE rc_sk.drifty (id int PRIMARY KEY, v text)")
		mustExec(t, ctx, c, "INSERT INTO rc_sk.customers SELECT g, 'c'||g FROM generate_series(1,50) g")
		mustExec(t, ctx, c, "INSERT INTO rc_sk.orders SELECT g, 1 + g % 50, 'o'||g FROM generate_series(1,500) g")
		mustExec(t, ctx, c, "INSERT INTO rc_sk.drifty SELECT g, 'd'||g FROM generate_series(1,300) g")
	}
	mustExec(t, ctx, tgt, "DELETE FROM rc_sk.drifty WHERE id = 7")
	// Count every row written to the target's tables: a copy would write hundreds.
	mustExec(t, ctx, tgt, "DROP SCHEMA IF EXISTS rc_sk_audit CASCADE")
	mustExec(t, ctx, tgt, "CREATE SCHEMA rc_sk_audit")
	mustExec(t, ctx, tgt, "CREATE TABLE rc_sk_audit.writes (tbl text)")
	mustExec(t, ctx, tgt, `CREATE FUNCTION rc_sk_audit.note() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN INSERT INTO rc_sk_audit.writes VALUES (TG_TABLE_NAME); RETURN NULL; END $$`)
	for _, tbl := range []string{"customers", "orders", "drifty"} {
		mustExec(t, ctx, tgt, "CREATE TRIGGER audit AFTER INSERT OR UPDATE ON rc_sk."+tbl+
			" FOR EACH ROW EXECUTE FUNCTION rc_sk_audit.note()")
	}
	mustExec(t, ctx, src, "DROP SCHEMA IF EXISTS replicare CASCADE")
	mustExec(t, ctx, tgt, "DROP SCHEMA IF EXISTS replicare_state CASCADE")
	t.Cleanup(func() {
		bg := context.Background()
		_, _ = src.Exec(bg, "DROP SCHEMA IF EXISTS rc_sk CASCADE")
		_, _ = src.Exec(bg, "DROP SCHEMA IF EXISTS replicare CASCADE")
		_, _ = tgt.Exec(bg, "DROP SCHEMA IF EXISTS rc_sk CASCADE")
		_, _ = tgt.Exec(bg, "DROP SCHEMA IF EXISTS rc_sk_audit CASCADE")
		_, _ = tgt.Exec(bg, "DROP SCHEMA IF EXISTS replicare_state CASCADE")
	})

	cfg, err := config.Load(writeConfig(t, harnessConfigYAML(`
syncs:
  - name: sk
    source: src
    targets: [dst]
    include: ["rc_sk.*"]
    tuning: { drain_interval: 100ms }
    skip_initial_copy: { enabled: true }
`)))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	// Install capture by hand (as `replicare capture install` does), then change the
	// source before the sync starts: those changes wait in the queue.
	eng, err := engine.Get("postgres")
	if err != nil {
		t.Fatal(err)
	}
	capSrc, err := eng.NewSource(cfg.Sources["src"].Conn.ConnConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := capSrc.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	schema, err := capSrc.Introspect(ctx, engine.Selection{Include: []string{"rc_sk.*"}})
	if err != nil {
		t.Fatal(err)
	}
	refs := make([]engine.TableRef, 0, len(schema.Tables))
	for _, tb := range schema.Tables {
		refs = append(refs, tb.Ref)
	}
	if err := capSrc.InstallCapture(ctx, refs); err != nil {
		t.Fatal(err)
	}
	_ = capSrc.Close(ctx)
	mustExec(t, ctx, src, "INSERT INTO rc_sk.customers VALUES (51, 'queued')")
	mustExec(t, ctx, src, "INSERT INTO rc_sk.orders VALUES (502, 51, 'queued')")
	mustExec(t, ctx, src, "DELETE FROM rc_sk.orders WHERE id = 11")
	mustExec(t, ctx, src, "UPDATE rc_sk.drifty SET v = 'queued' WHERE id = 20")

	run := func() (stop func()) {
		d, err := New(cfg, nil)
		if err != nil {
			t.Fatalf("new daemon: %v", err)
		}
		runCtx, cancelRun := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- d.Run(runCtx) }()
		return func() {
			cancelRun()
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("daemon Run returned %v", err)
				}
			case <-time.After(15 * time.Second):
				t.Fatal("daemon did not stop within 15s")
			}
		}
	}
	stop := run()

	// Bring-up marks all three tables copied without copying them.
	if !pollUntil(t, copyTimeout, func() bool {
		return count(t, ctx, tgt, "replicare_state.copy_progress WHERE sync = 'sk' AND done") == 3
	}) {
		t.Fatalf("tables not marked copied: %d", count(t, ctx, tgt, "replicare_state.copy_progress WHERE sync = 'sk' AND done"))
	}
	if n := count(t, ctx, tgt, "replicare_state.events WHERE sync = 'sk' AND event = 'copy.skipped'"); n != 1 {
		t.Errorf("copy.skipped events = %d, want 1", n)
	}

	// Changes queued before the start, and made after it, all stream.
	mustExec(t, ctx, src, "INSERT INTO rc_sk.orders VALUES (501, 3, 'new')")
	mustExec(t, ctx, src, "UPDATE rc_sk.customers SET name = 'renamed' WHERE id = 4")
	mustExec(t, ctx, src, "DELETE FROM rc_sk.orders WHERE id = 10")
	if !pollUntil(t, convergeTimeout, func() bool {
		return count(t, ctx, tgt, "rc_sk.orders WHERE id = 502") == 1 &&
			count(t, ctx, tgt, "rc_sk.orders WHERE id = 11") == 0 &&
			count(t, ctx, tgt, "rc_sk.drifty WHERE v = 'queued'") == 1 &&
			count(t, ctx, tgt, "rc_sk.orders WHERE id = 501") == 1 &&
			count(t, ctx, tgt, "rc_sk.customers WHERE name = 'renamed'") == 1 &&
			count(t, ctx, tgt, "rc_sk.orders WHERE id = 10") == 0
	}) {
		t.Fatal("changes queued before the start or made after it did not stream")
	}

	// Once caught up: customers and orders match and are cleared; drifty is reported.
	if !pollUntil(t, convergeTimeout, func() bool {
		return count(t, ctx, tgt, "replicare_state.events WHERE sync = 'sk' AND event = 'copy.skip_check_ok'") == 2 &&
			count(t, ctx, tgt, "replicare_state.events WHERE sync = 'sk' AND event = 'copy.skip_check_drift'") == 1
	}) {
		t.Fatalf("check results: ok=%d drift=%d",
			count(t, ctx, tgt, "replicare_state.events WHERE sync = 'sk' AND event = 'copy.skip_check_ok'"),
			count(t, ctx, tgt, "replicare_state.events WHERE sync = 'sk' AND event = 'copy.skip_check_drift'"))
	}
	var drifted, msg string
	if err := tgt.QueryRow(ctx, `SELECT table_name, message FROM replicare_state.events
		WHERE sync = 'sk' AND event = 'copy.skip_check_drift'`).Scan(&drifted, &msg); err != nil {
		t.Fatal(err)
	}
	if drifted != "drifty" {
		t.Errorf("drift reported for %q, want drifty (%s)", drifted, msg)
	}
	if n := count(t, ctx, tgt, "replicare_state.copy_progress WHERE sync = 'sk' AND skipped"); n != 1 {
		t.Errorf("tables still marked skipped = %d, want 1 (drifty)", n)
	}
	stop()

	// A restart copies nothing either, and checks drifty again.
	stop = run()
	if !pollUntil(t, convergeTimeout, func() bool {
		return count(t, ctx, tgt, "replicare_state.events WHERE sync = 'sk' AND event = 'copy.skip_check_drift'") == 2
	}) {
		t.Fatal("drifty was not checked again after a restart")
	}
	stop()
	if n := count(t, ctx, tgt, "replicare_state.events WHERE sync = 'sk' AND event = 'copy.skip_check_ok'"); n != 2 {
		t.Errorf("copy.skip_check_ok events after the restart = %d, want 2 (matched tables are not checked again)", n)
	}
	if n := count(t, ctx, tgt, "rc_sk_audit.writes"); n > 10 {
		t.Errorf("%d rows written to the target, want only the streamed changes: the copy was not skipped", n)
	}
	if n := count(t, ctx, tgt, "rc_sk.drifty WHERE id = 7"); n != 0 {
		t.Errorf("drifty row 7 is on the target: nothing should have repaired it")
	}
}
