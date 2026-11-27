package daemon

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/rudimk/replicare/internal/config"
	"github.com/rudimk/replicare/internal/observability/live"
)

// triggerTestDBs builds the production case on both sides: a table whose own trigger
// appends a version row to a second table (with ids from that database's own sequence),
// a trigger that stamps a column on its way in, and an append-only guard on the versions
// table, set on the target to fire always. Without trigger handling, replicare's copy
// fires all three on the target: the version rows clash with the copied ones (23505),
// the stamp is applied twice, and the guard refuses replicare's own updates.
func triggerTestDBs(t *testing.T, ctx context.Context) (src, tgt *pgx.Conn) {
	t.Helper()
	src = dial(t, ctx, envd("RC_SRC_HOST", "127.0.0.1"), envd("RC_SRC_PORT", "5440"), envd("RC_SRC_DB", "replicare_src"))
	tgt = dial(t, ctx, envd("RC_DST_HOST", "127.0.0.1"), envd("RC_DST_PORT", "5441"), envd("RC_DST_DB", "replicare_dst"))
	t.Cleanup(func() {
		bg := context.Background()
		_, _ = src.Exec(bg, "DROP SCHEMA IF EXISTS rc_tt CASCADE")
		_, _ = src.Exec(bg, "DROP SCHEMA IF EXISTS replicare CASCADE")
		_, _ = tgt.Exec(bg, "DROP SCHEMA IF EXISTS rc_tt CASCADE")
		_, _ = tgt.Exec(bg, "DROP SCHEMA IF EXISTS replicare_state CASCADE")
		_ = src.Close(bg)
		_ = tgt.Close(bg)
	})
	ddl := []string{
		"CREATE TABLE rc_tt.items (id int PRIMARY KEY, name text NOT NULL)",
		"CREATE TABLE rc_tt.item_versions (id bigserial PRIMARY KEY, item_id int NOT NULL, name text, op text NOT NULL)",
		`CREATE FUNCTION rc_tt.append_version() RETURNS trigger LANGUAGE plpgsql AS $$
		 BEGIN
		   IF TG_OP = 'DELETE' THEN
		     INSERT INTO rc_tt.item_versions (item_id, name, op) VALUES (OLD.id, OLD.name, TG_OP);
		   ELSE
		     INSERT INTO rc_tt.item_versions (item_id, name, op) VALUES (NEW.id, NEW.name, TG_OP);
		   END IF;
		   RETURN NULL;
		 END $$`,
		`CREATE FUNCTION rc_tt.stamp() RETURNS trigger LANGUAGE plpgsql AS $$
		 BEGIN NEW.name := NEW.name || '!'; RETURN NEW; END $$`,
		`CREATE FUNCTION rc_tt.deny() RETURNS trigger LANGUAGE plpgsql AS $$
		 BEGIN RAISE EXCEPTION 'item_versions is append-only'; END $$`,
		"CREATE TRIGGER items_stamp BEFORE INSERT OR UPDATE ON rc_tt.items FOR EACH ROW EXECUTE PROCEDURE rc_tt.stamp()",
		"CREATE TRIGGER items_versions_append AFTER INSERT OR UPDATE OR DELETE ON rc_tt.items FOR EACH ROW EXECUTE PROCEDURE rc_tt.append_version()",
		"CREATE TRIGGER item_versions_append_only BEFORE UPDATE OR DELETE ON rc_tt.item_versions FOR EACH ROW EXECUTE PROCEDURE rc_tt.deny()",
	}
	for _, c := range []*pgx.Conn{src, tgt} {
		mustExec(t, ctx, c, "DROP SCHEMA IF EXISTS rc_tt CASCADE")
		mustExec(t, ctx, c, "CREATE SCHEMA rc_tt")
		for _, q := range ddl {
			mustExec(t, ctx, c, q)
		}
	}
	mustExec(t, ctx, tgt, "ALTER TABLE rc_tt.item_versions ENABLE ALWAYS TRIGGER item_versions_append_only")
	mustExec(t, ctx, src, "DROP SCHEMA IF EXISTS replicare CASCADE")
	mustExec(t, ctx, tgt, "DROP SCHEMA IF EXISTS replicare_state CASCADE")
	mustExec(t, ctx, src, "INSERT INTO rc_tt.items SELECT g, 'n'||g FROM generate_series(1,20) g")
	return src, tgt
}

func triggerMode(t *testing.T, ctx context.Context, c *pgx.Conn, name string) string {
	t.Helper()
	var m string
	if err := c.QueryRow(ctx, "SELECT tgenabled::text FROM pg_trigger WHERE tgname = $1", name).Scan(&m); err != nil {
		t.Fatalf("trigger %s: %v", name, err)
	}
	return m
}

// sameRows reports whether a query returns the same rows on both sides.
func sameRows(t *testing.T, ctx context.Context, a, b *pgx.Conn, q string) bool {
	t.Helper()
	read := func(c *pgx.Conn) string {
		var s string
		if err := c.QueryRow(ctx, "SELECT COALESCE(string_agg(r::text, '|' ORDER BY r::text), '') FROM ("+q+") r").Scan(&s); err != nil {
			t.Fatal(err)
		}
		return s
	}
	return read(a) == read(b)
}

// TestDaemonKeepsTargetTriggersFromFiring: the target's own triggers do not fire on
// replicare's writes, during the copy or while streaming. The ordinary ones are skipped
// through replica mode and left exactly as they were; the one that fires always is
// switched off (recorded first) for as long as the sync runs, and pausing the sync
// switches it back on, in its own mode.
func TestDaemonKeepsTargetTriggersFromFiring(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), daemonTestBudget)
	defer cancel()
	src, tgt := triggerTestDBs(t, ctx)
	syncs := `
syncs:
  - name: tt
    source: src
    targets: [dst]
    include: ["rc_tt.*"]
    tuning: { drain_interval: 100ms }
`
	d := startDaemonFromYAML(t, ctx, syncs)

	const items = "SELECT id, name FROM rc_tt.items"
	const versions = "SELECT id, item_id, name, op FROM rc_tt.item_versions"
	if !pollUntil(t, copyTimeout, func() bool {
		return count(t, ctx, tgt, "rc_tt.items") == 20 && count(t, ctx, tgt, "rc_tt.item_versions") == 20
	}) {
		t.Fatalf("copy did not converge: %d items, %d versions on the target (want 20 and 20)",
			count(t, ctx, tgt, "rc_tt.items"), count(t, ctx, tgt, "rc_tt.item_versions"))
	}
	if !sameRows(t, ctx, src, tgt, items) || !sameRows(t, ctx, src, tgt, versions) {
		t.Fatal("target differs from the source after the copy (a trigger fired on replicare's writes)")
	}
	if m := triggerMode(t, ctx, tgt, "item_versions_append_only"); m != "D" {
		t.Errorf("always-firing guard is in mode %q while the sync runs, want switched off (D)", m)
	}
	for _, name := range []string{"items_stamp", "items_versions_append"} {
		if m := triggerMode(t, ctx, tgt, name); m != "O" {
			t.Errorf("ordinary trigger %s is in mode %q, want left as it was (O)", name, m)
		}
	}
	if n := count(t, ctx, tgt, "replicare_state.disabled_triggers WHERE sync = 'tt'"); n != 1 {
		t.Errorf("%d switched-off triggers recorded, want 1", n)
	}
	if n := count(t, ctx, tgt, "replicare_state.events WHERE sync = 'tt' AND event = 'trigger.replica_mode'"); n != 1 {
		t.Errorf("trigger.replica_mode events = %d, want 1", n)
	}

	// Streaming: an update and a delete on the source each add a version there, and
	// replicare's upsert of the versions table is not refused by the guard.
	mustExec(t, ctx, src, "UPDATE rc_tt.items SET name = 'z' WHERE id = 5")
	mustExec(t, ctx, src, "DELETE FROM rc_tt.items WHERE id = 6")
	if !pollUntil(t, convergeTimeout, func() bool {
		return count(t, ctx, tgt, "rc_tt.item_versions") == 22 && count(t, ctx, tgt, "rc_tt.items") == 19
	}) {
		t.Fatalf("streaming did not converge: %d versions (want 22), %d items (want 19)",
			count(t, ctx, tgt, "rc_tt.item_versions"), count(t, ctx, tgt, "rc_tt.items"))
	}
	if !sameRows(t, ctx, src, tgt, items) || !sameRows(t, ctx, src, tgt, versions) {
		t.Fatal("target differs from the source while streaming")
	}
	// A replayed version row (at-least-once) is rewritten with its own value: the guard
	// must not be in the way.
	mustExec(t, ctx, src, "INSERT INTO rc_tt.items VALUES (21, 'late')")
	if !pollUntil(t, convergeTimeout, func() bool { return count(t, ctx, tgt, "rc_tt.item_versions") == 23 }) {
		t.Fatal("streaming stopped")
	}
	d.stop()

	// Pausing the sync (the cutover step) switches the guard back on in its own mode.
	d = startDaemonFromYAML(t, ctx, `
syncs:
  - name: tt
    enabled: false
    source: src
    targets: [dst]
    include: ["rc_tt.*"]
`)
	defer d.stop()
	if !pollUntil(t, convergeTimeout, func() bool {
		return triggerMode(t, ctx, tgt, "item_versions_append_only") == "A" &&
			count(t, ctx, tgt, "replicare_state.disabled_triggers WHERE sync = 'tt'") == 0
	}) {
		t.Fatalf("pausing the sync did not switch the guard back on: mode %q, %d still recorded",
			triggerMode(t, ctx, tgt, "item_versions_append_only"), count(t, ctx, tgt, "replicare_state.disabled_triggers WHERE sync = 'tt'"))
	}
	// The application's own writes on the target fire every trigger again.
	mustExec(t, ctx, tgt, "SELECT setval('rc_tt.item_versions_id_seq', 1000)")
	mustExec(t, ctx, tgt, "INSERT INTO rc_tt.items VALUES (100, 'app')")
	if n := count(t, ctx, tgt, "rc_tt.items WHERE id = 100 AND name = 'app!'"); n != 1 {
		t.Error("the stamp trigger did not fire on the application's own write")
	}
	if n := count(t, ctx, tgt, "rc_tt.item_versions WHERE item_id = 100"); n != 1 {
		t.Error("the version trigger did not fire on the application's own write")
	}
	if _, err := tgt.Exec(ctx, "DELETE FROM rc_tt.item_versions WHERE item_id = 100"); err == nil {
		t.Error("the append-only guard did not fire on the application's own delete")
	}
}

// TestDaemonTargetTriggersOff: target_triggers.enabled: false leaves the triggers to
// fire, as before, and records nothing.
func TestDaemonTargetTriggersOff(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), daemonTestBudget)
	defer cancel()
	_, tgt := triggerTestDBs(t, ctx)
	mustExec(t, ctx, tgt, "DROP TRIGGER items_versions_append ON rc_tt.items")
	mustExec(t, ctx, tgt, "DROP TRIGGER item_versions_append_only ON rc_tt.item_versions")
	d := startDaemonFromYAML(t, ctx, `
syncs:
  - name: tt
    source: src
    targets: [dst]
    include: ["rc_tt.*"]
    target_triggers: { enabled: false }
    tuning: { drain_interval: 100ms }
`)
	defer d.stop()
	if !pollUntil(t, copyTimeout, func() bool { return count(t, ctx, tgt, "rc_tt.items") == 20 }) {
		t.Fatal("copy did not converge")
	}
	// The stamp fired on replicare's writes: the old behaviour, as asked.
	if n := count(t, ctx, tgt, "rc_tt.items WHERE name LIKE '%!!'"); n != 20 {
		t.Errorf("%d rows stamped twice, want 20 with trigger handling off", n)
	}
	if n := count(t, ctx, tgt, "replicare_state.disabled_triggers"); n != 0 {
		t.Errorf("%d triggers recorded with trigger handling off, want 0", n)
	}
}

// startDaemonFromYAML runs a daemon over the harness endpoints with these syncs.
func startDaemonFromYAML(t *testing.T, ctx context.Context, syncs string) runningDaemon {
	t.Helper()
	cfg, err := config.Load(writeConfig(t, harnessConfigYAML(syncs)))
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
	stopped := false
	return runningDaemon{d: d, stop: func() {
		if stopped {
			return
		}
		stopped = true
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

// TestDaemonChecksAndRepairsTablesCopiedWhileTriggersFired: a copy made while the
// target's own triggers fired (an earlier version, or the setting off) left wrong values
// on the target. The first start that holds the triggers back marks every table already
// copied for a check; once caught up, each is compared with the source and every row
// that differs is written again from the source, with nothing for the operator to do.
func TestDaemonChecksAndRepairsTablesCopiedWhileTriggersFired(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), daemonTestBudget)
	defer cancel()
	old, oldWait := skipCheckOptions, checkRepairWait
	skipCheckOptions = live.VerifyOptions{ChunkRows: 7, Pause: -1, RecheckAfter: time.Second}
	checkRepairWait = time.Second
	t.Cleanup(func() { skipCheckOptions, checkRepairWait = old, oldWait })

	src, tgt := triggerTestDBs(t, ctx)
	// Only the stamping trigger: it changes every value on its way in, silently.
	mustExec(t, ctx, tgt, "DROP TRIGGER items_versions_append ON rc_tt.items")
	mustExec(t, ctx, tgt, "DROP TRIGGER item_versions_append_only ON rc_tt.item_versions")
	const items = "SELECT id, name FROM rc_tt.items"
	sync := func(extra string) string {
		return `
syncs:
  - name: tt
    source: src
    targets: [dst]
    include: ["rc_tt.*"]
    tuning: { drain_interval: 100ms }
` + extra
	}

	// The earlier run: the triggers fire on replicare's writes.
	d := startDaemonFromYAML(t, ctx, sync("    target_triggers: { enabled: false }\n"))
	if !pollUntil(t, copyTimeout, func() bool { return count(t, ctx, tgt, "rc_tt.items") == 20 }) {
		t.Fatal("copy did not converge")
	}
	d.stop()
	if n := count(t, ctx, tgt, "rc_tt.items WHERE name LIKE '%!!'"); n != 20 {
		t.Fatalf("%d rows stamped twice by the first run, want 20 (the broken state to repair)", n)
	}

	// Now with the triggers held back: the copied tables are checked and repaired.
	d = startDaemonFromYAML(t, ctx, sync(""))
	defer d.stop()
	if !pollUntil(t, convergeTimeout, func() bool {
		return count(t, ctx, tgt, "replicare_state.events WHERE sync = 'tt' AND event = 'check.ok' AND table_name = 'items'") == 1
	}) {
		t.Fatalf("items not repaired: planned=%d repairing=%d drift=%d, %d rows still wrong",
			count(t, ctx, tgt, "replicare_state.events WHERE sync = 'tt' AND event = 'check.planned'"),
			count(t, ctx, tgt, "replicare_state.events WHERE sync = 'tt' AND event = 'check.repairing'"),
			count(t, ctx, tgt, "replicare_state.events WHERE sync = 'tt' AND event = 'check.drift'"),
			count(t, ctx, tgt, "rc_tt.items WHERE name LIKE '%!!'"))
	}
	if !sameRows(t, ctx, src, tgt, items) {
		t.Fatal("target still differs from the source after the repair")
	}
	if n := count(t, ctx, tgt, "replicare_state.events WHERE sync = 'tt' AND event = 'check.planned'"); n != 1 {
		t.Errorf("check.planned events = %d, want 1", n)
	}
	if n := count(t, ctx, tgt, "replicare_state.events WHERE sync = 'tt' AND event = 'check.repairing' AND table_name = 'items'"); n < 1 {
		t.Error("no check.repairing event for items")
	}
	if !pollUntil(t, convergeTimeout, func() bool {
		return count(t, ctx, tgt, "replicare_state.pending_checks WHERE sync = 'tt'") == 0
	}) {
		t.Errorf("%d checks still pending, want 0", count(t, ctx, tgt, "replicare_state.pending_checks WHERE sync = 'tt'"))
	}
	d.stop()

	// A later start does not plan the check again.
	d = startDaemonFromYAML(t, ctx, sync(""))
	time.Sleep(2 * time.Second)
	d.stop()
	if n := count(t, ctx, tgt, "replicare_state.events WHERE sync = 'tt' AND event = 'check.planned'"); n != 1 {
		t.Errorf("check.planned events after another start = %d, want still 1", n)
	}
}
