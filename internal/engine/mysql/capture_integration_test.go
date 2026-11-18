package mysql

import (
	"context"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

// deltaRows dumps a captured table's delta rows as "op|key" for assertions.
func deltaRows(t *testing.T, ctx context.Context, s *Source, ref engine.TableRef) []string {
	t.Helper()
	relID, pk, ok, err := s.lookupRegistry(ctx, ref)
	if err != nil || !ok {
		t.Fatalf("lookup registry %s: ok=%v err=%v", ref, ok, err)
	}
	sel := "rc_op"
	for _, k := range deltaColumns(len(pk)) {
		sel += ", " + bq(k)
	}
	rows, err := s.db.QueryContext(ctx, "SELECT "+sel+" FROM "+captureRef(deltaTableName(relID))+" ORDER BY delta_id")
	if err != nil {
		t.Fatalf("read deltas: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var op string
		var k []byte
		if err := rows.Scan(&op, &k); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, op+"|"+string(k))
	}
	return out
}

// TestCaptureLifecycle installs capture, exercises insert/update/delete and a
// PK-change, and asserts the delta rows are correct; then removes capture and
// confirms the source is clean.
func TestCaptureLifecycle(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	s := connectSource(t, ctx)
	t.Cleanup(func() { _, _ = s.db.Exec("DROP DATABASE IF EXISTS replicare") })

	mustExec(t, ctx, s.db, "CREATE TABLE rc_it.orders (id INT PRIMARY KEY, note VARCHAR(50)) ENGINE=InnoDB")
	ref := engine.TableRef{Schema: "rc_it", Name: "orders"}

	if err := s.InstallCapture(ctx, []engine.TableRef{ref}); err != nil {
		t.Fatalf("install capture: %v", err)
	}
	// Idempotent re-install must not error or lose deltas.
	if err := s.InstallCapture(ctx, []engine.TableRef{ref}); err != nil {
		t.Fatalf("re-install capture: %v", err)
	}

	mustExec(t, ctx, s.db,
		"INSERT INTO rc_it.orders VALUES (1, 'a')",    // I|1
		"UPDATE rc_it.orders SET note='b' WHERE id=1", // U|1 (value-only)
		"UPDATE rc_it.orders SET id=2 WHERE id=1",     // D|1 + U|2 (PK change)
		"DELETE FROM rc_it.orders WHERE id=2",         // D|2
	)

	got := deltaRows(t, ctx, s, ref)
	want := []string{"I|1", "U|1", "D|1", "U|2", "D|2"}
	if len(got) != len(want) {
		t.Fatalf("delta rows = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("delta[%d] = %q, want %q (all: %v)", i, got[i], want[i], got)
		}
	}

	// Consume: ReadDirtyKeys returns them; ConfirmConsumed removes them from the
	// unconsumed set for that target.
	dk, err := s.ReadDirtyKeys(ctx, ref, "dst", 0)
	if err != nil {
		t.Fatalf("read dirty keys: %v", err)
	}
	if len(dk) != 5 {
		t.Fatalf("dirty keys = %d, want 5", len(dk))
	}
	ids := make([]engine.DeltaID, len(dk))
	for i, d := range dk {
		ids[i] = d.DeltaID
	}
	if err := s.ConfirmConsumed(ctx, ref, "dst", ids); err != nil {
		t.Fatalf("confirm consumed: %v", err)
	}
	dk2, _ := s.ReadDirtyKeys(ctx, ref, "dst", 0)
	if len(dk2) != 0 {
		t.Errorf("after confirm, dirty keys = %d, want 0", len(dk2))
	}
	// A different target still sees them all (set-difference is per-target).
	dkOther, _ := s.ReadDirtyKeys(ctx, ref, "other", 0)
	if len(dkOther) != 5 {
		t.Errorf("other target dirty keys = %d, want 5", len(dkOther))
	}

	// Remove capture: triggers gone, delta/track gone, registry row gone.
	if err := s.RemoveCapture(ctx, []engine.TableRef{ref}); err != nil {
		t.Fatalf("remove capture: %v", err)
	}
	var n int
	if err := s.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM information_schema.TRIGGERS WHERE EVENT_OBJECT_SCHEMA='rc_it' AND EVENT_OBJECT_TABLE='orders'").Scan(&n); err != nil {
		t.Fatalf("count triggers: %v", err)
	}
	if n != 0 {
		t.Errorf("after remove, %d triggers remain", n)
	}
}

// TestPreExistingTriggerBlocks confirms capture install refuses a table that
// already has a conflicting AFTER trigger where MySQL allows only one trigger per
// event (before 5.7.2 — in practice a best-effort 5.6 source).
func TestPreExistingTriggerBlocks(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	s := connectSource(t, ctx)
	t.Cleanup(func() { _, _ = s.db.Exec("DROP DATABASE IF EXISTS replicare") })

	ver, _ := serverVersion(ctx, s.db)
	if ver >= multiTriggerMinVersion {
		t.Skip("the one-trigger-per-event limit only exists before MySQL 5.7.2")
	}
	mustExec(t, ctx, s.db,
		"CREATE TABLE rc_it.audited (id INT PRIMARY KEY, n INT) ENGINE=InnoDB",
		"CREATE TABLE rc_it.audit_log (id INT AUTO_INCREMENT PRIMARY KEY, msg VARCHAR(50)) ENGINE=InnoDB",
		"CREATE TRIGGER rc_it.user_trg AFTER INSERT ON rc_it.audited FOR EACH ROW INSERT INTO rc_it.audit_log (msg) VALUES ('x')",
	)
	err := s.InstallCapture(ctx, []engine.TableRef{{Schema: "rc_it", Name: "audited"}})
	if err == nil {
		t.Fatal("expected install to block on a pre-existing AFTER trigger")
	}
}

// TestCaptureCoexistsWithExistingTriggers: from MySQL 5.7.2 a table may carry several
// triggers per event, so a table with the application's own AFTER INSERT/UPDATE/DELETE
// triggers is captured normally — replicare's triggers sit alongside, and both the
// application's triggers and capture keep firing. (Before this, any MySQL < 8.0 was
// refused, needlessly blocking 5.7 sources.)
func TestCaptureCoexistsWithExistingTriggers(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	s := connectSource(t, ctx)
	t.Cleanup(func() { _, _ = s.db.Exec("DROP DATABASE IF EXISTS replicare") })

	ver, _ := serverVersion(ctx, s.db)
	// A fixed 5.7.2 here, not multiTriggerMinVersion: the test must fail, not skip, if
	// that constant is ever raised back above what MySQL actually allows.
	if ver < 50702 {
		t.Skip("before MySQL 5.7.2 an existing trigger blocks capture (TestPreExistingTriggerBlocks)")
	}
	mustExec(t, ctx, s.db,
		"CREATE TABLE rc_it.audited (id INT PRIMARY KEY, n INT) ENGINE=InnoDB",
		"CREATE TABLE rc_it.audit_log (id INT AUTO_INCREMENT PRIMARY KEY, msg VARCHAR(50)) ENGINE=InnoDB",
		"CREATE TRIGGER rc_it.user_ins AFTER INSERT ON rc_it.audited FOR EACH ROW INSERT INTO rc_it.audit_log (msg) VALUES ('ins')",
		"CREATE TRIGGER rc_it.user_upd AFTER UPDATE ON rc_it.audited FOR EACH ROW INSERT INTO rc_it.audit_log (msg) VALUES ('upd')",
		"CREATE TRIGGER rc_it.user_del AFTER DELETE ON rc_it.audited FOR EACH ROW INSERT INTO rc_it.audit_log (msg) VALUES ('del')",
	)
	ref := engine.TableRef{Schema: "rc_it", Name: "audited"}
	if err := s.InstallCapture(ctx, []engine.TableRef{ref}); err != nil {
		t.Fatalf("install must succeed next to the application's own triggers on MySQL %d: %v", ver, err)
	}
	mustExec(t, ctx, s.db,
		"INSERT INTO rc_it.audited VALUES (1, 1)",
		"UPDATE rc_it.audited SET n = 2 WHERE id = 1",
		"DELETE FROM rc_it.audited WHERE id = 1",
	)
	if got := deltaRows(t, ctx, s, ref); len(got) != 3 || got[0] != "I|1" || got[1] != "U|1" || got[2] != "D|1" {
		t.Errorf("capture deltas = %v, want [I|1 U|1 D|1]", got)
	}
	var appRows int
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM rc_it.audit_log").Scan(&appRows); err != nil {
		t.Fatal(err)
	}
	if appRows != 3 {
		t.Errorf("the application's own triggers fired %d times, want 3 (they must keep working)", appRows)
	}
	// Removing capture must leave the application's triggers in place.
	if err := s.RemoveCapture(ctx, []engine.TableRef{ref}); err != nil {
		t.Fatalf("remove capture: %v", err)
	}
	var appTriggers int
	if err := s.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM information_schema.TRIGGERS WHERE EVENT_OBJECT_SCHEMA = 'rc_it' AND EVENT_OBJECT_TABLE = 'audited'").Scan(&appTriggers); err != nil {
		t.Fatal(err)
	}
	if appTriggers != 3 {
		t.Errorf("after removing capture %d triggers remain, want the application's 3", appTriggers)
	}
}
