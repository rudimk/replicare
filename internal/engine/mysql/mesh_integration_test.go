package mysql

import (
	"context"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

// mysqlRegRow is a decoded version-register row for assertions.
type mysqlRegRow struct {
	phys    int64
	log     int
	node    string
	deleted bool
}

// readRegister returns the version-register row for a single-int-key value, or ok=false.
func readRegister(t *testing.T, ctx context.Context, s *Source, ref engine.TableRef, keyCol string, key int) (mysqlRegRow, bool) {
	t.Helper()
	var r mysqlRegRow
	var del int
	err := s.db.QueryRowContext(ctx,
		"SELECT rc_hlc_phys, rc_hlc_log, rc_node, rc_deleted FROM "+captureRef(registerTableName(ref))+
			" WHERE "+bq(keyCol)+" = ?", key).
		Scan(&r.phys, &r.log, &r.node, &del)
	if err != nil {
		return mysqlRegRow{}, false
	}
	r.deleted = del != 0
	return r, true
}

func mysqlVersionGreater(a, b mysqlRegRow) bool {
	if a.phys != b.phys {
		return a.phys > b.phys
	}
	return a.log > b.log
}

// TestMeshTriggerStampsVersionRegister proves the MySQL cluster-member capture
// triggers stamp the HLC version register (CLAUDE.md §5.3): local writes record
// (hlc, node), the HLC advances monotonically, a delete writes a tombstone, and a
// PK-change tombstones the old key while stamping the new one.
func TestMeshTriggerStampsVersionRegister(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	s := connectSource(t, ctx)
	mustExec(t, ctx, s.db, "DROP DATABASE IF EXISTS replicare")
	t.Cleanup(func() { _, _ = s.db.Exec("DROP DATABASE IF EXISTS replicare") })

	mustExec(t, ctx, s.db, "CREATE TABLE rc_it.orders (id INT PRIMARY KEY, note VARCHAR(50)) ENGINE=InnoDB")
	ref := engine.TableRef{Schema: "rc_it", Name: "orders"}
	if err := s.InstallOriginCapture(ctx, []engine.TableRef{ref}, "n1"); err != nil {
		t.Fatalf("InstallOriginCapture: %v", err)
	}

	mustExec(t, ctx, s.db, "INSERT INTO rc_it.orders VALUES (1, 'a'), (2, 'b')")
	r1, ok := readRegister(t, ctx, s, ref, "id", 1)
	if !ok || r1.node != "n1" || r1.deleted {
		t.Fatalf("register[1] = %+v ok=%v, want node n1, alive", r1, ok)
	}
	r2, _ := readRegister(t, ctx, s, ref, "id", 2)
	if !mysqlVersionGreater(r2, r1) {
		t.Errorf("register[2] %+v should be HLC-after register[1] %+v", r2, r1)
	}

	mustExec(t, ctx, s.db, "UPDATE rc_it.orders SET note='a2' WHERE id=1")
	r1b, _ := readRegister(t, ctx, s, ref, "id", 1)
	if !mysqlVersionGreater(r1b, r1) || r1b.deleted {
		t.Errorf("updated register[1] %+v should be HLC-after %+v and alive", r1b, r1)
	}

	mustExec(t, ctx, s.db, "DELETE FROM rc_it.orders WHERE id=2")
	r2b, ok := readRegister(t, ctx, s, ref, "id", 2)
	if !ok || !r2b.deleted {
		t.Fatalf("deleted register[2] = %+v ok=%v, want tombstone", r2b, ok)
	}
	if !mysqlVersionGreater(r2b, r2) {
		t.Errorf("tombstone register[2] %+v should be HLC-after %+v", r2b, r2)
	}

	mustExec(t, ctx, s.db, "UPDATE rc_it.orders SET id=100 WHERE id=1")
	old, ok := readRegister(t, ctx, s, ref, "id", 1)
	if !ok || !old.deleted {
		t.Errorf("after PK-change, register[1] = %+v ok=%v, want tombstone", old, ok)
	}
	nw, ok := readRegister(t, ctx, s, ref, "id", 100)
	if !ok || nw.deleted || nw.node != "n1" {
		t.Errorf("after PK-change, register[100] = %+v ok=%v, want alive node n1", nw, ok)
	}
}

// TestMeshRegisterAbsentOnOneWayCapture proves the BC invariant: a one-way
// InstallCapture creates NO mesh objects (no register, no hlc_state).
func TestMeshRegisterAbsentOnOneWayCapture(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	s := connectSource(t, ctx)
	mustExec(t, ctx, s.db, "DROP DATABASE IF EXISTS replicare")
	t.Cleanup(func() { _, _ = s.db.Exec("DROP DATABASE IF EXISTS replicare") })

	mustExec(t, ctx, s.db, "CREATE TABLE rc_it.orders (id INT PRIMARY KEY, note VARCHAR(50)) ENGINE=InnoDB")
	ref := engine.TableRef{Schema: "rc_it", Name: "orders"}
	if err := s.InstallCapture(ctx, []engine.TableRef{ref}); err != nil {
		t.Fatalf("InstallCapture: %v", err)
	}

	if tableExists(t, ctx, s, registerTableName(ref)) {
		t.Error("one-way capture created a version register (should be mesh-only)")
	}
	if tableExists(t, ctx, s, "hlc_state") {
		t.Error("one-way capture created hlc_state (should be mesh-only)")
	}
}

func tableExists(t *testing.T, ctx context.Context, s *Source, name string) bool {
	t.Helper()
	var n int
	if err := s.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA='replicare' AND TABLE_NAME=?", name).Scan(&n); err != nil {
		t.Fatalf("table exists check: %v", err)
	}
	return n > 0
}
