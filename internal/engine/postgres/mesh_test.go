package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

// regRow is a decoded version-register row for assertions.
type regRow struct {
	phys    int64
	log     int
	node    string
	deleted bool
}

// readRegister returns the version-register row for a single-int-key value, or ok=false.
func readRegister(t *testing.T, ctx context.Context, src *Source, ref engine.TableRef, keyCol string, key int) (regRow, bool) {
	t.Helper()
	var r regRow
	err := src.conn.QueryRow(ctx,
		"SELECT rc_hlc_phys, rc_hlc_log, rc_node, rc_deleted FROM "+qualifiedCapture(registerTableName(ref))+
			" WHERE "+quoteIdentifier(keyCol)+" = $1", key).
		Scan(&r.phys, &r.log, &r.node, &r.deleted)
	if err != nil {
		return regRow{}, false
	}
	return r, true
}

// TestMeshTriggerStampsVersionRegister proves the cluster-member capture trigger
// stamps the HLC version register (CLAUDE.md §5.3): local writes record (hlc, node),
// the HLC advances monotonically, a delete writes a tombstone, and a PK-change
// tombstones the old key while stamping the new one.
func TestMeshTriggerStampsVersionRegister(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	src := captureSource(t, ctx) // "source" role, clean capture schema
	setupSourceTables(t, ctx, src, "CREATE TABLE rc_it.orders (id int PRIMARY KEY, note text)")
	ref := engine.TableRef{Schema: "rc_it", Name: "orders"}
	if err := src.InstallOriginCapture(ctx, []engine.TableRef{ref}, "n1"); err != nil {
		t.Fatalf("InstallOriginCapture: %v", err)
	}

	// Insert two rows → two alive register entries stamped by node n1.
	mustExec(t, ctx, src.conn, "INSERT INTO rc_it.orders (id, note) VALUES (1, 'a'), (2, 'b')")
	r1, ok := readRegister(t, ctx, src, ref, "id", 1)
	if !ok || r1.node != "n1" || r1.deleted {
		t.Fatalf("register[1] = %+v ok=%v, want node n1, alive", r1, ok)
	}
	r2, _ := readRegister(t, ctx, src, ref, "id", 2)
	// HLC is strictly ordered across the two row stamps (same or later physical, and
	// on equal physical a higher logical).
	if !versionGreater(r2, r1) {
		t.Errorf("register[2] %+v should be HLC-after register[1] %+v", r2, r1)
	}

	// Update row 1 → its version advances past the earlier one.
	mustExec(t, ctx, src.conn, "UPDATE rc_it.orders SET note='a2' WHERE id=1")
	r1b, _ := readRegister(t, ctx, src, ref, "id", 1)
	if !versionGreater(r1b, r1) {
		t.Errorf("updated register[1] %+v should be HLC-after original %+v", r1b, r1)
	}
	if r1b.deleted {
		t.Errorf("updated register[1] should be alive, got tombstone")
	}

	// Delete row 2 → tombstone, version advanced.
	mustExec(t, ctx, src.conn, "DELETE FROM rc_it.orders WHERE id=2")
	r2b, ok := readRegister(t, ctx, src, ref, "id", 2)
	if !ok || !r2b.deleted {
		t.Fatalf("deleted register[2] = %+v ok=%v, want tombstone", r2b, ok)
	}
	if !versionGreater(r2b, r2) {
		t.Errorf("tombstone register[2] %+v should be HLC-after %+v", r2b, r2)
	}

	// PK-change 1 -> 100: old key tombstoned, new key alive.
	mustExec(t, ctx, src.conn, "UPDATE rc_it.orders SET id=100 WHERE id=1")
	old, ok := readRegister(t, ctx, src, ref, "id", 1)
	if !ok || !old.deleted {
		t.Errorf("after PK-change, register[1] = %+v ok=%v, want tombstone", old, ok)
	}
	nw, ok := readRegister(t, ctx, src, ref, "id", 100)
	if !ok || nw.deleted || nw.node != "n1" {
		t.Errorf("after PK-change, register[100] = %+v ok=%v, want alive node n1", nw, ok)
	}
}

// versionGreater reports whether a's (phys, log) strictly exceeds b's (node ignored
// here — the writes share node n1, so ordering is by HLC alone).
func versionGreater(a, b regRow) bool {
	if a.phys != b.phys {
		return a.phys > b.phys
	}
	return a.log > b.log
}

// TestMeshRegisterAbsentOnOneWayCapture proves the backward-compat invariant: a
// one-way InstallCapture creates NO mesh objects (no register table, no hlc_state), so
// the one-way source schema is unchanged.
func TestMeshRegisterAbsentOnOneWayCapture(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	src := captureSource(t, ctx)
	setupSourceTables(t, ctx, src, "CREATE TABLE rc_it.orders (id int PRIMARY KEY, note text)")
	ref := engine.TableRef{Schema: "rc_it", Name: "orders"}
	if err := src.InstallCapture(ctx, []engine.TableRef{ref}); err != nil {
		t.Fatalf("InstallCapture: %v", err)
	}

	if objectExists(t, ctx, src,
		`SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema=$1 AND table_name=$2)`,
		captureSchema, registerTableName(ref)) {
		t.Error("one-way capture created a version register (should be mesh-only)")
	}
	if objectExists(t, ctx, src,
		`SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema=$1 AND table_name='hlc_state')`,
		captureSchema) {
		t.Error("one-way capture created hlc_state (should be mesh-only)")
	}
}
