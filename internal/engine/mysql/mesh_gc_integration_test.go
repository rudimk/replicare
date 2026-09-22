package mysql

import (
	"context"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

// TestGCTombstonesReclaimsConsumed proves MySQL tombstone GC (CLAUDE.md §5.3): a
// tombstone is RETAINED while its delete delta is pending and RECLAIMED once the delta
// is gone (consumed by every peer = purged); live rows are never touched.
func TestGCTombstonesReclaimsConsumed(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	s := connectSource(t, ctx)
	s.EnableClusterReads()
	mustExec(t, ctx, s.db, "DROP DATABASE IF EXISTS replicare")
	t.Cleanup(func() { _, _ = s.db.Exec("DROP DATABASE IF EXISTS replicare") })

	mustExec(t, ctx, s.db, "CREATE TABLE rc_it.orders (id INT PRIMARY KEY, note VARCHAR(50)) ENGINE=InnoDB")
	ref := engine.TableRef{Schema: "rc_it", Name: "orders"}
	if err := s.InstallOriginCapture(ctx, []engine.TableRef{ref}, "n1"); err != nil {
		t.Fatalf("InstallOriginCapture: %v", err)
	}
	relID, _, _, _ := s.lookupRegistry(ctx, ref)

	mustExec(t, ctx, s.db, "INSERT INTO rc_it.orders VALUES (1, 'a'), (2, 'b')")
	mustExec(t, ctx, s.db, "DELETE FROM rc_it.orders WHERE id=1")
	if r, ok := readRegister(t, ctx, s, ref, "id", 1); !ok || !r.deleted {
		t.Fatalf("register[1] = %+v ok=%v, want tombstone", r, ok)
	}

	// Delete delta still pending -> tombstone retained.
	if n, err := s.GCTombstones(ctx, ref); err != nil || n != 0 {
		t.Fatalf("GC (pending) n=%d err=%v, want 0/nil", n, err)
	}
	if _, ok := readRegister(t, ctx, s, ref, "id", 1); !ok {
		t.Error("tombstone[1] removed while its delete delta is pending")
	}

	// Simulate the delete delta consumed by every peer and purged.
	mustExec(t, ctx, s.db, "DELETE FROM "+captureRef(deltaTableName(relID))+" WHERE k1 = 1")
	if n, err := s.GCTombstones(ctx, ref); err != nil || n != 1 {
		t.Fatalf("GC (consumed) n=%d err=%v, want 1/nil", n, err)
	}
	if _, ok := readRegister(t, ctx, s, ref, "id", 1); ok {
		t.Error("consumed tombstone[1] not reclaimed")
	}

	// Live row untouched even once its delta clears.
	mustExec(t, ctx, s.db, "DELETE FROM "+captureRef(deltaTableName(relID))+" WHERE k1 = 2")
	if _, err := s.GCTombstones(ctx, ref); err != nil {
		t.Fatalf("GC (live): %v", err)
	}
	if r, ok := readRegister(t, ctx, s, ref, "id", 2); !ok || r.deleted {
		t.Errorf("live register[2] = %+v ok=%v, want alive+present", r, ok)
	}
}

// TestGCTombstonesNoopOneWay proves GC is inert on a one-way source (no cluster reads
// → no register), so the one-way path is untouched.
func TestGCTombstonesNoopOneWay(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s := connectSource(t, ctx)
	t.Cleanup(func() { _, _ = s.db.Exec("DROP DATABASE IF EXISTS replicare") })
	mustExec(t, ctx, s.db, "CREATE TABLE rc_it.orders (id INT PRIMARY KEY) ENGINE=InnoDB")
	ref := engine.TableRef{Schema: "rc_it", Name: "orders"}
	if err := s.InstallCapture(ctx, []engine.TableRef{ref}); err != nil {
		t.Fatalf("InstallCapture: %v", err)
	}
	if n, err := s.GCTombstones(ctx, ref); err != nil || n != 0 {
		t.Fatalf("one-way GC n=%d err=%v, want 0/nil", n, err)
	}
}

// TestOriginMarkerSuppressesCapture proves loop suppression at the trigger level: a
// write on a connection carrying @replicare_apply is NOT captured (a replicare apply),
// while an ordinary write IS captured (a local change).
func TestOriginMarkerSuppressesCapture(t *testing.T) {
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
	relID, _, _, _ := s.lookupRegistry(ctx, ref)

	deltaCount := func() int {
		var n int
		if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+captureRef(deltaTableName(relID))).Scan(&n); err != nil {
			t.Fatalf("count deltas: %v", err)
		}
		return n
	}

	// A MARKED write (replicare's own apply) on a pinned connection -> not captured.
	conn, err := s.db.Conn(ctx)
	if err != nil {
		t.Fatalf("pin conn: %v", err)
	}
	if _, err := conn.ExecContext(ctx, "SET @replicare_apply = '1'"); err != nil {
		t.Fatalf("set marker: %v", err)
	}
	if _, err := conn.ExecContext(ctx, "INSERT INTO rc_it.orders VALUES (1, 'applied')"); err != nil {
		t.Fatalf("marked insert: %v", err)
	}
	_, _ = conn.ExecContext(ctx, "SET @replicare_apply = NULL")
	_ = conn.Close()
	if got := deltaCount(); got != 0 {
		t.Fatalf("marked write was captured: %d delta rows, want 0", got)
	}

	// An ordinary (unmarked) write IS captured.
	mustExec(t, ctx, s.db, "INSERT INTO rc_it.orders VALUES (2, 'local')")
	if got := deltaCount(); got != 1 {
		t.Fatalf("local write not captured: %d delta rows, want 1", got)
	}
}
