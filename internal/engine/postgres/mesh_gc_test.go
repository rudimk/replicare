package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

// TestGCTombstonesReclaimsConsumed proves tombstone GC (CLAUDE.md §5.3): a tombstone
// is RETAINED while its delete delta is still pending (a peer may not have applied it),
// and RECLAIMED once the delete delta is gone (consumed by every peer = purged). A live
// register row is never touched.
func TestGCTombstonesReclaimsConsumed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	src := captureSource(t, ctx)
	src.EnableClusterReads() // mark as a cluster source so GC engages
	setupSourceTables(t, ctx, src, "CREATE TABLE rc_it.orders (id int PRIMARY KEY, note text)")
	ref := engine.TableRef{Schema: "rc_it", Name: "orders"}
	if err := src.InstallOriginCapture(ctx, []engine.TableRef{ref}, "n1"); err != nil {
		t.Fatalf("InstallOriginCapture: %v", err)
	}
	relID, _, _ := lookupRegistry(ctx, src.conn, ref)

	// Insert then delete id=1 (tombstone), insert id=2 (stays live).
	mustExec(t, ctx, src.conn, "INSERT INTO rc_it.orders (id, note) VALUES (1, 'a'), (2, 'b')")
	mustExec(t, ctx, src.conn, "DELETE FROM rc_it.orders WHERE id=1")

	if r, ok := readRegister(t, ctx, src, ref, "id", 1); !ok || !r.deleted {
		t.Fatalf("register[1] = %+v ok=%v, want tombstone", r, ok)
	}

	// GC with the delete delta still present: the tombstone is RETAINED (a peer may
	// not have applied the delete yet).
	n, err := src.GCTombstones(ctx, ref)
	if err != nil {
		t.Fatalf("GCTombstones (pending): %v", err)
	}
	if n != 0 {
		t.Errorf("GC reclaimed %d tombstones with the delete delta still pending, want 0", n)
	}
	if _, ok := readRegister(t, ctx, src, ref, "id", 1); !ok {
		t.Error("tombstone[1] was removed while its delete delta is still pending")
	}

	// Simulate the delete delta being consumed by every peer and purged.
	mustExec(t, ctx, src.conn, "DELETE FROM "+qualifiedCapture(deltaTableName(relID))+" WHERE k1 = 1")

	// Now GC reclaims the tombstone.
	n, err = src.GCTombstones(ctx, ref)
	if err != nil {
		t.Fatalf("GCTombstones (consumed): %v", err)
	}
	if n != 1 {
		t.Errorf("GC reclaimed %d tombstones, want 1", n)
	}
	if _, ok := readRegister(t, ctx, src, ref, "id", 1); ok {
		t.Error("consumed tombstone[1] was not reclaimed")
	}

	// The live row's register entry is never touched by GC, even once its delta clears.
	mustExec(t, ctx, src.conn, "DELETE FROM "+qualifiedCapture(deltaTableName(relID))+" WHERE k1 = 2")
	if _, err := src.GCTombstones(ctx, ref); err != nil {
		t.Fatalf("GCTombstones (live): %v", err)
	}
	if r, ok := readRegister(t, ctx, src, ref, "id", 2); !ok || r.deleted {
		t.Errorf("live register[2] = %+v ok=%v, want alive and present (GC must not touch live rows)", r, ok)
	}
}

// TestGCTombstonesNoopOneWay proves GC is inert on a one-way source (no cluster reads
// enabled → no register), so the one-way path is untouched.
func TestGCTombstonesNoopOneWay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	src := captureSource(t, ctx)
	setupSourceTables(t, ctx, src, "CREATE TABLE rc_it.orders (id int PRIMARY KEY)")
	ref := engine.TableRef{Schema: "rc_it", Name: "orders"}
	if err := src.InstallCapture(ctx, []engine.TableRef{ref}); err != nil {
		t.Fatalf("InstallCapture: %v", err)
	}
	n, err := src.GCTombstones(ctx, ref)
	if err != nil {
		t.Fatalf("GCTombstones one-way: %v", err)
	}
	if n != 0 {
		t.Errorf("one-way GC reclaimed %d, want 0", n)
	}
}
