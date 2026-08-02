package mysql

import (
	"context"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

// consumeAll reads and confirms every currently-unconsumed delta for a target,
// simulating a fully-caught-up consumer (so purge can then reclaim the deltas).
func consumeAll(t *testing.T, ctx context.Context, src *Source, ref engine.TableRef, target engine.TargetID) {
	t.Helper()
	dirty, err := src.ReadDirtyKeys(ctx, ref, target, 0)
	if err != nil {
		t.Fatalf("read dirty for %s: %v", target, err)
	}
	if len(dirty) == 0 {
		return
	}
	ids := make([]engine.DeltaID, len(dirty))
	for i, d := range dirty {
		ids[i] = d.DeltaID
	}
	if err := src.ConfirmConsumed(ctx, ref, target, ids); err != nil {
		t.Fatalf("confirm consumed for %s: %v", target, err)
	}
}

// TestMySQLDeltaBacklogAndPurge is the MM5c backlog+purge keystone: DeltaBacklog
// accounts unconsumed deltas (rows, age, on-disk bytes), and the consumption-gated
// purge reclaims a delta only once EVERY configured target has consumed it
// (fan-out safe). No retention cap fires here.
func TestMySQLDeltaBacklogAndPurge(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	src := connectSource(t, ctx)
	t.Cleanup(func() { _, _ = src.db.Exec("DROP DATABASE IF EXISTS replicare") })

	mustExec(t, ctx, src.db, "CREATE TABLE rc_it.orders (id INT PRIMARY KEY, note VARCHAR(50)) ENGINE=InnoDB")
	ref := engine.TableRef{Schema: "rc_it", Name: "orders"}
	if err := src.InstallCapture(ctx, []engine.TableRef{ref}); err != nil {
		t.Fatalf("install capture: %v", err)
	}
	mustExec(t, ctx, src.db, "INSERT INTO rc_it.orders VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')")

	// Fresh backlog for target "a": all 5 unconsumed, non-zero age and on-disk bytes.
	time.Sleep(1100 * time.Millisecond)
	bl, err := src.DeltaBacklog(ctx, ref, "a")
	if err != nil {
		t.Fatalf("backlog a: %v", err)
	}
	if bl.Rows != 5 || !bl.HasBacklog {
		t.Fatalf("backlog a = %+v, want 5 rows", bl)
	}
	if bl.OldestAge <= 0 {
		t.Errorf("backlog a oldest age = %v, want > 0", bl.OldestAge)
	}
	if bl.Bytes <= 0 {
		t.Errorf("backlog a bytes = %d, want > 0 (delta table on-disk footprint)", bl.Bytes)
	}

	targets := []engine.TargetID{"a", "b"}

	// Target "a" consumes all; "b" has not. A fan-out purge must reclaim NOTHING —
	// every delta is still pinned by "b".
	consumeAll(t, ctx, src, ref, "a")
	if blA, _ := src.DeltaBacklog(ctx, ref, "a"); blA.Rows != 0 || blA.HasBacklog {
		t.Errorf("after a consumes, backlog a = %+v, want 0", blA)
	}
	ps, err := src.Purge(ctx, ref, targets, engine.RetentionPolicy{})
	if err != nil {
		t.Fatalf("purge (b pinning): %v", err)
	}
	if ps.DeltasPurged != 0 || len(ps.TargetsReseeded) != 0 {
		t.Fatalf("purge while b pins the queue = %+v, want nothing purged/reseeded", ps)
	}
	if blB, _ := src.DeltaBacklog(ctx, ref, "b"); blB.Rows != 5 {
		t.Errorf("b backlog = %+v, want 5 (deltas not purged)", blB)
	}

	// Now "b" consumes too: every delta is consumed by ALL targets and purges.
	consumeAll(t, ctx, src, ref, "b")
	ps, err = src.Purge(ctx, ref, targets, engine.RetentionPolicy{})
	if err != nil {
		t.Fatalf("purge (all consumed): %v", err)
	}
	if ps.DeltasPurged != 5 || len(ps.TargetsReseeded) != 0 {
		t.Fatalf("purge after all consumed = %+v, want 5 purged, none reseeded", ps)
	}
	// A brand-new target sees an empty queue — the deltas are physically gone.
	if blC, _ := src.DeltaBacklog(ctx, ref, "c"); blC.Rows != 0 || blC.HasBacklog {
		t.Errorf("fresh-target backlog after purge = %+v, want 0", blC)
	}
}

// TestMySQLRetentionForcesReseed is the MM5c retention keystone: a target whose
// pinned backlog exceeds a retention cap is SACRIFICED — its track is reset and
// the unpinned deltas are purged — so a slow/absent target can never grow the
// source unboundedly. Both the age cap and the size cap are exercised.
func TestMySQLRetentionForcesReseed(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	src := connectSource(t, ctx)
	t.Cleanup(func() { _, _ = src.db.Exec("DROP DATABASE IF EXISTS replicare") })

	mustExec(t, ctx, src.db, "CREATE TABLE rc_it.orders (id INT PRIMARY KEY, note VARCHAR(50)) ENGINE=InnoDB")
	ref := engine.TableRef{Schema: "rc_it", Name: "orders"}
	if err := src.InstallCapture(ctx, []engine.TableRef{ref}); err != nil {
		t.Fatalf("install capture: %v", err)
	}
	mustExec(t, ctx, src.db, "INSERT INTO rc_it.orders VALUES (1,'a'),(2,'b'),(3,'c')")

	// Age cap: let the (unconsumed) backlog age past a 1s cap, then purge with that
	// cap. The single target is over cap → sacrificed, its (empty) track reset, and
	// with no remaining targets the whole queue drains.
	time.Sleep(1300 * time.Millisecond)
	ps, err := src.Purge(ctx, ref, []engine.TargetID{"slow"}, engine.RetentionPolicy{MaxAgeSeconds: 1})
	if err != nil {
		t.Fatalf("purge (age cap): %v", err)
	}
	if len(ps.TargetsReseeded) != 1 || ps.TargetsReseeded[0] != "slow" {
		t.Fatalf("age-cap purge reseeded = %v, want [slow]", ps.TargetsReseeded)
	}
	if ps.DeltasPurged != 3 {
		t.Errorf("age-cap purge purged %d, want 3 (queue drained after sacrifice)", ps.DeltasPurged)
	}
	if bl, _ := src.DeltaBacklog(ctx, ref, "slow"); bl.Rows != 0 {
		t.Errorf("after reseed sacrifice, backlog = %+v, want 0", bl)
	}

	// Size cap: a fresh backlog with a 1-byte size cap is instantly over — the
	// laggard is sacrificed regardless of age.
	mustExec(t, ctx, src.db, "INSERT INTO rc_it.orders VALUES (10,'x'),(11,'y')")
	ps, err = src.Purge(ctx, ref, []engine.TargetID{"slow"}, engine.RetentionPolicy{MaxBytes: 1})
	if err != nil {
		t.Fatalf("purge (size cap): %v", err)
	}
	if len(ps.TargetsReseeded) != 1 || ps.TargetsReseeded[0] != "slow" {
		t.Fatalf("size-cap purge reseeded = %v, want [slow]", ps.TargetsReseeded)
	}
	if ps.DeltasPurged != 2 {
		t.Errorf("size-cap purge purged %d, want 2", ps.DeltasPurged)
	}
}
