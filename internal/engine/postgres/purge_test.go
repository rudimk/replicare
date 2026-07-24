package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

// Integration tests for M5c purge / bounded retention / forced reseed. Gated on
// REPLICARE_INTEGRATION=1 via captureSource -> harnessConn (skips otherwise).

// trackRowsFor returns the number of track rows recorded for a target.
func trackRowsFor(t *testing.T, ctx context.Context, src *Source, relID int, target engine.TargetID) int {
	t.Helper()
	var n int
	if err := src.conn.QueryRow(ctx,
		"SELECT count(*) FROM "+qualifiedCapture(trackTableName(relID))+" WHERE target = $1",
		string(target)).Scan(&n); err != nil {
		t.Fatalf("count track rows: %v", err)
	}
	return n
}

// TestPurgeConsumptionGated: a delta is purged only once the target has consumed
// it; unconfirmed deltas are retained (docs/reseed-state-machine.md §3.1).
func TestPurgeConsumptionGated(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	src := captureSource(t, ctx)
	setupSourceTables(t, ctx, src, "CREATE TABLE rc_it.orders (id int PRIMARY KEY)")
	ref := engine.TableRef{Schema: "rc_it", Name: "orders"}
	if err := src.InstallCapture(ctx, []engine.TableRef{ref}); err != nil {
		t.Fatalf("install: %v", err)
	}
	relID, _, _, _ := lookupCapture(ctx, src.conn, ref)
	mustExec(t, ctx, src.conn, "INSERT INTO rc_it.orders VALUES (1),(2),(3)")

	keys, err := src.ReadDirtyKeys(ctx, ref, "dst", 0)
	if err != nil {
		t.Fatalf("ReadDirtyKeys: %v", err)
	}
	// Confirm only the first two; purge must leave the third.
	if err := src.ConfirmConsumed(ctx, ref, "dst", dirtyIDs(keys[:2])); err != nil {
		t.Fatalf("ConfirmConsumed: %v", err)
	}
	stats, err := src.Purge(ctx, ref, []engine.TargetID{"dst"}, engine.RetentionPolicy{})
	if err != nil {
		t.Fatalf("Purge: %v", err)
	}
	if stats.DeltasPurged != 2 {
		t.Errorf("purged = %d, want 2", stats.DeltasPurged)
	}
	if len(stats.TargetsReseeded) != 0 {
		t.Errorf("unexpected reseed: %v", stats.TargetsReseeded)
	}
	if got := deltaCount(t, ctx, src, relID); got != 1 {
		t.Errorf("remaining deltas = %d, want 1 (the unconfirmed row)", got)
	}

	// Confirm the last one; a second purge clears it.
	remaining, _ := src.ReadDirtyKeys(ctx, ref, "dst", 0)
	if err := src.ConfirmConsumed(ctx, ref, "dst", dirtyIDs(remaining)); err != nil {
		t.Fatalf("ConfirmConsumed 2: %v", err)
	}
	if _, err := src.Purge(ctx, ref, []engine.TargetID{"dst"}, engine.RetentionPolicy{}); err != nil {
		t.Fatalf("Purge 2: %v", err)
	}
	if got := deltaCount(t, ctx, src, relID); got != 0 {
		t.Errorf("remaining deltas = %d, want 0", got)
	}
}

// TestPurgeFanoutConsumedByAll: with two targets a delta is purgeable only once
// EVERY target has consumed it (fan-out safe, docs/reseed-state-machine.md §3.1).
func TestPurgeFanoutConsumedByAll(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	src := captureSource(t, ctx)
	setupSourceTables(t, ctx, src, "CREATE TABLE rc_it.orders (id int PRIMARY KEY)")
	ref := engine.TableRef{Schema: "rc_it", Name: "orders"}
	if err := src.InstallCapture(ctx, []engine.TableRef{ref}); err != nil {
		t.Fatalf("install: %v", err)
	}
	relID, _, _, _ := lookupCapture(ctx, src.conn, ref)
	mustExec(t, ctx, src.conn, "INSERT INTO rc_it.orders VALUES (1),(2)")

	targets := []engine.TargetID{"a", "b"}
	ka, _ := src.ReadDirtyKeys(ctx, ref, "a", 0)
	if err := src.ConfirmConsumed(ctx, ref, "a", dirtyIDs(ka)); err != nil {
		t.Fatalf("confirm a: %v", err)
	}
	// Consumed by a only -> nothing purgeable yet.
	stats, err := src.Purge(ctx, ref, targets, engine.RetentionPolicy{})
	if err != nil {
		t.Fatalf("Purge (a only): %v", err)
	}
	if stats.DeltasPurged != 0 {
		t.Errorf("purged = %d, want 0 (b has not consumed)", stats.DeltasPurged)
	}
	if got := deltaCount(t, ctx, src, relID); got != 2 {
		t.Errorf("deltas = %d, want 2 retained", got)
	}

	// Now b consumes too -> purgeable by all.
	kb, _ := src.ReadDirtyKeys(ctx, ref, "b", 0)
	if err := src.ConfirmConsumed(ctx, ref, "b", dirtyIDs(kb)); err != nil {
		t.Fatalf("confirm b: %v", err)
	}
	stats, err = src.Purge(ctx, ref, targets, engine.RetentionPolicy{})
	if err != nil {
		t.Fatalf("Purge (a+b): %v", err)
	}
	if stats.DeltasPurged != 2 {
		t.Errorf("purged = %d, want 2", stats.DeltasPurged)
	}
	if got := deltaCount(t, ctx, src, relID); got != 0 {
		t.Errorf("deltas = %d, want 0", got)
	}
}

// TestPurgeRetentionAgeForcesReseed: an unconsumed backlog older than the age cap
// sacrifices the laggard target — its track is reset, it is reported as reseeded,
// and the now-unpinned deltas are purged (docs/reseed-state-machine.md §3.3).
func TestPurgeRetentionAgeForcesReseed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	src := captureSource(t, ctx)
	setupSourceTables(t, ctx, src, "CREATE TABLE rc_it.orders (id int PRIMARY KEY)")
	ref := engine.TableRef{Schema: "rc_it", Name: "orders"}
	if err := src.InstallCapture(ctx, []engine.TableRef{ref}); err != nil {
		t.Fatalf("install: %v", err)
	}
	relID, _, _, _ := lookupCapture(ctx, src.conn, ref)
	mustExec(t, ctx, src.conn, "INSERT INTO rc_it.orders VALUES (1),(2),(3)")
	// Record a prior consumption so we can prove the reseed resets the track.
	keys, _ := src.ReadDirtyKeys(ctx, ref, "dst", 0)
	if err := src.ConfirmConsumed(ctx, ref, "dst", dirtyIDs(keys[:1])); err != nil {
		t.Fatalf("confirm: %v", err)
	}
	if got := trackRowsFor(t, ctx, src, relID, "dst"); got != 1 {
		t.Fatalf("track rows before reseed = %d, want 1", got)
	}
	// Age the whole backlog well past the cap.
	mustExec(t, ctx, src.conn,
		"UPDATE "+qualifiedCapture(deltaTableName(relID))+" SET rc_at = now() - interval '48 hours'")

	stats, err := src.Purge(ctx, ref, []engine.TargetID{"dst"},
		engine.RetentionPolicy{MaxAgeSeconds: 3600})
	if err != nil {
		t.Fatalf("Purge: %v", err)
	}
	if len(stats.TargetsReseeded) != 1 || stats.TargetsReseeded[0] != "dst" {
		t.Fatalf("TargetsReseeded = %v, want [dst]", stats.TargetsReseeded)
	}
	// Single-target reseed unpins the whole queue -> all deltas purged.
	if got := deltaCount(t, ctx, src, relID); got != 0 {
		t.Errorf("deltas = %d, want 0 after reseed purge", got)
	}
	// Track was reset for the reseeding target.
	if got := trackRowsFor(t, ctx, src, relID, "dst"); got != 0 {
		t.Errorf("track rows after reseed = %d, want 0 (reset)", got)
	}
}

// TestPurgeRetentionHealthyTargetRetained: within the age cap, nothing is
// sacrificed and only consumed deltas purge (retention does not fire early).
func TestPurgeRetentionHealthyTargetRetained(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	src := captureSource(t, ctx)
	setupSourceTables(t, ctx, src, "CREATE TABLE rc_it.orders (id int PRIMARY KEY)")
	ref := engine.TableRef{Schema: "rc_it", Name: "orders"}
	if err := src.InstallCapture(ctx, []engine.TableRef{ref}); err != nil {
		t.Fatalf("install: %v", err)
	}
	relID, _, _, _ := lookupCapture(ctx, src.conn, ref)
	mustExec(t, ctx, src.conn, "INSERT INTO rc_it.orders VALUES (1),(2)")

	// Fresh backlog, generous cap -> no reseed, no purge (nothing consumed).
	stats, err := src.Purge(ctx, ref, []engine.TargetID{"dst"},
		engine.RetentionPolicy{MaxAgeSeconds: 86400})
	if err != nil {
		t.Fatalf("Purge: %v", err)
	}
	if len(stats.TargetsReseeded) != 0 {
		t.Errorf("unexpected reseed within cap: %v", stats.TargetsReseeded)
	}
	if got := deltaCount(t, ctx, src, relID); got != 2 {
		t.Errorf("deltas = %d, want 2 retained", got)
	}
}

// TestDeltaBacklog: the backlog signal reports unconsumed rows and the oldest
// unconsumed age; consumed rows drop out of the count.
func TestDeltaBacklog(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	src := captureSource(t, ctx)
	setupSourceTables(t, ctx, src, "CREATE TABLE rc_it.orders (id int PRIMARY KEY)")
	ref := engine.TableRef{Schema: "rc_it", Name: "orders"}
	if err := src.InstallCapture(ctx, []engine.TableRef{ref}); err != nil {
		t.Fatalf("install: %v", err)
	}
	relID, _, _, _ := lookupCapture(ctx, src.conn, ref)
	mustExec(t, ctx, src.conn, "INSERT INTO rc_it.orders VALUES (1),(2),(3)")

	bl, err := src.DeltaBacklog(ctx, ref, "dst")
	if err != nil {
		t.Fatalf("DeltaBacklog: %v", err)
	}
	if bl.Rows != 3 || !bl.HasBacklog {
		t.Errorf("backlog = %+v, want 3 rows", bl)
	}

	// Age the deltas and re-check the oldest-age signal.
	mustExec(t, ctx, src.conn,
		"UPDATE "+qualifiedCapture(deltaTableName(relID))+" SET rc_at = now() - interval '2 hours'")
	bl, err = src.DeltaBacklog(ctx, ref, "dst")
	if err != nil {
		t.Fatalf("DeltaBacklog aged: %v", err)
	}
	if bl.OldestAge < time.Hour {
		t.Errorf("oldest age = %s, want >= 1h", bl.OldestAge)
	}

	// Consume all -> backlog empties.
	keys, _ := src.ReadDirtyKeys(ctx, ref, "dst", 0)
	if err := src.ConfirmConsumed(ctx, ref, "dst", dirtyIDs(keys)); err != nil {
		t.Fatalf("confirm: %v", err)
	}
	bl, err = src.DeltaBacklog(ctx, ref, "dst")
	if err != nil {
		t.Fatalf("DeltaBacklog consumed: %v", err)
	}
	if bl.Rows != 0 || bl.HasBacklog {
		t.Errorf("backlog after consume = %+v, want empty", bl)
	}
}
