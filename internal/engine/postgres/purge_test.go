package postgres

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

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
// it; unconfirmed deltas are retained (.sisyphus/reseed-state-machine.md §3.1).
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
// EVERY target has consumed it (fan-out safe, .sisyphus/reseed-state-machine.md §3.1).
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
// and the now-unpinned deltas are purged (.sisyphus/reseed-state-machine.md §3.3).
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

// TestPurgeRetentionOnlySacrificesOwnTarget: with two targets over the age cap but only
// one the caller may give up on (RetentionPolicy.Sacrificable), only that one is
// reseeded, and the other target's unconsumed changes stay queued: a sync may never
// give up on another sync's target, which may be mid-copy or paused.
func TestPurgeRetentionOnlySacrificesOwnTarget(t *testing.T) {
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
	mustExec(t, ctx, src.conn,
		"UPDATE "+qualifiedCapture(deltaTableName(relID))+" SET rc_at = now() - interval '48 hours'")

	stats, err := src.Purge(ctx, ref, []engine.TargetID{"mine", "other"},
		engine.RetentionPolicy{MaxAgeSeconds: 3600, Sacrificable: []engine.TargetID{"mine"}})
	if err != nil {
		t.Fatalf("Purge: %v", err)
	}
	if len(stats.TargetsReseeded) != 1 || stats.TargetsReseeded[0] != "mine" {
		t.Fatalf("TargetsReseeded = %v, want [mine]", stats.TargetsReseeded)
	}
	// "other" never consumed them, so all three stay queued for it.
	if got := deltaCount(t, ctx, src, relID); got != 3 {
		t.Errorf("deltas = %d, want 3 still queued for the other target", got)
	}
	bl, err := src.DeltaBacklog(ctx, ref, "other")
	if err != nil || bl.Rows != 3 {
		t.Errorf("other target backlog = %+v (%v), want 3 rows", bl, err)
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

// TestPurgeXminHorizonBloatSurfaced (Momus M-4): a long idle-in-transaction
// session pins the source's xmin horizon, so autovacuum cannot reclaim the
// delta table's dead tuples even after purge deletes the rows — the documented
// bloat trap (CLAUDE.md §3.4). This asserts the two halves of the promise:
// purge still bounds LOGICAL growth (row count returns to zero), and the physical
// bloat is real and causally the xmin horizon's fault — it reclaims only once the
// pinning transaction ends. Our size signal (deltaTotalBytes) tracks the bloat.
func TestPurgeXminHorizonBloatSurfaced(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	src := captureSource(t, ctx)
	setupSourceTables(t, ctx, src, "CREATE TABLE rc_it.orders (id int PRIMARY KEY, note text)")
	ref := engine.TableRef{Schema: "rc_it", Name: "orders"}
	if err := src.InstallCapture(ctx, []engine.TableRef{ref}); err != nil {
		t.Fatalf("install: %v", err)
	}
	relID, _, _, _ := lookupCapture(ctx, src.conn, ref)
	deltaTable := qualifiedCapture(deltaTableName(relID))

	// Pin the xmin horizon: a second session holds a snapshot open for the whole
	// churn+purge window, so no dead tuple produced below it is reclaimable.
	pin, err := connect(ctx, harnessConn(t, "source"))
	if err != nil {
		t.Fatalf("pin connect: %v", err)
	}
	pinTx, err := pin.Begin(ctx)
	if err != nil {
		t.Fatalf("pin begin: %v", err)
	}
	if _, err := pinTx.Exec(ctx, "SELECT txid_current()"); err != nil {
		t.Fatalf("pin snapshot: %v", err)
	}
	pinClosed := false
	defer func() {
		if !pinClosed {
			_ = pinTx.Rollback(context.Background())
		}
		_ = pin.Close(context.Background())
	}()

	// Churn: several rounds of insert -> consume -> purge. Each round's deltas are
	// DELETEd (logical count returns to 0) but become dead tuples the pinned xmin
	// keeps unreclaimable.
	for round := 0; round < 3; round++ {
		lo := round*3000 + 1
		hi := (round + 1) * 3000
		mustExec(t, ctx, src.conn, fmt.Sprintf(
			"INSERT INTO rc_it.orders SELECT g, 'v'||g FROM generate_series(%d,%d) g", lo, hi))
		keys, err := src.ReadDirtyKeys(ctx, ref, "dst", 0)
		if err != nil {
			t.Fatalf("round %d read: %v", round, err)
		}
		if err := src.ConfirmConsumed(ctx, ref, "dst", dirtyIDs(keys)); err != nil {
			t.Fatalf("round %d confirm: %v", round, err)
		}
		if _, err := src.Purge(ctx, ref, []engine.TargetID{"dst"}, engine.RetentionPolicy{}); err != nil {
			t.Fatalf("round %d purge: %v", round, err)
		}
	}

	// Logical growth is bounded: purge removed every consumed delta row.
	if got := deltaCount(t, ctx, src, relID); got != 0 {
		t.Fatalf("delta logical count = %d, want 0 (purge bounds logical growth)", got)
	}

	// Physical bloat persists under the pinned horizon: VACUUM cannot reclaim.
	mustExec(t, ctx, src.conn, "VACUUM "+deltaTable)
	pinnedSize, err := src.deltaTotalBytes(ctx, relID)
	if err != nil {
		t.Fatalf("size under pin: %v", err)
	}
	if pinnedSize <= 16384 {
		t.Fatalf("expected visible delta bloat under pinned xmin, size = %d bytes", pinnedSize)
	}

	// Release the horizon; the same VACUUM now reclaims — proving the bloat was
	// the idle-transaction's xmin, not our purge failing.
	if err := pinTx.Rollback(ctx); err != nil {
		t.Fatalf("release pin: %v", err)
	}
	pinClosed = true
	mustExec(t, ctx, src.conn, "VACUUM "+deltaTable)
	freedSize, err := src.deltaTotalBytes(ctx, relID)
	if err != nil {
		t.Fatalf("size after release: %v", err)
	}
	if freedSize >= pinnedSize {
		t.Errorf("delta size did not shrink after releasing xmin: pinned=%d freed=%d", pinnedSize, freedSize)
	}
}

// TestPurgeRemovesTrackRowsWithDeltas: a purged delta's track rows go with it, so the
// track table never outgrows the live queue. (Earlier versions left them behind; on a
// busy production source the track table reached millions of rows and the purge,
// which reads it, slowed to minutes.)
func TestPurgeRemovesTrackRowsWithDeltas(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	src := captureSource(t, ctx)
	setupSourceTables(t, ctx, src, "CREATE TABLE rc_it.orders (id int PRIMARY KEY)")
	ref := engine.TableRef{Schema: "rc_it", Name: "orders"}
	if err := src.InstallCapture(ctx, []engine.TableRef{ref}); err != nil {
		t.Fatalf("install: %v", err)
	}
	relID, _, _, _ := lookupCapture(ctx, src.conn, ref)
	mustExec(t, ctx, src.conn, "INSERT INTO rc_it.orders SELECT g FROM generate_series(1,5) g")

	targets := []engine.TargetID{"a", "b"}
	keys, _ := src.ReadDirtyKeys(ctx, ref, "a", 0)
	if err := src.ConfirmConsumed(ctx, ref, "a", dirtyIDs(keys)); err != nil {
		t.Fatal(err)
	}
	// Only "a" has applied them: nothing is purgeable, and a's track rows must stay.
	if _, err := src.Purge(ctx, ref, targets, engine.RetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	if got := trackRowsFor(t, ctx, src, relID, "a"); got != 5 {
		t.Fatalf("a's track rows before b caught up = %d, want 5 (their deltas still exist)", got)
	}
	// "b" applies only the first three: those three deltas go, with every track row
	// for them; the other two deltas, and a's rows for them, stay.
	if err := src.ConfirmConsumed(ctx, ref, "b", dirtyIDs(keys[:3])); err != nil {
		t.Fatal(err)
	}
	stats, err := src.Purge(ctx, ref, targets, engine.RetentionPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if stats.DeltasPurged != 3 {
		t.Fatalf("purged = %d, want 3", stats.DeltasPurged)
	}
	if a, b := trackRowsFor(t, ctx, src, relID, "a"), trackRowsFor(t, ctx, src, relID, "b"); a != 2 || b != 0 {
		t.Fatalf("track rows after purge: a=%d b=%d, want a=2 b=0", a, b)
	}

	// The track table gets the delta table's prompt autovacuum.
	var opts []string
	if err := src.conn.QueryRow(ctx, "SELECT coalesce(reloptions, '{}') FROM pg_class WHERE oid = $1::regclass",
		qualifiedCapture(trackTableName(relID))).Scan(&opts); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(opts) == "[]" || !containsString(opts, "autovacuum_vacuum_scale_factor=0") {
		t.Errorf("track table reloptions = %v, want the aggressive autovacuum settings", opts)
	}
}

// TestSweepLeftoverTrackRows: track rows left behind by earlier versions (their deltas
// long purged) are not touched by the purge (which runs inside every streaming pass),
// and are cleared by SweepLeftoverTrack a bounded window at a time, while track rows of
// deltas still queued are left alone.
func TestSweepLeftoverTrackRows(t *testing.T) {
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
	keys, _ := src.ReadDirtyKeys(ctx, ref, "dst", 0)
	// "dst" applied the first delta but "other" did not, so it stays queued with a
	// real track row that must survive the sweep.
	if err := src.ConfirmConsumed(ctx, ref, "dst", dirtyIDs(keys[:1])); err != nil {
		t.Fatal(err)
	}
	// 299 leftovers, all below the oldest queued delta, as an earlier version would
	// have left them.
	mustExec(t, ctx, src.conn, fmt.Sprintf(
		"INSERT INTO %s (target, delta_id) SELECT 'dst', g FROM generate_series(-299, -1) g",
		qualifiedCapture(trackTableName(relID))))

	old := trackSweepWindow
	trackSweepWindow = 100
	t.Cleanup(func() { trackSweepWindow = old })

	targets := []engine.TargetID{"dst", "other"}
	// The purge leaves the leftovers alone: 299 leftovers + 1 real row.
	if _, err := src.Purge(ctx, ref, targets, engine.RetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	if got := trackRowsFor(t, ctx, src, relID, "dst"); got != 300 {
		t.Fatalf("after purge: dst track rows = %d, want 300 (the purge must not sweep)", got)
	}
	// Each sweep removes 100 ids' worth, then only the real row is left.
	for i, step := range []struct{ removed, left int }{{100, 200}, {100, 100}, {99, 1}, {0, 1}} {
		n, err := src.SweepLeftoverTrack(ctx, ref, targets)
		if err != nil {
			t.Fatal(err)
		}
		if n != int64(step.removed) {
			t.Fatalf("sweep %d removed %d rows, want %d", i+1, n, step.removed)
		}
		if got := trackRowsFor(t, ctx, src, relID, "dst"); got != step.left {
			t.Fatalf("after sweep %d: dst track rows = %d, want %d", i+1, got, step.left)
		}
	}
	if got := deltaCount(t, ctx, src, relID); got != 2 {
		t.Fatalf("queued deltas = %d, want 2 (neither was applied by both targets)", got)
	}
}

func containsString(xs []string, x string) bool {
	for _, s := range xs {
		if s == x {
			return true
		}
	}
	return false
}

// TestPurgeStatementTimeoutApplies: purge statements run under a short statement
// timeout, so a purge that turns slow gives up rather than loading the source for
// minutes, and the session's own setting is left as it was.
func TestPurgeStatementTimeoutApplies(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	src := captureSource(t, ctx)
	err := src.withStatementTimeout(ctx, 100*time.Millisecond, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, "SELECT pg_sleep(2)")
		return err
	})
	if err == nil || !strings.Contains(err.Error(), "57014") {
		t.Fatalf("a statement over the limit returned %v, want a statement timeout (57014)", err)
	}
	var setting string
	if err := src.conn.QueryRow(ctx, "SHOW statement_timeout").Scan(&setting); err != nil {
		t.Fatal(err)
	}
	if setting != "0" {
		t.Errorf("session statement_timeout after the purge = %q, want it untouched (0)", setting)
	}
}
