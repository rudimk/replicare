package apply

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/rudimk/replicare/internal/engine"
)

// distinctKeys and allIDs mirror what Drain computes, for the step-by-step
// keystone tests that drive the pieces directly.
func distinctKeys(dirty []engine.DirtyKey) []engine.KeyValues {
	var out []engine.KeyValues
	seen := map[string]bool{}
	for _, d := range dirty {
		sig := keySignature(d.Key)
		if !seen[sig] {
			seen[sig] = true
			out = append(out, d.Key)
		}
	}
	return out
}

func allIDs(dirty []engine.DirtyKey) []engine.DeltaID {
	out := make([]engine.DeltaID, len(dirty))
	for i, d := range dirty {
		out[i] = d.DeltaID
	}
	return out
}

func targetNote(t *testing.T, ctx context.Context, f *fixture, id int) (string, bool) {
	t.Helper()
	var note string
	err := f.rawTgt.QueryRow(ctx, "SELECT note FROM rc_it.orders WHERE id=$1", id).Scan(&note)
	if err == pgx.ErrNoRows {
		return "", false
	}
	if err != nil {
		t.Fatalf("target read id=%d: %v", id, err)
	}
	return note, true
}

// TestKeystoneCommitOrderHazard: a delta enqueued by an uncommitted transaction
// is invisible to a drain pass (so never skipped by a scalar cursor would-be
// bug), and is applied on a later pass once the transaction commits (§3.3).
func TestKeystoneCommitOrderHazard(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	f := newFixture(t, ctx, ordersDDL,
		"INSERT INTO rc_it.orders SELECT g, 'n'||g FROM generate_series(1,3) g")

	// A separate source connection holds an OPEN transaction that inserts id=10
	// (its delta is enqueued but uncommitted).
	t1, err := pgx.Connect(ctx, srcURL())
	if err != nil {
		t.Fatalf("connect t1: %v", err)
	}
	defer t1.Close(context.Background())
	exec(t, ctx, t1, "BEGIN")
	exec(t, ctx, t1, "INSERT INTO rc_it.orders VALUES (10, 'ten')")

	// A drain now must NOT see the uncommitted row.
	if _, err := DrainAll(ctx, f.src, f.sink, f.ref, f.target, 100); err != nil {
		t.Fatalf("drain (t1 open): %v", err)
	}
	if _, ok := targetNote(t, ctx, f, 10); ok {
		t.Fatal("uncommitted row 10 must not be applied to the target")
	}

	// Commit T1; the delta becomes visible and the next drain applies it.
	exec(t, ctx, t1, "COMMIT")
	if _, err := DrainAll(ctx, f.src, f.sink, f.ref, f.target, 100); err != nil {
		t.Fatalf("drain (t1 committed): %v", err)
	}
	if note, ok := targetNote(t, ctx, f, 10); !ok || note != "ten" {
		t.Errorf("committed row 10 should be applied, got note=%q ok=%v", note, ok)
	}
	f.assertConverged(t, ctx)
}

// TestKeystoneDeleteByIDRace: a change during a drain (after read, before
// confirm) inserts a new delta that survives confirming only the observed ids,
// so the PK is reprocessed and converges to the latest value (§3.3).
func TestKeystoneDeleteByIDRace(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	f := newFixture(t, ctx, ordersDDL, "")

	exec(t, ctx, f.rawSrc, "INSERT INTO rc_it.orders VALUES (1, 'v1')")
	observed, err := f.src.ReadDirtyKeys(ctx, f.ref, f.target, 100)
	if err != nil {
		t.Fatalf("read dirty: %v", err)
	}

	// The row changes AFTER we read the dirty keys but BEFORE we confirm.
	exec(t, ctx, f.rawSrc, "UPDATE rc_it.orders SET note='v2' WHERE id=1")

	// Apply the observed keys (re-read returns the CURRENT value, v2).
	cols, err := transportColumns(ctx, f.src, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	if err := pipeApply(ctx, f.src, f.sink, f.ref, cols, distinctKeys(observed)); err != nil {
		t.Fatalf("apply: %v", err)
	}
	// Confirm ONLY the observed delta ids.
	if err := f.src.ConfirmConsumed(ctx, f.ref, f.target, allIDs(observed)); err != nil {
		t.Fatalf("confirm: %v", err)
	}

	// The update's delta survived and keeps id=1 dirty.
	remaining, err := f.src.ReadDirtyKeys(ctx, f.ref, f.target, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(remaining) == 0 {
		t.Fatal("the mid-drain update's delta must survive (delete-by-id, not by PK)")
	}
	// Draining again converges to the latest value.
	if _, err := DrainAll(ctx, f.src, f.sink, f.ref, f.target, 100); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if note, _ := targetNote(t, ctx, f, 1); note != "v2" {
		t.Errorf("id=1 should converge to v2, got %q", note)
	}
	f.assertConverged(t, ctx)
}

// TestKeystoneCrashBeforeTrack: a crash between the apply and the track write
// replays the deltas on restart and converges without duplication (§3.3).
func TestKeystoneCrashBeforeTrack(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	f := newFixture(t, ctx, ordersDDL, "")

	exec(t, ctx, f.rawSrc, "INSERT INTO rc_it.orders VALUES (1, 'v1'), (2, 'v2')")
	dirty, err := f.src.ReadDirtyKeys(ctx, f.ref, f.target, 100)
	if err != nil {
		t.Fatal(err)
	}
	cols, err := transportColumns(ctx, f.src, f.ref)
	if err != nil {
		t.Fatal(err)
	}
	// Apply, then "crash" — never confirm.
	if err := pipeApply(ctx, f.src, f.sink, f.ref, cols, distinctKeys(dirty)); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if note, _ := targetNote(t, ctx, f, 1); note != "v1" {
		t.Fatalf("apply should have written id=1, got %q", note)
	}

	// Restart: the deltas are still unconsumed, so a full drain replays them.
	if _, err := DrainAll(ctx, f.src, f.sink, f.ref, f.target, 100); err != nil {
		t.Fatalf("drain after crash: %v", err)
	}
	f.assertConverged(t, ctx)
	// No duplication and the queue is now empty.
	var n int
	if err := f.rawTgt.QueryRow(ctx, "SELECT count(*) FROM rc_it.orders").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Errorf("target has %d rows, want 2 (no duplication)", n)
	}
	if again, _ := DrainAll(ctx, f.src, f.sink, f.ref, f.target, 100); again != 0 {
		t.Errorf("queue should be empty after replay, drained %d", again)
	}
}

// TestKeystoneCoalescing: many updates to one PK coalesce to a single re-read
// and the target converges to the final value (§3.3).
func TestKeystoneCoalescing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	f := newFixture(t, ctx, ordersDDL, "")

	exec(t, ctx, f.rawSrc, "INSERT INTO rc_it.orders VALUES (1, 'v0')")
	for i := 1; i <= 10; i++ {
		exec(t, ctx, f.rawSrc, "UPDATE rc_it.orders SET note='v"+strconv.Itoa(i)+"' WHERE id=1")
	}
	// 1 insert + 10 updates = 11 deltas for the single PK.
	dirty, err := f.src.ReadDirtyKeys(ctx, f.ref, f.target, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(dirty) != 11 {
		t.Fatalf("expected 11 deltas, got %d", len(dirty))
	}
	if dk := distinctKeys(dirty); len(dk) != 1 {
		t.Fatalf("11 deltas for one PK must coalesce to 1 distinct key, got %d", len(dk))
	}

	n, err := DrainAll(ctx, f.src, f.sink, f.ref, f.target, 100)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if n != 11 {
		t.Errorf("drained %d deltas, want 11", n)
	}
	if note, _ := targetNote(t, ctx, f, 1); note != "v10" {
		t.Errorf("id=1 should converge to the final value v10, got %q", note)
	}
}
