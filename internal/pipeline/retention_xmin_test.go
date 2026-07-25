package pipeline

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/rudimk/replicare/internal/engine"
)

// deltaRowCount returns the number of delta rows queued for a captured table on
// the source (its logical backlog, regardless of physical bloat).
func deltaRowCount(t *testing.T, ctx context.Context, rawSrc *pgx.Conn, table string) int {
	t.Helper()
	var relID int
	if err := rawSrc.QueryRow(ctx,
		"SELECT rel_id FROM replicare.captured WHERE schema_name='rc_it' AND table_name=$1", table).Scan(&relID); err != nil {
		t.Fatalf("lookup rel_id for %s: %v", table, err)
	}
	var n int
	if err := rawSrc.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM replicare."delta_%d"`, relID)).Scan(&n); err != nil {
		t.Fatalf("count deltas for %s: %v", table, err)
	}
	return n
}

// TestStreamRetentionProtectsSourceUnderXmin is the M9 shippable-gate capstone
// for source protection: with a pinned xmin horizon (autovacuum cannot reclaim)
// and the target DOWN, a streaming pass still bounds the source's LOGICAL delta
// growth via retention — it purges the over-cap backlog and flags the target for
// reseed — even though the drain itself failed. When the target recovers, the
// deferred reseed re-copies it to convergence, losing nothing. This proves
// retention/reseed protects a source we may not own during a target outage under
// a stalled xmin (CLAUDE.md §3.4).
func TestStreamRetentionProtectsSourceUnderXmin(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	rawSrc, rawTgt := freshFKSchema(t, ctx)
	syncer := buildSyncer(t, ctx)
	syncer.Retention = engine.RetentionPolicy{MaxAgeSeconds: 3600} // 1h age cap
	if err := syncer.Bringup(ctx); err != nil {
		t.Fatalf("Bringup: %v", err)
	}

	// Pin the source xmin horizon so no dead delta tuple is reclaimable.
	pin := rawConn(t, ctx, srcCfg())
	defer pin.Close(context.Background())
	pinTx, err := pin.Begin(ctx)
	if err != nil {
		t.Fatalf("pin begin: %v", err)
	}
	if _, err := pinTx.Exec(ctx, "SELECT txid_current()"); err != nil {
		t.Fatalf("pin snapshot: %v", err)
	}

	// Target goes down; changes pile up unconsumed on the source.
	down := true
	syncer.Sink = togglableSink{Sink: syncer.Sink, down: &down}
	mustExecC(t, ctx, rawSrc, "INSERT INTO rc_it.orders SELECT g, (g%20)+1, 'o'||g FROM generate_series(51, 70) g")

	// Age the backlog past the retention cap (simulates a long outage).
	relID := 0
	_ = rawSrc.QueryRow(ctx, "SELECT rel_id FROM replicare.captured WHERE table_name='orders'").Scan(&relID)
	mustExecC(t, ctx, rawSrc, fmt.Sprintf(`UPDATE replicare."delta_%d" SET rc_at = now() - interval '48 hours'`, relID))

	before := deltaRowCount(t, ctx, rawSrc, "orders")
	if before < 20 {
		t.Fatalf("expected an aged backlog before enforcement, got %d", before)
	}

	// A pass while DOWN fails the drain, but retention still runs and protects the
	// source: the over-cap deltas are purged and the target is flagged for reseed.
	if err := syncer.StreamOnce(ctx); err == nil {
		t.Fatalf("StreamOnce should fail while target is down")
	}
	after := deltaRowCount(t, ctx, rawSrc, "orders")
	if after >= before {
		t.Errorf("source logical delta backlog not bounded under xmin+outage: before=%d after=%d", before, after)
	}
	cur, _ := syncer.Store.LoadCursor(ctx, "s1", "dst", engine.TableRef{Schema: "rc_it", Name: "orders"})
	if !cur.NeedsReseed {
		t.Errorf("target should be flagged needs_reseed after over-cap enforcement")
	}

	// Recover: release the xmin pin, bring the target back. The deferred reseed
	// re-copies to current source state and converges (the purged rows are covered
	// by the re-copy — no delta lost).
	_ = pinTx.Rollback(ctx)
	down = false
	for i := 0; i < 2; i++ {
		if err := syncer.StreamOnce(ctx); err != nil {
			t.Fatalf("StreamOnce after recovery: %v", err)
		}
	}
	if got := countRows(t, ctx, rawTgt, "rc_it.orders"); got != 70 {
		t.Errorf("target did not converge after reseed: %d rows, want 70", got)
	}
}
