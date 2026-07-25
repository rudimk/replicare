package pipeline

import (
	"context"
	"testing"
	"time"
)

// Streaming-apply perf regression guard (M9). After bring-up, it queues a large
// delta backlog and times a single drain pass (dirty-key read → faithful re-read
// → staging upsert), asserting apply throughput stays above an explicit floor.
// Apply is heavier than copy (re-read + staging per pass); dev baseline is
// ~21k rows/s, and the floor is ~14x below for CI stability.
const applyThroughputFloorRowsPerSec = 1500

func TestPerfStreamApplyThroughput(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	rawSrc, rawTgt := freshFKSchema(t, ctx)
	syncer := buildSyncer(t, ctx)
	syncer.DrainBatch = 500_000 // drain the whole backlog in one pass to measure apply
	if err := syncer.Bringup(ctx); err != nil {
		t.Fatalf("Bringup: %v", err)
	}

	// Queue a large backlog of inserts (children of the seeded customers 1..20).
	const n = 50_000
	mustExecC(t, ctx, rawSrc,
		"INSERT INTO rc_it.orders SELECT g, (g%20)+1, 'o'||g FROM generate_series(1000, 1000+49999) g")

	start := time.Now()
	if err := syncer.StreamOnce(ctx); err != nil {
		t.Fatalf("StreamOnce: %v", err)
	}
	elapsed := time.Since(start)

	if got := countRows(t, ctx, rawTgt, "rc_it.orders"); got != 50+n {
		t.Fatalf("target orders = %d, want %d", got, 50+n)
	}
	rps := float64(n) / elapsed.Seconds()
	t.Logf("streaming apply throughput: %.0f rows/s (%d deltas in %s)", rps, n, elapsed.Round(time.Millisecond))
	if rps < applyThroughputFloorRowsPerSec {
		t.Errorf("apply throughput %.0f rows/s is below the floor of %d rows/s (regression)", rps, applyThroughputFloorRowsPerSec)
	}
}
