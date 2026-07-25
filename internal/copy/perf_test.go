package copy

import (
	"context"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

// Perf regression guard (M9). Not a micro-benchmark: it copies a realistic row
// count end-to-end (text COPY, chunked, parents-first) and asserts throughput
// stays above an explicit floor, so an accidental per-row round-trip or an N+1
// query pattern fails CI instead of silently slowing replication. The floor is
// set well below healthy throughput to stay stable across CI hardware (dev
// baseline: ~260k rows/s with 4 workers; the floor is ~80x below that), and the
// actual number is logged every run for tracking.
const copyThroughputFloorRowsPerSec = 3000

func TestPerfCopyThroughput(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	f := newFixture(t, ctx)
	exec(t, ctx, f.rawSrc, ordersDDL)
	exec(t, ctx, f.rawTgt, ordersDDL)

	const n = 100_000
	exec(t, ctx, f.rawSrc, "INSERT INTO rc_it.orders SELECT g, 'note '||g FROM generate_series(1,100000) g")

	ref := engine.TableRef{Schema: "rc_it", Name: "orders"}
	workers := f.newWorkers(t, ctx, 4)

	start := time.Now()
	if err := Component(ctx, workers, f.store, f.syncName, []engine.TableRef{ref}, engine.ChunkOptions{TargetRows: 5000}); err != nil {
		t.Fatalf("copy: %v", err)
	}
	elapsed := time.Since(start)

	if got := f.rowCount(t, ctx, f.rawTgt); got != n {
		t.Fatalf("target has %d rows, want %d", got, n)
	}
	rps := float64(n) / elapsed.Seconds()
	t.Logf("copy throughput: %.0f rows/s (%d rows in %s, 4 workers)", rps, n, elapsed.Round(time.Millisecond))
	if rps < copyThroughputFloorRowsPerSec {
		t.Errorf("copy throughput %.0f rows/s is below the floor of %d rows/s (regression)", rps, copyThroughputFloorRowsPerSec)
	}
}
