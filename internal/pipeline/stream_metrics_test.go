package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/observability"
	"github.com/rudimk/replicare/internal/observability/prom"
	"github.com/rudimk/replicare/internal/observability/telemetry"
)

// metricPresent reports whether the registry has emitted at least one sample for
// the named metric family — the "is this series populated on a live scrape?"
// check, independent of the metric's type (gauge/counter/histogram).
func metricPresent(t *testing.T, reg *prom.Registry, name string) bool {
	t.Helper()
	fams, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range fams {
		if f.GetName() == name && len(f.Metric) > 0 {
			return true
		}
	}
	return false
}

// TestSyncerMetricsPopulated is the regression for the observability emit-wiring
// gap: a live sync must publish more than target_up. It brings a sync up (initial
// copy → rows_copied_total), mutates the source, and runs streaming passes, then
// scrapes the registry and asserts the headline runtime series are all populated —
// exactly what a /metrics scrape of a healthy daemon should show.
func TestSyncerMetricsPopulated(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	rawSrc, _ := freshFKSchema(t, ctx)
	syncer := buildSyncer(t, ctx)

	// Back the telemetry with a real registry so we can scrape it (buildSyncer
	// wires a nil-registry Telemetry that discards metrics).
	reg := prom.New()
	syncer.Tel = telemetry.New(reg, nil, nil, nil)

	if err := syncer.Bringup(ctx); err != nil {
		t.Fatalf("Bringup: %v", err)
	}

	// Mutate the source so a streaming pass actually consumes deltas (non-zero
	// throughput and a moved backlog).
	mustExecC(t, ctx, rawSrc, "INSERT INTO rc_it.customers VALUES (21, 'c21')")
	mustExecC(t, ctx, rawSrc, "INSERT INTO rc_it.orders VALUES (51, 21, 'o51'), (52, 21, 'o52')")
	mustExecC(t, ctx, rawSrc, "UPDATE rc_it.orders SET note = 'o1-updated' WHERE id = 1")

	for i := 0; i < 3; i++ {
		if err := syncer.StreamOnce(ctx); err != nil {
			t.Fatalf("StreamOnce %d: %v", i, err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// The headline runtime series every healthy pass (or the initial copy) emits.
	want := []string{
		observability.MetricTargetUp,              // reachability
		observability.MetricRowsCopiedTotal,       // initial copy progressed
		observability.MetricApplyBatchSeconds,     // a drain pass ran
		observability.MetricThroughputRows,        // deltas were applied
		observability.MetricReplicationLagSeconds, // per-table lag refreshed
		observability.MetricPhaseInfo,             // per-table phase refreshed
	}
	for _, name := range want {
		if !metricPresent(t, reg, name) {
			t.Errorf("metric %q not populated on scrape (emit-wiring gap)", name)
		}
	}
}
