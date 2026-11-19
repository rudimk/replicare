package pipeline

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/observability"
	"github.com/rudimk/replicare/internal/observability/prom"
	"github.com/rudimk/replicare/internal/observability/telemetry"
)

// growingSink reports a target that grows by 1 MiB every time it is measured.
type growingSink struct {
	engine.Sink
	calls atomic.Int64
}

func (g *growingSink) DatabaseSize(context.Context) (int64, error) {
	return (g.calls.Add(1)) << 20, nil
}
func (g *growingSink) ReplicatedSize(context.Context, []engine.TableRef) (int64, error) {
	return g.calls.Load() << 20, nil
}

// TestSizesUpdateWhileCopying: the size gauges are re-measured while a component copies,
// not only at bring-up and between components. Before, a long copy left the dashboard's
// target size stuck at its starting value while the target grew by gigabytes.
func TestSizesUpdateWhileCopying(t *testing.T) {
	old := dbSizeInterval
	dbSizeInterval = 10 * time.Millisecond
	defer func() { dbSizeInterval = old }()

	reg := prom.New()
	sink := &growingSink{}
	s := &Syncer{Name: "s", Source: fakeDelSource{}, Sink: sink, Target: "dst", Tel: telemetry.New(reg, nil, nil, nil)}

	stop := s.measureSizesWhileCopying(context.Background())
	time.Sleep(80 * time.Millisecond) // a "copy" in progress
	stop()
	after := sink.calls.Load()
	if after < 3 {
		t.Fatalf("measured %d times during an 80ms copy at a 10ms interval, want several", after)
	}
	if v, ok := metricValue(t, reg, observability.MetricTargetDBBytes); !ok || int64(v) != after<<20 {
		t.Errorf("target size gauge = %v (present=%v), want the latest measurement %d", v, ok, after<<20)
	}
	time.Sleep(40 * time.Millisecond)
	if n := sink.calls.Load(); n != after {
		t.Errorf("still measuring after stop: %d calls, was %d", n, after)
	}
	stop() // idempotent
}
