package telemetry

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/observability"
	"github.com/rudimk/replicare/internal/observability/prom"
)

// etaTel is a Telemetry on a fresh registry with a clock the test moves by hand.
func etaTel() (*Telemetry, *prom.Registry, *time.Time) {
	reg := prom.New()
	tel := New(reg, nil, nil, nil)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	tel.eta.now = func() time.Time { return now }
	return tel, reg, &now
}

func near(a, b float64) bool { return math.Abs(a-b) < 0.5 }

func TestInitialCopyETA(t *testing.T) {
	tel, reg, now := etaTel()
	a, b := engine.TableRef{Schema: "public", Name: "a"}, engine.TableRef{Schema: "public", Name: "b"}
	labels := map[string]string{"sync": "s"}
	eta := func() (float64, bool) { return gaugeValue(t, reg, observability.MetricInitialCopyETA, labels) }

	tel.SetPhase("s", a, "initial_copy")
	tel.SetPhase("s", b, "initial_copy")
	tel.SetRowsCopiedTarget("s", a, 60_000)
	tel.SetRowsCopiedTarget("s", b, 60_000)
	if _, ok := eta(); ok {
		t.Fatal("published an ETA before there was any history")
	}

	// 6,000 rows over 60s = 100 rows/s; 114,000 left -> 1,140s.
	*now = now.Add(30 * time.Second)
	tel.AddRowsCopied("s", a, 3_000)
	*now = now.Add(30 * time.Second)
	tel.AddRowsCopied("s", a, 3_000)
	if v, ok := eta(); !ok || !near(v, 1_140) {
		t.Fatalf("ETA = %v (%v), want 1140s", v, ok)
	}

	// Table a finishes (its estimate was high); only b's rows are left.
	tel.SetPhase("s", a, "streaming")
	if v, ok := eta(); !ok || !near(v, 600) {
		t.Fatalf("ETA after a finished = %v (%v), want 600s (60,000 rows at 100/s)", v, ok)
	}

	// Nothing moves for longer than the window: stalled.
	*now = now.Add(11 * time.Minute)
	tel.RefreshETAs()
	if v, ok := eta(); !ok || v != etaStalled {
		t.Fatalf("ETA with no progress = %v (%v), want %v", v, ok, etaStalled)
	}

	// Everything finished: 0.
	tel.SetPhase("s", b, "streaming")
	if v, ok := eta(); !ok || v != 0 {
		t.Fatalf("ETA when done = %v (%v), want 0", v, ok)
	}
}

func TestInitialCopyETAWithoutEstimates(t *testing.T) {
	tel, reg, now := etaTel()
	k := engine.TableRef{Schema: "redis", Name: "db0"}
	tel.SetPhase("s", k, "initial_copy")
	*now = now.Add(2 * time.Minute)
	tel.AddRowsCopied("s", k, 500)
	if v, ok := gaugeValue(t, reg, observability.MetricInitialCopyETA, map[string]string{"sync": "s"}); ok {
		t.Fatalf("ETA = %v with no row estimate to measure against, want none", v)
	}
}

func TestCatchupETA(t *testing.T) {
	tel, reg, now := etaTel()
	a, b := engine.TableRef{Schema: "public", Name: "a"}, engine.TableRef{Schema: "public", Name: "b"}
	labels := map[string]string{"sync": "s", "target": "dst"}
	eta := func() (float64, bool) { return gaugeValue(t, reg, observability.MetricCatchupETA, labels) }
	age := time.Hour
	backlog := func(na, nb int64) {
		tel.SetBacklog("s", "dst", a, "c", engine.DeltaBacklog{Rows: na, OldestAge: age})
		tel.SetBacklog("s", "dst", b, "c", engine.DeltaBacklog{Rows: nb, OldestAge: age})
	}

	backlog(50_000, 50_000)
	if _, ok := eta(); ok {
		t.Fatal("published an ETA before there was any history")
	}
	// Shrinks by 12,000 rows in 2 minutes = 100 rows/s; 88,000 left -> 880s.
	*now = now.Add(time.Minute)
	backlog(45_000, 49_000)
	*now = now.Add(time.Minute)
	backlog(40_000, 48_000)
	if v, ok := eta(); !ok || !near(v, 880) {
		t.Fatalf("ETA = %v (%v), want 880s", v, ok)
	}

	// Growing: not catching up.
	*now = now.Add(time.Minute)
	backlog(90_000, 90_000)
	if v, ok := eta(); !ok || v != etaStalled {
		t.Fatalf("ETA while growing = %v (%v), want %v", v, ok, etaStalled)
	}

	// Target down: not catching up, whatever the backlog did.
	tel.TargetUnreachable(t.Context(), nil, "s", "dst", a, "c", engine.DeltaBacklog{Rows: 1}, 0, errors.New("down"))
	if v, ok := eta(); !ok || v != etaStalled {
		t.Fatalf("ETA with the target down = %v (%v), want %v", v, ok, etaStalled)
	}

	// Steady writes: a small backlog that stays put but whose oldest change is fresh
	// is caught up, not stalled.
	age = 5 * time.Second
	*now = now.Add(time.Minute)
	backlog(30, 20)
	*now = now.Add(time.Minute)
	backlog(30, 20)
	if v, ok := eta(); !ok || v != 0 {
		t.Fatalf("ETA under steady writes = %v (%v), want 0", v, ok)
	}

	// Caught up: 0.
	*now = now.Add(time.Minute)
	backlog(0, 0)
	if v, ok := eta(); !ok || v != 0 {
		t.Fatalf("ETA when caught up = %v (%v), want 0", v, ok)
	}
}

func TestETASeriesKeepsOneSampleBeforeTheWindow(t *testing.T) {
	var s etaSeries
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i <= 30; i++ {
		s.add(start.Add(time.Duration(i)*time.Minute), float64(i*60))
	}
	now := start.Add(30 * time.Minute)
	if first := s.samples[0].at; now.Sub(first) < etaWindow || now.Sub(first) > etaWindow+time.Minute {
		t.Errorf("oldest kept sample is %v old, want just past the %v window", now.Sub(first), etaWindow)
	}
	if r, ok := s.rate(now); !ok || !near(r, 1) {
		t.Errorf("rate = %v (%v), want 1/s", r, ok)
	}
}
