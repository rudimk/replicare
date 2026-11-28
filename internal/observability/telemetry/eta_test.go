package telemetry

import (
	"context"
	"errors"
	"fmt"
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
	tel.TargetUnreachable(context.Background(), nil, "s", "dst", a, "c", engine.DeltaBacklog{Rows: 1}, 0, errors.New("down"))
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

// TestInitialCopyETAWeighsBySize: a fast run of small rows must not make a table of big
// rows look quick. Table a: 100,000 small rows, 1 GB. Table b: 10,000 large rows, 9 GB.
// Copying a at 1,000 rows/s moves 10 MB/s, so the 9.9 GB left takes ~990s, where a row
// count would have said (99,000 + 10,000) / 1,000 = 109s.
func TestInitialCopyETAWeighsBySize(t *testing.T) {
	tel, reg, now := etaTel()
	a, b := engine.TableRef{Schema: "public", Name: "a"}, engine.TableRef{Schema: "public", Name: "b"}
	labels := map[string]string{"sync": "s"}
	const gb = 1_000_000_000
	for _, tb := range []engine.TableRef{a, b} {
		tel.SetPhase("s", tb, "initial_copy")
	}
	tel.SetRowsCopiedTarget("s", a, 100_000)
	tel.SetRowsCopiedTarget("s", b, 10_000)
	tel.SetCopyTableBytes("s", a, 1*gb)
	tel.SetCopyTableBytes("s", b, 9*gb)

	*now = now.Add(30 * time.Second)
	tel.AddRowsCopied("s", a, 30_000)
	*now = now.Add(30 * time.Second)
	tel.AddRowsCopied("s", a, 30_000)
	// 60,000 rows of a in 60s = 0.6 GB in 60s = 10 MB/s; left: 0.4 GB of a + 9 GB of b.
	v, ok := gaugeValue(t, reg, observability.MetricInitialCopyETA, labels)
	if !ok || math.Abs(v-940) > 1 {
		t.Fatalf("ETA = %v (%v), want 940s (9.4 GB left at 10 MB/s)", v, ok)
	}
}

// TestInitialCopyCountsTheLinkFillPass: a table linked in a loop is copied in two stages,
// and the second (filling in its links) is part of the work. With its rows all in, the
// copy is half done, not finished; the progress gauge only reads 1 once the table is done.
func TestInitialCopyCountsTheLinkFillPass(t *testing.T) {
	tel, reg, now := etaTel()
	m := engine.TableRef{Schema: "public", Name: "msg"}
	labels := map[string]string{"sync": "s"}
	gauge := func(name string, l map[string]string) (float64, bool) { return gaugeValue(t, reg, name, l) }

	tel.SetPhase("s", m, "initial_copy")
	tel.SetRowsCopiedTarget("s", m, 1_000)
	tel.SetCopyTableBytes("s", m, 1_000_000)
	tel.SetLinkFillPlanned("s", m)
	if v, ok := gauge(observability.MetricLinkFillRowsTarget, map[string]string{"sync": "s", "table": "public.msg"}); !ok || v != 1_000 {
		t.Fatalf("link-fill target = %v (%v), want 1000", v, ok)
	}

	*now = now.Add(time.Minute)
	tel.AddRowsCopied("s", m, 1_000)
	if v, ok := gauge(observability.MetricInitialCopyProgress, labels); !ok || math.Abs(v-0.5) > 0.001 {
		t.Fatalf("progress with the rows in but no links filled = %v (%v), want 0.5", v, ok)
	}
	// 1 MB of work in 60s; 1 MB (the fill) left: about 60s.
	if v, ok := gauge(observability.MetricInitialCopyETA, labels); !ok || math.Abs(v-60) > 1 {
		t.Fatalf("ETA = %v (%v), want ~60s for the link-filling pass still to run", v, ok)
	}

	*now = now.Add(time.Minute)
	tel.AddRowsFilled("s", m, 1_000)
	if v, ok := gauge(observability.MetricInitialCopyProgress, labels); !ok || v >= 1 {
		t.Fatalf("progress before the table switched to streaming = %v (%v), want under 1", v, ok)
	}
	tel.SetPhase("s", m, "streaming")
	if v, ok := gauge(observability.MetricInitialCopyProgress, labels); !ok || v != 1 {
		t.Fatalf("progress when done = %v (%v), want 1", v, ok)
	}
}

// TestInitialCopyETAKeepsMovingPastAStaleEstimate: the source's row estimate comes from
// its statistics and is often low. A table that keeps copying past it is still work being
// done; counting it as nothing made the measured rate fall and the ETA climb to years
// while the copy ran at full speed. Table a: estimated 1,000 rows, 1 GB (it really holds
// more). Table b: 10,000 rows, 9 GB.
func TestInitialCopyETAKeepsMovingPastAStaleEstimate(t *testing.T) {
	tel, reg, now := etaTel()
	a, b := engine.TableRef{Schema: "public", Name: "a"}, engine.TableRef{Schema: "public", Name: "b"}
	labels := map[string]string{"sync": "s"}
	const gb = 1_000_000_000
	for _, tb := range []engine.TableRef{a, b} {
		tel.SetPhase("s", tb, "initial_copy")
	}
	tel.SetRowsCopiedTarget("s", a, 1_000)
	tel.SetRowsCopiedTarget("s", b, 10_000)
	tel.SetCopyTableBytes("s", a, 1*gb)
	tel.SetCopyTableBytes("s", b, 9*gb)

	// 1,000 rows of a every minute (1 GB/min at a's 1 MB a row), long past its estimate.
	for range 12 {
		*now = now.Add(time.Minute)
		tel.AddRowsCopied("s", a, 1_000)
	}
	// Over the last 10 minutes: 10 GB in 600s. b's 9 GB left takes 540s.
	if v, ok := gaugeValue(t, reg, observability.MetricInitialCopyETA, labels); !ok || math.Abs(v-540) > 1 {
		t.Fatalf("ETA = %v (%v), want 540s: a table past its estimate is still moving", v, ok)
	}
	if v, ok := gaugeValue(t, reg, observability.MetricInitialCopyProgress, labels); !ok || math.Abs(v-0.1) > 0.001 {
		t.Fatalf("progress = %v (%v), want 0.1: a counts no more than its own size", v, ok)
	}
}

// TestInitialCopyETATableWithoutARowEstimate: a table the source has never analysed has no
// row estimate. It is still work to do (weighed at the other tables' bytes per row), not a
// table already done. Table a: 1,000 rows, 1 GB. Table b: no estimate, 1 GB.
func TestInitialCopyETATableWithoutARowEstimate(t *testing.T) {
	tel, reg, now := etaTel()
	a, b := engine.TableRef{Schema: "public", Name: "a"}, engine.TableRef{Schema: "public", Name: "b"}
	labels := map[string]string{"sync": "s"}
	const gb = 1_000_000_000
	for _, tb := range []engine.TableRef{a, b} {
		tel.SetPhase("s", tb, "initial_copy")
	}
	tel.SetRowsCopiedTarget("s", a, 1_000)
	tel.SetRowsCopiedTarget("s", b, -1)
	tel.SetCopyTableBytes("s", a, 1*gb)
	tel.SetCopyTableBytes("s", b, 1*gb)

	*now = now.Add(30 * time.Second)
	tel.AddRowsCopied("s", a, 250)
	*now = now.Add(30 * time.Second)
	tel.AddRowsCopied("s", a, 250)
	// 0.5 GB in 60s; 0.5 GB of a and all 1 GB of b left: 180s.
	if v, ok := gaugeValue(t, reg, observability.MetricInitialCopyETA, labels); !ok || math.Abs(v-180) > 1 {
		t.Fatalf("ETA = %v (%v), want 180s with b still to copy", v, ok)
	}
	if v, ok := gaugeValue(t, reg, observability.MetricInitialCopyProgress, labels); !ok || math.Abs(v-0.25) > 0.001 {
		t.Fatalf("progress = %v (%v), want 0.25", v, ok)
	}
	if st, ok := tel.CopyStatus("s"); !ok || st.RowsEstimate != 1_000 || st.Tables != 2 {
		t.Fatalf("copy status = %+v (%v), want 2 tables and an estimate of 1,000 rows", st, ok)
	}
}

// TestInitialCopyETAWaitsWhileTooEarlyToTell: under 1% done, the rate mostly reflects
// per-table set-up on small tables, so no ETA is published for the first half hour.
func TestInitialCopyETAWaitsWhileTooEarlyToTell(t *testing.T) {
	tel, reg, now := etaTel()
	a := engine.TableRef{Schema: "public", Name: "a"}
	labels := map[string]string{"sync": "s"}
	tel.SetPhase("s", a, "initial_copy")
	tel.SetRowsCopiedTarget("s", a, 100_000_000)
	for i := 1; i <= 31; i++ {
		*now = now.Add(time.Minute)
		tel.AddRowsCopied("s", a, 1_000)
		_, ok := gaugeValue(t, reg, observability.MetricInitialCopyETA, labels)
		if i < 30 && ok {
			t.Fatalf("ETA published after %d minutes at well under 1%% done", i)
		}
		if i == 31 && !ok {
			t.Fatal("no ETA after 31 minutes")
		}
	}
}

// A round over many tables takes minutes on a busy source. The first round after a start
// fills the backlog in table by table; sampled per table, the history began near zero and
// "grew", so the ETA read not-catching-up for its whole first window (production, 0.8.4).
// With the round marked, the history takes one sample per round, of the whole backlog.
func TestCatchupETASlowRoundsAfterStart(t *testing.T) {
	tel, reg, now := etaTel()
	labels := map[string]string{"sync": "s", "target": "dst"}
	tables := make([]engine.TableRef, 40)
	for i := range tables {
		tables[i] = engine.TableRef{Schema: "public", Name: fmt.Sprintf("t%02d", i)}
	}
	round := func(rows int64) {
		done := tel.BacklogRound("s", "dst")
		for _, tbl := range tables {
			*now = now.Add(5 * time.Second) // 40 tables, 200s for the round
			tel.SetBacklog("s", "dst", tbl, "c", engine.DeltaBacklog{Rows: rows, OldestAge: time.Hour})
		}
		done(true)
	}

	round(25_000) // 1,000,000 waiting
	*now = now.Add(5 * time.Minute)
	round(24_000) // 960,000: 40,000 fewer over 5m + 200s = 500s, i.e. 80 rows/s
	v, ok := gaugeValue(t, reg, observability.MetricCatchupETA, labels)
	if !ok || v == etaStalled {
		t.Fatalf("ETA after two slow rounds = %v (%v), want a duration, not stalled", v, ok)
	}
	if !near(v, 960_000/80.0) {
		t.Fatalf("ETA = %v, want %v", v, 960_000/80.0)
	}
}

// A round cut short (the reader stopped part-way) holds only part of the backlog; it must
// not enter the ETA's history, or the backlog seems to shrink and then jump back.
func TestCatchupETAIgnoresAnIncompleteRound(t *testing.T) {
	tel, reg, now := etaTel()
	labels := map[string]string{"sync": "s", "target": "dst"}
	a, b := engine.TableRef{Schema: "public", Name: "a"}, engine.TableRef{Schema: "public", Name: "b"}
	round := func(na, nb int64, complete bool) {
		done := tel.BacklogRound("s", "dst")
		*now = now.Add(time.Minute)
		tel.SetBacklog("s", "dst", a, "c", engine.DeltaBacklog{Rows: na, OldestAge: time.Hour})
		if complete {
			tel.SetBacklog("s", "dst", b, "c", engine.DeltaBacklog{Rows: nb, OldestAge: time.Hour})
		}
		done(complete)
	}
	round(500_000, 500_000, true) // 1,000,000
	*now = now.Add(5 * time.Minute)
	round(10, 0, false) // cut short after a, which is nearly empty now: no sample
	*now = now.Add(5 * time.Minute)
	round(400_000, 400_000, true) // 800,000: 200,000 fewer over 12 minutes
	v, ok := gaugeValue(t, reg, observability.MetricCatchupETA, labels)
	if !ok || v == etaStalled {
		t.Fatalf("ETA = %v (%v), want a duration", v, ok)
	}
	if want := 800_000 / (200_000 / (12 * 60.0)); !near(v, want) {
		t.Fatalf("ETA = %v, want %v", v, want)
	}
}
