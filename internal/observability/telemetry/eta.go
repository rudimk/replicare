package telemetry

import (
	"sync"
	"time"

	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/observability"
)

// The two ETA gauges are computed here, inside the daemon, so a dashboard shows a plain
// duration instead of a PromQL ratio the operator has to interpret:
//
//   - replicare_initial_copy_eta_seconds{sync}: rows still to copy (each copying table's
//     row estimate minus what it has copied) divided by the copy rate over the last
//     etaWindow. 0 once nothing is left to copy.
//   - replicare_catchup_eta_seconds{sync,target}: the target's delta backlog divided by
//     how fast that backlog has been shrinking over the last etaWindow. 0 when caught up:
//     no backlog, or nothing waiting longer than etaCaughtUp.
//
// Both use the value etaStalled (-1) for "not moving": no rows copied in the window, or
// a backlog that is flat, growing, or whose target is unreachable. A gauge with no
// series yet means there is not enough history to estimate (under etaMinSpan).

const (
	etaWindow  = 10 * time.Minute
	etaMinSpan = time.Minute
	etaStalled = -1.0
	// etaCaughtUp: a target whose oldest waiting change is younger than this is caught
	// up. Under steady writes the backlog never quite reaches 0 and stops shrinking,
	// which is healthy, not "not catching up".
	etaCaughtUp = time.Minute
)

type etaSample struct {
	at    time.Time
	value float64
}

// etaSeries keeps a running total's recent history: every sample inside the window, plus
// the newest one older than it, so a rate always spans the full window once it exists.
type etaSeries struct{ samples []etaSample }

func (s *etaSeries) add(at time.Time, v float64) {
	if n := len(s.samples); n > 0 && at.Sub(s.samples[n-1].at) < time.Second {
		s.samples[n-1] = etaSample{at, v} // several updates in one pass: keep the latest
	} else {
		s.samples = append(s.samples, etaSample{at, v})
	}
	cut := 0
	for i := 1; i < len(s.samples) && at.Sub(s.samples[i].at) >= etaWindow; i++ {
		cut = i
	}
	s.samples = s.samples[cut:]
}

// rate is the change per second from the oldest kept sample to now (not to the newest
// sample, so a total that stops moving shows a falling rate). ok is false until the
// history spans etaMinSpan.
func (s *etaSeries) rate(now time.Time) (perSec float64, ok bool) {
	if len(s.samples) == 0 {
		return 0, false
	}
	first, last := s.samples[0], s.samples[len(s.samples)-1]
	// Measure from the newest sample at or before the window's start (the window ends now,
	// not at the last update), so progress that stopped long ago doesn't count.
	for _, smp := range s.samples[1:] {
		if now.Sub(smp.at) < etaWindow {
			break
		}
		first = smp
	}
	span := now.Sub(first.at)
	if span < etaMinSpan {
		return 0, false
	}
	return (last.value - first.value) / span.Seconds(), true
}

type copyProgress struct {
	estimate map[string]int64 // table -> source row estimate
	copied   map[string]int64 // table -> rows copied this run
	finished map[string]bool  // table no longer in initial copy
	total    etaSeries        // rows copied, all tables
}

type catchupProgress struct {
	backlog map[string]int64         // table -> unconsumed delta rows
	oldest  map[string]time.Duration // table -> age of its oldest unconsumed delta
	total   etaSeries
	down    bool
}

type etaTracker struct {
	mu      sync.Mutex
	now     func() time.Time
	copy    map[string]*copyProgress       // by sync
	catchup map[[2]string]*catchupProgress // by (sync, target)
}

func newETATracker() *etaTracker {
	return &etaTracker{now: time.Now, copy: map[string]*copyProgress{}, catchup: map[[2]string]*catchupProgress{}}
}

func (e *etaTracker) copyFor(sync string) *copyProgress {
	c := e.copy[sync]
	if c == nil {
		c = &copyProgress{estimate: map[string]int64{}, copied: map[string]int64{}, finished: map[string]bool{}}
		e.copy[sync] = c
	}
	return c
}

func (e *etaTracker) catchupFor(sync string, target engine.TargetID) *catchupProgress {
	k := [2]string{sync, string(target)}
	c := e.catchup[k]
	if c == nil {
		c = &catchupProgress{backlog: map[string]int64{}, oldest: map[string]time.Duration{}}
		e.catchup[k] = c
	}
	return c
}

// copyETA returns the initial-copy ETA for a sync and whether there is one to publish.
func (e *etaTracker) copyETA(sync string) (float64, bool) {
	c := e.copy[sync]
	if c == nil || len(c.estimate) == 0 {
		return 0, false // no row estimates (an engine without one): nothing to measure against
	}
	var remaining int64
	copying := false
	for tbl, est := range c.estimate {
		if c.finished[tbl] {
			continue
		}
		copying = true
		if left := est - c.copied[tbl]; left > 0 {
			remaining += left
		}
	}
	if !copying {
		return 0, true
	}
	r, ok := c.total.rate(e.now())
	switch {
	case !ok:
		return 0, false
	case r <= 0:
		return etaStalled, true
	case remaining == 0:
		// Past the source's own estimate but not finished yet: no honest figure.
		return 0, false
	}
	return float64(remaining) / r, true
}

// catchupETA returns the catch-up ETA for a target and whether there is one to publish.
func (e *etaTracker) catchupETA(c *catchupProgress) (float64, bool) {
	if c.down {
		return etaStalled, true
	}
	var backlog int64
	for _, n := range c.backlog {
		backlog += n
	}
	var oldest time.Duration
	for _, a := range c.oldest {
		oldest = max(oldest, a)
	}
	if backlog == 0 || oldest < etaCaughtUp {
		return 0, true
	}
	r, ok := c.total.rate(e.now())
	switch {
	case !ok:
		return 0, false
	case r >= 0:
		return etaStalled, true
	}
	return float64(backlog) / -r, true
}

// --- Telemetry hooks ---

func (t *Telemetry) publishCopyETA(sync string) {
	if t.reg == nil {
		return
	}
	g := t.reg.Gauge(observability.MetricInitialCopyETA)
	if v, ok := t.eta.copyETA(sync); ok {
		g.WithLabelValues(sync).Set(v)
	} else {
		g.DeleteLabelValues(sync)
	}
}

func (t *Telemetry) publishCatchupETA(sync string, target engine.TargetID, c *catchupProgress) {
	if t.reg == nil {
		return
	}
	g := t.reg.Gauge(observability.MetricCatchupETA)
	if v, ok := t.eta.catchupETA(c); ok {
		g.WithLabelValues(sync, string(target)).Set(v)
	} else {
		g.DeleteLabelValues(sync, string(target))
	}
}

func (t *Telemetry) etaRowsCopied(sync string, table engine.TableRef, n int64) {
	t.eta.mu.Lock()
	defer t.eta.mu.Unlock()
	c := t.eta.copyFor(sync)
	c.copied[table.String()] += n
	var sum int64
	for _, v := range c.copied {
		sum += v
	}
	c.total.add(t.eta.now(), float64(sum))
	t.publishCopyETA(sync)
}

func (t *Telemetry) etaCopyEstimate(sync string, table engine.TableRef, total int64) {
	t.eta.mu.Lock()
	defer t.eta.mu.Unlock()
	c := t.eta.copyFor(sync)
	c.estimate[table.String()] = total
	if len(c.total.samples) == 0 {
		c.total.add(t.eta.now(), 0) // the copy's starting point, so the first rate is honest
	}
	t.publishCopyETA(sync)
}

func (t *Telemetry) etaPhase(sync string, table engine.TableRef, phase string) {
	t.eta.mu.Lock()
	defer t.eta.mu.Unlock()
	c := t.eta.copyFor(sync)
	c.finished[table.String()] = phase != "initial_copy"
	t.publishCopyETA(sync)
}

func (t *Telemetry) etaBacklog(sync string, target engine.TargetID, table engine.TableRef, bl engine.DeltaBacklog) {
	t.eta.mu.Lock()
	defer t.eta.mu.Unlock()
	c := t.eta.catchupFor(sync, target)
	c.down = false
	c.backlog[table.String()] = bl.Rows
	c.oldest[table.String()] = bl.OldestAge
	var sum int64
	for _, v := range c.backlog {
		sum += v
	}
	c.total.add(t.eta.now(), float64(sum))
	t.publishCatchupETA(sync, target, c)
}

func (t *Telemetry) etaTargetDown(sync string, target engine.TargetID) {
	t.eta.mu.Lock()
	defer t.eta.mu.Unlock()
	c := t.eta.catchupFor(sync, target)
	c.down = true
	t.publishCatchupETA(sync, target, c)
}

// RefreshETAs recomputes every ETA against the current time, so a copy or backlog that
// has stopped moving shows it even when no new progress arrives to trigger an update.
func (t *Telemetry) RefreshETAs() {
	t.eta.mu.Lock()
	defer t.eta.mu.Unlock()
	for sync := range t.eta.copy {
		t.publishCopyETA(sync)
	}
	for k, c := range t.eta.catchup {
		t.publishCatchupETA(k[0], engine.TargetID(k[1]), c)
	}
}
