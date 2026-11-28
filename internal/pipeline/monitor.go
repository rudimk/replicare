package pipeline

import (
	"context"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

// The streaming monitor (decision, 0.8.4, from a production catch-up). The backlog, lag,
// database sizes and the per-minute progress line used to come from the end of each pass,
// on the streaming connections: a long pass left the dashboard blank and the logs silent,
// a failed one published nothing, and reading every change queue in full on every pass
// added a large share of each pass on a busy source. The monitor does all of it on a
// source and a target connection of its own, on its own clock, so the numbers show from
// the start of streaming whatever the passes are doing, and a slow or failed read never
// touches a streaming connection.

var (
	// monitorInterval is the least time between two backlog rounds. A round that takes
	// longer waits twice its own length before the next, so on a big schema the monitor
	// never reads the source more than about a third of the time.
	monitorInterval = 30 * time.Second
	// monitorReadTimeout bounds one round on the monitor's own connections (a round that
	// overruns only costs the monitor its connection, which it opens again).
	monitorReadTimeout = 5 * time.Minute
)

// startMonitor starts the monitor when the syncer can open connections of its own
// (OpenSource), and returns a func that stops it and waits for it. Without OpenSource the
// passes keep publishing the numbers themselves.
func (s *Syncer) startMonitor(ctx context.Context) func() {
	if s.OpenSource == nil {
		return func() {}
	}
	s.monitorOn.Store(true)
	mctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{}, 2)
	go func() { defer func() { done <- struct{}{} }(); s.monitorBacklogs(mctx) }()
	go func() { defer func() { done <- struct{}{} }(); s.monitorProgressLog(mctx) }()
	return func() {
		cancel()
		<-done
		<-done
		s.monitorOn.Store(false)
	}
}

// monitorBacklogs publishes every table's backlog and lag, marks the target caught up,
// and refreshes the size gauges, round after round.
func (s *Syncer) monitorBacklogs(ctx context.Context) {
	var src engine.Source
	var sink engine.Sink
	defer func() {
		if src != nil {
			_ = src.Close(context.Background())
		}
		if sink != nil {
			_ = sink.Close(context.Background())
		}
	}()
	for {
		start := time.Now()
		if src == nil {
			if c, err := s.OpenSource(ctx); err == nil {
				src = c
			} else if ctx.Err() == nil {
				s.log(ctx, "streaming monitor: connect to the source (tried again next round)", err)
			}
		}
		if sink == nil && s.OpenSink != nil {
			if c, err := s.OpenSink(ctx); err == nil {
				sink = c
			}
		}
		if src != nil {
			rctx, cancel := context.WithTimeout(ctx, monitorReadTimeout)
			backlogs, err := s.publishBacklogsFrom(rctx, src)
			if err == nil {
				s.noteBacklogs(backlogs)
				if allCaughtUp(backlogs, s.Replicable) {
					s.noteCaughtUp()
				}
				s.measureDBSizesOn(rctx, src, sink)
			}
			s.Tel.RefreshETAs()
			cancel()
			if err != nil && ctx.Err() == nil {
				// Most likely the connection is gone: open a fresh one next round.
				_ = src.Close(context.Background())
				src = nil
				if sink != nil {
					_ = sink.Close(context.Background())
					sink = nil
				}
			}
		}
		wait := max(monitorInterval, 2*time.Since(start))
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}

// monitorProgressLog writes the per-minute progress line whether or not a pass finishes.
func (s *Syncer) monitorProgressLog(ctx context.Context) {
	t := time.NewTicker(progressLogInterval / 4)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.logStreamProgress(ctx) // writes at most once per progressLogInterval
		}
	}
}
