package pipeline

import (
	"context"
	"fmt"
	"time"

	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/observability"
	"github.com/rudimk/replicare/internal/state"
)

// The leftover-track sweep (engine.TrackSweeper). Versions before 0.7.0 never deleted
// a delta's track rows, so a source that ran one can hold millions of them. Removing
// them used to happen inside the purge, on every streaming pass, and on a busy source
// every pass then waited on large deletes. Now it runs here: in the background, on a
// short-lived connection of its own, one small window at a time with a pause between,
// so applying changes never waits for it.

// trackSweepPause is the wait between two sweep statements, to keep the load on the
// source low.
var trackSweepPause = 250 * time.Millisecond

// trackSweepInterval is the wait between sweep rounds. A round is cheap when there is
// nothing to remove (two index lookups per table); a table whose queue was empty
// during a round is swept by a later one.
var trackSweepInterval = 10 * time.Minute

// startTrackSweeper runs sweep rounds in the background until ctx ends, and returns a
// func that waits for it to stop. A no-op when the source has nothing to sweep with.
func (s *Syncer) startTrackSweeper(ctx context.Context) (wait func()) {
	if _, ok := s.Source.(engine.TrackSweeper); !ok || s.OpenSource == nil || len(s.Replicable) == 0 {
		return func() {}
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			removed, err := s.sweepTrackRound(ctx)
			if ctx.Err() != nil {
				return
			}
			switch {
			case err != nil:
				s.recordEvent(ctx, state.Event{Sync: s.Name, Target: string(s.Target), Level: "WARN",
					Event:   observability.EventTrackSweepFailed,
					Message: fmt.Sprintf("removing leftover track rows (tried again in %s): %v", trackSweepInterval, err)})
			case removed > 0:
				s.recordEvent(ctx, state.Event{Sync: s.Name, Target: string(s.Target), Level: "INFO",
					Event:   observability.EventTrackSwept,
					Message: fmt.Sprintf("removed %d leftover track rows from before 0.7.0", removed)})
			}
			if !sleepCtx(ctx, trackSweepInterval) {
				return
			}
		}
	}()
	return func() { <-done }
}

// sweepTrackRound opens a source connection and clears every replicated table's
// leftover track rows for this target, one window at a time, then closes it.
func (s *Syncer) sweepTrackRound(ctx context.Context) (int64, error) {
	src, err := s.OpenSource(ctx)
	if err != nil {
		return 0, fmt.Errorf("connect source: %w", err)
	}
	defer func() { _ = src.Close(context.Background()) }()
	sw, ok := src.(engine.TrackSweeper)
	if !ok {
		return 0, nil
	}
	targets := []engine.TargetID{s.Target}
	var total int64
	for _, t := range s.Replicable {
		for {
			n, err := sw.SweepLeftoverTrack(ctx, t, targets)
			total += n
			if err != nil {
				return total, err
			}
			if n == 0 {
				break
			}
			if !sleepCtx(ctx, trackSweepPause) {
				return total, ctx.Err()
			}
		}
	}
	return total, nil
}

// sleepCtx waits d, or returns false if ctx ends first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
