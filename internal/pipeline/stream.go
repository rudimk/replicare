package pipeline

import (
	"context"
	"errors"
	"time"

	"github.com/rudimk/replicare/internal/apply"
	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/observability/telemetry"
	"github.com/rudimk/replicare/internal/reseed"
	"github.com/rudimk/replicare/internal/state"
)

// Stream runs the continuous streaming phase until ctx is cancelled (CLAUDE.md
// §3.3, §8.1). Each tick drains every FK component (FK-ordered, retrying),
// refreshes the health/backlog signal, and enforces retention (routine purge +
// forced reseed). Because a drain pass checkpoints atomically (apply → confirm),
// cancelling BETWEEN passes is already a clean stop — the current pass finishes,
// then Stream returns ctx.Err(). That is the graceful-shutdown contract the
// daemon relies on (SIGTERM → cancel → drain-in-flight + checkpoint).
//
// A per-tick error is transient by policy: it is surfaced (health signal already
// fired) and the loop continues, so a downed target is retried and a loud halt
// keeps re-firing until the operator fixes the cause — it never silently aborts
// the sync. Only ctx cancellation ends the loop.
func (s *Syncer) Stream(ctx context.Context) error {
	interval := s.DrainInterval
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := s.streamOnce(ctx); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return err
				}
				s.log(ctx, "stream pass error", err)
			}
		}
	}
}

// StreamOnce runs a single streaming pass: drain every component, refresh health,
// then enforce retention. Exposed so a caller (or test) can drive convergence
// deterministically at a quiescent point instead of on the ticker.
func (s *Syncer) StreamOnce(ctx context.Context) error { return s.streamOnce(ctx) }

func (s *Syncer) streamOnce(ctx context.Context) error {
	// Drain each FK component in one FK-ordered, retrying pass. A failure here
	// (typically a target that went away) does NOT short-circuit the pass: the
	// retention enforcement below is source-side and must still run to protect a
	// source we may not own while the target is down (CLAUDE.md §3.4).
	var drainErr error
	for _, comp := range s.Components {
		if _, err := apply.DrainComponentRetrying(ctx, s.Source, s.Sink,
			comp.Order, s.Target, s.DrainBatch, apply.DefaultRetryPolicy); err != nil {
			drainErr = err
			break
		}
	}

	// Retention ALWAYS runs (source-side): routine purge of consumed deltas, and —
	// if the backlog is over the cap — sacrifice the laggard target (reset its
	// track, purge the unpinned deltas, flag it needs_reseed). This bounds source
	// growth even during a target outage; the actual re-copy is deferred to a
	// healthy pass below.
	reseeded, enforceErr := reseed.Enforce(ctx, s.reseedDeps(), s.Name, s.Replicable,
		[]engine.TargetID{s.Target}, s.Retention)

	if drainErr != nil {
		// Target unhealthy: the source is now protected (Enforce ran); surface the
		// failure and retry next pass. The needs_reseed flag, if set, persists and
		// is picked up once the target recovers.
		s.reportDrainFailure(ctx, drainErr)
		return drainErr
	}
	if enforceErr != nil {
		return enforceErr
	}

	// Healthy pass: refresh per-table backlog gauges every tick, but only emit the
	// escalating retention log once the backlog is actually approaching the cap
	// (half or more) — logging it at proximity 0 every tick is just noise.
	for _, t := range s.Replicable {
		bl, err := s.Source.DeltaBacklog(ctx, t, s.Target)
		if err != nil {
			continue
		}
		s.Tel.SetBacklog(s.Name, s.Target, t, bl)
		if prox := telemetry.RetentionProximity(bl, s.Retention); prox >= 0.5 {
			s.Tel.RetentionApproaching(ctx, s.Name, s.Target, t, bl, prox)
		}
	}
	s.Tel.SetTargetUp(s.Name, s.Target, true)

	// Reseed the target when either the retention cap forced it (Enforce, above) or
	// an operator flagged it via `replicare reseed` (a cursor marked needs_reseed).
	needReseed := containsTarget(reseeded, s.Target)
	if !needReseed {
		var err error
		if needReseed, err = s.targetNeedsReseed(ctx); err != nil {
			return err
		}
	}
	if needReseed {
		s.Tel.IncReseed(s.Name, s.Target)
		for _, comp := range s.Components {
			if err := reseed.Run(ctx, s.reseedDeps(), s.Name, s.Target, comp.Order, s.ChunkOpts); err != nil {
				return err
			}
		}
	}
	return nil
}

// targetNeedsReseed reports whether any of this target's cursors is flagged
// needs_reseed (an operator-forced reseed via `replicare reseed`).
func (s *Syncer) targetNeedsReseed(ctx context.Context) (bool, error) {
	cursors, err := s.Store.ListCursors(ctx, s.Name)
	if err != nil {
		return false, err
	}
	for _, c := range cursors {
		if c.Target == s.Target && c.NeedsReseed {
			return true, nil
		}
	}
	return false, nil
}

// containsTarget reports whether ts contains t.
func containsTarget(ts []engine.TargetID, t engine.TargetID) bool {
	for _, x := range ts {
		if x == t {
			return true
		}
	}
	return false
}

// reportDrainFailure fires the cross-channel signal for a failed drain: a target
// that no longer answers is flagged unreachable (all channels); a data-level
// failure is left to the apply layer's loud halt and just logged here.
func (s *Syncer) reportDrainFailure(ctx context.Context, cause error) {
	if !targetUnreachable(ctx, s.Sink) {
		s.log(ctx, "drain pass failed (target reachable)", cause)
		return
	}
	var bl engine.DeltaBacklog
	var repr engine.TableRef
	if len(s.Replicable) > 0 {
		repr = s.Replicable[0]
		bl, _ = s.Source.DeltaBacklog(ctx, repr, s.Target)
	}
	s.Tel.TargetUnreachable(ctx, nil, s.Name, s.Target, repr, bl,
		telemetry.RetentionProximity(bl, s.Retention), cause)
}

// reseedDeps builds the reseed orchestration dependencies from the Syncer.
func (s *Syncer) reseedDeps() reseed.Deps {
	return reseed.Deps{Src: s.Source, Sink: s.Sink, Workers: s.Workers, Store: s.Store}
}

// log records a streaming-loop event durably (best-effort) so operators see
// transient pass failures without a live metrics scrape.
func (s *Syncer) log(ctx context.Context, msg string, cause error) {
	s.recordEvent(ctx, state.Event{
		Sync: s.Name, Target: string(s.Target), Level: "WARN",
		Event: "stream.pass_error", Message: msg + ": " + cause.Error(),
	})
}
