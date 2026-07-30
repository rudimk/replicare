package pipeline

import (
	"context"
	"fmt"
	"time"

	"github.com/rudimk/replicare/internal/copy"
	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/observability"
	"github.com/rudimk/replicare/internal/observability/telemetry"
	"github.com/rudimk/replicare/internal/state"
)

// Syncer runs one sync's lifecycle for a single target (CLAUDE.md §4, §8): bring
// a cold sync up to streaming, then drain continuously. It is built on the engine
// interfaces plus the M4 copy driver and M5* drain/reseed, so it is
// engine-neutral. Fan-out (one source → several targets) runs one Syncer per
// target in v1 — copy progress is keyed per (sync, table), so a shared
// multi-target copy is deferred (fan-out interface present, not hardened).
type Syncer struct {
	Name    string
	Source  engine.Source
	Sink    engine.Sink
	Target  engine.TargetID
	Workers []copy.Worker // copy pool for the initial copy (each a Source/Sink pair)
	Store   state.StateStore
	Tel     *telemetry.Telemetry

	// Plan, from pre-flight (topo-ordered components over the replicable tables).
	Components []engine.Component
	Replicable []engine.TableRef

	ChunkOpts     engine.ChunkOptions
	DrainBatch    int
	DrainInterval time.Duration
	Retention     engine.RetentionPolicy
}

// Bringup takes a cold sync to streaming (CLAUDE.md §4): it installs capture
// FIRST (so changes queue into delta tables before and during the copy), then
// for each FK component runs the resumable, parents-first initial copy and cuts
// the component over to streaming. It is idempotent and resumable — capture
// install is idempotent, the copy resumes from its checkpoints, and a component
// already cut over is re-copied only where progress says it is unfinished — so a
// restart mid-bringup continues cleanly.
func (s *Syncer) Bringup(ctx context.Context) error {
	if len(s.Workers) == 0 {
		return fmt.Errorf("syncer %s: no copy workers", s.Name)
	}
	// Capture-first: install over every replicable table before copying, so the
	// delta queue is already capturing when the copy window opens.
	if err := s.Source.InstallCapture(ctx, s.Replicable); err != nil {
		return fmt.Errorf("syncer %s: install capture: %w", s.Name, err)
	}
	s.recordEvent(ctx, state.Event{
		Sync: s.Name, Level: "INFO", Event: observability.EventCaptureInstalled,
		Message: fmt.Sprintf("capture installed on %d tables", len(s.Replicable)),
	})

	for _, comp := range s.Components {
		if err := s.copyAndCutover(ctx, comp); err != nil {
			return err
		}
	}
	return nil
}

// copyAndCutover copies one component parents-first, then flips each of its
// tables' cursors to streaming for this target. The copy is checkpointed by the
// StateStore, so an interrupted component resumes rather than restarting.
func (s *Syncer) copyAndCutover(ctx context.Context, comp engine.Component) error {
	progress := copy.WithProgress(func(t engine.TableRef, n int64) {
		s.Tel.AddRowsCopied(s.Name, t, n)
	})
	if err := copy.Component(ctx, s.Workers, s.Store, s.Name, comp.Order, s.ChunkOpts, progress); err != nil {
		return fmt.Errorf("syncer %s: copy component: %w", s.Name, err)
	}
	for _, t := range comp.Order {
		cur, err := s.Store.LoadCursor(ctx, s.Name, s.Target, t)
		if err != nil {
			return fmt.Errorf("syncer %s: load cursor %s: %w", s.Name, t, err)
		}
		cur.Phase = state.PhaseStreaming
		if err := s.Store.SaveCursor(ctx, s.Name, cur); err != nil {
			return fmt.Errorf("syncer %s: cutover cursor %s: %w", s.Name, t, err)
		}
	}
	s.recordEvent(ctx, state.Event{
		Sync: s.Name, Target: string(s.Target), Level: "INFO", Event: observability.EventCutover,
		Message: fmt.Sprintf("component of %d tables cut over to streaming", len(comp.Order)),
	})
	return nil
}

// recordEvent persists an operational event when a StateStore is present, logging
// a best-effort failure rather than aborting the lifecycle for an audit write.
func (s *Syncer) recordEvent(ctx context.Context, e state.Event) {
	if s.Store == nil {
		return
	}
	_ = s.Store.RecordEvent(ctx, e)
}
