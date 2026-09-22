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

	// ClusterMode marks this syncer as one edge of an active-active cluster
	// (multi-master): its source installs loop-suppressing origin-aware capture and
	// its sink is origin-marking, so replicare's own applies are not re-captured and
	// echoed back around the mesh (CLAUDE.md §6, docs/multi-master.md §5.4). Off (the
	// default) is the one-way path, byte-identical to the pre-multi-master daemon.
	ClusterMode bool

	// ApplyConcurrency is how many of a component's tables may apply at once during
	// streaming (CLAUDE.md §8 parallel delta apply). 1 (the default) is the
	// strictly-sequential per-table drain. Higher values fan the per-table apply
	// across a pool built from the primary connection plus the copy Workers (idle
	// during streaming), bounded by how many pairs are available.
	ApplyConcurrency int

	// Heartbeat, if set, is called once per streaming iteration (after each pass,
	// success or handled error) so an external liveness probe can tell a cycling
	// loop from a wedged one (a hung query on a dropped socket). Optional; nil is a
	// no-op.
	Heartbeat func()

	// sweepCursors holds the per-unit target-scan cursor for the delete
	// reconciliation sweep (redis-plan §0.4), carried across streaming passes. Only
	// used when the engine implements the KeyLister/KeyExister capability (Redis);
	// nil/unused for capture-driven engines.
	sweepCursors map[engine.TableRef]uint64
	// sweepStarted stamps the start of each unit's current delete-sweep pass, so its
	// duration (the delete-reconciliation-lag gauge, RM8) is published on completion.
	sweepStarted map[engine.TableRef]time.Time

	// compIdx maps each replicable table to its FK-component id (the component's
	// first sorted member), for the per-component metric label. Built lazily.
	compIdx map[engine.TableRef]string
	// lastDBSize throttles the DB-size metric queries (they scan catalogs, so we
	// emit them on an interval, not every drain pass).
	lastDBSize time.Time
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
	// delta queue is already capturing when the copy window opens. A cluster edge
	// installs the loop-suppressing origin-aware variant (its sink marks its writes,
	// so they are not re-captured); a one-way sync installs the byte-identical
	// unguarded capture.
	if err := s.installCapture(ctx); err != nil {
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

// installCapture installs the appropriate capture variant for this syncer: origin-
// aware (loop-suppressing) for a cluster edge, plain for a one-way sync. A cluster
// edge REQUIRES the source to implement OriginAwareCapturer — the config layer only
// admits cluster engines that do, so a missing implementation is a build-time bug.
func (s *Syncer) installCapture(ctx context.Context) error {
	if s.ClusterMode {
		oc, ok := s.Source.(engine.OriginAwareCapturer)
		if !ok {
			return fmt.Errorf("syncer %s: cluster mode requires an origin-aware source", s.Name)
		}
		return oc.InstallOriginCapture(ctx, s.Replicable)
	}
	return s.Source.InstallCapture(ctx, s.Replicable)
}

// copyAndCutover copies one component parents-first, then flips each of its
// tables' cursors to streaming for this target. The copy is checkpointed by the
// StateStore, so an interrupted component resumes rather than restarting.
func (s *Syncer) copyAndCutover(ctx context.Context, comp engine.Component) error {
	// A cyclic component has no parents-first order, so the plain chunked copy would
	// fail; delegate the whole component to the engine's cycle-safe copier when it
	// offers one (Postgres/MySQL). Acyclic components (and engines without a cyclic
	// copier) take the normal chunked path.
	if comp.HasCycle() {
		if err := s.copyCyclicComponent(ctx, comp); err != nil {
			return err
		}
	} else {
		if err := copy.Component(ctx, s.Workers, s.Store, s.Name, s.Target, comp.Order, s.ChunkOpts, s.copyOptions()...); err != nil {
			return fmt.Errorf("syncer %s: copy component: %w", s.Name, err)
		}
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

// copyCyclicComponent loads a cyclic FK component via the engine's cycle-safe
// copier (Postgres/MySQL). It is coarse-checkpointed: the whole component is one
// unit (unchunked), so on restart it re-runs unless every table is already marked
// copied. An engine without a CyclicComponentCopier falls back to the plain chunked
// copy (which cannot order a cycle, but preserves prior behavior rather than
// erroring on an unexpected engine).
func (s *Syncer) copyCyclicComponent(ctx context.Context, comp engine.Component) error {
	if len(s.Workers) == 0 {
		return fmt.Errorf("syncer %s: cyclic copy: no workers", s.Name)
	}
	copier, ok := s.Workers[0].Sink.(engine.CyclicComponentCopier)
	if !ok {
		return copy.Component(ctx, s.Workers, s.Store, s.Name, s.Target, comp.Order, s.ChunkOpts, s.copyOptions()...)
	}

	// Coarse resume: skip if every table in the component is already copied.
	done := 0
	for _, t := range comp.Order {
		prog, err := s.Store.LoadCopyProgress(ctx, s.Name, s.Target, t)
		if err != nil {
			return fmt.Errorf("syncer %s: cyclic copy: load progress %s: %w", s.Name, t, err)
		}
		if prog.Done {
			done++
		}
	}
	if done == len(comp.Order) {
		return nil
	}

	if err := copier.CopyCyclicComponent(ctx, s.Workers[0].Src, comp.Tables); err != nil {
		return fmt.Errorf("syncer %s: cyclic copy: %w", s.Name, err)
	}
	// Mark every table copied so cutover proceeds and a restart resumes.
	for _, t := range comp.Order {
		if err := s.Store.SaveCopyProgress(ctx, s.Name, state.CopyProgress{Target: s.Target, Table: t, Done: true}); err != nil {
			return fmt.Errorf("syncer %s: cyclic copy: mark done %s: %w", s.Name, t, err)
		}
	}
	return nil
}

// copyOptions builds the initial-copy options for this syncer: the rows-copied
// progress callback, plus — for a cluster edge — the idempotent merge load mode, so
// the concurrent cross-edge copy in a mesh does not collide on the target PK
// (copy.WithLoadMode / CLAUDE.md §4.1).
func (s *Syncer) copyOptions() []copy.Option {
	opts := []copy.Option{copy.WithProgress(func(t engine.TableRef, n int64) {
		s.Tel.AddRowsCopied(s.Name, t, n)
	})}
	if s.ClusterMode {
		opts = append(opts, copy.WithLoadMode(engine.LoadMerge))
	}
	return opts
}

// recordEvent persists an operational event when a StateStore is present, logging
// a best-effort failure rather than aborting the lifecycle for an audit write.
func (s *Syncer) recordEvent(ctx context.Context, e state.Event) {
	if s.Store == nil {
		return
	}
	_ = s.Store.RecordEvent(ctx, e)
}
