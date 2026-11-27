package pipeline

import (
	"context"
	"fmt"
	"sync"
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
	// echoed back around the mesh (CLAUDE.md §6, .sisyphus/multi-master-design.md §5.4). Off (the
	// default) is the one-way path, byte-identical to the pre-multi-master daemon.
	ClusterMode bool

	// NodeID is this edge's SOURCE node's replication-origin identity (cluster mode
	// only). The source's capture trigger stamps local changes into the version
	// register as (hlc, NodeID) for HLC last-write-wins (CLAUDE.md §6). Empty on the
	// one-way path.
	NodeID string

	// SyncSequences opts this one-way sync into continuous sequence syncing: on a lazy
	// interval during streaming the Syncer advances each replicable table's owned
	// identity/serial/AUTO_INCREMENT counter on the target to max(id)+1 (the same
	// mechanism as `replicare reseed-sequences`), so a promoted DR node's counters are
	// already past the replicated ids at cutover (CLAUDE.md §13). Off (the default) is
	// the pre-existing path. It is NEVER set on a cluster edge — the daemon only wires it
	// from a one-way sync's config, and config load refuses it on a mesh-member target.
	SyncSequences bool

	// ApplyConcurrency is how many of a component's tables may apply at once during
	// streaming (CLAUDE.md §8 parallel delta apply). 1 (the default) is the
	// strictly-sequential per-table drain. Higher values fan the per-table apply
	// across a pool built from the primary connection plus the copy Workers (idle
	// during streaming), bounded by how many pairs are available.
	ApplyConcurrency int

	// DeferIndexes takes the target's plain indexes down before each table's initial
	// copy (and before a reseed's re-copy) and builds them again once the sync streams
	// (CLAUDE.md §4.1 "deferred target indexes"). Never set on a cluster edge. Needs a
	// Sink implementing engine.IndexDeferrer and a store implementing state.IndexStore;
	// otherwise a no-op.
	DeferIndexes bool
	// GuardTriggers keeps the target's own triggers from firing on replicare's writes
	// (see triggers.go). Off: any an earlier run switched off are switched back on.
	// Needs a Sink implementing engine.TriggerGuard and a store implementing
	// state.TriggerStore; otherwise nothing is done.
	GuardTriggers bool
	// IndexBuild tunes each index build's session (memory, parallel workers).
	IndexBuild engine.IndexBuildOptions
	// OpenSink opens a new connection to this target. The index rebuilder runs on two of
	// its own (one builds, one reads progress), since a build can take hours and streaming
	// keeps the existing ones busy. Nil disables the rebuilder.
	OpenSink func(ctx context.Context) (engine.Sink, error)
	// OpenSource opens a new connection to the source. The leftover-track sweep
	// (engine.TrackSweeper) runs on one of its own, opened for each round and closed after,
	// so it never shares the streaming connection or delays a pass. Nil disables it.
	OpenSource func(ctx context.Context) (engine.Source, error)

	// PurgeTargets lists, per table, every target that reads that table's changes from
	// this source: the other targets of a fan-out sync, other syncs from the same
	// database, and paused syncs that have started. A queued change is deleted only
	// once all of them have it; deleting it once this syncer's own target had it lost
	// changes for a target that was slower or down at the time. Nil, or a table not
	// listed, means this syncer's own target only (a source with a single consumer).
	PurgeTargets map[engine.TableRef][]engine.TargetID

	// SkipInitialCopy marks every table that has not started copying as copied without
	// copying it (the sync's skip_initial_copy), for a target that already holds the
	// source's data. Never set on a cluster edge.
	SkipInitialCopy bool
	// SkipCheck compares the given skipped tables of this target against the source,
	// row by row, once the target has caught up. Nil turns the check off
	// (skip_initial_copy.verify: false); the tables then stay marked as unchecked.
	SkipCheck func(ctx context.Context, tables []engine.TableRef) ([]SkipCheckResult, error)
	// VerifySkipped: tables that skipped their copy are checked too (the sync's
	// skip_initial_copy.verify). Tables waiting for an automatic check are checked
	// whenever SkipCheck is set.
	VerifySkipped bool
	// CheckRepairWait is how long the check waits after queueing rows that differ before
	// comparing again (0 = a minute).
	CheckRepairWait time.Duration

	// Heartbeat, if set, is called whenever streaming makes progress — after each pass
	// (success or handled error), and inside a pass as work completes (a table's apply
	// committing, a component attempt returning, a reseed's table emptied or chunk
	// copied) — so an external liveness probe can tell a slow but moving loop from a
	// wedged one (a hung query on a dropped socket). Must be safe for concurrent use
	// (pooled applies and copy workers call it). Optional; nil is a no-op.
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
	// lastSeqSync throttles continuous sequence syncing (SyncSequences): the per-table
	// counter advance runs on a lazy interval, not every drain pass. Zero → due now.
	lastSeqSync time.Time

	// indexMu is held while index definitions are saved and dropped, and while one
	// index builds, so a reseed never drops an index the rebuilder is building.
	indexMu sync.Mutex
	// indexKick wakes the rebuilder after a reseed has dropped indexes again.
	indexKick chan struct{}

	// caughtUp is closed on the first healthy pass where every table's oldest queued
	// change is under a minute old; the skipped-copy check waits for it. Nil when no
	// check is waiting.
	caughtUp     chan struct{}
	caughtUpOnce sync.Once

	// progress accumulates streaming passes for the minute-by-minute progress line.
	progress streamProgress
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
	// Before anything is written to the target: keep its own triggers from firing on
	// replicare's writes (replica mode, or switched off and saved).
	if err := s.guardTargetTriggers(ctx); err != nil {
		return fmt.Errorf("syncer %s: target triggers: %w", s.Name, err)
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
	// skip_initial_copy: with capture on, mark the tables that have not started copying
	// as copied. Everything below then treats them as an earlier run's finished copy:
	// no copy, no index deferral, no rows-to-copy estimate, straight to cutover.
	if err := s.skipCopies(ctx); err != nil {
		return err
	}

	// Publish every table's phase up front, so the metrics page shows the copy is under
	// way from the first scrape — before this, nothing was reported until a table reached
	// streaming, and a long copy served an EMPTY /metrics page that looked broken.
	// Tables already copied on an earlier run report streaming once their loop starts.
	for _, comp := range s.Components {
		for _, t := range comp.Order {
			s.Tel.SetPhase(s.Name, t, string(state.PhaseInitialCopy))
		}
	}
	// The dashboard's initial-copy and health panels need these during the copy too:
	// how many rows each table is expected to hold, and that both ends are up (bring-up
	// connected to both, or it would not be here).
	s.publishCopyTargets(ctx)
	s.Tel.SetSourceUp(s.Name, true)
	s.Tel.SetTargetUp(s.Name, s.Target, true)
	s.refreshDBSizes(ctx)

	for _, comp := range s.Components {
		if err := s.copyAndCutover(ctx, comp); err != nil {
			return err
		}
		s.refreshDBSizes(ctx) // throttled; shows the target growing between components
	}
	return nil
}

// publishCopyTargets publishes replicare_initial_copy_rows_target — the source's own
// row estimate — for every table this run still has to copy, so a dashboard can show
// rows copied as a percentage. Tables already copied on an earlier run are skipped (this
// run will not copy them, so a 0%-of-N series would mislead). Best-effort: an engine
// without estimates, or a failed lookup, just leaves the series unset.
func (s *Syncer) publishCopyTargets(ctx context.Context) {
	est, ok := s.Source.(engine.RowEstimator)
	if !ok {
		return
	}
	var todo []engine.TableRef
	for _, comp := range s.Components {
		for _, t := range comp.Order {
			if s.Store != nil {
				if prog, err := s.Store.LoadCopyProgress(ctx, s.Name, s.Target, t); err == nil && prog.Done {
					continue
				}
			}
			todo = append(todo, t)
		}
	}
	if len(todo) == 0 {
		return
	}
	qctx, cancel := context.WithTimeout(ctx, healthTimeout)
	defer cancel()
	rows, err := est.EstimateRows(qctx, todo)
	if err != nil {
		return
	}
	for _, t := range todo {
		if n, ok := rows[t]; ok {
			s.Tel.SetRowsCopiedTarget(s.Name, t, n)
		}
	}
	// Table sizes weight the copy ETA, so a table of large rows (message bodies) counts
	// for its size, not its row count. Best-effort: without them the ETA counts rows.
	if sizer, ok := s.Source.(engine.TableSizeEstimator); ok {
		bctx, bcancel := context.WithTimeout(ctx, healthTimeout)
		defer bcancel()
		if sizes, err := sizer.EstimateTableBytes(bctx, todo); err == nil {
			for _, t := range todo {
				if b, ok := sizes[t]; ok && rows[t] > 0 {
					s.Tel.SetCopyTableBytes(s.Name, t, b)
				}
			}
		}
	}
}

// installCapture installs the appropriate capture variant for this syncer: origin-
// aware (loop-suppressing) for a cluster edge, plain for a one-way sync. A cluster
// edge REQUIRES the source to implement OriginAwareCapturer — the config layer only
// admits cluster engines that do, so a missing implementation is a build-time bug.
func (s *Syncer) installCapture(ctx context.Context) error {
	ctx = s.withCaptureProgress(ctx, len(s.Replicable))
	if s.ClusterMode {
		oc, ok := s.Source.(engine.OriginAwareCapturer)
		if !ok {
			return fmt.Errorf("syncer %s: cluster mode requires an origin-aware source", s.Name)
		}
		return oc.InstallOriginCapture(ctx, s.Replicable, s.NodeID)
	}
	return s.Source.InstallCapture(ctx, s.Replicable)
}

// copyAndCutover copies one component parents-first, then flips each of its
// tables' cursors to streaming for this target. The copy is checkpointed by the
// StateStore, so an interrupted component resumes rather than restarting.
func (s *Syncer) copyAndCutover(ctx context.Context, comp engine.Component) error {
	// A component whose every table is already copied (an earlier run, or skipped)
	// has nothing to copy: cut it over without announcing a copy.
	if s.componentCopied(ctx, comp) {
		return s.cutover(ctx, comp)
	}
	// A cyclic component has no parents-first order, so the plain chunked copy would
	// fail; delegate the whole component to the engine's cycle-safe copier when it
	// offers one (Postgres/MySQL). Acyclic components (and engines without a cyclic
	// copier) take the normal chunked path.
	//
	// First, take the target's plain indexes down for the tables this run copies, before
	// the size measurer starts (it shares the primary connection this uses).
	s.deferIndexesForComponent(ctx, comp)
	stopSizes := s.measureSizesWhileCopying(ctx)
	s.recordEvent(ctx, state.Event{Sync: s.Name, Target: string(s.Target), Level: "INFO",
		Event:   observability.EventCopyComponentStarted,
		Message: fmt.Sprintf("copying a component of %d tables (from %s)", len(comp.Order), comp.Order[0]),
	})
	if comp.HasCycle() {
		if err := s.copyCyclicComponent(ctx, comp); err != nil {
			stopSizes()
			return err
		}
	} else {
		if err := copy.Component(ctx, s.Workers, s.Store, s.Name, s.Target, comp.Order, s.ChunkOpts, s.copyOptions()...); err != nil {
			stopSizes()
			return fmt.Errorf("syncer %s: copy component: %w", s.Name, err)
		}
	}
	stopSizes()
	return s.cutover(ctx, comp)
}

// componentCopied reports whether every table of the component is marked copied. An
// unreadable progress counts as not copied, so the copy path (which reads it again and
// fails loud) decides.
func (s *Syncer) componentCopied(ctx context.Context, comp engine.Component) bool {
	for _, t := range comp.Order {
		prog, err := s.Store.LoadCopyProgress(ctx, s.Name, s.Target, t)
		if err != nil || !prog.Done {
			return false
		}
	}
	return true
}

// cutover flips each of the component's tables to streaming for this target.
func (s *Syncer) cutover(ctx context.Context, comp engine.Component) error {
	for _, t := range comp.Order {
		cur, err := s.Store.LoadCursor(ctx, s.Name, s.Target, t)
		if err != nil {
			return fmt.Errorf("syncer %s: load cursor %s: %w", s.Name, t, err)
		}
		cur.Phase = state.PhaseStreaming
		if err := s.Store.SaveCursor(ctx, s.Name, cur); err != nil {
			return fmt.Errorf("syncer %s: cutover cursor %s: %w", s.Name, t, err)
		}
		// The table's copy is finished: say so now rather than when the last component's
		// copy is done and streaming starts, so the copy ETA stops counting it.
		s.Tel.SetPhase(s.Name, t, string(state.PhaseStreaming))
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

	// Coarse retry backstop: a cyclic component copied from a LIVE source can hit a
	// transient FK violation when a parent row inserted mid-copy is referenced by a
	// child copied from a later snapshot (§3.3/§4). The engine copier already retries
	// per-child by re-copying parents; this outer loop covers deeper multi-level skew
	// and MySQL's whole-component paths, so a normal write burst converges here instead
	// of the daemon crash-looping. A non-transient error still halts loud immediately.
	if err := s.copyCyclicWithRetry(ctx, copier, comp); err != nil {
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

// cyclicCopyMaxAttempts bounds the coarse retry around a cyclic component copy on a
// live source. The engine copier already converges cheaply per-child; this outer
// loop is the backstop for deeper skew and MySQL's whole-component paths.
const cyclicCopyMaxAttempts = 10

// copyCyclicWithRetry runs the engine's cyclic component copy, retrying on a
// transient FK violation (a live-source snapshot skew, §3.3/§4) with backoff. Each
// re-run re-reads the current source, so a parent inserted mid-copy is picked up and
// the component converges. A non-transient error halts immediately; on exhaustion the
// last transient error is returned (a clean restart re-attempts) rather than looping
// forever in-process.
func (s *Syncer) copyCyclicWithRetry(ctx context.Context, copier engine.CyclicComponentCopier, comp engine.Component) error {
	var lastErr error
	for attempt := 0; attempt < cyclicCopyMaxAttempts; attempt++ {
		progress := func(t engine.TableRef, n int64) { s.Tel.AddRowsCopied(s.Name, t, n) }
		stages := engine.CyclicCopyProgress{
			Copied:      progress,
			FillPlanned: func(t engine.TableRef) { s.Tel.SetLinkFillPlanned(s.Name, t) },
			Filled:      func(t engine.TableRef, n int64) { s.Tel.AddRowsFilled(s.Name, t, n) },
			FillStarted: func(t engine.TableRef) {
				s.recordEvent(ctx, state.Event{Sync: s.Name, Target: string(s.Target), Table: t, Level: "INFO",
					Event: observability.EventLinkFillStarted, Message: "rows copied; filling in the links between rows"})
			},
			FillDone: func(t engine.TableRef) {
				s.recordEvent(ctx, state.Event{Sync: s.Name, Target: string(s.Target), Table: t, Level: "INFO",
					Event: observability.EventLinkFillDone, Message: "links filled in"})
			},
		}
		var err error
		if pc, ok := copier.(engine.ParallelCyclicCopier); ok {
			// The rest of the copy pool is idle while a cyclic component loads (components
			// copy one at a time), so lend it to the copier for a table's chunks.
			helpers := make([]engine.ConnPair, 0, len(s.Workers)-1)
			for _, w := range s.Workers[1:] {
				helpers = append(helpers, engine.ConnPair{Src: w.Src, Sink: w.Sink})
			}
			err = pc.CopyCyclicComponentWith(ctx, s.Workers[0].Src, helpers, comp.Tables, stages)
		} else {
			err = copier.CopyCyclicComponent(ctx, s.Workers[0].Src, comp.Tables, progress)
		}
		if err == nil {
			return nil
		}
		if !engine.IsTransientConstraint(err) {
			return err
		}
		lastErr = err
		s.recordEvent(ctx, state.Event{
			Sync: s.Name, Target: string(s.Target), Level: "WARN", Event: observability.EventCopyRetry,
			Message: fmt.Sprintf("cyclic component copy hit a transient FK violation (live-source skew), retry %d/%d: %v",
				attempt+1, cyclicCopyMaxAttempts, err),
		})
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(cyclicCopyBackoff(attempt)):
		}
	}
	return lastErr
}

// cyclicCopyBackoff grows the wait between coarse cyclic-copy retries, capped so a
// converging write burst is caught quickly without hammering the source.
func cyclicCopyBackoff(attempt int) time.Duration {
	d := time.Duration(1<<attempt) * time.Second
	if d > 30*time.Second {
		d = 30 * time.Second
	}
	return d
}

// copyOptions builds the initial-copy options for this syncer: the rows-copied
// progress callback, plus — for a cluster edge — the idempotent merge load mode, so
// the concurrent cross-edge copy in a mesh does not collide on the target PK
// (copy.WithLoadMode / CLAUDE.md §4.1).
func (s *Syncer) copyOptions() []copy.Option {
	opts := []copy.Option{copy.WithProgress(func(t engine.TableRef, n int64) {
		s.Tel.AddRowsCopied(s.Name, t, n)
	}), copy.WithEvents(s.copyEvent)}
	if s.ClusterMode {
		opts = append(opts, copy.WithLoadMode(engine.LoadMerge))
	}
	return opts
}

// copyEvent records an event the copy reports: in the state store's events and the
// logs, except DEBUG ones and chunk retries, which only go to the logs.
func (s *Syncer) copyEvent(ctx context.Context, level, event string, table engine.TableRef, msg string) {
	e := state.Event{Sync: s.Name, Target: string(s.Target), Table: table, Level: level, Event: event, Message: msg}
	if level == "DEBUG" || event == copy.EventStorageRetry {
		if s.Tel != nil {
			s.Tel.LogEvent(ctx, e)
		}
		return
	}
	s.recordEvent(ctx, e)
}

// recordEvent persists an operational event when a StateStore is present, logging
// a best-effort failure rather than aborting the lifecycle for an audit write.
func (s *Syncer) recordEvent(ctx context.Context, e state.Event) {
	if s.Tel != nil {
		s.Tel.LogEvent(ctx, e)
	}
	if s.Store == nil {
		return
	}
	_ = s.Store.RecordEvent(ctx, e)
}

// measureSizesWhileCopying re-measures the source / target size gauges every
// dbSizeInterval while a component copies, and returns a func that stops it and waits for
// any measurement in flight. Without it the gauges kept their bring-up values for the whole
// copy (a large cyclic component is one long copy), so the dashboard showed a target stuck
// at its starting size while it grew by gigabytes.
//
// It measures on the primary Source/Sink pair, which the copy never uses (the copy runs on
// the Workers), and it is stopped before the cutover, which does use the pair: a pgx
// connection is not safe for concurrent use. A measurement is never cancelled midway by
// stop (cancelling a pgx query closes the connection); only daemon shutdown cancels ctx.
func (s *Syncer) measureSizesWhileCopying(ctx context.Context) (stop func()) {
	quit := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(dbSizeInterval)
		defer t.Stop()
		lastLog := time.Now()
		for {
			select {
			case <-quit:
				return
			case <-ctx.Done():
				return
			case <-t.C:
				s.measureDBSizes(ctx)
				s.lastDBSize = time.Now()
				s.Tel.RefreshETAs() // a copy that has stopped moving shows it
				s.logCopyProgress(ctx, &lastLog)
			}
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() { close(quit) })
		<-done
	}
}
