package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/observability"
	"github.com/rudimk/replicare/internal/observability/telemetry"
	"github.com/rudimk/replicare/internal/state"
)

// Deferred target indexes (CLAUDE.md §4.1): a table's plain indexes are taken down
// just before its initial copy, so the copy does not maintain every index row by row,
// and built again, without blocking writes, once the whole sync is streaming. The
// definitions live in the state store, written before each drop, so a crash at any
// point leaves every dropped index recorded; each is also logged in full when dropped.

// indexRetryWait is how long the rebuilder waits after a pass that left indexes
// unbuilt (a failed build, a lost connection) before trying again.
var indexRetryWait = time.Minute

// indexPollInterval is how often the rebuilder reads the build in flight's progress.
var indexPollInterval = 10 * time.Second

// deferIndexes takes down the plain target indexes of tables about to be copied. It
// never stops the copy: if anything goes wrong before an index is dropped, that
// index simply stays, and the copy runs with it as it always did.
func (s *Syncer) deferIndexes(ctx context.Context, tables []engine.TableRef) {
	s.deferIndexesWith(ctx, tables, nil)
}

// deferIndexesWith is deferIndexes with a progress signal, called after each index
// is dealt with: a drop can wait and retry on a busy table, and during a reseed (inside
// a streaming pass) the liveness check needs to see that it is moving.
func (s *Syncer) deferIndexesWith(ctx context.Context, tables []engine.TableRef, progress func()) {
	if !s.DeferIndexes || len(tables) == 0 {
		return
	}
	d, ok := s.Sink.(engine.IndexDeferrer)
	if !ok {
		return
	}
	store, ok := s.Store.(state.IndexStore)
	if !ok {
		return
	}
	s.indexMu.Lock()
	defer s.indexMu.Unlock()

	idx, skips, err := d.DeferrableIndexes(ctx, tables)
	if err != nil {
		s.log(ctx, "list target indexes to drop for the copy; copying with them in place", err)
		return
	}
	for _, sk := range skips {
		s.recordEvent(ctx, state.Event{Sync: s.Name, Target: string(s.Target), Table: sk.Table, Level: "WARN",
			Event:   observability.EventIndexKept,
			Message: "keeping this table's indexes during the copy (it will be slower): " + sk.Reason})
	}
	if len(idx) == 0 {
		return
	}
	// Record first, drop second: a crash in between leaves a saved definition for an
	// index that still exists, which the rebuild finds valid and marks built.
	if err := store.SaveIndexes(ctx, s.Name, s.Target, idx); err != nil {
		s.log(ctx, "save target index definitions; copying with the indexes in place", err)
		return
	}
	for _, ix := range idx {
		err := d.DropDeferredIndex(ctx, ix)
		if progress != nil {
			progress()
		}
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.log(ctx, fmt.Sprintf("drop index %s for the copy; it stays in place", ix.Name), err)
			continue
		}
		s.recordEvent(ctx, state.Event{Sync: s.Name, Target: string(s.Target), Table: ix.Table, Level: "INFO",
			Event: observability.EventIndexDropped,
			Message: fmt.Sprintf("dropped index %s for the initial copy; replicare builds it again once the sync is streaming. "+
				"To build it by hand instead: %s", ix.Name, ix.Definition),
			Attrs: map[string]any{"index": ix.Name, "definition": ix.Definition}})
	}
	s.publishIndexCounts(ctx)
}

// deferIndexesForComponent takes down the indexes of the component's tables that this
// run is about to copy: every table of a cyclic component that is not wholly copied
// (it is copied again from the start), else just the tables not marked copied.
func (s *Syncer) deferIndexesForComponent(ctx context.Context, comp engine.Component) {
	if !s.DeferIndexes {
		return
	}
	var todo []engine.TableRef
	for _, t := range comp.Order {
		prog, err := s.Store.LoadCopyProgress(ctx, s.Name, s.Target, t)
		if err != nil || !prog.Done {
			todo = append(todo, t)
		}
	}
	if comp.HasCycle() && len(todo) > 0 {
		todo = comp.Order
	}
	s.deferIndexes(ctx, todo)
}

// publishIndexCounts refreshes the deferred / built gauges for this target.
func (s *Syncer) publishIndexCounts(ctx context.Context) {
	store, ok := s.Store.(state.IndexStore)
	if !ok || s.Tel == nil {
		return
	}
	saved, err := store.ListIndexes(ctx, s.Name)
	if err != nil {
		return
	}
	total, built := countIndexes(saved, s.Target)
	if total > 0 {
		s.Tel.SetDeferredIndexes(s.Name, s.Target, total, built)
	}
}

func countIndexes(saved []state.SavedIndex, target engine.TargetID) (total, built int) {
	for _, si := range saved {
		if si.Target != target {
			continue
		}
		total++
		if si.Built {
			built++
		}
	}
	return total, built
}

// kickIndexRebuild wakes the rebuilder (a reseed has just dropped indexes again).
func (s *Syncer) kickIndexRebuild() {
	if s.indexKick == nil {
		return
	}
	select {
	case s.indexKick <- struct{}{}:
	default:
	}
}

// startIndexRebuilder runs the rebuilder for this target in the background while the
// sync streams, and returns a func that waits for it to stop once ctx is cancelled.
// It runs whether or not deferral is on now, so indexes dropped by an earlier run
// (before the setting was turned off) are still put back.
func (s *Syncer) startIndexRebuilder(ctx context.Context) (wait func()) {
	_, isDeferrer := s.Sink.(engine.IndexDeferrer)
	store, isStore := s.Store.(state.IndexStore)
	if !isDeferrer || !isStore || s.OpenSink == nil {
		return func() {}
	}
	s.indexKick = make(chan struct{}, 1)
	r := &IndexRebuild{
		Sync: s.Name, Target: s.Target, Store: store, Open: s.OpenSink, Options: s.IndexBuild,
		Tel: s.Tel, Lock: &s.indexMu, Event: s.recordEvent,
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			left, err := r.Run(ctx)
			if ctx.Err() != nil {
				return
			}
			if err != nil {
				s.log(ctx, "index rebuild", err)
			}
			var retry <-chan time.Time
			if left > 0 || err != nil {
				retry = time.After(indexRetryWait)
			}
			select {
			case <-ctx.Done():
				return
			case <-s.indexKick:
			case <-retry:
			}
		}
	}()
	return func() { <-done }
}

// IndexRebuild builds a target's saved, not-yet-built indexes one at a time, in the
// order the store lists them (indexes on linking columns first), and refreshes each
// table's statistics once all of its indexes are back. It is shared by the daemon
// (in the background while streaming) and `replicare indexes restore`.
type IndexRebuild struct {
	Sync    string
	Target  engine.TargetID
	Store   state.IndexStore
	Open    func(ctx context.Context) (engine.Sink, error) // a new target connection
	Options engine.IndexBuildOptions
	Tel     *telemetry.Telemetry                     // optional
	Lock    sync.Locker                              // optional; held while one index builds
	Event   func(ctx context.Context, e state.Event) // optional
}

// Run does one pass and reports how many indexes are still not built.
func (r *IndexRebuild) Run(ctx context.Context) (left int, err error) {
	saved, err := r.Store.ListIndexes(ctx, r.Sync)
	if err != nil {
		return 0, err
	}
	var pending []engine.DeferredIndex
	byTable := map[engine.TableRef]int{} // indexes of each table still to build
	for _, si := range saved {
		if si.Target == r.Target && !si.Built {
			pending = append(pending, si.Index)
			byTable[si.Index.Table]++
		}
	}
	total, built := countIndexes(saved, r.Target)
	r.counts(total, built)
	if len(pending) == 0 {
		return 0, nil
	}
	r.event(ctx, state.Event{Sync: r.Sync, Target: string(r.Target), Level: "INFO", Event: observability.EventIndexRebuildStarted,
		Message: fmt.Sprintf("building %d of %d target indexes again (one at a time, without blocking writes)", len(pending), total)})

	builder, err := r.open(ctx)
	if err != nil {
		return len(pending), fmt.Errorf("connect target for index builds: %w", err)
	}
	defer func() { _ = builder.Close(context.Background()) }()
	monitor, err := r.open(ctx)
	if err != nil {
		return len(pending), fmt.Errorf("connect target for index build progress: %w", err)
	}
	defer func() { _ = monitor.Close(context.Background()) }()
	bd := builder.(engine.IndexDeferrer)
	md := monitor.(engine.IndexDeferrer)
	session, err := bd.BuildSession(ctx)
	if err != nil {
		return len(pending), err
	}

	left = len(pending)
	var firstErr error
	for _, ix := range pending {
		if ctx.Err() != nil {
			return left, ctx.Err()
		}
		start := time.Now()
		err := r.buildOne(ctx, bd, md, session, ix)
		r.progress(ix, "", engine.IndexBuildProgress{})
		if err != nil {
			if ctx.Err() != nil {
				return left, ctx.Err()
			}
			r.event(ctx, state.Event{Sync: r.Sync, Target: string(r.Target), Table: ix.Table, Level: "WARN",
				Event: observability.EventIndexBuildFailed, Message: fmt.Sprintf("building index %s failed; retrying later: %v", ix.Name, err),
				Attrs: map[string]any{"index": ix.Name}})
			if firstErr == nil {
				firstErr = err
			}
			// A lost connection fails every build after it; stop and retry with new ones.
			if builder.HealthCheck(ctx) != nil || monitor.HealthCheck(ctx) != nil {
				return left, firstErr
			}
			continue
		}
		if err := r.Store.MarkIndexBuilt(ctx, r.Sync, r.Target, ix.Table, ix.Name); err != nil {
			return left, err
		}
		left--
		built++
		r.counts(total, built)
		r.event(ctx, state.Event{Sync: r.Sync, Target: string(r.Target), Table: ix.Table, Level: "INFO",
			Event:   observability.EventIndexBuilt,
			Message: fmt.Sprintf("index %s built in %s (%d of %d back)", ix.Name, time.Since(start).Round(time.Second), built, total),
			Attrs:   map[string]any{"index": ix.Name}})
		byTable[ix.Table]--
		if byTable[ix.Table] == 0 {
			if err := bd.AnalyzeTables(ctx, []engine.TableRef{ix.Table}); err != nil && ctx.Err() == nil {
				r.event(ctx, state.Event{Sync: r.Sync, Target: string(r.Target), Table: ix.Table, Level: "WARN",
					Event: observability.EventIndexBuildFailed, Message: fmt.Sprintf("refreshing statistics after the rebuild failed: %v", err)})
			}
		}
	}
	if left == 0 {
		r.event(ctx, state.Event{Sync: r.Sync, Target: string(r.Target), Level: "INFO", Event: observability.EventIndexRebuildDone,
			Message: fmt.Sprintf("all %d target indexes are built again", total)})
	}
	return left, firstErr
}

// buildOne builds one index while polling its progress on the monitor connection.
func (r *IndexRebuild) buildOne(ctx context.Context, bd, md engine.IndexDeferrer, session int64, ix engine.DeferredIndex) error {
	if r.Lock != nil {
		r.Lock.Lock()
		defer r.Lock.Unlock()
	}
	r.progress(ix, ix.Name, engine.IndexBuildProgress{Phase: "starting"})
	stop := make(chan struct{})
	polled := make(chan struct{})
	go func() {
		defer close(polled)
		t := time.NewTicker(indexPollInterval)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				// Not tied to ctx: the monitor connection is only for these reads, and a
				// time limit keeps a stuck read from holding the build's result back.
				pctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				p, ok, err := md.IndexBuildProgress(pctx, session)
				cancel()
				if err == nil && ok {
					r.progress(ix, ix.Name, p)
				}
			}
		}
	}()
	err := bd.BuildDeferredIndex(ctx, ix, r.Options)
	close(stop)
	<-polled
	return err
}

func (r *IndexRebuild) open(ctx context.Context) (engine.Sink, error) {
	if r.Open == nil {
		return nil, errors.New("no way to open a target connection")
	}
	sk, err := r.Open(ctx)
	if err != nil {
		return nil, err
	}
	if _, ok := sk.(engine.IndexDeferrer); !ok {
		_ = sk.Close(context.Background())
		return nil, errors.New("this target engine cannot build indexes")
	}
	return sk, nil
}

func (r *IndexRebuild) counts(total, built int) {
	if r.Tel != nil && total > 0 {
		r.Tel.SetDeferredIndexes(r.Sync, r.Target, total, built)
	}
}

func (r *IndexRebuild) progress(ix engine.DeferredIndex, name string, p engine.IndexBuildProgress) {
	if r.Tel != nil {
		r.Tel.SetIndexBuild(r.Sync, r.Target, ix.Table, name, p)
	}
}

func (r *IndexRebuild) event(ctx context.Context, e state.Event) {
	if r.Event != nil {
		r.Event(ctx, e)
	}
}
