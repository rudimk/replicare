package daemon

import (
	"context"
	"fmt"

	"github.com/rudimk/replicare/internal/config"
	"github.com/rudimk/replicare/internal/copy"
	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/pipeline"
)

// buildSyncer constructs a connected, ready single-target Syncer from the config
// for one (sync, target) pair: it opens the source, the target, and a copy worker
// pool sized by the sync's connection caps, runs pre-flight (refusing to start on
// a blocking incompatibility, CLAUDE.md §4.2), and maps the neutral tuning knobs.
// The returned cleanup closes every connection it opened; it is also called on a
// mid-construction error so a partial build never leaks connections.
func (d *Daemon) buildSyncer(ctx context.Context, sync *config.Sync, targetName string) (*pipeline.Syncer, func(), error) {
	srcEp := d.cfg.Sources[sync.Source]
	tgtEp := d.cfg.Targets[targetName]
	if srcEp == nil || tgtEp == nil {
		return nil, nil, fmt.Errorf("unknown source %q or target %q", sync.Source, targetName)
	}
	eng, err := engine.Get(srcEp.Engine)
	if err != nil {
		return nil, nil, err
	}

	// Track everything we open so a failure (or shutdown) closes it all.
	var closers []func()
	cleanup := func() {
		for i := len(closers) - 1; i >= 0; i-- {
			closers[i]()
		}
	}
	fail := func(err error) (*pipeline.Syncer, func(), error) {
		cleanup()
		return nil, nil, err
	}

	source, err := d.openSource(ctx, eng, srcEp, &closers)
	if err != nil {
		return fail(fmt.Errorf("connect source: %w", err))
	}
	sink, err := d.openSink(ctx, eng, tgtEp, &closers)
	if err != nil {
		return fail(fmt.Errorf("connect target: %w", err))
	}

	// Copy worker pool: parallel chunks come from having several Source/Sink
	// pairs (a Source/Sink is not concurrency-safe). Sized by the source cap,
	// bounded to at least one and to the target cap.
	poolN := workerCount(sync.Tuning.Pool)
	workers := make([]copy.Worker, 0, poolN)
	for i := 0; i < poolN; i++ {
		ws, err := d.openSource(ctx, eng, srcEp, &closers)
		if err != nil {
			return fail(fmt.Errorf("connect copy source: %w", err))
		}
		wk, err := d.openSink(ctx, eng, tgtEp, &closers)
		if err != nil {
			return fail(fmt.Errorf("connect copy target: %w", err))
		}
		workers = append(workers, copy.Worker{Src: ws, Sink: wk})
	}

	// Pre-flight for the topo-ordered components; refuse to start if blocked.
	sel := engine.Selection{Include: sync.Include, Exclude: sync.Exclude}
	srcSchema, err := source.Introspect(ctx, sel)
	if err != nil {
		return fail(fmt.Errorf("introspect source: %w", err))
	}
	tgtSchema, err := sink.Introspect(ctx, sel)
	if err != nil {
		return fail(fmt.Errorf("introspect target: %w", err))
	}
	// Prime every copy-worker source with the SYNC selection. Engines that filter
	// by selection at copy-read time (Redis: key-glob SCAN) need each worker to hold
	// the real selection before the copy layer's later per-table column probe
	// introspects it with a table-ref selection (first-write-wins; otherwise a Redis
	// worker would filter its SCAN by the unit ref and copy nothing). Harmless —
	// a redundant read-only introspect — for capture-driven engines (Postgres/MySQL).
	for _, w := range workers {
		if _, err := w.Src.Introspect(ctx, sel); err != nil {
			return fail(fmt.Errorf("introspect copy source: %w", err))
		}
	}
	srcVer, err := source.ServerVersion(ctx)
	if err != nil {
		return fail(fmt.Errorf("source version: %w", err))
	}
	tgtVer, err := sink.ServerVersion(ctx)
	if err != nil {
		return fail(fmt.Errorf("target version: %w", err))
	}
	report := eng.Preflight(sync.Name, srcVer, tgtVer, srcSchema, tgtSchema)
	if report.Blocked() {
		return fail(fmt.Errorf("pre-flight blocked (%d blocking findings); fix the target schema before starting", blockingCount(report)))
	}

	syncer := &pipeline.Syncer{
		Name:          sync.Name,
		Source:        source,
		Sink:          sink,
		Target:        engine.TargetID(targetName),
		Workers:       workers,
		Store:         d.store,
		Tel:           d.tel,
		Components:    report.Components,
		Replicable:    report.Replicable,
		ChunkOpts:     engine.ChunkOptions{TargetRows: chunkRows(sync.Tuning)},
		DrainBatch:    drainBatch(sync.Tuning),
		DrainInterval: sync.Tuning.DrainInterval.Duration(),
		Retention:     retentionPolicy(sync.Tuning.Retention),
	}
	return syncer, cleanup, nil
}

func (d *Daemon) openSource(ctx context.Context, eng engine.Engine, ep *config.Endpoint, closers *[]func()) (engine.Source, error) {
	s, err := eng.NewSource(ep.Conn.ConnConfig())
	if err != nil {
		return nil, err
	}
	if err := s.Connect(ctx); err != nil {
		return nil, err
	}
	*closers = append(*closers, func() { _ = s.Close(context.Background()) })
	return s, nil
}

func (d *Daemon) openSink(ctx context.Context, eng engine.Engine, ep *config.Endpoint, closers *[]func()) (engine.Sink, error) {
	s, err := eng.NewSink(ep.Conn.ConnConfig())
	if err != nil {
		return nil, err
	}
	if err := s.Connect(ctx); err != nil {
		return nil, err
	}
	*closers = append(*closers, func() { _ = s.Close(context.Background()) })
	return s, nil
}

// retentionPolicy maps the neutral config retention block to the engine policy.
func retentionPolicy(r config.Retention) engine.RetentionPolicy {
	return engine.RetentionPolicy{
		MaxAgeSeconds: int64(r.MaxAge.Duration().Seconds()),
		MaxBytes:      r.MaxBytes.Bytes(),
	}
}

// workerCount sizes the copy pool from the connection caps: at least one pair,
// no more than either cap allows. The main source/sink connection is separate, so
// the pool draws on the remaining budget.
func workerCount(p config.Pool) int {
	n := p.MaxSourceConns - 1
	if p.MaxTargetConns-1 < n {
		n = p.MaxTargetConns - 1
	}
	if n < 1 {
		return 1
	}
	return n
}

// chunkRows / drainBatch pick conservative defaults; per-engine CDC tuning can
// override these later via the engine block (CLAUDE.md §11).
func chunkRows(config.Tuning) int  { return 10000 }
func drainBatch(config.Tuning) int { return 1000 }

func blockingCount(r *engine.PreflightReport) int {
	n := 0
	for _, f := range r.Findings {
		if f.Severity == engine.SevBlock {
			n++
		}
	}
	return n
}
