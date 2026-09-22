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
	sel := engine.Selection{Include: sync.Include, Exclude: sync.Exclude}
	return d.buildSyncerCore(ctx, sync.Name, srcEp, tgtEp, sel, sync.Tuning, targetName, false, "", "")
}

// buildClusterEdge constructs a connected Syncer for one directed edge of an
// active-active cluster (source node → target node): the same lifecycle as a one-way
// sync, but in cluster mode — the source installs loop-suppressing origin-aware
// capture and every sink is origin-marking, so replicare's own applies are not
// re-captured and echoed back around the mesh (CLAUDE.md §6). Both endpoints are
// cluster Nodes (each is simultaneously a source and a target).
func (d *Daemon) buildClusterEdge(ctx context.Context, e clusterEdge) (*pipeline.Syncer, func(), error) {
	srcEp := d.cfg.Nodes[e.srcNode]
	tgtEp := d.cfg.Nodes[e.dstNode]
	if srcEp == nil || tgtEp == nil {
		return nil, nil, fmt.Errorf("unknown cluster node %q or %q", e.srcNode, e.dstNode)
	}
	sel := engine.Selection{Include: e.cluster.Include, Exclude: e.cluster.Exclude}
	return d.buildSyncerCore(ctx, e.name(), srcEp, tgtEp, sel, e.cluster.Tuning, e.dstNode, true, srcEp.NodeID, tgtEp.NodeID)
}

// buildSyncerCore is the shared build path for a one-way sync target and a cluster
// edge. clusterMode selects loop suppression: the returned Syncer installs origin-
// aware capture and every sink it opened (main + copy pool) is switched to origin
// marking. clusterMode=false is exactly the pre-multi-master path.
func (d *Daemon) buildSyncerCore(ctx context.Context, name string, srcEp, tgtEp *config.Endpoint,
	sel engine.Selection, tuning config.Tuning, targetName string, clusterMode bool, srcNodeID, tgtNodeID string) (*pipeline.Syncer, func(), error) {
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
	if clusterMode {
		if err := enableClusterReads(source); err != nil {
			return fail(err)
		}
		if err := enableOriginMarking(sink, tgtNodeID); err != nil {
			return fail(err)
		}
	}

	// Copy worker pool: parallel chunks come from having several Source/Sink
	// pairs (a Source/Sink is not concurrency-safe). Sized by the source cap,
	// bounded to at least one and to the target cap.
	poolN := workerCount(tuning.Pool)
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
		if clusterMode {
			if err := enableClusterReads(ws); err != nil {
				return fail(err)
			}
			if err := enableOriginMarking(wk, tgtNodeID); err != nil {
				return fail(err)
			}
		}
		workers = append(workers, copy.Worker{Src: ws, Sink: wk})
	}

	// Pre-flight for the topo-ordered components; refuse to start if blocked.
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
	report := eng.Preflight(name, srcVer, tgtVer, srcSchema, tgtSchema)
	if report.Blocked() {
		return fail(fmt.Errorf("pre-flight blocked (%d blocking findings); fix the target schema before starting", blockingCount(report)))
	}

	syncer := &pipeline.Syncer{
		Name:             name,
		Source:           source,
		Sink:             sink,
		Target:           engine.TargetID(targetName),
		Workers:          workers,
		Store:            d.store,
		Tel:              d.tel,
		Components:       report.Components,
		Replicable:       report.Replicable,
		ChunkOpts:        engine.ChunkOptions{TargetRows: chunkRows(tuning)},
		DrainBatch:       drainBatch(tuning),
		DrainInterval:    tuning.DrainInterval.Duration(),
		ApplyConcurrency: tuning.ApplyConcurrency,
		Retention:        retentionPolicy(tuning.Retention),
		ClusterMode:      clusterMode,
		NodeID:           srcNodeID,
	}
	// Mark the streaming-liveness heartbeat once per pass (runSync registers the
	// key after bring-up); lets /healthz restart a wedged pod.
	key := healthKey(name, targetName)
	syncer.Heartbeat = func() { d.beat.Mark(key) }
	return syncer, cleanup, nil
}

// enableOriginMarking switches a cluster member's sink to origin-marking so its
// apply/copy writes carry the loop-suppression marker. The engine must implement
// OriginMarkingSink to be a cluster member (config validation admits only such
// engines), so a missing implementation is a build-time error, not a silent no-op.
func enableOriginMarking(sink engine.Sink, nodeID string) error {
	m, ok := sink.(engine.OriginMarkingSink)
	if !ok {
		return fmt.Errorf("engine sink does not support origin marking (cannot be a cluster member)")
	}
	m.EnableOriginMarking(nodeID)
	return nil
}

// enableClusterReads switches a cluster member's source to version-aware re-read so
// each re-read row carries its mesh version for HLC-LWW. The engine must implement
// ClusterReadSource to be a cluster member.
func enableClusterReads(src engine.Source) error {
	c, ok := src.(engine.ClusterReadSource)
	if !ok {
		return fmt.Errorf("engine source does not support cluster reads (cannot be a cluster member)")
	}
	c.EnableClusterReads()
	return nil
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
func chunkRows(config.Tuning) int { return 10000 }

// drainBatch is the max dirty deltas applied per table per drain pass. It is the
// per-table streaming throughput ceiling together with drain_interval
// (~drain_batch/drain_interval rows/s per table). Configurable via
// tuning.drain_batch; applyDefaults fills the conservative 1000 default, so a
// zero here (an unvalidated caller) still falls back rather than draining nothing.
func drainBatch(t config.Tuning) int {
	if t.DrainBatch > 0 {
		return t.DrainBatch
	}
	return defaultDrainBatchFallback
}

// defaultDrainBatchFallback mirrors config.defaultDrainBatch for the zero-value
// guard above (the config package owns the canonical default applied at load).
const defaultDrainBatchFallback = 1000

func blockingCount(r *engine.PreflightReport) int {
	n := 0
	for _, f := range r.Findings {
		if f.Severity == engine.SevBlock {
			n++
		}
	}
	return n
}
