package pipeline

import (
	"context"
	"errors"
	"strconv"
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
			err := s.streamOnce(ctx)
			// Heartbeat on every completed iteration — success OR handled error —
			// so a liveness probe distinguishes a cycling loop (healthy, even when
			// the target is down and each pass errors) from a wedged one that never
			// returns. A pass that hangs never reaches here, so the probe goes stale.
			if s.Heartbeat != nil {
				s.Heartbeat()
			}
			if err != nil {
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
	var consumed int
	drainStart := time.Now()
	pool := s.applyPool()
	for _, comp := range s.Components {
		n, err := apply.DrainComponentRetryingPool(ctx, pool, len(pool),
			comp.Order, s.Target, s.DrainBatch, comp.HasCycle(), apply.DefaultRetryPolicy)
		consumed += n
		if err != nil {
			drainErr = err
			break
		}
	}
	// Apply-batch latency + throughput (only meaningful when a pass did work).
	if drainErr == nil {
		elapsed := time.Since(drainStart)
		s.Tel.ObserveApplyBatch(s.Name, s.Target, elapsed.Seconds())
		if consumed > 0 && elapsed > 0 {
			s.Tel.SetThroughput(s.Name, float64(consumed)/elapsed.Seconds())
		}
	}

	// Retention ALWAYS runs (source-side): routine purge of consumed deltas, and —
	// if the backlog is over the cap — sacrifice the laggard target (reset its
	// track, purge the unpinned deltas, flag it needs_reseed). This bounds source
	// growth even during a target outage; the actual re-copy is deferred to a
	// healthy pass below.
	enf, enforceErr := reseed.Enforce(ctx, s.reseedDeps(), s.Name, s.Replicable,
		[]engine.TargetID{s.Target}, s.Retention)
	for tbl, n := range enf.Purged {
		s.Tel.AddPurged(s.Name, tbl, n)
	}

	if drainErr != nil {
		// Target unhealthy: the source is now protected (Enforce ran); surface the
		// failure and retry next pass. The needs_reseed flag, if set, persists and
		// is picked up once the target recovers.
		s.Tel.IncError(s.Name, "drain")
		s.reportDrainFailure(ctx, drainErr)
		// Distinguish a source-side failure in the up gauges: a bounded ping tells
		// whether the source is reachable (reportDrainFailure already handles the
		// target-reachability signal).
		s.Tel.SetSourceUp(s.Name, s.sourceReachable(ctx))
		// A drain failure may be a dropped connection (network blip, DB failover,
		// idle reap). Health-check both endpoints and reconnect any that are down,
		// so the next pass runs on a live connection instead of erroring forever
		// (Postgres holds a single conn with no auto-redial) — no process restart.
		s.reconnectIfDown(ctx)
		return drainErr
	}
	if enforceErr != nil {
		s.Tel.IncError(s.Name, "retention")
		return enforceErr
	}

	// Healthy pass: refresh per-table backlog + lag gauges and phase, but only emit
	// the escalating retention log once the backlog is actually approaching the cap
	// (half or more) — logging it at proximity 0 every tick is just noise. Refresh
	// the cursor so its age reflects liveness (not a stale cutover timestamp).
	for _, t := range s.Replicable {
		s.Tel.SetPhase(s.Name, t, string(state.PhaseStreaming))
		bl, err := s.Source.DeltaBacklog(ctx, t, s.Target)
		if err != nil {
			continue
		}
		comp := s.componentOf(t)
		s.Tel.SetBacklog(s.Name, s.Target, t, comp, bl)
		s.Tel.SetReplicationLag(s.Name, s.Target, t, comp, bl.OldestAge.Seconds())
		if prox := telemetry.RetentionProximity(bl, s.Retention); prox >= 0.5 {
			s.Tel.RetentionApproaching(ctx, s.Name, s.Target, t, comp, bl, prox)
		}
	}
	s.Tel.SetTargetUp(s.Name, s.Target, true)
	s.Tel.SetSourceUp(s.Name, true) // the drain succeeded, so the source answered
	s.refreshDBSizes(ctx)
	s.touchCursors(ctx)

	// Delete reconciliation (redis-plan §0.4): AFTER the drain, on a healthy pass
	// (you cannot DEL on a down target). A no-op for capture-driven engines
	// (Postgres/MySQL don't implement KeyLister/KeyExister). Best-effort and
	// idempotent — a failed sweep is logged and retried next pass, never aborting
	// streaming.
	if err := s.deleteReconcile(ctx); err != nil {
		s.log(ctx, "delete reconciliation error", err)
	}

	// Reseed the target when either the retention cap forced it (Enforce, above) or
	// an operator flagged it via `replicare reseed` (a cursor marked needs_reseed).
	needReseed := containsTarget(enf.Reseeded, s.Target)
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

// touchCursors refreshes each replicable table's cursor timestamp on a healthy
// pass, so the status API's cursor age reflects streaming liveness rather than a
// frozen cutover time. It preserves every field (a plain Load+Save) so it never
// clobbers an operator-set needs_reseed flag.
func (s *Syncer) touchCursors(ctx context.Context) {
	for _, t := range s.Replicable {
		cur, err := s.Store.LoadCursor(ctx, s.Name, s.Target, t)
		if err != nil {
			continue
		}
		_ = s.Store.SaveCursor(ctx, s.Name, cur)
	}
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
	s.Tel.TargetUnreachable(ctx, nil, s.Name, s.Target, repr, s.componentOf(repr), bl,
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

// healthTimeout bounds a HealthCheck / reconnect attempt so a dead socket fails
// fast instead of re-wedging the streaming loop.
const healthTimeout = 10 * time.Second

// dbSizeInterval throttles the source/target DB-size metric: the queries scan
// catalogs, so they run on this cadence rather than every drain pass.
const dbSizeInterval = 30 * time.Second

// componentOf returns the FK-component id of a table (the component's first sorted
// member, per CLAUDE.md §8.1), for the per-component metric label. Built once.
func (s *Syncer) componentOf(t engine.TableRef) string {
	if s.compIdx == nil {
		s.compIdx = make(map[engine.TableRef]string)
		for _, c := range s.Components {
			id := ""
			if len(c.Tables) > 0 {
				id = c.Tables[0].String()
			}
			for _, ref := range c.Tables {
				s.compIdx[ref] = id
			}
		}
	}
	return s.compIdx[t]
}

// sourceReachable reports whether the source answers a bounded health-check, for
// the source-up gauge on a failed pass.
func (s *Syncer) sourceReachable(ctx context.Context) bool {
	hctx, cancel := context.WithTimeout(ctx, healthTimeout)
	defer cancel()
	return s.Source.HealthCheck(hctx) == nil
}

// refreshDBSizes emits the source/target size gauges when the engine reports them
// (engine.DBSizer — Postgres/MySQL; a no-op otherwise), throttled to
// dbSizeInterval. It emits BOTH figures: the whole-database size (source counts
// replicare's capture schema + delta bloat + unreplicated tables, so it runs
// larger than the target) and the replicated-data size (just the selected tables,
// the apples-to-apples source↔target comparison; CLAUDE.md §3.4). Best-effort: a
// failed size query is skipped, never fatal.
func (s *Syncer) refreshDBSizes(ctx context.Context) {
	now := time.Now()
	if !s.lastDBSize.IsZero() && now.Sub(s.lastDBSize) < dbSizeInterval {
		return
	}
	s.lastDBSize = now
	if sz, ok := s.Source.(engine.DBSizer); ok {
		qctx, cancel := context.WithTimeout(ctx, healthTimeout)
		if b, err := sz.DatabaseSize(qctx); err == nil {
			s.Tel.SetSourceDBBytes(s.Name, b)
		}
		if b, err := sz.ReplicatedSize(qctx, s.Replicable); err == nil {
			s.Tel.SetSourceReplicatedBytes(s.Name, b)
		}
		cancel()
	}
	if sz, ok := s.Sink.(engine.DBSizer); ok {
		qctx, cancel := context.WithTimeout(ctx, healthTimeout)
		if b, err := sz.DatabaseSize(qctx); err == nil {
			s.Tel.SetTargetDBBytes(s.Name, s.Target, b)
		}
		if b, err := sz.ReplicatedSize(qctx, s.Replicable); err == nil {
			s.Tel.SetTargetReplicatedBytes(s.Name, s.Target, b)
		}
		cancel()
	}
}

// reconnectIfDown health-checks the source and target after a failed drain pass
// and reconnects any endpoint whose connection is down. A healthy endpoint is
// left untouched (the drain failure was a data error, handled elsewhere by the
// loud-halt policy). Reconnection is Close + Connect, which for Postgres re-dials
// the single pgx connection and re-applies session GUCs, and for MySQL/Redis
// rebuilds the pooled client. Best-effort and non-fatal: a reconnect that itself
// fails (server still down) is logged and retried on the next pass, so the daemon
// never crash-loops on an outage — it heals when the endpoint returns.
func (s *Syncer) reconnectIfDown(ctx context.Context) {
	if ctx.Err() != nil {
		return // shutting down; don't churn connections
	}
	// Reconnect every connection the drain uses — the primary pair (pool[0], also
	// used for gather/telemetry) and any copy-worker pairs recruited for concurrent
	// apply — so a dropped worker connection heals too, not just the primary.
	for i, c := range s.applyPool() {
		src, sink := c.Src, c.Sink
		s.reconnectEndpoint(ctx, poolRole("source", i), src.HealthCheck, src.Close, src.Connect)
		s.reconnectEndpoint(ctx, poolRole("target", i), sink.HealthCheck, sink.Close, sink.Connect)
	}
}

// applyPool builds the streaming apply connection pool: the primary (source,sink)
// pair plus up to ApplyConcurrency-1 copy-worker pairs (idle during streaming).
// pool[0] is the primary — the connection gather and telemetry also use — so at
// concurrency 1 the pool is exactly today's single pair.
func (s *Syncer) applyPool() []apply.Conn {
	conns := []apply.Conn{{Src: s.Source, Sink: s.Sink}}
	for i := 0; i < len(s.Workers) && len(conns) < s.ApplyConcurrency; i++ {
		conns = append(conns, apply.Conn{Src: s.Workers[i].Src, Sink: s.Workers[i].Sink})
	}
	return conns
}

// poolRole labels a pool endpoint for reconnect logs (primary vs. a worker pair).
func poolRole(kind string, i int) string {
	if i == 0 {
		return kind
	}
	return kind + " worker " + strconv.Itoa(i)
}

func (s *Syncer) reconnectEndpoint(ctx context.Context, role string,
	healthCheck, closeConn, connect func(context.Context) error) {
	hctx, cancel := context.WithTimeout(ctx, healthTimeout)
	healthErr := healthCheck(hctx)
	cancel()
	if healthErr == nil {
		return // connection is alive; the drain failure was not connectivity
	}
	s.log(ctx, "reconnecting "+role+" after health check failed", healthErr)
	_ = closeConn(context.Background())
	cctx, ccancel := context.WithTimeout(ctx, healthTimeout)
	defer ccancel()
	if err := connect(cctx); err != nil {
		s.log(ctx, "reconnect "+role+" failed (will retry next pass)", err)
		return
	}
	s.recordEvent(ctx, state.Event{
		Sync: s.Name, Target: string(s.Target), Level: "WARN",
		Event: "stream.reconnected", Message: role + " connection re-established after failure",
	})
}
