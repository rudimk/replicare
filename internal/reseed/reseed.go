// Package reseed is the engine-neutral M5c orchestration for bounded retention
// and forced reseed (CLAUDE.md §3.4; docs/reseed-state-machine.md). It sits on
// the Source / Sink / StateStore interfaces and reuses the M4 copy driver and the
// M5a/M5b drain, so it is engine-independent.
//
// Two entry points:
//
//   - Enforce runs a retention/purge pass over an FK component's tables and marks
//     any target it sacrifices as needs-reseed in the StateStore. Call it each
//     drain cycle to bound source growth.
//   - Run recovers one sacrificed target: it re-derives that target's copy of the
//     component from current source state and cuts it back to streaming. Capture
//     is never uninstalled, so changes during the reseed are captured and
//     reconciled by the resumed drain — no delta is lost (design doc §4.3).
package reseed

import (
	"context"
	"fmt"
	"log/slog"
	"sort"

	"github.com/rudimk/replicare/internal/apply"
	"github.com/rudimk/replicare/internal/copy"
	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/observability"
	"github.com/rudimk/replicare/internal/state"
)

// Deps carries the collaborators the reseed orchestration drives. Src/Sink are
// the single-connection pair used for purge and cutover; Workers is the parallel
// copy pool used for the re-copy (each a Source/Sink pair — a Source/Sink is not
// safe for concurrent use, so parallelism comes from multiple workers).
type Deps struct {
	Src     engine.Source
	Sink    engine.Sink
	Workers []copy.Worker
	Store   state.StateStore
	Log     *slog.Logger
}

func (d Deps) logger() *slog.Logger {
	if d.Log != nil {
		return d.Log
	}
	return slog.Default()
}

// EnforceResult reports the outcome of an Enforce pass: which targets now need a
// reseed, and how many delta rows were purged per table (for the purged-delta
// metric).
type EnforceResult struct {
	Reseeded []engine.TargetID
	Purged   map[engine.TableRef]int64
}

// Enforce runs a purge/retention pass over the component's tables for the full
// target set and returns the targets that now require a reseed (union across the
// component's tables — the reseed re-copies the whole component) plus per-table
// purge counts. For each reseeding target it marks every component-table cursor
// needs-reseed + phase=initial_copy in the StateStore and records the
// target.needs_reseed event (design doc §3, §6). Purge itself already reset the
// sacrificed targets' source track.
func Enforce(ctx context.Context, d Deps, syncName string, tablesTopo []engine.TableRef,
	targets []engine.TargetID, ret engine.RetentionPolicy) (EnforceResult, error) {

	res := EnforceResult{Purged: map[engine.TableRef]int64{}}
	reseedSet := map[engine.TargetID]bool{}
	for _, t := range tablesTopo {
		stats, err := d.Src.Purge(ctx, t, targets, ret)
		if err != nil {
			return EnforceResult{}, fmt.Errorf("reseed: enforce purge %s: %w", t, err)
		}
		if stats.DeltasPurged > 0 {
			res.Purged[t] = stats.DeltasPurged
		}
		for _, tg := range stats.TargetsReseeded {
			reseedSet[tg] = true
		}
	}
	if len(reseedSet) == 0 {
		return res, nil
	}

	reseeded := make([]engine.TargetID, 0, len(reseedSet))
	for tg := range reseedSet {
		reseeded = append(reseeded, tg)
	}
	sort.Slice(reseeded, func(i, j int) bool { return reseeded[i] < reseeded[j] })

	for _, tg := range reseeded {
		for _, t := range tablesTopo {
			cur, err := d.Store.LoadCursor(ctx, syncName, tg, t)
			if err != nil {
				return EnforceResult{}, fmt.Errorf("reseed: enforce load cursor %s/%s: %w", tg, t, err)
			}
			cur.NeedsReseed = true
			cur.Phase = state.PhaseInitialCopy
			if err := d.Store.SaveCursor(ctx, syncName, cur); err != nil {
				return EnforceResult{}, fmt.Errorf("reseed: enforce mark cursor %s/%s: %w", tg, t, err)
			}
		}
		// Durable + log signals (the metric channel is finalized in M6). ERROR
		// level: a forced reseed means a target fell far enough behind to be reset.
		if err := d.Store.RecordEvent(ctx, state.Event{
			Sync:    syncName,
			Target:  string(tg),
			Level:   "ERROR",
			Event:   observability.EventTargetNeedsReseed,
			Message: "delta backlog exceeded retention cap; target scheduled for reseed",
		}); err != nil {
			return EnforceResult{}, fmt.Errorf("reseed: enforce record event %s: %w", tg, err)
		}
		d.logger().Error("target scheduled for reseed",
			observability.AttrSync, syncName, observability.AttrTarget, string(tg))
	}
	res.Reseeded = reseeded
	return res, nil
}

// Run recovers one reseeding target over the component's tables (topological
// order, parents first). It is idempotent and safe to re-invoke after a crash
// (design doc §4.4): empty every target table child->parent (so target FKs hold
// during the delete-all — within the DML-only grant, no TRUNCATE), reset the
// component's copy progress, re-copy parents-first, then cut back to streaming by
// clearing needs-reseed. It deliberately does NOT drain to empty: capture never
// stopped, so the steady-state drain reconciles retained and in-flight deltas
// exactly as the initial cutover does (CLAUDE.md §4, M5a).
func Run(ctx context.Context, d Deps, syncName string, target engine.TargetID,
	tablesTopo []engine.TableRef, opts engine.ChunkOptions) error {
	if len(tablesTopo) == 0 {
		return nil
	}
	if len(d.Workers) == 0 {
		return fmt.Errorf("reseed %s: no copy workers", target)
	}

	// 1. Empty the target tables, child -> parent, so referencing rows go before
	//    referenced rows and live target FKs are never violated mid-delete.
	for i := len(tablesTopo) - 1; i >= 0; i-- {
		if err := d.Sink.DeleteRange(ctx, tablesTopo[i], nil, nil); err != nil {
			return fmt.Errorf("reseed %s: clear target %s: %w", target, tablesTopo[i], err)
		}
	}
	// 2. Reset copy progress so the re-copy runs from scratch into the now-empty
	//    target (a fresh watermark makes the driver plan every chunk and direct
	//    COPY, correct because the table is empty).
	for _, t := range tablesTopo {
		if err := d.Store.SaveCopyProgress(ctx, syncName, state.CopyProgress{Table: t}); err != nil {
			return fmt.Errorf("reseed %s: reset progress %s: %w", target, t, err)
		}
	}
	// 3. Re-copy parents-first (chunks parallel across the workers).
	if err := copy.Component(ctx, d.Workers, d.Store, syncName, tablesTopo, opts); err != nil {
		return fmt.Errorf("reseed %s: re-copy: %w", target, err)
	}
	// 4. Cut back to streaming: clear needs-reseed and set phase. The resumed
	//    drain (delta MINUS track) reconciles everything captured since — the
	//    purged deltas' keys are covered by the fresh copy (design doc §4.3).
	for _, t := range tablesTopo {
		cur, err := d.Store.LoadCursor(ctx, syncName, target, t)
		if err != nil {
			return fmt.Errorf("reseed %s: load cursor %s: %w", target, t, err)
		}
		cur.NeedsReseed = false
		cur.Phase = state.PhaseStreaming
		if err := d.Store.SaveCursor(ctx, syncName, cur); err != nil {
			return fmt.Errorf("reseed %s: cutover cursor %s: %w", target, t, err)
		}
	}
	if err := d.Store.RecordEvent(ctx, state.Event{
		Sync:    syncName,
		Target:  string(target),
		Level:   "INFO",
		Event:   observability.EventCutover,
		Message: "reseed complete; target re-copied and resumed streaming",
	}); err != nil {
		return fmt.Errorf("reseed %s: record cutover: %w", target, err)
	}
	d.logger().Info("reseed complete",
		observability.AttrSync, syncName, observability.AttrTarget, string(target))
	return nil
}

// DrainToConvergence drains a component's backlog to empty for a target using the
// FK-ordered retrying apply (M5b). It is a convenience for callers that want to
// force convergence at a quiescent point (e.g. tests, operator reseed); the
// steady-state daemon loop uses the per-pass drain directly. Bounded by churn per
// pass; returns the total deltas consumed.
func DrainToConvergence(ctx context.Context, d Deps, tablesTopo []engine.TableRef,
	target engine.TargetID, batch int, cyclic bool) (int, error) {
	total := 0
	for {
		n, err := apply.DrainComponentRetrying(ctx, d.Src, d.Sink, tablesTopo, target, batch, cyclic, apply.DefaultRetryPolicy)
		if err != nil {
			return total, err
		}
		total += n
		if n == 0 {
			return total, nil
		}
	}
}
