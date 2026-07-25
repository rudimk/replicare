// Package pipeline is the sync orchestration layer (CLAUDE.md §8). M6 lands the
// first instrumented step — a single streaming drain pass wrapped in the
// cross-channel telemetry facade — so the §3.4 rule (a degrading target is
// visible in metrics, logs, AND traces at once) holds on the real drain path.
// The full multi-sync daemon loop is assembled on top of this in M7.
package pipeline

import (
	"context"

	"github.com/rudimk/replicare/internal/apply"
	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/observability/telemetry"
	"github.com/rudimk/replicare/internal/observability/tracing"
)

// DrainTable runs one instrumented streaming drain pass for a table/target and
// emits the health signal across all channels (CLAUDE.md §3.4):
//
//   - It reads the source-side backlog up front so the drain span and the
//     failure signal carry blast-radius (backlog rows/age, retention proximity)
//     even when the target is down.
//   - On a pass that the target will not accept, it probes the sink; if the probe
//     also fails the target is unreachable and TargetUnreachable fires the span
//     error status, the reachability gauge (0), the backlog gauges, an ERROR log,
//     and a durable event — all at once. If the probe succeeds the target is up
//     and the failure is data-related (type/constraint, §4.2), so the span is
//     marked errored but the target is NOT flagged unreachable.
//   - On success it marks the target reachable and reports retention proximity
//     (escalating WARN->ERROR as the backlog nears the cap).
//
// It returns the number of deltas consumed and the drain error (nil on success).
func DrainTable(ctx context.Context, tm *telemetry.Telemetry, src engine.Source, sink engine.Sink,
	sync string, target engine.TargetID, table engine.TableRef, batch int, ret engine.RetentionPolicy) (int, error) {

	// Backlog is a source-side read, so it is available even if the target is down.
	bl, blErr := src.DeltaBacklog(ctx, table, target)
	prox := telemetry.RetentionProximity(bl, ret)

	ctx, span := tm.StartDrainSpan(ctx, sync, target, table, bl, prox)
	defer span.End()

	n, err := apply.Drain(ctx, src, sink, table, target, batch)
	if err != nil {
		if targetUnreachable(ctx, sink) {
			tm.TargetUnreachable(ctx, span, sync, target, table, bl, prox, err)
		} else {
			// Target is up; the pass failed on the data (halt/retry is handled by
			// the apply layer). Record the span error without flagging reachability.
			tracing.Fail(span, err)
		}
		return 0, err
	}

	// Success: refresh reachability + backlog, and escalate if nearing the cap.
	tm.TargetReachable(ctx, sync, target, table, bl)
	if blErr == nil && prox > 0 {
		tm.RetentionApproaching(ctx, sync, target, table, bl, prox)
	}
	return n, nil
}

// targetUnreachable probes the sink to decide whether a drain failure is the
// target being down (probe fails) versus a data-level error (probe succeeds).
func targetUnreachable(ctx context.Context, sink engine.Sink) bool {
	_, err := sink.ServerVersion(ctx)
	return err != nil
}
