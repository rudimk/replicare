// Package telemetry is the cross-channel emitter that enforces replicare's
// hard observability rule (CLAUDE.md §3.4/§10): a degrading target — unreachable
// and/or its delta backlog climbing toward the retention cap — must be visible in
// metrics, logs, AND traces simultaneously, never just one channel. It bundles
// the Prometheus registry, the OTel tracer, an slog logger, and an optional
// durable event sink behind one facade so an emit site fires every channel with a
// single call and they cannot fall out of sync.
package telemetry

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/trace"

	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/observability"
	"github.com/rudimk/replicare/internal/observability/prom"
	"github.com/rudimk/replicare/internal/observability/tracing"
	"github.com/rudimk/replicare/internal/state"
)

// EventSink persists an operational event (the StateStore's RecordEvent). It is
// optional; a nil sink skips the durable channel.
type EventSink interface {
	RecordEvent(ctx context.Context, e state.Event) error
}

// Telemetry fans one observation out to every channel. Any channel may be nil
// (metrics/events) or defaulted (tracer→noop, log→slog.Default), so the same
// facade works in tests, one-shot CLIs, and the daemon.
type Telemetry struct {
	reg    *prom.Registry
	tracer *tracing.Tracer
	log    *slog.Logger
	events EventSink
}

// New builds a Telemetry. A nil registry disables metrics; a nil tracer becomes
// a no-op tracer; a nil logger becomes slog.Default; a nil sink skips durable
// events.
func New(reg *prom.Registry, tracer *tracing.Tracer, log *slog.Logger, events EventSink) *Telemetry {
	if tracer == nil {
		tracer = tracing.Noop()
	}
	if log == nil {
		log = slog.Default()
	}
	return &Telemetry{reg: reg, tracer: tracer, log: log, events: events}
}

// --- Metric emitters (all guard a nil registry) ---

// SetTargetUp sets the per-target reachability gauge (1=up, 0=down).
func (t *Telemetry) SetTargetUp(sync string, target engine.TargetID, up bool) {
	if t.reg == nil {
		return
	}
	v := 0.0
	if up {
		v = 1
	}
	t.reg.Gauge(observability.MetricTargetUp).WithLabelValues(sync, string(target)).Set(v)
}

// SetBacklog publishes a target/table's unconsumed-delta backlog gauges.
func (t *Telemetry) SetBacklog(sync string, target engine.TargetID, table engine.TableRef, bl engine.DeltaBacklog) {
	if t.reg == nil {
		return
	}
	tbl := table.String()
	t.reg.Gauge(observability.MetricDeltaBacklog).WithLabelValues(sync, string(target), tbl).Set(float64(bl.Rows))
	t.reg.Gauge(observability.MetricDeltaBacklogBytes).WithLabelValues(sync, string(target), tbl).Set(float64(bl.Bytes))
	t.reg.Gauge(observability.MetricDeltaOldestAgeSeconds).WithLabelValues(sync, string(target), tbl).Set(bl.OldestAge.Seconds())
}

// AddPurged advances the purged-delta counter.
func (t *Telemetry) AddPurged(sync string, table engine.TableRef, n int64) {
	if t.reg == nil || n <= 0 {
		return
	}
	t.reg.Counter(observability.MetricDeltaPurgedTotal).WithLabelValues(sync, table.String()).Add(float64(n))
}

// IncReseed advances the forced-reseed counter for a target.
func (t *Telemetry) IncReseed(sync string, target engine.TargetID) {
	if t.reg == nil {
		return
	}
	t.reg.Counter(observability.MetricReseedTotal).WithLabelValues(sync, string(target)).Inc()
}

// ObserveApplyBatch records an apply-batch duration.
func (t *Telemetry) ObserveApplyBatch(sync string, target engine.TargetID, seconds float64) {
	if t.reg == nil {
		return
	}
	t.reg.Histogram(observability.MetricApplyBatchSeconds).WithLabelValues(sync, string(target)).Observe(seconds)
}

// SetReplicationLag publishes a target/table's replication lag (the age of the
// oldest unconsumed delta, or 0 when caught up).
func (t *Telemetry) SetReplicationLag(sync string, target engine.TargetID, table engine.TableRef, seconds float64) {
	if t.reg == nil {
		return
	}
	t.reg.Gauge(observability.MetricReplicationLagSeconds).WithLabelValues(sync, string(target), table.String()).Set(seconds)
}

// SetDeleteLag publishes how long the last completed delete-reconciliation sweep
// took for a unit — the staleness signal for Redis's capture-less deletes (RM8).
func (t *Telemetry) SetDeleteLag(sync string, target engine.TargetID, table engine.TableRef, seconds float64) {
	if t.reg == nil {
		return
	}
	t.reg.Gauge(observability.MetricDeleteReconcileLag).WithLabelValues(sync, string(target), table.String()).Set(seconds)
}

// AddDeletes advances the delete-sweep counter for a unit.
func (t *Telemetry) AddDeletes(sync string, target engine.TargetID, table engine.TableRef, n int64) {
	if t.reg == nil || n <= 0 {
		return
	}
	t.reg.Counter(observability.MetricDeletesReconciled).WithLabelValues(sync, string(target), table.String()).Add(float64(n))
}

// SetThroughput publishes the current apply/copy throughput for a sync (rows/sec).
func (t *Telemetry) SetThroughput(sync string, rowsPerSec float64) {
	if t.reg == nil {
		return
	}
	t.reg.Gauge(observability.MetricThroughputRows).WithLabelValues(sync).Set(rowsPerSec)
}

// SetPhase marks a table's current lifecycle phase (initial_copy/streaming) as an
// info gauge (value 1 on the active phase label).
func (t *Telemetry) SetPhase(sync string, table engine.TableRef, phase string) {
	if t.reg == nil {
		return
	}
	t.reg.Gauge(observability.MetricPhaseInfo).WithLabelValues(sync, table.String(), phase).Set(1)
}

// AddRowsCopied advances the initial-copy rows counter for a table.
func (t *Telemetry) AddRowsCopied(sync string, table engine.TableRef, n int64) {
	if t.reg == nil || n <= 0 {
		return
	}
	t.reg.Counter(observability.MetricRowsCopiedTotal).WithLabelValues(sync, table.String()).Add(float64(n))
}

// SetRowsCopiedTarget publishes the estimated total rows to copy for a table (the
// progress denominator).
func (t *Telemetry) SetRowsCopiedTarget(sync string, table engine.TableRef, total int64) {
	if t.reg == nil {
		return
	}
	t.reg.Gauge(observability.MetricRowsCopiedTarget).WithLabelValues(sync, table.String()).Set(float64(total))
}

// IncError advances the error counter for a category (e.g. "drain", "apply").
func (t *Telemetry) IncError(sync, category string) {
	if t.reg == nil {
		return
	}
	t.reg.Counter(observability.MetricErrorsTotal).WithLabelValues(sync, category).Inc()
}

// --- Span helper ---

// StartDrainSpan starts a delta-drain span carrying the target/table backlog
// context, so if the drain fails the span already has the blast-radius
// attributes (CLAUDE.md §3.4). The caller ends the span (via TargetReachable /
// TargetUnreachable, or span.End()).
func (t *Telemetry) StartDrainSpan(ctx context.Context, sync string, target engine.TargetID,
	table engine.TableRef, bl engine.DeltaBacklog, proximity float64) (context.Context, trace.Span) {
	return t.tracer.Start(ctx, observability.SpanDeltaDrain,
		tracing.AttrSync(sync), tracing.AttrTarget(string(target)), tracing.AttrTable(table.String()),
		tracing.AttrDeltaBacklog(bl.Rows), tracing.AttrOldestAgeSeconds(bl.OldestAge.Seconds()),
		tracing.AttrRetentionProximity(proximity))
}

// --- Cross-channel emitters ---

// TargetUnreachable fires ALL channels for an unreachable target during a drain
// (CLAUDE.md §3.4): span → error status + error event; metrics → reachability
// gauge 0 + backlog gauges; log → ERROR target.unreachable with backlog/cause;
// durable event. span may be nil (no active drain span).
func (t *Telemetry) TargetUnreachable(ctx context.Context, span trace.Span, sync string,
	target engine.TargetID, table engine.TableRef, bl engine.DeltaBacklog, proximity float64, cause error) {

	if span != nil {
		tracing.Fail(span, cause)
	}
	t.SetTargetUp(sync, target, false)
	t.SetBacklog(sync, target, table, bl)

	t.log.LogAttrs(ctx, slog.LevelError, "target unreachable",
		slog.String("event", observability.EventTargetUnreachable),
		slog.String(observability.AttrSync, sync),
		slog.String(observability.AttrTarget, string(target)),
		slog.String(observability.AttrTable, table.String()),
		slog.Int64(observability.AttrDeltaBacklog, bl.Rows),
		slog.Float64(observability.AttrOldestAgeSeconds, bl.OldestAge.Seconds()),
		slog.Float64(observability.AttrRetentionProx, proximity),
		slog.String(observability.AttrError, cause.Error()))

	t.recordEvent(ctx, state.Event{
		Sync: sync, Target: string(target), Table: table, Level: "ERROR",
		Event: observability.EventTargetUnreachable, Message: cause.Error(),
		Attrs: map[string]any{
			observability.AttrDeltaBacklog:  bl.Rows,
			observability.AttrRetentionProx: proximity,
		},
	})
}

// TargetReachable marks a target healthy again: reachability gauge 1, refreshed
// backlog gauges, and (once, on recovery) an INFO target.recovered log + event.
func (t *Telemetry) TargetReachable(ctx context.Context, sync string, target engine.TargetID,
	table engine.TableRef, bl engine.DeltaBacklog) {
	t.SetTargetUp(sync, target, true)
	t.SetBacklog(sync, target, table, bl)
	t.log.LogAttrs(ctx, slog.LevelInfo, "target reachable",
		slog.String("event", observability.EventTargetRecovered),
		slog.String(observability.AttrSync, sync),
		slog.String(observability.AttrTarget, string(target)),
		slog.String(observability.AttrTable, table.String()))
}

// RetentionApproaching publishes backlog gauges and logs the delta backlog's
// proximity to the retention cap at an ESCALATING level — INFO below half, WARN
// past half, ERROR near the cap — so alerts fire on "backlog climbing toward the
// cap" long before a forced reseed (CLAUDE.md §3.4). At WARN and above it also
// records a durable retention.cap_approaching event.
func (t *Telemetry) RetentionApproaching(ctx context.Context, sync string, target engine.TargetID,
	table engine.TableRef, bl engine.DeltaBacklog, proximity float64) {

	t.SetBacklog(sync, target, table, bl)
	level := slog.LevelInfo
	switch {
	case proximity >= 0.9:
		level = slog.LevelError
	case proximity >= 0.5:
		level = slog.LevelWarn
	}
	t.log.LogAttrs(ctx, level, "delta backlog approaching retention cap",
		slog.String("event", observability.EventRetentionApproach),
		slog.String(observability.AttrSync, sync),
		slog.String(observability.AttrTarget, string(target)),
		slog.String(observability.AttrTable, table.String()),
		slog.Int64(observability.AttrDeltaBacklog, bl.Rows),
		slog.Float64(observability.AttrOldestAgeSeconds, bl.OldestAge.Seconds()),
		slog.Float64(observability.AttrRetentionProx, proximity))

	if level >= slog.LevelWarn {
		t.recordEvent(ctx, state.Event{
			Sync: sync, Target: string(target), Table: table, Level: levelName(level),
			Event: observability.EventRetentionApproach,
			Attrs: map[string]any{observability.AttrRetentionProx: proximity},
		})
	}
}

func (t *Telemetry) recordEvent(ctx context.Context, e state.Event) {
	if t.events == nil {
		return
	}
	if err := t.events.RecordEvent(ctx, e); err != nil {
		t.log.Warn("failed to record durable event", "event", e.Event, observability.AttrError, err.Error())
	}
}

func levelName(l slog.Level) string {
	switch {
	case l >= slog.LevelError:
		return "ERROR"
	case l >= slog.LevelWarn:
		return "WARN"
	default:
		return "INFO"
	}
}

// RetentionProximity is the fraction (0..1, clamped) of the nearest ACTIVE
// retention cap the backlog has reached — the max of the age and size ratios.
// Zero when no cap is set. It is the headline "how close to a forced reseed?"
// signal shared by the log escalation and the AttrRetentionProx attribute.
func RetentionProximity(bl engine.DeltaBacklog, ret engine.RetentionPolicy) float64 {
	prox := 0.0
	if ret.MaxAgeSeconds > 0 {
		if r := bl.OldestAge.Seconds() / float64(ret.MaxAgeSeconds); r > prox {
			prox = r
		}
	}
	if ret.MaxBytes > 0 {
		if r := float64(bl.Bytes) / float64(ret.MaxBytes); r > prox {
			prox = r
		}
	}
	if prox > 1 {
		prox = 1
	}
	return prox
}
