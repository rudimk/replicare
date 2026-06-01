// Package observability defines the F2 observability contract: the single source
// of truth for every metric, span, and structured-log field replicare emits.
//
// Locked in M0 so that M2–M5 emit against a fixed registry and M6 merely verifies
// it (no late retrofitting; see .sisyphus plan F2 and CLAUDE.md §10). A hard
// requirement encoded here: target unreachability + delta growth must be visible
// across ALL three channels — metrics, traces, AND logs (CLAUDE.md §3.4).
package observability

// MetricKind is the Prometheus metric type.
type MetricKind string

const (
	Counter   MetricKind = "counter"
	Gauge     MetricKind = "gauge"
	Histogram MetricKind = "histogram"
)

// Metric describes one metric in the contract.
type Metric struct {
	Name   string
	Kind   MetricKind
	Help   string
	Labels []string
}

// Common label keys. Kept as constants so emit sites and the registry agree.
const (
	LabelSync   = "sync"
	LabelTarget = "target"
	LabelTable  = "table"
	LabelPhase  = "phase"
	LabelEngine = "engine"
)

// Metric names. Prefix everything with "replicare_".
const (
	MetricRowsCopiedTotal       = "replicare_rows_copied_total"
	MetricRowsCopiedTarget      = "replicare_initial_copy_rows_target"
	MetricThroughputRows        = "replicare_throughput_rows_per_second"
	MetricReplicationLagSeconds = "replicare_replication_lag_seconds"
	MetricDeltaBacklog          = "replicare_delta_backlog_rows"
	MetricDeltaBacklogBytes     = "replicare_delta_backlog_bytes"
	MetricDeltaOldestAgeSeconds = "replicare_delta_oldest_unconsumed_age_seconds"
	MetricDeltaPurgedTotal      = "replicare_delta_purged_total"
	MetricReseedTotal           = "replicare_reseed_total"
	MetricTargetUp              = "replicare_target_up"
	MetricApplyBatchSeconds     = "replicare_apply_batch_seconds"
	MetricErrorsTotal           = "replicare_errors_total"
	MetricPhaseInfo             = "replicare_table_phase_info"
)

// Metrics is the full metric registry. M6 asserts the running registry matches.
var Metrics = []Metric{
	{MetricRowsCopiedTotal, Counter, "Rows copied during initial copy", []string{LabelSync, LabelTable}},
	{MetricRowsCopiedTarget, Gauge, "Estimated total rows to copy (denominator for progress)", []string{LabelSync, LabelTable}},
	{MetricThroughputRows, Gauge, "Current apply/copy throughput in rows/sec", []string{LabelSync}},
	{MetricReplicationLagSeconds, Gauge, "Replication lag in seconds", []string{LabelSync, LabelTarget, LabelTable}},
	{MetricDeltaBacklog, Gauge, "Unconsumed delta rows", []string{LabelSync, LabelTarget, LabelTable}},
	{MetricDeltaBacklogBytes, Gauge, "Estimated unconsumed delta bytes", []string{LabelSync, LabelTarget, LabelTable}},
	{MetricDeltaOldestAgeSeconds, Gauge, "Age of the oldest unconsumed delta", []string{LabelSync, LabelTarget, LabelTable}},
	{MetricDeltaPurgedTotal, Counter, "Delta rows purged", []string{LabelSync, LabelTable}},
	{MetricReseedTotal, Counter, "Forced reseeds triggered by retention cap", []string{LabelSync, LabelTarget}},
	{MetricTargetUp, Gauge, "Target reachability (1=up, 0=down)", []string{LabelSync, LabelTarget}},
	{MetricApplyBatchSeconds, Histogram, "Apply batch duration", []string{LabelSync, LabelTarget}},
	{MetricErrorsTotal, Counter, "Errors by category", []string{LabelSync, "category"}},
	{MetricPhaseInfo, Gauge, "Table phase (initial_copy/streaming) as an info gauge", []string{LabelSync, LabelTable, LabelPhase}},
}

// Span names for OTel traces. Drain/apply spans against an unreachable target
// MUST be recorded with error status + backlog attributes (CLAUDE.md §3.4).
const (
	SpanInitialCopyChunk = "replicare.initial_copy.chunk"
	SpanCaptureInstall   = "replicare.capture.install"
	SpanDeltaDrain       = "replicare.delta.drain"
	SpanApplyComponent   = "replicare.apply.component"
	SpanReseed           = "replicare.reseed"
	SpanPreflight        = "replicare.preflight"
)

// Spans is the full span registry.
var Spans = []string{
	SpanInitialCopyChunk,
	SpanCaptureInstall,
	SpanDeltaDrain,
	SpanApplyComponent,
	SpanReseed,
	SpanPreflight,
}

// Span/log attribute keys shared across traces and structured logs so the three
// channels stay consistent.
const (
	AttrSync             = "sync"
	AttrTarget           = "target"
	AttrTable            = "table"
	AttrPhase            = "phase"
	AttrDeltaBacklog     = "delta_backlog"
	AttrOldestAgeSeconds = "oldest_unconsumed_age_seconds"
	AttrRetentionProx    = "retention_cap_proximity" // 0..1 toward the cap
	AttrError            = "error"
)

// Structured-log event names (slog "event" field). Target degradation escalates
// WARN->ERROR via these and must coincide with the metric/span signals.
const (
	EventTargetUnreachable = "target.unreachable"
	EventTargetRecovered   = "target.recovered"
	EventTargetNeedsReseed = "target.needs_reseed"
	EventRetentionApproach = "retention.cap_approaching"
	EventPreflightBlocked  = "preflight.blocked"
	EventCaptureInstalled  = "capture.installed"
	EventCaptureRemoved    = "capture.removed"
	EventCutover           = "table.cutover_to_streaming"
)
