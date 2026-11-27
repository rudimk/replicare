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
	LabelSync      = "sync"
	LabelTarget    = "target"
	LabelTable     = "table"
	LabelPhase     = "phase"
	LabelEngine    = "engine"
	LabelComponent = "component"
	LabelIndex     = "index"
	// LabelHow says how a target trigger is kept from firing on replicare's writes:
	// "replica_mode" (it skips them, nothing changed) or "switched_off".
	LabelHow = "how"
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
	// How long a table's last purge took, in seconds. The purge runs on the source every
	// pass; one that takes more than a second or so is loading a database we may not own.
	MetricDeltaPurgeSeconds = "replicare_delta_purge_seconds"
	// MetricTrackLeftoversRemovedTotal counts the track rows left behind by versions
	// before 0.7.0 that the background sweep has removed, per table.
	MetricTrackLeftoversRemovedTotal = "replicare_track_leftovers_removed_total"
	// MetricCopySkippedTables counts the tables a sync marked copied without copying
	// (skip_initial_copy), by where their check against the source stands: pending,
	// ok, drift, or off (the check is turned off).
	MetricCopySkippedTables = "replicare_copy_skipped_tables"
	// A sync target (or cluster edge) whose start-up failed and is waiting to try again:
	// 1 while waiting, 0 once it is running. The counter counts the failures. One sync
	// failing to start no longer stops the daemon's other syncs, so this is how it shows.
	MetricSyncStartFailed        = "replicare_sync_start_failed"
	MetricSyncStartFailuresTotal = "replicare_sync_start_failures_total"
	// MetricSyncTables is how many tables a sync target replicates (its selection, less
	// the tables skipped for having no usable key): 0 means it replicates nothing.
	MetricSyncTables    = "replicare_sync_tables"
	MetricReseedTotal   = "replicare_reseed_total"
	MetricTargetUp      = "replicare_target_up"
	MetricSourceUp      = "replicare_source_up"
	MetricSourceDBBytes = "replicare_source_db_bytes"
	MetricTargetDBBytes = "replicare_target_db_bytes"
	// Replicated-data size: just the sync's selected tables (heap+indexes+TOAST),
	// as opposed to the whole-database *_db_bytes above. This is the
	// apples-to-apples source↔target figure — it excludes replicare's own capture
	// schema and delta-table bloat on the source and any unreplicated tables, so a
	// converged sync should show source ≈ target here even when the whole-DB
	// gauges differ (CLAUDE.md §3.4). Engines that report a size implement it.
	MetricSourceReplicatedBytes = "replicare_source_replicated_bytes"
	MetricTargetReplicatedBytes = "replicare_target_replicated_bytes"
	MetricApplyBatchSeconds     = "replicare_apply_batch_seconds"
	MetricErrorsTotal           = "replicare_errors_total"
	MetricPhaseInfo             = "replicare_table_phase_info"
	// Delete-reconciliation signals (redis-plan RM8). For capture-driven engines
	// (Postgres/MySQL) deletes flow through the delta path and these stay unset;
	// Redis, whose deletes are found by the target-vs-source sweep, publishes them.
	MetricDeleteReconcileLag = "replicare_delete_reconciliation_lag_seconds"
	MetricDeletesReconciled  = "replicare_deletes_reconciled_total"
	// ETAs, computed in the daemon so a dashboard shows a duration, not a ratio.
	// -1 means "not moving" (no copy progress, or a backlog that is not shrinking);
	// no series means not enough history yet.
	MetricInitialCopyETA = "replicare_initial_copy_eta_seconds"
	MetricCatchupETA     = "replicare_catchup_eta_seconds"
	// Initial-copy progress, 0..1, by the same measure as the ETA (size-weighted, and
	// counting the link-filling pass of tables in a loop). Under 1 until every table is done.
	MetricInitialCopyProgress = "replicare_initial_copy_progress_ratio"
	// The link-filling pass of tables in a loop of foreign keys: rows whose links were set,
	// and the rows the pass will touch (the table's row estimate).
	MetricLinkFillRowsTotal  = "replicare_link_fill_rows_total"
	MetricLinkFillRowsTarget = "replicare_link_fill_rows_target"
	// Deferred target indexes: plain indexes taken down for the initial copy and built
	// again once the sync streams. Total saved and how many are back, per target, and the
	// build in flight: which index, its phase, and how far through that phase it is.
	MetricIndexesDeferred = "replicare_deferred_indexes"
	// MetricTargetTriggers counts the target's own triggers kept from firing on
	// replicare's writes, by how (LabelHow).
	MetricTargetTriggers       = "replicare_target_triggers"
	MetricIndexesBuilt         = "replicare_deferred_indexes_built"
	MetricIndexBuildInfo       = "replicare_index_build_info"
	MetricIndexBuildPhaseRatio = "replicare_index_build_phase_progress_ratio"
)

// Metrics is the full metric registry. M6 asserts the running registry matches.
var Metrics = []Metric{
	{MetricRowsCopiedTotal, Counter, "Rows copied during initial copy", []string{LabelSync, LabelTable}},
	{MetricRowsCopiedTarget, Gauge, "Estimated total rows to copy (denominator for progress)", []string{LabelSync, LabelTable}},
	{MetricThroughputRows, Gauge, "Current apply/copy throughput in rows/sec", []string{LabelSync}},
	{MetricReplicationLagSeconds, Gauge, "Replication lag in seconds", []string{LabelSync, LabelTarget, LabelTable, LabelComponent}},
	{MetricDeltaBacklog, Gauge, "Unconsumed delta rows", []string{LabelSync, LabelTarget, LabelTable, LabelComponent}},
	{MetricDeltaBacklogBytes, Gauge, "Estimated unconsumed delta bytes", []string{LabelSync, LabelTarget, LabelTable, LabelComponent}},
	{MetricDeltaOldestAgeSeconds, Gauge, "Age of the oldest unconsumed delta", []string{LabelSync, LabelTarget, LabelTable, LabelComponent}},
	{MetricDeltaPurgeSeconds, Gauge, "How long the table's last delta purge on the source took, in seconds", []string{LabelSync, LabelTable}},
	{MetricDeltaPurgedTotal, Counter, "Delta rows purged", []string{LabelSync, LabelTable}},
	{MetricTrackLeftoversRemovedTotal, Counter, "Track rows left by versions before 0.7.0 that the background sweep removed", []string{LabelSync, LabelTarget, LabelTable}},
	{MetricCopySkippedTables, Gauge, "Tables marked copied without being copied (skip_initial_copy), by where their check against the source stands (pending, ok, drift, off)", []string{LabelSync, LabelTarget, "check"}},
	{MetricSyncStartFailed, Gauge, "1 while a sync target is not running because its last attempt failed (starting up, or while streaming) and is waiting to be tried again; 0 once it streams", []string{LabelSync, LabelTarget}},
	{MetricSyncStartFailuresTotal, Counter, "Failed attempts to run a sync target, starting up or while streaming (each is retried; the other syncs keep running)", []string{LabelSync, LabelTarget}},
	{MetricSyncTables, Gauge, "Tables the sync target replicates (selected and with a usable key); 0 means nothing is replicated", []string{LabelSync, LabelTarget}},
	{MetricReseedTotal, Counter, "Forced reseeds triggered by retention cap", []string{LabelSync, LabelTarget}},
	{MetricTargetUp, Gauge, "Target reachability (1=up, 0=down)", []string{LabelSync, LabelTarget}},
	{MetricSourceUp, Gauge, "Source reachability (1=up, 0=down)", []string{LabelSync}},
	{MetricSourceDBBytes, Gauge, "Source database size in bytes — the whole database (engines that report it)", []string{LabelSync}},
	{MetricTargetDBBytes, Gauge, "Target database size in bytes — the whole database (engines that report it)", []string{LabelSync, LabelTarget}},
	{MetricSourceReplicatedBytes, Gauge, "Size in bytes of just the replicated tables on the source (heap+indexes+TOAST; engines that report it)", []string{LabelSync}},
	{MetricTargetReplicatedBytes, Gauge, "Size in bytes of just the replicated tables on the target (heap+indexes+TOAST; engines that report it)", []string{LabelSync, LabelTarget}},
	{MetricApplyBatchSeconds, Histogram, "Apply batch duration", []string{LabelSync, LabelTarget}},
	{MetricErrorsTotal, Counter, "Errors by category", []string{LabelSync, "category"}},
	{MetricPhaseInfo, Gauge, "Table phase (initial_copy/streaming) as an info gauge", []string{LabelSync, LabelTable, LabelPhase}},
	{MetricDeleteReconcileLag, Gauge, "Seconds the last completed delete-reconciliation sweep took (staleness of capture-less deletes)", []string{LabelSync, LabelTarget, LabelTable}},
	{MetricDeletesReconciled, Counter, "Keys deleted by the target-vs-source delete sweep", []string{LabelSync, LabelTarget, LabelTable}},
	{MetricInitialCopyETA, Gauge, "Estimated seconds until the initial copy finishes (-1: no progress in the last 10 minutes)", []string{LabelSync}},
	{MetricInitialCopyProgress, Gauge, "Share of the initial copy done, 0..1: size-weighted, counting the link-filling pass; under 1 until every table is done", []string{LabelSync}},
	{MetricLinkFillRowsTotal, Counter, "Rows whose links the link-filling pass has set, for tables in a loop of foreign keys", []string{LabelSync, LabelTable}},
	{MetricLinkFillRowsTarget, Gauge, "Rows the link-filling pass will touch (the table's row estimate)", []string{LabelSync, LabelTable}},
	{MetricTargetTriggers, Gauge, "The target's own triggers kept from firing on replicare's writes, by how: replica_mode (they skip replicare's connections) or switched_off", []string{LabelSync, LabelTarget, LabelHow}},
	{MetricIndexesDeferred, Gauge, "Plain target indexes taken down for the initial copy, to be built again once the sync streams", []string{LabelSync, LabelTarget}},
	{MetricIndexesBuilt, Gauge, "Of those, how many have been built again and are usable", []string{LabelSync, LabelTarget}},
	{MetricIndexBuildInfo, Gauge, "1 for the index being built right now, labelled with its table, name and build phase", []string{LabelSync, LabelTarget, LabelTable, LabelIndex, LabelPhase}},
	{MetricIndexBuildPhaseRatio, Gauge, "How far the index build in flight is through its current phase, 0 to 1 (phases that report no progress read 0)", []string{LabelSync, LabelTarget}},
	{MetricCatchupETA, Gauge, "Estimated seconds until the target's delta backlog is cleared at its current rate of shrinking (0: caught up; -1: not shrinking or target down)", []string{LabelSync, LabelTarget}},
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
	EventCopyRetry         = "copy.transient_retry"
	// Initial-copy stages, logged at INFO so the copy's progress shows in the logs, not
	// only in metrics: a component's copy begins, and a table's link-filling pass (tables
	// in a loop of foreign keys) begins and ends.
	EventCopyComponentStarted = "copy.component_started"
	EventLinkFillStarted      = "copy.link_fill_started"
	EventLinkFillDone         = "copy.link_fill_done"
	// Deferred target indexes: an index taken down before its table's copy (the message
	// carries its full CREATE statement, so it can always be rebuilt by hand), a table
	// whose indexes are left in place and why, the rebuild starting, each index back,
	// a build that failed (retried), and every index back.
	EventIndexDropped = "index.dropped"
	// The target's own triggers: kept from firing on replicare's writes by replica mode,
	// one switched off (the message carries the statement that switches it back on), one
	// switched back on, one that no longer exists when it was to be switched back on.
	EventTriggerReplicaMode  = "trigger.replica_mode"
	EventTriggerDisabled     = "trigger.switched_off"
	EventTriggerRestored     = "trigger.switched_on"
	EventTriggerGone         = "trigger.gone"
	EventIndexKept           = "index.kept"
	EventIndexRebuildStarted = "index.rebuild_started"
	EventIndexBuilt          = "index.built"
	EventIndexBuildFailed    = "index.build_failed"
	EventIndexRebuildDone    = "index.rebuild_done"
	// Leftover track rows (from versions before 0.7.0) removed by the background sweep,
	// and a sweep round that failed (tried again later).
	EventTrackSwept       = "track.leftovers_removed"
	EventTrackSweepFailed = "track.sweep_failed"
	// skip_initial_copy: tables marked copied without being copied, the check against
	// the source starting once the target has caught up, a table that matches, a table
	// that differs (ERROR: the target is missing data or holds data the source doesn't),
	// and a check that could not run (tried again later).
	// Start-up and progress, so the logs alone show what each sync is doing: a sync
	// target's start-up failing (retried, the other syncs carry on), the tables it
	// selected and skipped, each pre-flight finding, capture being installed table by
	// table, and a progress line every minute during the initial copy and streaming.
	EventSyncStartFailed      = "sync.start_failed"
	EventSyncStarting         = "sync.starting"
	EventSyncTables           = "sync.tables_selected"
	EventPreflightFinding     = "preflight.finding"
	EventCaptureInstalling    = "capture.installing"
	EventCopyProgress         = "copy.progress"
	EventStreamProgress       = "stream.progress"
	EventCopySkipped          = "copy.skipped"
	EventCopySkipCheckStarted = "copy.skip_check_started"
	EventCopySkipCheckOK      = "copy.skip_check_ok"
	EventCopySkipCheckDrift   = "copy.skip_check_drift"
	EventCopySkipCheckFailed  = "copy.skip_check_failed"
)
