package daemon

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/observability"
	"github.com/rudimk/replicare/internal/state"
)

// Start-up of each sync target (and cluster edge) is isolated: a failure to build or
// bring one up is logged loudly, counted, and retried with a growing wait, while the
// daemon's other syncs keep running. Before, one sync failing to start stopped every sync
// and exited the daemon, so a single misconfigured or unreachable sync kept restarting
// all the others (and their copies). Only daemon-wide problems (the state store) still
// stop the daemon.

// startRetryMin and startRetryMax bound the wait between start-up attempts.
var (
	startRetryMin = 30 * time.Second
	startRetryMax = 10 * time.Minute
)

// runWithRetry runs one job until ctx ends. run builds and brings the job up, calls
// started once it is streaming, and streams until ctx ends. A run that returns an error
// (it failed to start, or stopped) is retried after a wait that doubles up to
// startRetryMax and resets once the job has started.
func (d *Daemon) runWithRetry(ctx context.Context, job, target string, run func(ctx context.Context, started func()) error) {
	wait := startRetryMin
	for attempt := 1; ; attempt++ {
		started := false
		// A new attempt is under way, so the job is no longer waiting to be retried. Cleared
		// here, not only once it streams: an attempt that gets past a failure spends its
		// whole initial copy before streaming, and must not read as failing all that time.
		d.tel.SetSyncStartFailed(job, target, false)
		err := run(ctx, func() {
			started = true
			d.tel.SetSyncStartFailed(job, target, false)
		})
		if ctx.Err() != nil {
			return
		}
		what := "could not start"
		if started {
			wait, attempt = startRetryMin, 1
			what = "stopped streaming"
		}
		if err == nil {
			err = fmt.Errorf("stopped without an error")
		}
		d.tel.SetSyncStartFailed(job, target, true)
		d.tel.IncSyncStartFailure(job, target)
		e := state.Event{Sync: job, Target: target, Level: "ERROR", Event: observability.EventSyncStartFailed,
			Message: fmt.Sprintf("%s (attempt %d; starting again in %s; the other syncs keep running): %v",
				what, attempt, wait, err)}
		d.tel.LogEvent(ctx, e)
		_ = d.store.RecordEvent(ctx, e)
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		wait = min(wait*2, startRetryMax)
	}
}

// logStage logs one start-up stage of a job.
func (d *Daemon) logStage(ctx context.Context, job, target, msg string) {
	d.tel.LogEvent(ctx, state.Event{Sync: job, Target: target, Level: "INFO", Event: observability.EventSyncStarting, Message: msg})
}

// logPreflight logs every pre-flight finding at its own level, then one line with what
// the job will replicate: the tables selected, those skipped for having no usable key,
// and an ERROR when nothing is left to replicate.
func (d *Daemon) logPreflight(ctx context.Context, job, target string, matched int, sel engine.Selection, r *engine.PreflightReport) {
	for _, f := range r.Findings {
		level := "INFO"
		switch f.Severity {
		case engine.SevWarn:
			level = "WARN"
		case engine.SevBlock:
			level = "ERROR"
		}
		d.tel.LogEvent(ctx, state.Event{Sync: job, Target: target, Table: f.Table, Level: level,
			Event: observability.EventPreflightFinding, Message: fmt.Sprintf("pre-flight %s: %s", f.Category, f.Message)})
	}
	n := len(r.Replicable)
	d.tel.SetSyncTables(job, target, n)
	selection := fmt.Sprintf("include %s, exclude %s", globList(sel.Include), globList(sel.Exclude))
	switch {
	case n == 0 && matched == 0:
		d.tel.LogEvent(ctx, state.Event{Sync: job, Target: target, Level: "ERROR", Event: observability.EventSyncTables,
			Message: fmt.Sprintf("replicates nothing: the selection (%s) matches no table on the source. Check the patterns (schema.table, case-sensitive) and that the source connection points at the right database", selection)})
	case n == 0:
		d.tel.LogEvent(ctx, state.Event{Sync: job, Target: target, Level: "ERROR", Event: observability.EventSyncTables,
			Message: fmt.Sprintf("replicates nothing: the selection (%s) matches %s, but none has a primary key or unique key, which replicare needs to tell rows apart", selection, tablesWord(matched))})
	case len(r.Skipped) > 0:
		d.tel.LogEvent(ctx, state.Event{Sync: job, Target: target, Level: "WARN", Event: observability.EventSyncTables,
			Message: fmt.Sprintf("replicating %s (%s); %s skipped for having no primary key or unique key: %s",
				tablesWord(n), selection, tablesWord(len(r.Skipped)), skippedList(r.Skipped))})
	default:
		d.tel.LogEvent(ctx, state.Event{Sync: job, Target: target, Level: "INFO", Event: observability.EventSyncTables,
			Message: fmt.Sprintf("replicating %s (%s)", tablesWord(n), selection)})
	}
}

// blockedDetail lists every blocking pre-flight finding, one per line, for the error a
// blocked start returns.
func blockedDetail(r *engine.PreflightReport) string {
	var b strings.Builder
	for _, f := range r.Findings {
		if f.Severity != engine.SevBlock {
			continue
		}
		if f.Table != (engine.TableRef{}) {
			fmt.Fprintf(&b, "\n  - %s: %s", f.Table, f.Message)
		} else {
			fmt.Fprintf(&b, "\n  - %s", f.Message)
		}
	}
	return b.String()
}

func globList(g []string) string {
	if len(g) == 0 {
		return "(none)"
	}
	return "[" + strings.Join(g, ", ") + "]"
}

// skippedList names the skipped tables, up to a limit.
func skippedList(sk []engine.SkippedTable) string {
	const limit = 50
	names := make([]string, 0, min(len(sk), limit))
	for i, s := range sk {
		if i == limit {
			break
		}
		names = append(names, s.Table.String())
	}
	out := strings.Join(names, ", ")
	if len(sk) > limit {
		out += fmt.Sprintf(" and %d more", len(sk)-limit)
	}
	return out
}

func tablesWord(n int) string {
	if n == 1 {
		return "1 table"
	}
	return fmt.Sprintf("%d tables", n)
}

// since formats the time a stage took, for the start-up lines.
func since(t time.Time) string { return time.Since(t).Round(100 * time.Millisecond).String() }
