package pipeline

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/observability"
	"github.com/rudimk/replicare/internal/state"
)

// Skipping the initial copy (the sync's skip_initial_copy). For a target that already
// holds the source's data, say the old source after a cutover, now the target of the
// sync running the other way, the copy would rewrite every row for nothing. With the
// option on, bring-up marks each table that has not started copying as copied, and the
// sync streams the changes queued since capture went on. That is only right when the
// target matched the source at the moment capture was installed, so the mark is kept
// as "skipped" until a check confirms it: once the target has caught up, every skipped
// table is compared against the source, row by row, and any difference is reported
// loudly (an ERROR event and log, a metric, and `status`). A table that differs keeps
// its mark, so the next start checks it again; a reseed copies it for real.

// SkipCheckResult is the outcome of comparing one skipped table against the source.
type SkipCheckResult struct {
	Table engine.TableRef
	// Match: the target holds exactly the source's rows.
	Match bool
	// Detail describes a difference, or why the table could not be compared.
	Detail string
	// Failed: the comparison could not run for this table (not a difference); it is
	// tried again later.
	Failed bool
}

// skipCheckRetry is the wait before trying again a check that could not run.
var skipCheckRetry = 5 * time.Minute

// caughtUpAge is the age under which the oldest queued change for every table counts
// as caught up: the same "nothing has waited over a minute" the catch-up ETA uses.
const caughtUpAge = time.Minute

// skipCopies marks every table that has not started copying as copied without copying
// it (skip_initial_copy). A table part-way through a copy finishes it, and one flagged
// for a reseed is copied by the reseed. Runs after capture is installed, before any
// copy. A progress read or write that fails stops bring-up: guessing either way could
// copy over a live target for nothing or leave a table uncopied.
func (s *Syncer) skipCopies(ctx context.Context) error {
	if !s.SkipInitialCopy {
		return nil
	}
	var skipped, started []engine.TableRef
	for _, comp := range s.Components {
		for _, t := range comp.Order {
			prog, err := s.Store.LoadCopyProgress(ctx, s.Name, s.Target, t)
			if err != nil {
				return fmt.Errorf("syncer %s: skip copy: load progress %s: %w", s.Name, t, err)
			}
			if prog.Done {
				continue
			}
			if len(prog.Watermark) > 0 || len(prog.Completed) > 0 {
				started = append(started, t)
				continue
			}
			cur, err := s.Store.LoadCursor(ctx, s.Name, s.Target, t)
			if err != nil {
				return fmt.Errorf("syncer %s: skip copy: load cursor %s: %w", s.Name, t, err)
			}
			if cur.NeedsReseed {
				continue
			}
			if err := s.Store.SaveCopyProgress(ctx, s.Name, state.CopyProgress{
				Target: s.Target, Table: t, Done: true, Skipped: true,
			}); err != nil {
				return fmt.Errorf("syncer %s: skip copy: mark %s: %w", s.Name, t, err)
			}
			skipped = append(skipped, t)
		}
	}
	if len(started) > 0 {
		s.recordEvent(ctx, state.Event{Sync: s.Name, Target: string(s.Target), Level: "INFO",
			Event: observability.EventCopySkipped,
			Message: fmt.Sprintf("skip_initial_copy: %s had started copying and will finish the copy: %s",
				tablesWord(len(started)), joinRefs(started))})
	}
	if len(skipped) == 0 {
		return nil
	}
	level, check := "INFO", "each is checked against the source once the target has caught up"
	if s.SkipCheck == nil {
		level, check = "WARN", "the check against the source is turned off (skip_initial_copy.verify: false), so nothing confirms the target holds the same data"
	}
	s.recordEvent(ctx, state.Event{Sync: s.Name, Target: string(s.Target), Level: level,
		Event: observability.EventCopySkipped,
		Message: fmt.Sprintf("skip_initial_copy: marked %s copied without copying; %s",
			tablesWord(len(skipped)), check)})
	return nil
}

// startSkipCheck runs the check of this target's skipped tables in the background once
// the target has caught up, and returns a func that waits for it to stop. A no-op when
// the check is off or nothing is waiting for it.
func (s *Syncer) startSkipCheck(ctx context.Context) (wait func()) {
	if s.Store == nil {
		return func() {}
	}
	pending, err := s.skippedTables(ctx)
	if err != nil {
		s.log(ctx, "read which tables skipped their copy; their check does not run until the next start", err)
		return func() {}
	}
	if len(pending) == 0 {
		return func() {}
	}
	if s.SkipCheck == nil {
		s.Tel.SetCopySkipped(s.Name, s.Target, 0, 0, 0, len(pending))
		return func() {}
	}
	s.Tel.SetCopySkipped(s.Name, s.Target, len(pending), 0, 0, 0)
	s.caughtUp = make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		select {
		case <-ctx.Done():
			return
		case <-s.caughtUp:
		}
		s.runSkipCheck(ctx, pending)
	}()
	return func() { <-done }
}

// noteCaughtUp signals the skipped-copy check that every table's oldest queued change
// is under a minute old. Safe to call on every pass; only the first call counts.
func (s *Syncer) noteCaughtUp() {
	if s.caughtUp == nil {
		return
	}
	s.caughtUpOnce.Do(func() { close(s.caughtUp) })
}

// runSkipCheck compares the pending tables against the source, retrying those whose
// comparison could not run, until every one has matched or differed, or ctx ends.
func (s *Syncer) runSkipCheck(ctx context.Context, pending []engine.TableRef) {
	var ok, drift int
	for len(pending) > 0 {
		s.recordEvent(ctx, state.Event{Sync: s.Name, Target: string(s.Target), Level: "INFO",
			Event:   observability.EventCopySkipCheckStarted,
			Message: fmt.Sprintf("checking %s that skipped the copy against the source", tablesWord(len(pending)))})
		results, err := s.SkipCheck(ctx, pending)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			s.recordEvent(ctx, state.Event{Sync: s.Name, Target: string(s.Target), Level: "WARN",
				Event:   observability.EventCopySkipCheckFailed,
				Message: fmt.Sprintf("checking the tables that skipped their copy (tried again in %s): %v", skipCheckRetry, err)})
			if !sleepCtx(ctx, skipCheckRetry) {
				return
			}
			continue
		}
		byTable := make(map[engine.TableRef]SkipCheckResult, len(results))
		for _, r := range results {
			byTable[r.Table] = r
		}
		var retry []engine.TableRef
		for _, t := range pending {
			r, found := byTable[t]
			switch {
			case !found || r.Failed:
				detail := "no result"
				if found {
					detail = r.Detail
				}
				retry = append(retry, t)
				s.recordEvent(ctx, state.Event{Sync: s.Name, Target: string(s.Target), Table: t, Level: "WARN",
					Event:   observability.EventCopySkipCheckFailed,
					Message: fmt.Sprintf("checking %s, which skipped its copy (tried again in %s): %s", t, skipCheckRetry, detail)})
			case r.Match:
				if err := s.Store.SaveCopyProgress(ctx, s.Name, state.CopyProgress{Target: s.Target, Table: t, Done: true}); err != nil {
					retry = append(retry, t)
					s.log(ctx, fmt.Sprintf("record that %s matches the source", t), err)
					continue
				}
				ok++
				s.recordEvent(ctx, state.Event{Sync: s.Name, Target: string(s.Target), Table: t, Level: "INFO",
					Event:   observability.EventCopySkipCheckOK,
					Message: fmt.Sprintf("%s skipped its copy and matches the source", t)})
			default:
				drift++
				s.recordEvent(ctx, state.Event{Sync: s.Name, Target: string(s.Target), Table: t, Level: "ERROR",
					Event: observability.EventCopySkipCheckDrift,
					Message: fmt.Sprintf("%s skipped its copy but does not match the source: %s. The target did not hold the source's data when capture was installed; "+
						"changes still stream, but these rows stay wrong until the table is copied (`replicare reseed`)", t, r.Detail)})
			}
		}
		s.Tel.SetCopySkipped(s.Name, s.Target, len(retry), ok, drift, 0)
		pending = retry
		if len(pending) > 0 && !sleepCtx(ctx, skipCheckRetry) {
			return
		}
	}
}

// skippedTables lists this target's tables marked copied without being copied and not
// yet confirmed by a check, in component order.
func (s *Syncer) skippedTables(ctx context.Context) ([]engine.TableRef, error) {
	all, err := s.Store.ListCopyProgress(ctx, s.Name)
	if err != nil {
		return nil, err
	}
	marked := map[engine.TableRef]bool{}
	for _, p := range all {
		if p.Target == s.Target && p.Done && p.Skipped {
			marked[p.Table] = true
		}
	}
	var out []engine.TableRef
	for _, comp := range s.Components {
		for _, t := range comp.Order {
			if marked[t] {
				out = append(out, t)
			}
		}
	}
	return out, nil
}

// allCaughtUp reports whether every table's oldest queued change is under a minute
// old, from the backlog read on a healthy pass.
func allCaughtUp(backlogs map[engine.TableRef]engine.DeltaBacklog, tables []engine.TableRef) bool {
	for _, t := range tables {
		bl, ok := backlogs[t]
		if !ok || bl.OldestAge >= caughtUpAge {
			return false
		}
	}
	return true
}

func joinRefs(ts []engine.TableRef) string {
	names := make([]string, len(ts))
	for i, t := range ts {
		names[i] = t.String()
	}
	return strings.Join(names, ", ")
}

func tablesWord(n int) string {
	if n == 1 {
		return "1 table"
	}
	return fmt.Sprintf("%d tables", n)
}
