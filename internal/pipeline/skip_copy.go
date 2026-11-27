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

// SkipCheckResult is the outcome of comparing one table against the source.
type SkipCheckResult struct {
	Table engine.TableRef
	// Match: the target holds exactly the source's rows.
	Match bool
	// Detail describes a difference, or why the table could not be compared.
	Detail string
	// Failed: the comparison could not run for this table (not a difference); it is
	// tried again later.
	Failed bool
	// Keys are the keys that still differ (in the engine's own form), for repairing them;
	// Differing is how many rows were found to differ, and Partial says more differ than
	// Keys holds (they are found on the next round).
	Keys      []engine.KeyValues
	Differing int
	Partial   bool
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

// checkKind says why a table is being checked: it skipped its copy, or it is waiting for
// an automatic check (it may differ from the source for another reason).
type checkKind int

const (
	kindSkipped checkKind = 1 << iota
	kindCheck
)

// repairWait is how long the check waits, after queueing the rows that differ, before it
// compares the table again; maxRepairRounds is how many rounds in a row may fail to
// shrink the difference before the check gives up and reports the table loudly.
var (
	repairWait      = time.Minute
	maxRepairRounds = 3
)

// startSkipCheck runs the check of this target's tables that skipped their copy, and of
// those waiting for an automatic check, in the background once the target has caught
// up, and returns a func that waits for it to stop. A no-op when nothing is waiting.
func (s *Syncer) startSkipCheck(ctx context.Context) (wait func()) {
	if s.Store == nil {
		return func() {}
	}
	skipped, err := s.skippedTables(ctx)
	if err != nil {
		s.log(ctx, "read which tables skipped their copy; their check does not run until the next start", err)
		return func() {}
	}
	checks, err := s.checkTables(ctx)
	if err != nil {
		s.log(ctx, "read which tables wait for a check; it does not run until the next start", err)
		return func() {}
	}
	kinds := map[engine.TableRef]checkKind{}
	if len(skipped) > 0 && !s.VerifySkipped {
		s.Tel.SetCopySkipped(s.Name, s.Target, 0, 0, 0, len(skipped))
		skipped = nil
	}
	for _, t := range skipped {
		kinds[t] |= kindSkipped
	}
	for _, t := range checks {
		kinds[t] |= kindCheck
	}
	var pending []engine.TableRef
	for _, comp := range s.Components {
		for _, t := range comp.Order {
			if kinds[t] != 0 {
				pending = append(pending, t)
			}
		}
	}
	if len(pending) == 0 || s.SkipCheck == nil {
		return func() {}
	}
	s.Tel.SetCopySkipped(s.Name, s.Target, len(skipped), 0, 0, 0)
	s.Tel.SetTablesToCheck(s.Name, s.Target, len(checks), 0, 0, 0)
	s.caughtUp = make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		select {
		case <-ctx.Done():
			return
		case <-s.caughtUp:
		}
		s.runSkipCheck(ctx, pending, kinds)
	}()
	return func() { <-done }
}

// noteCaughtUp signals the check that every table's oldest queued change is under a
// minute old. Safe to call on every pass; only the first call counts.
func (s *Syncer) noteCaughtUp() {
	if s.caughtUp == nil {
		return
	}
	s.caughtUpOnce.Do(func() { close(s.caughtUp) })
}

// checkEvent picks the event name for a table by why it is checked.
func checkEvent(k checkKind, skipped, check string) string {
	if k&kindSkipped != 0 {
		return skipped
	}
	return check
}

// runSkipCheck compares the pending tables against the source until every one matches,
// or ctx ends. A table that differs is repaired: the keys that differ are queued on the
// source as changes, so streaming rewrites each row from the source (or deletes it), and
// the table is compared again after repairWait. One whose difference stops shrinking
// for maxRepairRounds rounds, or that cannot be repaired here, is reported at ERROR and
// left marked, so the next start tries again. A comparison that could not run is tried
// again after skipCheckRetry.
func (s *Syncer) runSkipCheck(ctx context.Context, pending []engine.TableRef, kinds map[engine.TableRef]checkKind) {
	requeuer, _ := s.Source.(engine.KeyRequeuer)
	cs, _ := s.Store.(state.CheckStore)
	lastDiff := map[engine.TableRef]int{}
	stuck := map[engine.TableRef]int{}
	standing := map[engine.TableRef]string{} // ok, drift, repairing; pending otherwise
	publish := func() {
		var sp, sok, sdrift, cp, crep, cok, cdrift int
		for t, k := range kinds {
			st := standing[t]
			if k&kindSkipped != 0 {
				switch st {
				case "ok":
					sok++
				case "drift":
					sdrift++
				default:
					sp++
				}
			}
			if k&kindCheck != 0 {
				switch st {
				case "ok":
					cok++
				case "drift":
					cdrift++
				case "repairing":
					crep++
				default:
					cp++
				}
			}
		}
		s.Tel.SetCopySkipped(s.Name, s.Target, sp, sok, sdrift, 0)
		s.Tel.SetTablesToCheck(s.Name, s.Target, cp, crep, cok, cdrift)
	}
	for len(pending) > 0 {
		s.recordEvent(ctx, state0(s, "INFO", engine.TableRef{}, checkEvent(kinds[pending[0]], observability.EventCopySkipCheckStarted, observability.EventCheckStarted),
			fmt.Sprintf("checking %s against the source", tablesWord(len(pending)))))
		results, err := s.SkipCheck(ctx, pending)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			s.recordEvent(ctx, state0(s, "WARN", engine.TableRef{}, observability.EventCheckFailed,
				fmt.Sprintf("checking tables against the source (tried again in %s): %v", skipCheckRetry, err)))
			if !sleepCtx(ctx, skipCheckRetry) {
				return
			}
			continue
		}
		byTable := make(map[engine.TableRef]SkipCheckResult, len(results))
		for _, r := range results {
			byTable[r.Table] = r
		}
		var retry, repairing []engine.TableRef
		for _, t := range pending {
			k := kinds[t]
			r, found := byTable[t]
			switch {
			case !found || r.Failed:
				detail := "no result"
				if found {
					detail = r.Detail
				}
				retry = append(retry, t)
				s.recordEvent(ctx, state0(s, "WARN", t, checkEvent(k, observability.EventCopySkipCheckFailed, observability.EventCheckFailed),
					fmt.Sprintf("checking %s against the source (tried again in %s): %s", t, skipCheckRetry, detail)))
			case r.Match:
				if err := s.clearChecked(ctx, t, k, cs); err != nil {
					retry = append(retry, t)
					s.log(ctx, fmt.Sprintf("record that %s matches the source", t), err)
					continue
				}
				standing[t] = "ok"
				msg := fmt.Sprintf("%s matches the source", t)
				if k&kindSkipped != 0 {
					msg = fmt.Sprintf("%s skipped its copy and matches the source", t)
				}
				s.recordEvent(ctx, state0(s, "INFO", t, checkEvent(k, observability.EventCopySkipCheckOK, observability.EventCheckOK), msg))
			default:
				if lastDiff[t] > 0 && r.Differing >= lastDiff[t] {
					stuck[t]++
				} else {
					stuck[t] = 0
				}
				lastDiff[t] = r.Differing
				if requeuer == nil || len(r.Keys) == 0 || stuck[t] >= maxRepairRounds {
					standing[t] = "drift"
					why := "these rows could not be repaired here"
					if stuck[t] >= maxRepairRounds {
						why = fmt.Sprintf("sending these rows again did not fix them in %d rounds (check that the target accepts them: an error in the streaming logs says why)", maxRepairRounds)
					}
					s.recordEvent(ctx, state0(s, "ERROR", t, checkEvent(k, observability.EventCopySkipCheckDrift, observability.EventCheckDrift),
						fmt.Sprintf("%s does not match the source: %s. Changes still stream, but %s; it is checked again on the next start, or copy it again with `replicare reseed`", t, r.Detail, why)))
					continue
				}
				if err := requeuer.RequeueKeys(ctx, t, r.Keys); err != nil {
					retry = append(retry, t)
					s.recordEvent(ctx, state0(s, "WARN", t, checkEvent(k, observability.EventCopySkipCheckFailed, observability.EventCheckFailed),
						fmt.Sprintf("queueing the rows of %s that differ from the source (tried again in %s): %v", t, skipCheckRetry, err)))
					continue
				}
				standing[t] = "repairing"
				more := ""
				if r.Partial {
					more = "; more differ than one round lists, and are found on the next"
				}
				s.recordEvent(ctx, state0(s, "WARN", t, checkEvent(k, observability.EventCopySkipCheckRepairing, observability.EventCheckRepairing),
					fmt.Sprintf("%s differs from the source: %s. Queued those %d rows to be written again from the source%s; checked again in %s",
						t, r.Detail, len(r.Keys), more, s.repairWait())))
				repairing = append(repairing, t)
			}
		}
		publish()
		pending = append(repairing, retry...)
		if len(pending) == 0 {
			return
		}
		wait := skipCheckRetry
		if len(repairing) > 0 {
			wait = s.repairWait()
		}
		if !sleepCtx(ctx, wait) {
			return
		}
	}
}

func (s *Syncer) repairWait() time.Duration {
	if s.CheckRepairWait > 0 {
		return s.CheckRepairWait
	}
	return repairWait
}

// state0 builds one of this target's events.
func state0(s *Syncer, level string, t engine.TableRef, event, msg string) state.Event {
	return state.Event{Sync: s.Name, Target: string(s.Target), Table: t, Level: level, Event: event, Message: msg}
}

// clearChecked records that a table matches the source: it no longer counts as
// skipped, and its pending check is done.
func (s *Syncer) clearChecked(ctx context.Context, t engine.TableRef, k checkKind, cs state.CheckStore) error {
	if k&kindSkipped != 0 {
		if err := s.Store.SaveCopyProgress(ctx, s.Name, state.CopyProgress{Target: s.Target, Table: t, Done: true}); err != nil {
			return err
		}
	}
	if k&kindCheck != 0 && cs != nil {
		return cs.ClearCheck(ctx, s.Name, s.Target, t)
	}
	return nil
}

// checkTables lists this target's tables waiting for an automatic check.
func (s *Syncer) checkTables(ctx context.Context) ([]engine.TableRef, error) {
	cs, ok := s.Store.(state.CheckStore)
	if !ok {
		return nil, nil
	}
	all, err := cs.ListChecks(ctx, s.Name)
	if err != nil {
		return nil, err
	}
	var out []engine.TableRef
	for _, c := range all {
		if c.Target == s.Target {
			out = append(out, c.Table)
		}
	}
	return out, nil
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
