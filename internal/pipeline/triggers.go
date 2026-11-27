package pipeline

import (
	"context"
	"fmt"
	"strings"

	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/observability"
	"github.com/rudimk/replicare/internal/state"
)

// The target's own triggers (CLAUDE.md §4.2 "the target's own triggers"). A trigger on a
// target table fires on replicare's writes like on anyone else's, and on a replica that
// is wrong: a trigger that writes a version or audit row writes a second copy of a row
// the source's own trigger already wrote (and replicare copies), a trigger that stamps a
// column changes the value replicare moved, and an append-only guard refuses the write.
// So before anything is written to the target:
//
//   - triggers set to fire in ordinary sessions are skipped by running replicare's own
//     target connections in replica mode, when this role may and the target holds none
//     of replicare's own capture triggers on these tables (the target is then also a
//     source, and its capture must see replicare's writes). They stay on for everyone
//     else; nothing is changed, so nothing needs undoing;
//   - every trigger that would still fire (set to fire always, or replica-only ones in
//     replica mode, or all of them when replica mode is not available) is switched off,
//     its mode saved first, and switched back on when the sync is paused, when the
//     setting is turned off, or by `replicare triggers restore`;
//   - a trigger this role may not switch off refuses the start, listing what to do.

// guardTargetTriggers runs at the start of bring-up.
func (s *Syncer) guardTargetTriggers(ctx context.Context) error {
	g, ok := s.Sink.(engine.TriggerGuard)
	if !ok {
		return nil
	}
	ts, ok := s.Store.(state.TriggerStore)
	if !ok {
		return nil
	}
	if !s.GuardTriggers {
		// Turned off (or never on): put back anything an earlier run switched off.
		n, err := RestoreTargetTriggers(ctx, ts, g, s.Name, s.Target, s.recordEvent)
		if err != nil {
			return fmt.Errorf("switch target triggers back on: %w", err)
		}
		if n > 0 {
			s.Tel.SetTargetTriggers(s.Name, s.Target, 0, 0)
		}
		return nil
	}

	all, err := g.TargetTriggers(ctx, s.Replicable)
	if err != nil {
		return err
	}
	var app []engine.TargetTrigger
	capture := false
	for _, t := range all {
		if t.Replicare {
			capture = true
			continue
		}
		app = append(app, t)
	}
	ordinary := 0
	for _, t := range app {
		if t.Mode == "O" {
			ordinary++
		}
	}
	replica := false
	if ordinary > 0 && !capture {
		if replica, err = g.ReplicaModeAllowed(ctx); err != nil {
			return err
		}
	}
	var off, notOwned []engine.TargetTrigger
	for _, t := range app {
		if !t.Fires(replica) {
			continue
		}
		if !t.Owned {
			notOwned = append(notOwned, t)
			continue
		}
		off = append(off, t)
	}
	if len(notOwned) > 0 {
		return fmt.Errorf("%s on the target would fire on replicare's writes and make the target differ "+
			"from the source, and this role does not own %s, so it may not switch them off. Connect as the "+
			"owner of these tables%s, or switch them off on the target yourself: %s",
			triggersWord(len(notOwned)), theseTables(notOwned), replicaHint(ordinary, capture, replica), statementList(notOwned, g, false))
	}

	if replica {
		sinks := []engine.Sink{s.Sink}
		for _, w := range s.Workers {
			sinks = append(sinks, w.Sink)
		}
		for _, sk := range sinks {
			sg, ok := sk.(engine.TriggerGuard)
			if !ok {
				return fmt.Errorf("copy target does not support replica mode")
			}
			if err := sg.UseReplicaMode(ctx, true); err != nil {
				return err
			}
		}
		s.recordEvent(ctx, state.Event{Sync: s.Name, Target: string(s.Target), Level: "INFO",
			Event: observability.EventTriggerReplicaMode,
			Message: fmt.Sprintf("%s on the target skip replicare's writes: its connections run in replica mode. "+
				"They stay on for everyone else, and nothing on the target was changed", triggersWord(ordinary))})
	} else if ordinary > 0 {
		why := "this role may not use replica mode on the target"
		if capture {
			why = "the target also holds replicare's own change capture on these tables, which must see replicare's writes"
		}
		s.recordEvent(ctx, state.Event{Sync: s.Name, Target: string(s.Target), Level: "INFO",
			Event:   observability.EventTriggerReplicaMode,
			Message: fmt.Sprintf("replica mode not used (%s), so the target's own triggers are switched off instead", why)})
	}

	if len(off) > 0 {
		// Saved before any is switched off, so a crash in between never loses a mode.
		if err := ts.SaveTriggers(ctx, s.Name, s.Target, off); err != nil {
			return fmt.Errorf("save target triggers: %w", err)
		}
		for _, t := range off {
			if err := g.DisableTrigger(ctx, t); err != nil {
				return err
			}
			s.recordEvent(ctx, state.Event{Sync: s.Name, Target: string(s.Target), Table: t.Table, Level: "INFO",
				Event: observability.EventTriggerDisabled,
				Message: fmt.Sprintf("switched off the target's trigger %s, which would fire on replicare's writes; "+
					"it is switched back on when the sync is paused, or by `replicare triggers restore`. By hand: %s",
					t.Name, g.RestoreStatement(t))})
		}
	}
	saved, err := ts.ListTriggers(ctx, s.Name)
	if err != nil {
		return fmt.Errorf("list switched-off target triggers: %w", err)
	}
	switchedOff := 0
	for _, st := range saved {
		if st.Target == s.Target {
			switchedOff++
		}
	}
	skipped := 0
	if replica {
		skipped = ordinary
	}
	s.Tel.SetTargetTriggers(s.Name, s.Target, skipped, switchedOff)
	return nil
}

// RestoreTargetTriggers switches back on every trigger saved for (sync, target), in the
// mode it had before replicare switched it off, and forgets each once it is back. A
// trigger that no longer exists is forgotten with a WARN. It returns how many it handled.
func RestoreTargetTriggers(ctx context.Context, ts state.TriggerStore, g engine.TriggerGuard, sync string,
	target engine.TargetID, record func(context.Context, state.Event)) (int, error) {
	saved, err := ts.ListTriggers(ctx, sync)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, st := range saved {
		if st.Target != target {
			continue
		}
		found, err := g.RestoreTrigger(ctx, st.Trigger)
		if err != nil {
			return n, err
		}
		if err := ts.ForgetTrigger(ctx, sync, target, st.Trigger.Table, st.Trigger.Name); err != nil {
			return n, err
		}
		n++
		e := state.Event{Sync: sync, Target: string(target), Table: st.Trigger.Table, Level: "INFO",
			Event: observability.EventTriggerRestored, Message: fmt.Sprintf("switched the target's trigger %s back on (%s)", st.Trigger.Name, g.RestoreStatement(st.Trigger))}
		if !found {
			e.Level, e.Event = "WARN", observability.EventTriggerGone
			e.Message = fmt.Sprintf("the target's trigger %s, switched off earlier, no longer exists; nothing to switch back on", st.Trigger.Name)
		}
		if record != nil {
			record(ctx, e)
		}
	}
	return n, nil
}

func triggersWord(n int) string {
	if n == 1 {
		return "1 of the app's own triggers"
	}
	return fmt.Sprintf("%d of the app's own triggers", n)
}

func theseTables(ts []engine.TargetTrigger) string {
	seen := map[engine.TableRef]bool{}
	var names []string
	for _, t := range ts {
		if !seen[t.Table] {
			seen[t.Table] = true
			names = append(names, t.Table.String())
		}
	}
	if len(names) > 10 {
		return strings.Join(names[:10], ", ") + fmt.Sprintf(" and %d more tables", len(names)-10)
	}
	return strings.Join(names, ", ")
}

func replicaHint(ordinary int, capture, replica bool) string {
	if ordinary == 0 || capture || replica {
		return ""
	}
	return " (or give this role the right to use replica mode, which covers the ordinary ones without switching anything off)"
}

func statementList(ts []engine.TargetTrigger, g engine.TriggerGuard, restore bool) string {
	var b strings.Builder
	for i, t := range ts {
		if i == 20 {
			fmt.Fprintf(&b, "\n  … and %d more", len(ts)-20)
			break
		}
		if restore {
			fmt.Fprintf(&b, "\n  %s;", g.RestoreStatement(t))
		} else {
			off := t
			off.Mode = "D"
			fmt.Fprintf(&b, "\n  %s;", g.RestoreStatement(off))
		}
	}
	return b.String()
}
