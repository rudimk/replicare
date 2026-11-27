package pipeline

import (
	"context"
	"strings"
	"testing"

	"github.com/rudimk/replicare/internal/copy"
	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/state"
)

// fakeGuard is a target with triggers, for the decisions in guardTargetTriggers.
type fakeGuard struct {
	engine.Sink
	triggers     []engine.TargetTrigger
	replicaOK    bool
	replicaOn    bool
	disabled     []string
	restored     []string
	missingOnSet map[string]bool
}

func (g *fakeGuard) TargetTriggers(context.Context, []engine.TableRef) ([]engine.TargetTrigger, error) {
	return g.triggers, nil
}
func (g *fakeGuard) ReplicaModeAllowed(context.Context) (bool, error) { return g.replicaOK, nil }
func (g *fakeGuard) UseReplicaMode(_ context.Context, on bool) error {
	g.replicaOn = on
	return nil
}
func (g *fakeGuard) DisableTrigger(_ context.Context, t engine.TargetTrigger) error {
	g.disabled = append(g.disabled, t.Name)
	return nil
}
func (g *fakeGuard) RestoreTrigger(_ context.Context, t engine.TargetTrigger) (bool, error) {
	g.restored = append(g.restored, t.Name+":"+t.Mode)
	return !g.missingOnSet[t.Name], nil
}
func (g *fakeGuard) RestoreStatement(t engine.TargetTrigger) string { return "restore " + t.Name }

type fakeTriggerStore struct {
	state.StateStore
	saved  []state.SavedTrigger
	events []state.Event
}

func (s *fakeTriggerStore) SaveTriggers(_ context.Context, _ string, target engine.TargetID, ts []engine.TargetTrigger) error {
	for _, t := range ts {
		s.saved = append(s.saved, state.SavedTrigger{Target: target, Trigger: t})
	}
	return nil
}
func (s *fakeTriggerStore) ListTriggers(context.Context, string) ([]state.SavedTrigger, error) {
	return append([]state.SavedTrigger(nil), s.saved...), nil
}
func (s *fakeTriggerStore) ForgetTrigger(_ context.Context, _ string, target engine.TargetID, tbl engine.TableRef, name string) error {
	kept := s.saved[:0]
	for _, st := range s.saved {
		if st.Target != target || st.Trigger.Table != tbl || st.Trigger.Name != name {
			kept = append(kept, st)
		}
	}
	s.saved = kept
	return nil
}
func (s *fakeTriggerStore) RecordEvent(_ context.Context, e state.Event) error {
	s.events = append(s.events, e)
	return nil
}

var ttTable = engine.TableRef{Schema: "public", Name: "items"}

func trig(name, mode string, owned bool) engine.TargetTrigger {
	return engine.TargetTrigger{Table: ttTable, Name: name, Mode: mode, Owned: owned, Replicare: strings.HasPrefix(name, "replicare_")}
}

func triggerSyncer(main *fakeGuard, store *fakeTriggerStore, workers ...*fakeGuard) *Syncer {
	s := &Syncer{Name: "s", Target: "dst", Sink: main, Store: store, GuardTriggers: true, Replicable: []engine.TableRef{ttTable}}
	for _, w := range workers {
		s.Workers = append(s.Workers, copy.Worker{Sink: w})
	}
	return s
}

func TestGuardTargetTriggers(t *testing.T) {
	ordinary, always, replicaOnly := trig("versions_append", "O", true), trig("append_only", "A", true), trig("on_replica", "R", true)
	off := trig("already_off", "D", true)

	t.Run("replica mode covers the ordinary ones; the rest are switched off", func(t *testing.T) {
		g, w := &fakeGuard{triggers: []engine.TargetTrigger{ordinary, always, replicaOnly, off}, replicaOK: true}, &fakeGuard{}
		st := &fakeTriggerStore{}
		if err := triggerSyncer(g, st, w).guardTargetTriggers(context.Background()); err != nil {
			t.Fatal(err)
		}
		if !g.replicaOn || !w.replicaOn {
			t.Errorf("replica mode on main=%v worker=%v, want both", g.replicaOn, w.replicaOn)
		}
		if got := strings.Join(g.disabled, ","); got != "append_only,on_replica" {
			t.Errorf("switched off %q, want the always and replica-only ones", got)
		}
		if len(st.saved) != 2 || st.saved[0].Trigger.Mode != "A" || st.saved[1].Trigger.Mode != "R" {
			t.Errorf("saved %+v, want each with its own mode", st.saved)
		}
	})

	t.Run("without replica mode the ordinary ones are switched off too", func(t *testing.T) {
		g := &fakeGuard{triggers: []engine.TargetTrigger{ordinary, always, replicaOnly}}
		if err := triggerSyncer(g, &fakeTriggerStore{}).guardTargetTriggers(context.Background()); err != nil {
			t.Fatal(err)
		}
		if g.replicaOn {
			t.Error("replica mode used without permission")
		}
		if got := strings.Join(g.disabled, ","); got != "versions_append,append_only" {
			t.Errorf("switched off %q, want the ordinary and always ones (replica-only ones don't fire)", got)
		}
	})

	t.Run("replicare's own capture on the target rules out replica mode", func(t *testing.T) {
		g := &fakeGuard{triggers: []engine.TargetTrigger{ordinary, trig("replicare_trg_42", "O", false)}, replicaOK: true}
		if err := triggerSyncer(g, &fakeTriggerStore{}).guardTargetTriggers(context.Background()); err != nil {
			t.Fatal(err)
		}
		if g.replicaOn {
			t.Error("replica mode used on a database whose own capture must see replicare's writes")
		}
		if got := strings.Join(g.disabled, ","); got != "versions_append" {
			t.Errorf("switched off %q, want only the app's trigger, never replicare's own", got)
		}
	})

	t.Run("a trigger this role may not switch off refuses the start, touching nothing", func(t *testing.T) {
		g := &fakeGuard{triggers: []engine.TargetTrigger{always, trig("not_mine", "A", false)}, replicaOK: true}
		err := triggerSyncer(g, &fakeTriggerStore{}).guardTargetTriggers(context.Background())
		if err == nil || !strings.Contains(err.Error(), "not_mine") || !strings.Contains(err.Error(), "does not own") {
			t.Fatalf("err = %v, want a refusal naming the trigger", err)
		}
		if len(g.disabled) != 0 {
			t.Errorf("switched off %v before refusing, want nothing", g.disabled)
		}
	})

	t.Run("no triggers: nothing changes", func(t *testing.T) {
		g := &fakeGuard{replicaOK: true}
		if err := triggerSyncer(g, &fakeTriggerStore{}).guardTargetTriggers(context.Background()); err != nil {
			t.Fatal(err)
		}
		if g.replicaOn || len(g.disabled) != 0 {
			t.Errorf("replica=%v disabled=%v on a target without triggers, want nothing", g.replicaOn, g.disabled)
		}
	})

	t.Run("turned off: what an earlier run switched off comes back on", func(t *testing.T) {
		st := &fakeTriggerStore{saved: []state.SavedTrigger{
			{Target: "dst", Trigger: always}, {Target: "dst", Trigger: trig("dropped_since", "O", true)}, {Target: "other", Trigger: ordinary}}}
		g := &fakeGuard{missingOnSet: map[string]bool{"dropped_since": true}}
		s := triggerSyncer(g, st)
		s.GuardTriggers = false
		if err := s.guardTargetTriggers(context.Background()); err != nil {
			t.Fatal(err)
		}
		if got := strings.Join(g.restored, ","); got != "append_only:A,dropped_since:O" {
			t.Errorf("restored %q, want this target's, each in its own mode", got)
		}
		if len(st.saved) != 1 || st.saved[0].Target != "other" {
			t.Errorf("left saved %+v, want only the other target's", st.saved)
		}
		var gone bool
		for _, e := range st.events {
			gone = gone || (e.Level == "WARN" && strings.Contains(e.Message, "dropped_since"))
		}
		if !gone {
			t.Error("no WARN for a trigger that no longer exists")
		}
	})
}

func TestTargetTriggerFires(t *testing.T) {
	for _, c := range []struct {
		mode             string
		ordinary, replic bool
	}{{"O", true, false}, {"A", true, true}, {"R", false, true}, {"D", false, false}} {
		tr := engine.TargetTrigger{Mode: c.mode}
		if tr.Fires(false) != c.ordinary || tr.Fires(true) != c.replic {
			t.Errorf("mode %s: fires ordinary=%v replica=%v, want %v %v", c.mode, tr.Fires(false), tr.Fires(true), c.ordinary, c.replic)
		}
	}
}
