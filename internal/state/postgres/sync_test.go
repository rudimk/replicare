package statepg

import (
	"context"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/state"
)

func TestSyncCRUD(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s := openTestStore(t, ctx)

	def := state.SyncDef{
		Name:      "s1",
		Source:    "src",
		Targets:   []engine.TargetID{"dst1", "dst2"},
		Selection: engine.Selection{Include: []string{"public.*"}, Exclude: []string{"*_audit"}},
	}
	if err := s.PutSync(ctx, def); err != nil {
		t.Fatalf("PutSync: %v", err)
	}

	got, err := s.GetSync(ctx, "s1")
	if err != nil {
		t.Fatalf("GetSync: %v", err)
	}
	if got.Name != "s1" || got.Source != "src" {
		t.Errorf("got %+v", got)
	}
	if len(got.Targets) != 2 || got.Targets[0] != "dst1" || got.Targets[1] != "dst2" {
		t.Errorf("targets = %v", got.Targets)
	}
	if len(got.Selection.Include) != 1 || got.Selection.Include[0] != "public.*" {
		t.Errorf("include = %v", got.Selection.Include)
	}
	if len(got.Selection.Exclude) != 1 || got.Selection.Exclude[0] != "*_audit" {
		t.Errorf("exclude = %v", got.Selection.Exclude)
	}

	// Upsert overwrites in place.
	def.Source = "src2"
	def.Targets = []engine.TargetID{"only"}
	if err := s.PutSync(ctx, def); err != nil {
		t.Fatalf("PutSync (update): %v", err)
	}
	got, err = s.GetSync(ctx, "s1")
	if err != nil {
		t.Fatalf("GetSync after update: %v", err)
	}
	if got.Source != "src2" || len(got.Targets) != 1 || got.Targets[0] != "only" {
		t.Errorf("update not applied: %+v", got)
	}

	// A second sync, then ListSyncs is ordered by name.
	if err := s.PutSync(ctx, state.SyncDef{Name: "a-first", Source: "x", Targets: []engine.TargetID{"t"}}); err != nil {
		t.Fatalf("PutSync a-first: %v", err)
	}
	list, err := s.ListSyncs(ctx)
	if err != nil {
		t.Fatalf("ListSyncs: %v", err)
	}
	if len(list) != 2 || list[0].Name != "a-first" || list[1].Name != "s1" {
		t.Errorf("ListSyncs order = %v", names2(list))
	}
}

func TestGetSyncNotFound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s := openTestStore(t, ctx)

	if _, err := s.GetSync(ctx, "nope"); err == nil {
		t.Fatal("expected not-found error for missing sync")
	}
}

func TestPutSyncEmptySelection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s := openTestStore(t, ctx)

	// nil Include/Exclude and single target must round-trip without NULL errors.
	if err := s.PutSync(ctx, state.SyncDef{Name: "s", Source: "src", Targets: []engine.TargetID{"t"}}); err != nil {
		t.Fatalf("PutSync with empty selection: %v", err)
	}
	got, err := s.GetSync(ctx, "s")
	if err != nil {
		t.Fatalf("GetSync: %v", err)
	}
	if len(got.Selection.Include) != 0 || len(got.Selection.Exclude) != 0 {
		t.Errorf("expected empty selection, got %+v", got.Selection)
	}
}

func names2(defs []state.SyncDef) []string {
	out := make([]string, len(defs))
	for i, d := range defs {
		out[i] = d.Name
	}
	return out
}
