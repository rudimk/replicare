package statepg

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/state"
)

// reopen simulates a process restart: a fresh Store against the same (already
// migrated) state DB, without dropping the schema.
func reopen(t *testing.T, ctx context.Context) *Store {
	t.Helper()
	s := New(stateConn(t))
	if err := s.Open(ctx); err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	return s
}

func TestCopyProgressResumeAfterRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s := openTestStore(t, ctx)

	if err := s.PutSync(ctx, state.SyncDef{Name: "s1", Source: "src", Targets: []engine.TargetID{"dst"}}); err != nil {
		t.Fatalf("PutSync: %v", err)
	}

	tbl := engine.TableRef{Schema: "public", Name: "orders"}
	saved := state.CopyProgress{
		Table:     tbl,
		Done:      false,
		Watermark: engine.KeyValues{"c100"},
		Completed: []state.CompletedRange{
			{Lo: engine.KeyValues{"a"}, Hi: engine.KeyValues{"b"}},
			{Lo: engine.KeyValues{"d"}, Hi: engine.KeyValues{"e"}},
		},
	}
	if err := s.SaveCopyProgress(ctx, "s1", saved); err != nil {
		t.Fatalf("SaveCopyProgress: %v", err)
	}

	// Restart: a brand-new Store must resume from exactly the saved progress.
	s2 := reopen(t, ctx)
	got, err := s2.LoadCopyProgress(ctx, "s1", tbl)
	if err != nil {
		t.Fatalf("LoadCopyProgress: %v", err)
	}
	if !reflect.DeepEqual(got, saved) {
		t.Errorf("resumed progress mismatch:\n got  %+v\n want %+v", got, saved)
	}
}

func TestCopyProgressFreshWhenAbsent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s := openTestStore(t, ctx)
	if err := s.PutSync(ctx, state.SyncDef{Name: "s1", Source: "src", Targets: []engine.TargetID{"dst"}}); err != nil {
		t.Fatalf("PutSync: %v", err)
	}

	tbl := engine.TableRef{Schema: "public", Name: "never_copied"}
	got, err := s.LoadCopyProgress(ctx, "s1", tbl)
	if err != nil {
		t.Fatalf("LoadCopyProgress: %v", err)
	}
	if got.Done || got.Watermark != nil || len(got.Completed) != 0 {
		t.Errorf("absent progress should be fresh, got %+v", got)
	}
	if got.Table != tbl {
		t.Errorf("Table = %+v, want %+v", got.Table, tbl)
	}
}

func TestCopyProgressIntegerKeyPrecision(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s := openTestStore(t, ctx)
	if err := s.PutSync(ctx, state.SyncDef{Name: "s1", Source: "src", Targets: []engine.TargetID{"dst"}}); err != nil {
		t.Fatalf("PutSync: %v", err)
	}

	// A large integer key beyond float64's exact range must survive as JSON.
	const big = int64(9007199254740993) // 2^53 + 1
	tbl := engine.TableRef{Schema: "public", Name: "big"}
	if err := s.SaveCopyProgress(ctx, "s1", state.CopyProgress{Table: tbl, Watermark: engine.KeyValues{big}}); err != nil {
		t.Fatalf("SaveCopyProgress: %v", err)
	}
	got, err := reopen(t, ctx).LoadCopyProgress(ctx, "s1", tbl)
	if err != nil {
		t.Fatalf("LoadCopyProgress: %v", err)
	}
	// Stored via json.Number, so it comes back as a json.Number preserving the
	// exact digits (no float64 rounding).
	num, ok := got.Watermark[0].(json.Number)
	if !ok {
		t.Fatalf("watermark[0] = %T, want json.Number", got.Watermark[0])
	}
	if num.String() != "9007199254740993" {
		t.Errorf("integer key lost precision: got %s", num.String())
	}
}

func TestCursorResumeAfterRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s := openTestStore(t, ctx)
	if err := s.PutSync(ctx, state.SyncDef{Name: "s1", Source: "src", Targets: []engine.TargetID{"dst"}}); err != nil {
		t.Fatalf("PutSync: %v", err)
	}

	tbl := engine.TableRef{Schema: "public", Name: "orders"}
	saved := state.Cursor{
		Target:      "dst",
		Table:       tbl,
		Phase:       state.PhaseStreaming,
		LastDelta:   42,
		NeedsReseed: true,
	}
	if err := s.SaveCursor(ctx, "s1", saved); err != nil {
		t.Fatalf("SaveCursor: %v", err)
	}

	got, err := reopen(t, ctx).LoadCursor(ctx, "s1", "dst", tbl)
	if err != nil {
		t.Fatalf("LoadCursor: %v", err)
	}
	if !reflect.DeepEqual(got, saved) {
		t.Errorf("resumed cursor mismatch:\n got  %+v\n want %+v", got, saved)
	}
}

func TestCursorFreshWhenAbsent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s := openTestStore(t, ctx)
	if err := s.PutSync(ctx, state.SyncDef{Name: "s1", Source: "src", Targets: []engine.TargetID{"dst"}}); err != nil {
		t.Fatalf("PutSync: %v", err)
	}

	got, err := s.LoadCursor(ctx, "s1", "dst", engine.TableRef{Schema: "public", Name: "new"})
	if err != nil {
		t.Fatalf("LoadCursor: %v", err)
	}
	if got.Phase != state.PhaseInitialCopy || got.LastDelta != 0 || got.NeedsReseed {
		t.Errorf("absent cursor should be fresh initial-copy, got %+v", got)
	}
}
