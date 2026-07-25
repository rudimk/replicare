package statepg

import (
	"context"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/state"
)

// TestListReadsForStatus exercises the M6 status aggregate reads: per-sync copy
// progress, cursors (with UpdatedAt lag signal), and recent events (newest first).
func TestListReadsForStatus(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s := openTestStore(t, ctx)

	if err := s.PutSync(ctx, state.SyncDef{Name: "s1", Source: "src", Targets: []engine.TargetID{"dst"}}); err != nil {
		t.Fatalf("PutSync: %v", err)
	}
	orders := engine.TableRef{Schema: "public", Name: "orders"}
	items := engine.TableRef{Schema: "public", Name: "items"}

	if err := s.SaveCopyProgress(ctx, "s1", state.CopyProgress{Table: orders, Done: true}); err != nil {
		t.Fatalf("save progress orders: %v", err)
	}
	if err := s.SaveCopyProgress(ctx, "s1", state.CopyProgress{Table: items, Watermark: engine.KeyValues{"50"}}); err != nil {
		t.Fatalf("save progress items: %v", err)
	}
	if err := s.SaveCursor(ctx, "s1", state.Cursor{Target: "dst", Table: orders, Phase: state.PhaseStreaming, LastDelta: 99}); err != nil {
		t.Fatalf("save cursor orders: %v", err)
	}
	if err := s.SaveCursor(ctx, "s1", state.Cursor{Target: "dst", Table: items, Phase: state.PhaseInitialCopy, NeedsReseed: true}); err != nil {
		t.Fatalf("save cursor items: %v", err)
	}

	// Copy progress list.
	progs, err := s.ListCopyProgress(ctx, "s1")
	if err != nil {
		t.Fatalf("ListCopyProgress: %v", err)
	}
	if len(progs) != 2 {
		t.Fatalf("copy progress rows = %d, want 2", len(progs))
	}
	// ordered by schema, table: items then orders.
	if progs[0].Table != items || progs[1].Table != orders {
		t.Errorf("progress order = %v, %v", progs[0].Table, progs[1].Table)
	}
	if !progs[1].Done {
		t.Errorf("orders should be done")
	}
	if progs[0].UpdatedAt.IsZero() {
		t.Errorf("copy progress UpdatedAt should be populated")
	}

	// Cursor list.
	curs, err := s.ListCursors(ctx, "s1")
	if err != nil {
		t.Fatalf("ListCursors: %v", err)
	}
	if len(curs) != 2 {
		t.Fatalf("cursors = %d, want 2", len(curs))
	}
	byTable := map[string]state.Cursor{}
	for _, c := range curs {
		byTable[c.Table.Name] = c
	}
	if byTable["orders"].Phase != state.PhaseStreaming || byTable["orders"].LastDelta != 99 {
		t.Errorf("orders cursor = %+v", byTable["orders"])
	}
	if !byTable["items"].NeedsReseed {
		t.Errorf("items cursor should be needs-reseed")
	}
	if byTable["orders"].UpdatedAt.IsZero() {
		t.Errorf("cursor UpdatedAt (lag signal) should be populated")
	}

	// Events, newest first.
	for _, ev := range []string{"capture.installed", "target.needs_reseed", "table.cutover_to_streaming"} {
		if err := s.RecordEvent(ctx, state.Event{Sync: "s1", Target: "dst", Level: "INFO", Event: ev}); err != nil {
			t.Fatalf("record %s: %v", ev, err)
		}
	}
	evs, err := s.RecentEvents(ctx, "s1", 2)
	if err != nil {
		t.Fatalf("RecentEvents: %v", err)
	}
	if len(evs) != 2 {
		t.Fatalf("recent events = %d, want 2 (limited)", len(evs))
	}
	if evs[0].Event != "table.cutover_to_streaming" {
		t.Errorf("newest event = %q, want the last recorded", evs[0].Event)
	}
	if evs[0].CreatedAt.IsZero() {
		t.Errorf("event CreatedAt should be populated")
	}
}
