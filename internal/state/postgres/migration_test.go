package statepg

import (
	"context"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/migrate"
	"github.com/rudimk/replicare/internal/pgmigrate"
	"github.com/rudimk/replicare/internal/state"
)

// TestVersionBumpPreservesState proves an in-place schema upgrade preserves
// existing state (syncs, cursors) — the state schema lives on a DB we may not own
// and must migrate, never drop-recreate (CLAUDE.md §9, F4).
func TestVersionBumpPreservesState(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s := openTestStore(t, ctx) // schema at v1

	// Seed pre-existing in-flight state.
	def := state.SyncDef{Name: "s1", Source: "src", Targets: []engine.TargetID{"dst"}}
	if err := s.PutSync(ctx, def); err != nil {
		t.Fatalf("PutSync: %v", err)
	}
	tbl := engine.TableRef{Schema: "public", Name: "orders"}
	cur := state.Cursor{Target: "dst", Table: tbl, Phase: state.PhaseStreaming, LastDelta: 7}
	if err := s.SaveCursor(ctx, "s1", cur); err != nil {
		t.Fatalf("SaveCursor: %v", err)
	}

	// A hypothetical v2 migration: add a column. Reuses the real v1 migrations so
	// Apply runs only the pending v2 against the already-v1 database.
	v2 := migrate.Set{
		Name:         schemaSet.Name,
		VersionTable: schemaSet.VersionTable,
		Migrations: append(append([]migrate.Migration{}, schemaSet.Migrations...),
			migrate.Migration{
				Version:    2,
				Name:       "add_syncs_note",
				Statements: []string{"ALTER TABLE replicare_state.syncs ADD COLUMN note text"},
			}),
	}
	applied, err := migrate.Apply(ctx, pgmigrate.New(s.pool), v2)
	if err != nil {
		t.Fatalf("apply v2: %v", err)
	}
	if len(applied) != 1 || applied[0] != 2 {
		t.Fatalf("expected only v2 to apply, got %v", applied)
	}

	// Version is now 2.
	var version int
	if err := s.pool.QueryRow(ctx, "SELECT COALESCE(MAX(version),0) FROM "+versionTable).Scan(&version); err != nil {
		t.Fatalf("read version: %v", err)
	}
	if version != 2 {
		t.Errorf("version = %d, want 2", version)
	}

	// The new column exists.
	var colExists bool
	if err := s.pool.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM information_schema.columns
		WHERE table_schema=$1 AND table_name='syncs' AND column_name='note')`,
		stateSchema).Scan(&colExists); err != nil {
		t.Fatalf("check column: %v", err)
	}
	if !colExists {
		t.Error("expected the v2-added column 'note' to exist")
	}

	// Crucially, pre-existing state survived the upgrade unchanged.
	gotSync, err := s.GetSync(ctx, "s1")
	if err != nil {
		t.Fatalf("GetSync after upgrade: %v", err)
	}
	if gotSync.Source != "src" || len(gotSync.Targets) != 1 || gotSync.Targets[0] != "dst" {
		t.Errorf("sync not preserved across upgrade: %+v", gotSync)
	}
	gotCur, err := s.LoadCursor(ctx, "s1", "dst", tbl)
	if err != nil {
		t.Fatalf("LoadCursor after upgrade: %v", err)
	}
	if gotCur.Phase != state.PhaseStreaming || gotCur.LastDelta != 7 {
		t.Errorf("cursor not preserved across upgrade: %+v", gotCur)
	}

	// Re-applying v2 is a no-op (idempotent).
	again, err := migrate.Apply(ctx, pgmigrate.New(s.pool), v2)
	if err != nil {
		t.Fatalf("re-apply v2: %v", err)
	}
	if len(again) != 0 {
		t.Errorf("re-apply should be a no-op, applied %v", again)
	}
}
