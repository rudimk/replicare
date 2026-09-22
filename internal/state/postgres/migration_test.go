package statepg

import (
	"context"
	"fmt"
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

	// A hypothetical next migration (v3, on top of the real v1+v2): add a column.
	// Reuses the real migrations so Apply runs only the pending v3 against the
	// already-upgraded database.
	next := migrate.Set{
		Name:         schemaSet.Name,
		VersionTable: schemaSet.VersionTable,
		Migrations: append(append([]migrate.Migration{}, schemaSet.Migrations...),
			migrate.Migration{
				Version:    3,
				Name:       "add_syncs_note",
				Statements: []string{"ALTER TABLE replicare_state.syncs ADD COLUMN note text"},
			}),
	}
	applied, err := migrate.Apply(ctx, pgmigrate.New(s.pool), next)
	if err != nil {
		t.Fatalf("apply v3: %v", err)
	}
	if len(applied) != 1 || applied[0] != 3 {
		t.Fatalf("expected only v3 to apply, got %v", applied)
	}

	// Version is now 3.
	var version int
	if err := s.pool.QueryRow(ctx, "SELECT COALESCE(MAX(version),0) FROM "+versionTable).Scan(&version); err != nil {
		t.Fatalf("read version: %v", err)
	}
	if version != 3 {
		t.Errorf("version = %d, want 3", version)
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

	// Re-applying is a no-op (idempotent).
	again, err := migrate.Apply(ctx, pgmigrate.New(s.pool), next)
	if err != nil {
		t.Fatalf("re-apply v3: %v", err)
	}
	if len(again) != 0 {
		t.Errorf("re-apply should be a no-op, applied %v", again)
	}
}

// TestCopyProgressV2Backfill proves the MM2 migration: a v1 (target-less)
// copy_progress row is fanned out to one row PER TARGET found in the cursors
// table, preserving each row's progress, and a copy_progress row with no matching
// cursor (only possible mid-initial-copy before cutover) is dropped so that one
// table re-copies once. Existing per-target progress must survive the upgrade.
func TestCopyProgressV2Backfill(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s := New(stateConn(t))
	if err := s.Open(ctx); err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := s.pool.Exec(ctx, "DROP SCHEMA IF EXISTS "+stateSchema+" CASCADE"); err != nil {
		t.Fatalf("drop schema: %v", err)
	}
	t.Cleanup(func() {
		_, _ = s.pool.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+stateSchema+" CASCADE")
		_ = s.Close(context.Background())
	})

	// Migrate to v1 only (the pre-MM2, target-less copy_progress schema).
	v1 := migrate.Set{
		Name:         schemaSet.Name,
		VersionTable: schemaSet.VersionTable,
		Migrations:   schemaSet.Migrations[:1],
	}
	if _, err := migrate.Apply(ctx, pgmigrate.New(s.pool), v1); err != nil {
		t.Fatalf("apply v1: %v", err)
	}

	// Seed pre-v2 state directly (the store methods assume the v2 schema): a sync,
	// a target-less copy_progress row for `orders` (done, with a watermark) that has
	// cursors for two targets, and a `orphan` row with NO cursor.
	exec := func(sql string) {
		t.Helper()
		if _, err := s.pool.Exec(ctx, sql); err != nil {
			t.Fatalf("seed %q: %v", sql, err)
		}
	}
	exec(`INSERT INTO replicare_state.syncs (name, source) VALUES ('s1','src')`)
	exec(`INSERT INTO replicare_state.copy_progress (sync, schema_name, table_name, done, watermark)
	      VALUES ('s1','public','orders', true, '["z"]'::jsonb)`)
	exec(`INSERT INTO replicare_state.copy_progress (sync, schema_name, table_name, done)
	      VALUES ('s1','public','orphan', true)`)
	exec(`INSERT INTO replicare_state.cursors (sync, target, schema_name, table_name)
	      VALUES ('s1','dst1','public','orders'), ('s1','dst2','public','orders')`)

	// Apply the full set → runs the v2 backfill.
	if _, err := migrate.Apply(ctx, pgmigrate.New(s.pool), schemaSet); err != nil {
		t.Fatalf("apply v2: %v", err)
	}

	orders := engine.TableRef{Schema: "public", Name: "orders"}
	orphan := engine.TableRef{Schema: "public", Name: "orphan"}

	// `orders` was fanned out to both targets, progress preserved.
	for _, tgt := range []engine.TargetID{"dst1", "dst2"} {
		got, err := s.LoadCopyProgress(ctx, "s1", tgt, orders)
		if err != nil {
			t.Fatalf("LoadCopyProgress(%s): %v", tgt, err)
		}
		if !got.Done || len(got.Watermark) != 1 || fmt.Sprint(got.Watermark[0]) != "z" {
			t.Errorf("backfilled progress for %s = %+v, want done + watermark [z]", tgt, got)
		}
	}

	// `orphan` (no cursor) was dropped → re-copies (fresh, not done).
	fresh, err := s.LoadCopyProgress(ctx, "s1", "dst1", orphan)
	if err != nil {
		t.Fatalf("LoadCopyProgress(orphan): %v", err)
	}
	if fresh.Done {
		t.Errorf("orphan (no cursor) should have been dropped and re-copy fresh, got %+v", fresh)
	}

	// Exactly two copy_progress rows survive (orders×{dst1,dst2}); none is target-less.
	var total, nullTargets int
	if err := s.pool.QueryRow(ctx, "SELECT count(*), count(*) FILTER (WHERE target IS NULL) FROM replicare_state.copy_progress").Scan(&total, &nullTargets); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if total != 2 || nullTargets != 0 {
		t.Errorf("after backfill: total=%d nullTargets=%d, want total=2 nullTargets=0", total, nullTargets)
	}
}
