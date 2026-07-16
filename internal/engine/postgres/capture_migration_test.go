package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/migrate"
	"github.com/rudimk/replicare/internal/pgmigrate"
)

// TestSourceSchemaVersionBumpPreservesDeltas proves an in-place upgrade of the
// source `replicare` schema preserves queued deltas — the source schema lives on
// a DB we may not own and must migrate, never drop-recreate (CLAUDE.md §3, F4).
func TestSourceSchemaVersionBumpPreservesDeltas(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	src := captureSource(t, ctx)
	setupSourceTables(t, ctx, src, "CREATE TABLE rc_it.orders (id int PRIMARY KEY)")

	ref := engine.TableRef{Schema: "rc_it", Name: "orders"}
	if err := src.InstallCapture(ctx, []engine.TableRef{ref}); err != nil {
		t.Fatalf("install: %v", err)
	}
	relID, _, _, _ := lookupCapture(ctx, src.conn, ref)

	// Queue some deltas.
	mustExec(t, ctx, src.conn, "INSERT INTO rc_it.orders VALUES (1),(2),(3)")
	before := deltaCount(t, ctx, src, relID)
	if before != 3 {
		t.Fatalf("expected 3 queued deltas, got %d", before)
	}

	// A hypothetical v2 source-schema migration: add a column to the registry.
	// Reuses the real v1 migrations so only the pending v2 runs.
	v2 := migrate.Set{
		Name:         sourceSchemaSet.Name,
		VersionTable: sourceSchemaSet.VersionTable,
		Migrations: append(append([]migrate.Migration{}, sourceSchemaSet.Migrations...),
			migrate.Migration{
				Version:    2,
				Name:       "add_captured_note",
				Statements: []string{"ALTER TABLE replicare.captured ADD COLUMN note text"},
			}),
	}
	applied, err := migrate.Apply(ctx, pgmigrate.New(src.conn), v2)
	if err != nil {
		t.Fatalf("apply v2: %v", err)
	}
	if len(applied) != 1 || applied[0] != 2 {
		t.Fatalf("expected only v2 to apply, got %v", applied)
	}

	// Version advanced in place.
	var version int
	if err := src.conn.QueryRow(ctx, "SELECT COALESCE(MAX(version),0) FROM "+captureVersionTable).Scan(&version); err != nil {
		t.Fatalf("read version: %v", err)
	}
	if version != 2 {
		t.Errorf("version = %d, want 2", version)
	}

	// Queued deltas survived the upgrade, and the registry row is intact.
	if after := deltaCount(t, ctx, src, relID); after != before {
		t.Errorf("queued deltas not preserved across upgrade: %d -> %d", before, after)
	}
	if _, ok, err := lookupRegistry(ctx, src.conn, ref); err != nil || !ok {
		t.Errorf("registry row lost across upgrade: ok=%v err=%v", ok, err)
	}

	// And consumption still works post-upgrade.
	keys, err := src.ReadDirtyKeys(ctx, ref, "dst", 0)
	if err != nil {
		t.Fatalf("ReadDirtyKeys after upgrade: %v", err)
	}
	if len(keys) != 3 {
		t.Errorf("expected 3 dirty keys after upgrade, got %d", len(keys))
	}
}
