package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

// setupSourceTables creates a clean rc_it schema with the given DDL on the source
// and registers cleanup.
func setupSourceTables(t *testing.T, ctx context.Context, src *Source, ddl ...string) {
	t.Helper()
	mustExec(t, ctx, src.conn, "DROP SCHEMA IF EXISTS rc_it CASCADE")
	mustExec(t, ctx, src.conn, "CREATE SCHEMA rc_it")
	for _, s := range ddl {
		mustExec(t, ctx, src.conn, s)
	}
	t.Cleanup(func() {
		_, _ = src.conn.Exec(context.Background(), "DROP SCHEMA IF EXISTS rc_it CASCADE")
	})
}

func objectExists(t *testing.T, ctx context.Context, src *Source, query string, args ...any) bool {
	t.Helper()
	var exists bool
	if err := src.conn.QueryRow(ctx, query, args...).Scan(&exists); err != nil {
		t.Fatalf("existence check: %v", err)
	}
	return exists
}

func deltaCount(t *testing.T, ctx context.Context, src *Source, relID int) int {
	t.Helper()
	var n int
	if err := src.conn.QueryRow(ctx, "SELECT count(*) FROM "+qualifiedCapture(deltaTableName(relID))).Scan(&n); err != nil {
		t.Fatalf("count deltas: %v", err)
	}
	return n
}

func TestInstallCaptureCreatesObjectsAndFires(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	src := captureSource(t, ctx)
	setupSourceTables(t, ctx, src,
		"CREATE TABLE rc_it.orders (id int PRIMARY KEY, note text)")

	ref := engine.TableRef{Schema: "rc_it", Name: "orders"}
	if err := src.InstallCapture(ctx, []engine.TableRef{ref}); err != nil {
		t.Fatalf("InstallCapture: %v", err)
	}

	relID, ok, err := lookupRegistry(ctx, src.conn, ref)
	if err != nil || !ok {
		t.Fatalf("registry lookup: ok=%v err=%v", ok, err)
	}

	// Delta and track tables exist.
	if !objectExists(t, ctx, src,
		`SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema=$1 AND table_name=$2)`,
		captureSchema, deltaTableName(relID)) {
		t.Error("delta table missing")
	}
	if !objectExists(t, ctx, src,
		`SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema=$1 AND table_name=$2)`,
		captureSchema, trackTableName(relID)) {
		t.Error("track table missing")
	}
	// Trigger attached to the source table.
	if !objectExists(t, ctx, src,
		`SELECT EXISTS(SELECT 1 FROM pg_trigger WHERE tgname=$1 AND tgrelid=$2::regclass AND NOT tgisinternal)`,
		triggerName(relID), qualifyTable(ref)) {
		t.Error("capture trigger missing on source table")
	}

	// Trigger fires: INSERT/UPDATE/DELETE produce delta rows.
	mustExec(t, ctx, src.conn, "INSERT INTO rc_it.orders (id, note) VALUES (1, 'a'), (2, 'b')")
	mustExec(t, ctx, src.conn, "UPDATE rc_it.orders SET note='c' WHERE id=1")
	mustExec(t, ctx, src.conn, "DELETE FROM rc_it.orders WHERE id=2")
	// 2 inserts + 1 update + 1 delete = 4 delta rows (no PK change).
	if got := deltaCount(t, ctx, src, relID); got != 4 {
		t.Errorf("delta count = %d, want 4", got)
	}

	// Ops recorded correctly.
	var inserts, updates, deletes int
	if err := src.conn.QueryRow(ctx,
		"SELECT count(*) FILTER (WHERE rc_op='I'), count(*) FILTER (WHERE rc_op='U'), count(*) FILTER (WHERE rc_op='D') FROM "+
			qualifiedCapture(deltaTableName(relID))).Scan(&inserts, &updates, &deletes); err != nil {
		t.Fatalf("op counts: %v", err)
	}
	if inserts != 2 || updates != 1 || deletes != 1 {
		t.Errorf("op counts I/U/D = %d/%d/%d, want 2/1/1", inserts, updates, deletes)
	}
}

func TestInstallCapturePKUpdateEnqueuesBothKeys(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	src := captureSource(t, ctx)
	setupSourceTables(t, ctx, src,
		"CREATE TABLE rc_it.items (id int PRIMARY KEY, note text)")

	ref := engine.TableRef{Schema: "rc_it", Name: "items"}
	if err := src.InstallCapture(ctx, []engine.TableRef{ref}); err != nil {
		t.Fatalf("InstallCapture: %v", err)
	}
	relID, _, _ := lookupRegistry(ctx, src.conn, ref)

	mustExec(t, ctx, src.conn, "INSERT INTO rc_it.items (id, note) VALUES (1, 'x')")
	// PK-changing update: id 1 -> 100 must enqueue BOTH old (delete) and new (upsert).
	mustExec(t, ctx, src.conn, "UPDATE rc_it.items SET id=100 WHERE id=1")

	rows, err := src.conn.Query(ctx,
		"SELECT rc_op, k1 FROM "+qualifiedCapture(deltaTableName(relID))+" ORDER BY delta_id")
	if err != nil {
		t.Fatalf("query deltas: %v", err)
	}
	defer rows.Close()
	type d struct {
		op string
		k  int
	}
	var got []d
	for rows.Next() {
		var op string
		var k int
		if err := rows.Scan(&op, &k); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, d{op, k})
	}
	// Expect: I/1 (insert), then D/1 (old key) and U/100 (new key).
	want := []d{{"I", 1}, {"D", 1}, {"U", 100}}
	if len(got) != len(want) {
		t.Fatalf("delta rows = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("delta[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestInstallCaptureIdempotentPreservesDeltas(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	src := captureSource(t, ctx)
	setupSourceTables(t, ctx, src,
		"CREATE TABLE rc_it.orders (id int PRIMARY KEY)")

	ref := engine.TableRef{Schema: "rc_it", Name: "orders"}
	if err := src.InstallCapture(ctx, []engine.TableRef{ref}); err != nil {
		t.Fatalf("first install: %v", err)
	}
	relID, _, _ := lookupRegistry(ctx, src.conn, ref)
	mustExec(t, ctx, src.conn, "INSERT INTO rc_it.orders VALUES (1)")
	if deltaCount(t, ctx, src, relID) != 1 {
		t.Fatal("expected 1 delta before re-install")
	}

	// Re-install must be a no-op that preserves the queued delta.
	if err := src.InstallCapture(ctx, []engine.TableRef{ref}); err != nil {
		t.Fatalf("re-install: %v", err)
	}
	relID2, _, _ := lookupRegistry(ctx, src.conn, ref)
	if relID2 != relID {
		t.Errorf("rel_id changed across re-install: %d -> %d", relID, relID2)
	}
	if deltaCount(t, ctx, src, relID) != 1 {
		t.Error("re-install lost the queued delta")
	}
}

func TestRemoveCaptureLeavesClean(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	src := captureSource(t, ctx)
	setupSourceTables(t, ctx, src,
		"CREATE TABLE rc_it.orders (id int PRIMARY KEY)")

	ref := engine.TableRef{Schema: "rc_it", Name: "orders"}
	if err := src.InstallCapture(ctx, []engine.TableRef{ref}); err != nil {
		t.Fatalf("install: %v", err)
	}
	relID, _, _ := lookupRegistry(ctx, src.conn, ref)

	if err := src.RemoveCapture(ctx, []engine.TableRef{ref}); err != nil {
		t.Fatalf("remove: %v", err)
	}

	// Trigger, delta/track tables, and registry row are gone.
	if objectExists(t, ctx, src,
		`SELECT EXISTS(SELECT 1 FROM pg_trigger WHERE tgname=$1 AND tgrelid=$2::regclass AND NOT tgisinternal)`,
		triggerName(relID), qualifyTable(ref)) {
		t.Error("trigger still present after remove")
	}
	if objectExists(t, ctx, src,
		`SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema=$1 AND table_name=$2)`,
		captureSchema, deltaTableName(relID)) {
		t.Error("delta table still present after remove")
	}
	_, ok, err := lookupRegistry(ctx, src.conn, ref)
	if err != nil {
		t.Fatalf("registry lookup: %v", err)
	}
	if ok {
		t.Error("registry row still present after remove")
	}
}
