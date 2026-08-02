package mysql

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/apply"
	"github.com/rudimk/replicare/internal/engine"
)

// fastRetry keeps the FK-retry loop snappy in tests (the production default waits
// up to seconds between attempts).
var fastRetry = apply.RetryPolicy{MaxAttempts: 5, BaseBackoff: 50 * time.Millisecond, MaxBackoff: 200 * time.Millisecond}

// drainComp drains a component to convergence via the neutral FK-ordered retrying
// apply — the exact production path (stream.go), exercised end to end here.
func drainComp(t *testing.T, ctx context.Context, src *Source, sink *Sink, topo []engine.TableRef, cyclic bool) {
	t.Helper()
	for {
		n, err := apply.DrainComponentRetrying(ctx, src, sink, topo, "dst", 100, cyclic, fastRetry)
		if err != nil {
			t.Fatalf("DrainComponentRetrying: %v", err)
		}
		if n == 0 {
			return
		}
	}
}

// TestComponentFKOrderedApply is the MM5b acyclic keystone: a parent+child
// component with a live target FK converges through the neutral component drain.
// Because upserts apply parent->child and deletes child->parent, the target FK
// holds at every step — a naive per-table apply would violate it. Insert, update,
// PK-stable delete of a child then its parent all reconcile.
func TestComponentFKOrderedApply(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	src := connectSource(t, ctx)
	sink := connectSink(t, ctx)
	t.Cleanup(func() { _, _ = src.db.Exec("DROP DATABASE IF EXISTS replicare") })

	ddl := []string{
		"CREATE TABLE rc_it.parent (id INT PRIMARY KEY, label VARCHAR(50)) ENGINE=InnoDB",
		"CREATE TABLE rc_it.child (id INT PRIMARY KEY, parent_id INT NOT NULL, note VARCHAR(50), " +
			"FOREIGN KEY (parent_id) REFERENCES parent(id)) ENGINE=InnoDB",
	}
	mustExec(t, ctx, src.db, ddl...)
	mustExec(t, ctx, sink.db, "DROP DATABASE IF EXISTS rc_it", "CREATE DATABASE rc_it")
	mustExec(t, ctx, sink.db, ddl...)
	t.Cleanup(func() { _, _ = sink.db.Exec("DROP DATABASE IF EXISTS rc_it") })

	parent := engine.TableRef{Schema: "rc_it", Name: "parent"}
	child := engine.TableRef{Schema: "rc_it", Name: "child"}
	topo := []engine.TableRef{parent, child} // parents first
	if err := src.InstallCapture(ctx, topo); err != nil {
		t.Fatalf("install capture: %v", err)
	}

	// All writes after capture is installed; target starts empty.
	mustExec(t, ctx, src.db,
		"INSERT INTO rc_it.parent VALUES (1,'p1'),(2,'p2')",
		"INSERT INTO rc_it.child VALUES (10,1,'c10'),(11,1,'c11'),(12,2,'c12')",
		"UPDATE rc_it.child SET note='c10new' WHERE id=10",
		"UPDATE rc_it.parent SET label='p1new' WHERE id=1",
		"DELETE FROM rc_it.child WHERE id=12", // remove child before its parent
		"DELETE FROM rc_it.parent WHERE id=2", // now parent 2 is unreferenced
	)

	drainComp(t, ctx, src, sink, topo, false)

	// Row-by-row convergence on both tables.
	assertTablesEqual(t, ctx, src, sink, "SELECT id, label FROM rc_it.parent ORDER BY id")
	assertTablesEqual(t, ctx, src, sink, "SELECT id, parent_id, note FROM rc_it.child ORDER BY id")
}

// TestCyclicComponentApplyConverges is the MM5b cyclic keystone: a mutually
// NOT-NULL-referencing pair (a<->b) streams through the neutral drain with
// cyclic=true, so MySQL applies under FOREIGN_KEY_CHECKS=0 and the pre-commit
// verify passes (both rows present) — proving a cycle InnoDB could never load
// row-by-row converges, and later fully deletes.
func TestCyclicComponentApplyConverges(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	src := connectSource(t, ctx)
	sink := connectSink(t, ctx)
	t.Cleanup(func() { _, _ = src.db.Exec("DROP DATABASE IF EXISTS replicare") })

	ddl := []string{
		"CREATE TABLE rc_it.a (id INT PRIMARY KEY, b_id INT NOT NULL) ENGINE=InnoDB",
		"CREATE TABLE rc_it.b (id INT PRIMARY KEY, a_id INT NOT NULL, " +
			"FOREIGN KEY (a_id) REFERENCES a(id)) ENGINE=InnoDB",
		"ALTER TABLE rc_it.a ADD FOREIGN KEY (b_id) REFERENCES b(id)",
	}
	mustExec(t, ctx, src.db, ddl...)
	mustExec(t, ctx, sink.db, "DROP DATABASE IF EXISTS rc_it", "CREATE DATABASE rc_it")
	mustExec(t, ctx, sink.db, ddl...)
	t.Cleanup(func() { _, _ = sink.db.Exec("DROP DATABASE IF EXISTS rc_it") })

	a := engine.TableRef{Schema: "rc_it", Name: "a"}
	b := engine.TableRef{Schema: "rc_it", Name: "b"}
	topo := []engine.TableRef{a, b}
	if err := src.InstallCapture(ctx, topo); err != nil {
		t.Fatalf("install capture: %v", err)
	}

	// Bootstrap a mutually-referencing pair on the source (a cycle needs checks off
	// to insert; both rows exist before checks resume). Triggers still capture both.
	mustExec(t, ctx, src.db,
		"SET FOREIGN_KEY_CHECKS = 0",
		"INSERT INTO rc_it.a VALUES (1, 10)",
		"INSERT INTO rc_it.b VALUES (10, 1)",
		"SET FOREIGN_KEY_CHECKS = 1",
	)

	drainComp(t, ctx, src, sink, topo, true)
	assertTablesEqual(t, ctx, src, sink, "SELECT id, b_id FROM rc_it.a ORDER BY id")
	assertTablesEqual(t, ctx, src, sink, "SELECT id, a_id FROM rc_it.b ORDER BY id")

	// Delete the whole cycle and converge to empty.
	mustExec(t, ctx, src.db,
		"SET FOREIGN_KEY_CHECKS = 0",
		"DELETE FROM rc_it.a WHERE id=1",
		"DELETE FROM rc_it.b WHERE id=10",
		"SET FOREIGN_KEY_CHECKS = 1",
	)
	drainComp(t, ctx, src, sink, topo, true)
	if n := countRows(t, ctx, sink, "rc_it.a"); n != 0 {
		t.Errorf("target a not empty after cycle delete: %d rows", n)
	}
	if n := countRows(t, ctx, sink, "rc_it.b"); n != 0 {
		t.Errorf("target b not empty after cycle delete: %d rows", n)
	}
}

// TestCyclicApplyOrphanHaltsLoud proves the pre-commit orphan verify (Momus
// 2nd-pass B1/m1): under FOREIGN_KEY_CHECKS=0 a DeleteAbsent strands a NON-dirty,
// non-staged child; the whole-component verify catches it BEFORE commit, so the
// pass halts loud and rolls back — the parent delete is NOT visible afterward. A
// dirty-scoped check would have missed the stranded child.
func TestCyclicApplyOrphanHaltsLoud(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	sink := connectSink(t, ctx)

	ddl := []string{
		"CREATE TABLE rc_it.parent (id INT PRIMARY KEY) ENGINE=InnoDB",
		"CREATE TABLE rc_it.child (id INT PRIMARY KEY, parent_id INT NOT NULL, " +
			"FOREIGN KEY (parent_id) REFERENCES parent(id)) ENGINE=InnoDB",
	}
	mustExec(t, ctx, sink.db, "DROP DATABASE IF EXISTS rc_it", "CREATE DATABASE rc_it")
	mustExec(t, ctx, sink.db, ddl...)
	t.Cleanup(func() { _, _ = sink.db.Exec("DROP DATABASE IF EXISTS rc_it") })

	// Pre-seed a referentially-valid target: parent(1) with a child(100) pointing at
	// it. The child is NOT part of this pass's dirty set.
	mustExec(t, ctx, sink.db,
		"INSERT INTO rc_it.parent VALUES (1)",
		"INSERT INTO rc_it.child VALUES (100, 1)",
	)

	parent := engine.TableRef{Schema: "rc_it", Name: "parent"}
	child := engine.TableRef{Schema: "rc_it", Name: "child"}

	// cyclic=true routes to FOREIGN_KEY_CHECKS=0; componentTables carries BOTH tables
	// so the verify scans the child edge even though only parent is staged/deleted.
	tx, err := sink.BeginApply(ctx, true, []engine.TableRef{parent, child})
	if err != nil {
		t.Fatalf("BeginApply: %v", err)
	}
	// Stage parent with an EMPTY re-read (parent id=1 is "deleted at source"), then
	// delete it: with checks off this strands child(100).
	if err := tx.StageUpsert(ctx, parent, []string{"id"}, strings.NewReader("")); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("StageUpsert parent: %v", err)
	}
	if err := tx.DeleteAbsent(ctx, parent, []engine.KeyValues{{"1"}}); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("DeleteAbsent parent: %v", err)
	}
	err = tx.Commit(ctx)
	if err == nil {
		t.Fatal("expected a loud orphan halt at commit, got nil")
	}
	if !strings.Contains(err.Error(), "orphan") {
		t.Errorf("halt error should name the orphan, got: %v", err)
	}
	// The verify is transient-classified? No — a cyclic orphan is a hard halt.
	if engine.IsTransientConstraint(err) {
		t.Errorf("cyclic orphan halt must NOT be transient (would thrash the retry), got transient: %v", err)
	}
	// Rolled back: parent(1) survives, so nothing referentially broken is visible.
	if n := countRows(t, ctx, sink, "rc_it.parent"); n != 1 {
		t.Errorf("parent should survive the rolled-back pass: %d rows, want 1", n)
	}
	if n := countRows(t, ctx, sink, "rc_it.child"); n != 1 {
		t.Errorf("child should be untouched: %d rows, want 1", n)
	}
}

// assertTablesEqual runs the same query on source and target and fails on any
// row difference (faithful convergence).
func assertTablesEqual(t *testing.T, ctx context.Context, src *Source, sink *Sink, query string) {
	t.Helper()
	want := dumpRows(t, ctx, src.db, query)
	got := dumpRows(t, ctx, sink.db, query)
	if len(want) != len(got) {
		t.Fatalf("%q: row count target=%d source=%d\n target=%v\n source=%v", query, len(got), len(want), got, want)
	}
	for i := range want {
		if want[i] != got[i] {
			t.Fatalf("%q: row %d differs: source=%q target=%q", query, i, want[i], got[i])
		}
	}
}

// dumpRows returns each row of query as a tab-joined string of its column values
// (NULL rendered "<null>"), for order-sensitive row comparison.
func dumpRows(t *testing.T, ctx context.Context, db *sql.DB, query string) []string {
	t.Helper()
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	defer func() { _ = rows.Close() }()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("columns %q: %v", query, err)
	}
	var out []string
	for rows.Next() {
		raw := make([][]byte, len(cols))
		dest := make([]any, len(cols))
		for i := range raw {
			dest[i] = &raw[i]
		}
		if err := rows.Scan(dest...); err != nil {
			t.Fatalf("scan %q: %v", query, err)
		}
		parts := make([]string, len(cols))
		for i, b := range raw {
			if b == nil {
				parts[i] = "<null>"
			} else {
				parts[i] = string(b)
			}
		}
		out = append(out, strings.Join(parts, "\t"))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows %q: %v", query, err)
	}
	return out
}

// countRows returns the row count of a table.
func countRows(t *testing.T, ctx context.Context, sink *Sink, table string) int {
	t.Helper()
	var n int
	if err := sink.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}
