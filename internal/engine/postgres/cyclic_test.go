package postgres

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

func TestSelfRefFKCols(t *testing.T) {
	table := engine.Table{
		Ref: ref("public.emp"),
		ForeignKeys: []engine.ForeignKey{
			fkN("emp_mgr", "public.emp", "public.emp", []string{"manager_id"}, false),
			fkN("emp_dept", "public.emp", "public.dept", []string{"dept_id"}, false), // not self-ref
		},
	}
	got := selfRefFKCols(table)
	if len(got) != 1 || got[0] != "manager_id" {
		t.Errorf("selfRefFKCols = %v, want [manager_id]", got)
	}
}

func TestSubtractCols(t *testing.T) {
	got := subtractCols([]string{"id", "manager_id", "name"}, []string{"manager_id"})
	if len(got) != 2 || got[0] != "id" || got[1] != "name" {
		t.Errorf("subtractCols = %v, want [id name]", got)
	}
}

// TestLoadCyclicNullFill loads a self-referential table with a nullable FK via
// NULL-then-fill and verifies the FK column is filled and consistent.
func TestLoadCyclicNullFill(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	src := captureSource(t, ctx)
	sink := sinkTarget(t, ctx)

	// Nullable, non-deferrable self-ref FK. A manager chain: row 1 has no
	// manager, each subsequent row reports to the previous.
	srcDDL := "CREATE TABLE rc_it.emp (id int PRIMARY KEY, manager_id int REFERENCES rc_it.emp(id), name text)"
	setupSourceTables(t, ctx, src, srcDDL)
	mustExecTarget(t, ctx, sink, "CREATE SCHEMA rc_it")
	mustExecTarget(t, ctx, sink, srcDDL)
	mustExec(t, ctx, src.conn, "INSERT INTO rc_it.emp SELECT g, NULLIF(g-1,0), 'e'||g FROM generate_series(1,20) g")

	ref := engine.TableRef{Schema: "rc_it", Name: "emp"}
	if err := LoadCyclicNullFill(ctx, src, sink, ref); err != nil {
		t.Fatalf("LoadCyclicNullFill: %v", err)
	}

	sel := "SELECT id::text, coalesce(manager_id::text,'<n>'), name FROM rc_it.emp ORDER BY id"
	if !eqLines(dumpText(t, ctx, src.conn, sel), dumpText(t, ctx, sink.conn, sel)) {
		t.Error("null-fill did not reproduce the source faithfully")
	}
	// No dangling manager references.
	var orphans int
	if err := sink.conn.QueryRow(ctx,
		"SELECT count(*) FROM rc_it.emp e WHERE e.manager_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM rc_it.emp m WHERE m.id=e.manager_id)").Scan(&orphans); err != nil {
		t.Fatal(err)
	}
	if orphans != 0 {
		t.Errorf("%d rows have a dangling manager_id after fill", orphans)
	}
}

// TestLoadCyclicDeferredSelfCycle loads a self-referential table with a NOT NULL,
// DEFERRABLE FK containing a true cycle — only the deferred strategy can load it
// (NULL-then-fill can't null a NOT NULL column).
func TestLoadCyclicDeferredSelfCycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	src := captureSource(t, ctx)
	sink := sinkTarget(t, ctx)

	// Source FK deferrable so we can insert a 2-cycle; target FK deferrable so
	// the deferred load can commit it.
	srcDDL := "CREATE TABLE rc_it.emp (id int PRIMARY KEY, buddy_id int NOT NULL REFERENCES rc_it.emp(id) DEFERRABLE INITIALLY DEFERRED, name text)"
	tgtDDL := "CREATE TABLE rc_it.emp (id int PRIMARY KEY, buddy_id int NOT NULL REFERENCES rc_it.emp(id) DEFERRABLE INITIALLY IMMEDIATE, name text)"
	setupSourceTables(t, ctx, src, srcDDL)
	mustExecTarget(t, ctx, sink, "CREATE SCHEMA rc_it")
	mustExecTarget(t, ctx, sink, tgtDDL)
	// A 2-cycle plus a self-buddy.
	mustExec(t, ctx, src.conn, "INSERT INTO rc_it.emp VALUES (1, 2, 'a'), (2, 1, 'b'), (3, 3, 'c')")

	ref := engine.TableRef{Schema: "rc_it", Name: "emp"}
	if err := LoadCyclicDeferred(ctx, src, sink, []engine.TableRef{ref}); err != nil {
		t.Fatalf("LoadCyclicDeferred: %v", err)
	}
	sel := "SELECT id::text, buddy_id::text, name FROM rc_it.emp ORDER BY id"
	if !eqLines(dumpText(t, ctx, src.conn, sel), dumpText(t, ctx, sink.conn, sel)) {
		t.Error("deferred load did not reproduce the source faithfully")
	}
}

// TestLoadCyclicDeferredMultiTable loads two tables with mutual DEFERRABLE FKs in
// one deferred transaction.
func TestLoadCyclicDeferredMultiTable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	src := captureSource(t, ctx)
	sink := sinkTarget(t, ctx)

	aDDL := "CREATE TABLE rc_it.a (id int PRIMARY KEY, b_id int NOT NULL, name text)"
	bDDL := "CREATE TABLE rc_it.b (id int PRIMARY KEY, a_id int NOT NULL, name text)"
	fkAB := "ALTER TABLE rc_it.a ADD CONSTRAINT a_b FOREIGN KEY (b_id) REFERENCES rc_it.b(id) DEFERRABLE INITIALLY %s"
	fkBA := "ALTER TABLE rc_it.b ADD CONSTRAINT b_a FOREIGN KEY (a_id) REFERENCES rc_it.a(id) DEFERRABLE INITIALLY %s"

	setupSourceTables(t, ctx, src, aDDL, bDDL,
		fmt.Sprintf(fkAB, "DEFERRED"), fmt.Sprintf(fkBA, "DEFERRED"))
	mustExecTarget(t, ctx, sink, "CREATE SCHEMA rc_it")
	mustExecTarget(t, ctx, sink, aDDL)
	mustExecTarget(t, ctx, sink, bDDL)
	mustExecTarget(t, ctx, sink, fmt.Sprintf(fkAB, "IMMEDIATE"))
	mustExecTarget(t, ctx, sink, fmt.Sprintf(fkBA, "IMMEDIATE"))
	// Insert the mutual cycle within one transaction so the INITIALLY DEFERRED
	// checks pass at commit (both rows present).
	mustExec(t, ctx, src.conn, "BEGIN")
	mustExec(t, ctx, src.conn, "INSERT INTO rc_it.a VALUES (1, 1, 'a1')")
	mustExec(t, ctx, src.conn, "INSERT INTO rc_it.b VALUES (1, 1, 'b1')")
	mustExec(t, ctx, src.conn, "COMMIT")

	a := engine.TableRef{Schema: "rc_it", Name: "a"}
	b := engine.TableRef{Schema: "rc_it", Name: "b"}
	if err := LoadCyclicDeferred(ctx, src, sink, []engine.TableRef{a, b}); err != nil {
		t.Fatalf("LoadCyclicDeferred multi-table: %v", err)
	}
	for _, tbl := range []string{"a", "b"} {
		var n int
		if err := sink.conn.QueryRow(ctx, "SELECT count(*) FROM rc_it."+tbl).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Errorf("table %s has %d rows, want 1", tbl, n)
		}
	}
}
