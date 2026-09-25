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

// tgtCount is a small scan helper for the populated-target tests below.
func tgtCount(t *testing.T, ctx context.Context, sink *Sink, q string) int {
	t.Helper()
	var n int
	if err := sink.conn.QueryRow(ctx, q).Scan(&n); err != nil {
		t.Fatalf("count %q: %v", q, err)
	}
	return n
}

// TestLoadCyclicNullFillIdempotentPrepopulated is the cyclic-path twin of the chunked
// idempotent-copy fix: a NULL-fill copy into a target that ALREADY holds these rows (a
// restart/reschedule after a lost state store, or a re-run) must converge, not collide
// on the PK. Before the fix pass 1 did a direct COPY and errored with
// `duplicate key value violates unique constraint "..._pkey" (23505)`.
func TestLoadCyclicNullFillIdempotentPrepopulated(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	src := captureSource(t, ctx)
	sink := sinkTarget(t, ctx)

	ddl := "CREATE TABLE rc_it.emp (id int PRIMARY KEY, manager_id int REFERENCES rc_it.emp(id), name text)"
	setupSourceTables(t, ctx, src, ddl)
	mustExecTarget(t, ctx, sink, "CREATE SCHEMA rc_it")
	mustExecTarget(t, ctx, sink, ddl)
	mustExec(t, ctx, src.conn, "INSERT INTO rc_it.emp SELECT g, NULLIF(g-1,0), 'e'||g FROM generate_series(1,30) g")
	// Pre-existing, previously-replicated rows (stale name; manager_id NULL to avoid an
	// FK on the seed itself) — the exact "populated target" the copy must tolerate.
	mustExecTarget(t, ctx, sink, "INSERT INTO rc_it.emp SELECT g, NULL, 'stale' FROM generate_series(1,20) g")

	ref := engine.TableRef{Schema: "rc_it", Name: "emp"}
	if err := LoadCyclicNullFill(ctx, src, sink, ref); err != nil {
		t.Fatalf("LoadCyclicNullFill into a populated target: %v", err)
	}
	sel := "SELECT id::text, coalesce(manager_id::text,'<n>'), name FROM rc_it.emp ORDER BY id"
	if !eqLines(dumpText(t, ctx, src.conn, sel), dumpText(t, ctx, sink.conn, sel)) {
		t.Error("idempotent null-fill did not converge to source over a populated target")
	}
	if stale := tgtCount(t, ctx, sink, "SELECT count(*) FROM rc_it.emp WHERE name='stale'"); stale != 0 {
		t.Errorf("%d stale rows survived — upsert did not correct pre-existing values", stale)
	}
	if orphans := tgtCount(t, ctx, sink,
		"SELECT count(*) FROM rc_it.emp e WHERE e.manager_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM rc_it.emp m WHERE m.id=e.manager_id)"); orphans != 0 {
		t.Errorf("%d dangling manager_id after fill", orphans)
	}
}

// TestCopyCyclicComponentNullFillPrepopulated reproduces the reported failure exactly:
// a multi-table nullable FK cycle copied via CopyCyclicComponent (-> nullFillComponent)
// into a PRE-POPULATED target. Before the fix: `cyclic null-fill pass 1 (...): ERROR:
// duplicate key value violates unique constraint (23505)`.
func TestCopyCyclicComponentNullFillPrepopulated(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	src := captureSource(t, ctx)
	sink := sinkTarget(t, ctx)

	aDDL := "CREATE TABLE rc_it.a (id int PRIMARY KEY, b_id int, name text)"
	bDDL := "CREATE TABLE rc_it.b (id int PRIMARY KEY, a_id int, name text)"
	fkAB := "ALTER TABLE rc_it.a ADD CONSTRAINT a_b FOREIGN KEY (b_id) REFERENCES rc_it.b(id)"
	fkBA := "ALTER TABLE rc_it.b ADD CONSTRAINT b_a FOREIGN KEY (a_id) REFERENCES rc_it.a(id)"
	setupSourceTables(t, ctx, src, aDDL, bDDL, fkAB, fkBA)
	mustExecTarget(t, ctx, sink, "CREATE SCHEMA rc_it")
	for _, d := range []string{aDDL, bDDL, fkAB, fkBA} {
		mustExecTarget(t, ctx, sink, d)
	}
	// Source: nullable mutual cycle, seeded NULL-then-filled (non-deferrable FKs).
	mustExec(t, ctx, src.conn, "INSERT INTO rc_it.a SELECT g, NULL, 'a'||g FROM generate_series(1,5) g")
	mustExec(t, ctx, src.conn, "INSERT INTO rc_it.b SELECT g, NULL, 'b'||g FROM generate_series(1,5) g")
	mustExec(t, ctx, src.conn, "UPDATE rc_it.a SET b_id = id")
	mustExec(t, ctx, src.conn, "UPDATE rc_it.b SET a_id = id")
	// Pre-existing target rows (stale, FK cols NULL).
	mustExecTarget(t, ctx, sink, "INSERT INTO rc_it.a SELECT g, NULL, 'stale' FROM generate_series(1,3) g")
	mustExecTarget(t, ctx, sink, "INSERT INTO rc_it.b SELECT g, NULL, 'stale' FROM generate_series(1,3) g")

	aRef := engine.TableRef{Schema: "rc_it", Name: "a"}
	bRef := engine.TableRef{Schema: "rc_it", Name: "b"}
	if err := sink.CopyCyclicComponent(ctx, src, []engine.TableRef{aRef, bRef}); err != nil {
		t.Fatalf("CopyCyclicComponent into a populated target: %v", err)
	}
	for _, tbl := range []string{"a", "b"} {
		fk := "b_id"
		if tbl == "b" {
			fk = "a_id"
		}
		sel := fmt.Sprintf("SELECT id::text, coalesce(%s::text,'<n>'), name FROM rc_it.%s ORDER BY id", fk, tbl)
		if !eqLines(dumpText(t, ctx, src.conn, sel), dumpText(t, ctx, sink.conn, sel)) {
			t.Errorf("table %s did not converge to source over a populated target", tbl)
		}
		if stale := tgtCount(t, ctx, sink, "SELECT count(*) FROM rc_it."+tbl+" WHERE name='stale'"); stale != 0 {
			t.Errorf("table %s: %d stale rows survived", tbl, stale)
		}
	}
}

// TestLoadCyclicDeferredIdempotentPrepopulated covers the deferred strategy's copy into
// a populated target (staging+upsert inside the one deferred txn).
func TestLoadCyclicDeferredIdempotentPrepopulated(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	src := captureSource(t, ctx)
	sink := sinkTarget(t, ctx)

	srcDDL := "CREATE TABLE rc_it.emp (id int PRIMARY KEY, buddy_id int NOT NULL REFERENCES rc_it.emp(id) DEFERRABLE INITIALLY DEFERRED, name text)"
	tgtDDL := "CREATE TABLE rc_it.emp (id int PRIMARY KEY, buddy_id int NOT NULL REFERENCES rc_it.emp(id) DEFERRABLE INITIALLY IMMEDIATE, name text)"
	setupSourceTables(t, ctx, src, srcDDL)
	mustExecTarget(t, ctx, sink, "CREATE SCHEMA rc_it")
	mustExecTarget(t, ctx, sink, tgtDDL)
	mustExec(t, ctx, src.conn, "INSERT INTO rc_it.emp VALUES (1, 2, 'a'), (2, 1, 'b'), (3, 3, 'c')")
	// Pre-existing target rows (stale name), seeded inside a deferred txn so the NOT NULL
	// cyclic FK holds at commit.
	mustExecTarget(t, ctx, sink, "BEGIN")
	mustExecTarget(t, ctx, sink, "SET CONSTRAINTS ALL DEFERRED")
	mustExecTarget(t, ctx, sink, "INSERT INTO rc_it.emp VALUES (1, 2, 'stale'), (2, 1, 'stale')")
	mustExecTarget(t, ctx, sink, "COMMIT")

	ref := engine.TableRef{Schema: "rc_it", Name: "emp"}
	if err := LoadCyclicDeferred(ctx, src, sink, []engine.TableRef{ref}); err != nil {
		t.Fatalf("LoadCyclicDeferred into a populated target: %v", err)
	}
	sel := "SELECT id::text, buddy_id::text, name FROM rc_it.emp ORDER BY id"
	if !eqLines(dumpText(t, ctx, src.conn, sel), dumpText(t, ctx, sink.conn, sel)) {
		t.Error("deferred load did not converge to source over a populated target")
	}
	if stale := tgtCount(t, ctx, sink, "SELECT count(*) FROM rc_it.emp WHERE name='stale'"); stale != 0 {
		t.Errorf("%d stale rows survived deferred upsert", stale)
	}
}
