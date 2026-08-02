package mysql

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

// TestMergeLoadUpsert copies into a NON-empty target via the merge path and
// asserts it upserts idempotently (existing row updated, new row inserted).
func TestMergeLoadUpsert(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	src := connectSource(t, ctx)
	sink := connectSink(t, ctx)

	ddl := "CREATE TABLE rc_it.m (id INT PRIMARY KEY, v VARCHAR(20)) ENGINE=InnoDB"
	mustExec(t, ctx, src.db, ddl)
	mustExec(t, ctx, sink.db, "DROP DATABASE IF EXISTS rc_it", "CREATE DATABASE rc_it", ddl)
	t.Cleanup(func() { _, _ = sink.db.Exec("DROP DATABASE IF EXISTS rc_it") })

	mustExec(t, ctx, src.db, "INSERT INTO rc_it.m VALUES (1,'new1'),(2,'new2')")
	mustExec(t, ctx, sink.db, "INSERT INTO rc_it.m VALUES (1,'old1')") // pre-existing (to be updated)

	ref := engine.TableRef{Schema: "rc_it", Name: "m"}
	tbl, _ := src.tableMeta(ctx, ref)
	cols := transportCols(tbl)
	// Merge-mode copy of the whole table.
	pr, pw := io.Pipe()
	errc := make(chan error, 1)
	go func() { e := streamCols(ctx, src.db, ref, cols, pw); _ = pw.CloseWithError(e); errc <- e }()
	if _, err := sink.BulkLoad(ctx, ref, cols, pr, engine.LoadMerge); err != nil {
		t.Fatalf("merge load: %v", err)
	}
	_ = pr.CloseWithError(nil)
	<-errc

	got := map[int]string{}
	rows, _ := sink.db.QueryContext(ctx, "SELECT id, v FROM rc_it.m ORDER BY id")
	defer rows.Close()
	for rows.Next() {
		var id int
		var v string
		_ = rows.Scan(&id, &v)
		got[id] = v
	}
	if got[1] != "new1" || got[2] != "new2" || len(got) != 2 {
		t.Fatalf("after merge: %+v, want {1:new1, 2:new2}", got)
	}
}

// TestLoadCyclicNullFillSelfRef loads a self-referential nullable-FK table via
// NULL-then-fill and asserts the FK links are restored.
func TestLoadCyclicNullFillSelfRef(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	src := connectSource(t, ctx)
	sink := connectSink(t, ctx)

	ddl := "CREATE TABLE rc_it.emp (id INT PRIMARY KEY, mgr INT NULL, name VARCHAR(20), " +
		"CONSTRAINT fk_mgr FOREIGN KEY (mgr) REFERENCES rc_it.emp(id)) ENGINE=InnoDB"
	mustExec(t, ctx, src.db, ddl)
	mustExec(t, ctx, sink.db, "DROP DATABASE IF EXISTS rc_it", "CREATE DATABASE rc_it", ddl)
	t.Cleanup(func() { _, _ = sink.db.Exec("DROP DATABASE IF EXISTS rc_it") })
	// 1 is top; 2,3 report to 1; 1 reports to nobody.
	mustExec(t, ctx, src.db, "INSERT INTO rc_it.emp VALUES (1,NULL,'ceo'),(2,1,'a'),(3,1,'b')")

	ref := engine.TableRef{Schema: "rc_it", Name: "emp"}
	if err := LoadCyclicNullFill(ctx, src, sink, ref); err != nil {
		t.Fatalf("null-fill: %v", err)
	}
	// Verify mgr links restored and FK holds.
	var n int
	if err := sink.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM rc_it.emp WHERE mgr=1").Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 2 {
		t.Errorf("employees reporting to 1 = %d, want 2 (null-fill did not restore FK)", n)
	}
	var mgr *int
	_ = sink.db.QueryRowContext(ctx, "SELECT mgr FROM rc_it.emp WHERE id=1").Scan(&mgr)
	if mgr != nil {
		t.Errorf("ceo mgr = %v, want NULL", *mgr)
	}
}

// TestLoadCyclicFKChecks loads a NOT NULL mutual cycle via FOREIGN_KEY_CHECKS=0 +
// pre-commit verification, and confirms an orphan is caught pre-commit (rolled
// back, nothing committed).
func TestLoadCyclicFKChecks(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	src := connectSource(t, ctx)
	sink := connectSink(t, ctx)

	setup := []string{
		"CREATE TABLE rc_it.b (id INT PRIMARY KEY, a_id INT NOT NULL) ENGINE=InnoDB",
		"CREATE TABLE rc_it.a (id INT PRIMARY KEY, b_id INT NOT NULL, " +
			"CONSTRAINT fk_ab FOREIGN KEY (b_id) REFERENCES rc_it.b(id)) ENGINE=InnoDB",
		"ALTER TABLE rc_it.b ADD CONSTRAINT fk_ba FOREIGN KEY (a_id) REFERENCES rc_it.a(id)",
	}
	mustExec(t, ctx, src.db, setup...)
	mustExec(t, ctx, sink.db, "DROP DATABASE IF EXISTS rc_it", "CREATE DATABASE rc_it")
	mustExec(t, ctx, sink.db, setup...)
	t.Cleanup(func() { _, _ = sink.db.Exec("DROP DATABASE IF EXISTS rc_it") })
	// Seed the source's mutual cycle with checks off.
	mustExec(t, ctx, src.db, "SET FOREIGN_KEY_CHECKS=0",
		"INSERT INTO rc_it.a VALUES (1,1)", "INSERT INTO rc_it.b VALUES (1,1)",
		"SET FOREIGN_KEY_CHECKS=1")

	a := engine.TableRef{Schema: "rc_it", Name: "a"}
	b := engine.TableRef{Schema: "rc_it", Name: "b"}
	if err := LoadCyclicFKChecks(ctx, src, sink, []engine.TableRef{a, b}); err != nil {
		t.Fatalf("cyclic FK_CHECKS load: %v", err)
	}
	var na, nb int
	_ = sink.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM rc_it.a").Scan(&na)
	_ = sink.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM rc_it.b").Scan(&nb)
	if na != 1 || nb != 1 {
		t.Fatalf("loaded a=%d b=%d, want 1/1", na, nb)
	}

	// Orphan path: loading only `a` (which references b ids not loaded) must be
	// caught pre-commit and rolled back — target `a` stays empty.
	mustExec(t, ctx, sink.db, "SET FOREIGN_KEY_CHECKS=0", "DELETE FROM rc_it.a", "DELETE FROM rc_it.b", "SET FOREIGN_KEY_CHECKS=1")
	err := LoadCyclicFKChecks(ctx, src, sink, []engine.TableRef{a})
	if err == nil {
		t.Fatal("expected an orphan to be caught pre-commit")
	}
	var nAfter int
	_ = sink.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM rc_it.a").Scan(&nAfter)
	if nAfter != 0 {
		t.Errorf("after orphan rollback, a has %d rows, want 0 (nothing committed)", nAfter)
	}
}
