package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/rudimk/replicare/internal/engine"
)

func TestClearDeleteSQL(t *testing.T) {
	if got := clearDeleteSQL([]string{`"s"."a"`}); got != `DELETE FROM "s"."a"` {
		t.Errorf("one table: %s", got)
	}
	want := `WITH d0 AS (DELETE FROM "s"."a"), d1 AS (DELETE FROM "s"."b") DELETE FROM "s"."c"`
	if got := clearDeleteSQL([]string{`"s"."a"`, `"s"."b"`, `"s"."c"`}); got != want {
		t.Errorf("three tables:\n got %s\nwant %s", got, want)
	}
}

// TestWithPgDetail: the DETAIL line (which names the offending key) is appended once and
// the error stays matchable; transient FK classification still sees through it.
func TestWithPgDetail(t *testing.T) {
	pe := &pgconn.PgError{Code: "23505", Message: `duplicate key value violates unique constraint "u"`, Detail: "Key (email, parent)=(a, 51) already exists."}
	err := withPgDetail(fmt.Errorf("upsert: %w", pe))
	if !strings.Contains(err.Error(), "Key (email, parent)=(a, 51) already exists.") {
		t.Fatalf("detail missing: %v", err)
	}
	var got *pgconn.PgError
	if !errors.As(err, &got) || got.Code != "23505" {
		t.Fatalf("lost the PgError: %v", err)
	}
	if again := withPgDetail(err); again.Error() != err.Error() {
		t.Errorf("detail appended twice: %v", again)
	}
	if withPgDetail(nil) != nil {
		t.Error("nil should stay nil")
	}
	fk := &pgconn.PgError{Code: "23503", Message: "fk", Detail: "Key (a_id)=(9) is not present in table \"a\"."}
	if c := classifyFKViolation(fk); !engine.IsTransientConstraint(c) || !strings.Contains(c.Error(), "is not present") {
		t.Errorf("FK violation should stay transient and carry its detail: %v", c)
	}
}

// TestCyclicCopyClearsStaleLoopWithoutTruncate: a loop across two tables, the target
// holding a stale earlier copy (including a row the source no longer has), and a target
// role without the TRUNCATE privilege. Clearing takes the single DELETE statement, and
// the copy converges with the stale rows gone.
func TestCyclicCopyClearsStaleLoopWithoutTruncate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	clearTryTruncate = false
	defer func() { clearTryTruncate = true }()
	src := captureSource(t, ctx)
	sink := sinkTarget(t, ctx)

	aDDL := "CREATE TABLE rc_it.a (id int PRIMARY KEY, b_id int, name text)"
	bDDL := "CREATE TABLE rc_it.b (id int PRIMARY KEY, a_id int, name text)"
	fkAB := "ALTER TABLE rc_it.a ADD CONSTRAINT a_b FOREIGN KEY (b_id) REFERENCES rc_it.b(id) ON DELETE RESTRICT"
	fkBA := "ALTER TABLE rc_it.b ADD CONSTRAINT b_a FOREIGN KEY (a_id) REFERENCES rc_it.a(id)"
	setupSourceTables(t, ctx, src, aDDL, bDDL, fkAB, fkBA)
	mustExecTarget(t, ctx, sink, "CREATE SCHEMA rc_it")
	for _, d := range []string{aDDL, bDDL, fkAB, fkBA} {
		mustExecTarget(t, ctx, sink, d)
	}
	mustExec(t, ctx, src.conn, "INSERT INTO rc_it.a SELECT g, NULL, 'a'||g FROM generate_series(1,5) g")
	mustExec(t, ctx, src.conn, "INSERT INTO rc_it.b SELECT g, NULL, 'b'||g FROM generate_series(1,5) g")
	mustExec(t, ctx, src.conn, "UPDATE rc_it.a SET b_id = id")
	mustExec(t, ctx, src.conn, "UPDATE rc_it.b SET a_id = id")
	// Stale earlier copy on the target, links filled, plus a pair the source no longer has.
	mustExecTarget(t, ctx, sink, "INSERT INTO rc_it.a SELECT g, NULL, 'stale' FROM generate_series(1,9) g")
	mustExecTarget(t, ctx, sink, "INSERT INTO rc_it.b SELECT g, g, 'stale' FROM generate_series(1,9) g")
	mustExecTarget(t, ctx, sink, "UPDATE rc_it.a SET b_id = id")

	aRef, bRef := ref("rc_it.a"), ref("rc_it.b")
	if err := sink.CopyCyclicComponent(ctx, src, []engine.TableRef{aRef, bRef}, nil); err != nil {
		t.Fatalf("CopyCyclicComponent over a stale loop: %v", err)
	}
	for _, q := range []string{
		"SELECT id::text, coalesce(b_id::text,'<n>'), name FROM rc_it.a ORDER BY id",
		"SELECT id::text, coalesce(a_id::text,'<n>'), name FROM rc_it.b ORDER BY id",
	} {
		if !eqLines(dumpText(t, ctx, src.conn, q), dumpText(t, ctx, sink.conn, q)) {
			t.Errorf("did not converge: %s", q)
		}
	}
}

// TestClearForReloadSkipsOutsideReference: a target table outside the component points
// into it, so the tables are left alone and the copy merges as before.
func TestClearForReloadSkipsOutsideReference(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	src, sink := setupAcct(t, ctx, acctSrcConstraintDDL, acctTgtConstraintDDL, "ASC")
	acct := ref("rc_it.acct")
	if err := sink.CopyCyclicComponent(ctx, src, []engine.TableRef{acct}, nil); err != nil {
		t.Fatalf("first copy: %v", err)
	}
	mustExecTarget(t, ctx, sink, "CREATE TABLE rc_it.note (id int PRIMARY KEY, acct_id int REFERENCES rc_it.acct(id) ON DELETE CASCADE)")
	mustExecTarget(t, ctx, sink, "INSERT INTO rc_it.note VALUES (1, 150)")

	cleared, err := sink.clearComponentForReload(ctx, []engine.TableRef{acct})
	if err != nil || cleared {
		t.Fatalf("clearComponentForReload = %v, %v; want false, nil", cleared, err)
	}
	if err := sink.CopyCyclicComponent(ctx, src, []engine.TableRef{acct}, nil); err != nil {
		t.Fatalf("copy (merge path): %v", err)
	}
	requireConverged(t, ctx, src, sink)
	if n := tgtCount(t, ctx, sink, "SELECT count(*) FROM rc_it.note"); n != 1 {
		t.Errorf("a row outside the component was touched: %d rows left, want 1", n)
	}
}

// TestClearForReloadSkipsMeshMember: on an active-active node the target's rows are a
// peer's own data, never leftovers to clear.
func TestClearForReloadSkipsMeshMember(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	sink := sinkTarget(t, ctx)
	mustExecTarget(t, ctx, sink, "CREATE SCHEMA rc_it")
	for _, s := range acctTgtConstraintDDL {
		mustExecTarget(t, ctx, sink, s)
	}
	mustExecTarget(t, ctx, sink, "INSERT INTO rc_it.acct VALUES (1,'a',NULL,'root')")
	sink.origin = true
	defer func() { sink.origin = false }()
	cleared, err := sink.clearComponentForReload(ctx, []engine.TableRef{ref("rc_it.acct")})
	if err != nil || cleared {
		t.Fatalf("clearComponentForReload on a mesh member = %v, %v; want false, nil", cleared, err)
	}
	if n := tgtCount(t, ctx, sink, "SELECT count(*) FROM rc_it.acct"); n != 1 {
		t.Errorf("mesh member's rows were cleared: %d left, want 1", n)
	}
}
