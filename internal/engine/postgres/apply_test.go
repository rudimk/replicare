package postgres

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

// applyOnePass pipes RereadCurrent -> ApplyPass for a set of dirty keys.
func applyOnePass(t *testing.T, ctx context.Context, src *Source, sink *Sink, ref engine.TableRef, cols []string, dirtyKeys []engine.KeyValues) {
	t.Helper()
	pr, pw := io.Pipe()
	errc := make(chan error, 1)
	go func() {
		err := src.RereadCurrent(ctx, ref, dirtyKeys, pw)
		_ = pw.CloseWithError(err)
		errc <- err
	}()
	applyErr := sink.ApplyPass(ctx, ref, cols, dirtyKeys, pr)
	_ = pr.CloseWithError(applyErr)
	if rereadErr := <-errc; rereadErr != nil {
		t.Fatalf("RereadCurrent: %v", rereadErr)
	}
	if applyErr != nil {
		t.Fatalf("ApplyPass: %v", applyErr)
	}
}

func TestApplyPassInsertUpdateDelete(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	src := captureSource(t, ctx)
	sink := sinkTarget(t, ctx)

	ddl := "CREATE TABLE rc_it.orders (id int PRIMARY KEY, note text)"
	setupSourceTables(t, ctx, src, ddl)
	mustExecTarget(t, ctx, sink, "CREATE SCHEMA rc_it")
	mustExecTarget(t, ctx, sink, ddl)

	// Target before: 1,2 stale; 3 untouched (not dirty).
	mustExecTarget(t, ctx, sink, "INSERT INTO rc_it.orders VALUES (1,'old'),(2,'old'),(3,'keep')")
	// Source current: 1 updated, 2 deleted (absent), 3 same, 4 new.
	mustExec(t, ctx, src.conn, "INSERT INTO rc_it.orders VALUES (1,'v1new'),(3,'keep'),(4,'new')")

	ref := engine.TableRef{Schema: "rc_it", Name: "orders"}
	table, _ := src.tableMeta(ctx, ref)
	cols := transportColumns(table)

	// Dirty keys from a drain: 1 (update), 2 (delete), 4 (insert).
	dirty := []engine.KeyValues{{"1"}, {"2"}, {"4"}}
	applyOnePass(t, ctx, src, sink, ref, cols, dirty)

	// Target converged: 1 updated, 4 inserted, 2 deleted, 3 untouched.
	sel := "SELECT id::text, note FROM rc_it.orders ORDER BY id"
	got := dumpText(t, ctx, sink.conn, sel)
	want := []string{"1|v1new", "3|keep", "4|new"}
	if !eqLines(got, want) {
		t.Errorf("target after apply = %v, want %v", got, want)
	}
}

func TestApplyPassAllDeletes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	src := captureSource(t, ctx)
	sink := sinkTarget(t, ctx)

	ddl := "CREATE TABLE rc_it.orders (id int PRIMARY KEY, note text)"
	setupSourceTables(t, ctx, src, ddl)
	mustExecTarget(t, ctx, sink, "CREATE SCHEMA rc_it")
	mustExecTarget(t, ctx, sink, ddl)
	mustExecTarget(t, ctx, sink, "INSERT INTO rc_it.orders VALUES (1,'a'),(2,'b')")
	// Source has neither row (both deleted).

	ref := engine.TableRef{Schema: "rc_it", Name: "orders"}
	table, _ := src.tableMeta(ctx, ref)
	cols := transportColumns(table)
	applyOnePass(t, ctx, src, sink, ref, cols, []engine.KeyValues{{"1"}, {"2"}})

	var n int
	if err := sink.conn.QueryRow(ctx, "SELECT count(*) FROM rc_it.orders").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("expected all rows deleted, got %d", n)
	}
}

func TestApplyPassIdempotent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	src := captureSource(t, ctx)
	sink := sinkTarget(t, ctx)

	ddl := "CREATE TABLE rc_it.orders (id int PRIMARY KEY, note text)"
	setupSourceTables(t, ctx, src, ddl)
	mustExecTarget(t, ctx, sink, "CREATE SCHEMA rc_it")
	mustExecTarget(t, ctx, sink, ddl)
	mustExec(t, ctx, src.conn, "INSERT INTO rc_it.orders VALUES (1,'x'),(2,'y')")

	ref := engine.TableRef{Schema: "rc_it", Name: "orders"}
	table, _ := src.tableMeta(ctx, ref)
	cols := transportColumns(table)
	dirty := []engine.KeyValues{{"1"}, {"2"}}

	// Applying the same pass twice must converge to the same state (at-least-once).
	applyOnePass(t, ctx, src, sink, ref, cols, dirty)
	applyOnePass(t, ctx, src, sink, ref, cols, dirty)

	sel := "SELECT id::text, note FROM rc_it.orders ORDER BY id"
	if !eqLines(dumpText(t, ctx, sink.conn, sel), []string{"1|x", "2|y"}) {
		t.Error("re-applying the same pass should be idempotent")
	}
}
