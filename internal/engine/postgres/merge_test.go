package postgres

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

func TestMergeInsertSQL(t *testing.T) {
	pk := []captureCol{{"id", "integer"}}
	sql := mergeInsertSQL(
		engine.TableRef{Schema: "public", Name: "orders"},
		"replicare_stg",
		[]string{"id", "note", "amount"}, pk, colSetOf(pk), false)
	contains(t, sql, `INSERT INTO "public"."orders" ("id", "note", "amount") SELECT`)
	contains(t, sql, `FROM "replicare_stg" ON CONFLICT ("id")`)
	contains(t, sql, `DO UPDATE SET "note" = EXCLUDED."note", "amount" = EXCLUDED."amount"`)
}

func TestMergeInsertSQLKeyOnlyDoNothing(t *testing.T) {
	// A table whose columns are all part of the key -> nothing to update.
	pk := []captureCol{{"a", "integer"}, {"b", "integer"}}
	sql := mergeInsertSQL(engine.TableRef{Schema: "public", Name: "t"}, "stg",
		[]string{"a", "b"}, pk, colSetOf(pk), false)
	contains(t, sql, "ON CONFLICT (\"a\", \"b\") DO NOTHING")
}

func TestMergeInsertSQLIdentityOverriding(t *testing.T) {
	pk := []captureCol{{"id", "bigint"}}
	sql := mergeInsertSQL(engine.TableRef{Schema: "public", Name: "t"}, "stg",
		[]string{"id", "v"}, pk, colSetOf(pk), true)
	contains(t, sql, `("id", "v") OVERRIDING SYSTEM VALUE SELECT`)
}

// copyOneChunkMode pipes one chunk with an explicit load mode.
func copyOneChunkMode(t *testing.T, ctx context.Context, src *Source, sink *Sink, ref engine.TableRef, cols []string, chunk engine.Chunk, mode engine.LoadMode) {
	t.Helper()
	pr, pw := io.Pipe()
	errc := make(chan error, 1)
	go func() {
		err := src.CopyChunk(ctx, chunk, pw)
		_ = pw.CloseWithError(err)
		errc <- err
	}()
	loadErr := sink.BulkLoad(ctx, ref, cols, pr, mode)
	_ = pr.CloseWithError(loadErr)
	if copyErr := <-errc; copyErr != nil {
		t.Fatalf("CopyChunk: %v", copyErr)
	}
	if loadErr != nil {
		t.Fatalf("BulkLoad(merge): %v", loadErr)
	}
}

func TestBulkLoadMergeIntoNonEmptyTarget(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	src := captureSource(t, ctx)
	sink := sinkTarget(t, ctx)

	ddl := "CREATE TABLE rc_it.orders (id int PRIMARY KEY, note text, amount numeric(10,2))"
	setupSourceTables(t, ctx, src, ddl)
	mustExecTarget(t, ctx, sink, "CREATE SCHEMA rc_it")
	mustExecTarget(t, ctx, sink, ddl)

	// Source is authoritative: ids 1..30.
	mustExec(t, ctx, src.conn, "INSERT INTO rc_it.orders SELECT g, 'note '||g, (g*1.5)::numeric(10,2) FROM generate_series(1,30) g")
	// Target starts NON-empty with stale values for ids 1..10 (must be updated)
	// and an id 5 with a wrong note (conflict -> update).
	mustExecTarget(t, ctx, sink, "INSERT INTO rc_it.orders SELECT g, 'STALE', 0 FROM generate_series(1,10) g")

	ref := engine.TableRef{Schema: "rc_it", Name: "orders"}
	table, _ := src.tableMeta(ctx, ref)
	cols := transportColumns(table)
	chunks, err := src.PlanChunks(ctx, ref, engine.ChunkOptions{TargetRows: 8})
	if err != nil {
		t.Fatalf("PlanChunks: %v", err)
	}
	for _, c := range chunks {
		copyOneChunkMode(t, ctx, src, sink, ref, cols, c, engine.LoadMerge)
	}

	// Target converged to source: 1..10 updated (no dup-key errors), 11..30
	// inserted.
	var n int
	if err := sink.conn.QueryRow(ctx, "SELECT count(*) FROM rc_it.orders").Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 30 {
		t.Fatalf("target has %d rows, want 30", n)
	}
	sel := "SELECT id::text, note, amount::text FROM rc_it.orders ORDER BY id"
	if !eqLines(dumpText(t, ctx, src.conn, sel), dumpText(t, ctx, sink.conn, sel)) {
		t.Error("merge did not converge the target to the source")
	}
	// No STALE rows remain.
	var stale int
	if err := sink.conn.QueryRow(ctx, "SELECT count(*) FROM rc_it.orders WHERE note='STALE'").Scan(&stale); err != nil {
		t.Fatalf("stale count: %v", err)
	}
	if stale != 0 {
		t.Errorf("%d stale rows remain after merge", stale)
	}
}
