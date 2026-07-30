package postgres

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/rudimk/replicare/internal/engine"
)

// sinkTarget connects a Sink to the harness target and cleans the rc_it schema.
func sinkTarget(t *testing.T, ctx context.Context) *Sink {
	t.Helper()
	sink := &Sink{cfg: harnessConn(t, "target")}
	if err := sink.Connect(ctx); err != nil {
		t.Fatalf("connect target: %v", err)
	}
	if _, err := sink.conn.Exec(ctx, "DROP SCHEMA IF EXISTS rc_it CASCADE"); err != nil {
		t.Fatalf("clean target: %v", err)
	}
	t.Cleanup(func() {
		bg := context.Background()
		_, _ = sink.conn.Exec(bg, "DROP SCHEMA IF EXISTS rc_it CASCADE")
		_ = sink.Close(bg)
	})
	return sink
}

func mustExecTarget(t *testing.T, ctx context.Context, sink *Sink, sql string) {
	t.Helper()
	if _, err := sink.conn.Exec(ctx, sql); err != nil {
		t.Fatalf("target exec: %v\nSQL: %s", err, sql)
	}
}

// copyOneChunk pipes a single chunk source->target via io.Pipe.
func copyOneChunk(t *testing.T, ctx context.Context, src *Source, sink *Sink, ref engine.TableRef, cols []string, chunk engine.Chunk) {
	t.Helper()
	pr, pw := io.Pipe()
	errc := make(chan error, 1)
	go func() {
		err := src.CopyChunk(ctx, chunk, pw)
		_ = pw.CloseWithError(err)
		errc <- err
	}()
	_, loadErr := sink.BulkLoad(ctx, ref, cols, pr, engine.LoadDirect)
	_ = pr.CloseWithError(loadErr)
	copyErr := <-errc
	if copyErr != nil {
		t.Fatalf("CopyChunk: %v", copyErr)
	}
	if loadErr != nil {
		t.Fatalf("BulkLoad: %v", loadErr)
	}
}

// dumpText returns every row's columns rendered as text for a faithful
// source/target comparison.
func dumpText(t *testing.T, ctx context.Context, conn *pgx.Conn, selectSQL string) []string {
	t.Helper()
	rows, err := conn.Query(ctx, selectSQL)
	if err != nil {
		t.Fatalf("dump query: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			t.Fatalf("dump scan: %v", err)
		}
		line := ""
		for i, v := range vals {
			if i > 0 {
				line += "|"
			}
			if v == nil {
				line += "<NULL>"
			} else {
				line += v.(string)
			}
		}
		out = append(out, line)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("dump rows: %v", err)
	}
	return out
}

func eqLines(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestCopyChunkPipeFaithful(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	src := captureSource(t, ctx)
	sink := sinkTarget(t, ctx)

	ddl := `CREATE TABLE rc_it.orders (
		id int PRIMARY KEY,
		note text,
		amount numeric(12,2),
		created timestamptz
	)`
	setupSourceTables(t, ctx, src, ddl)
	mustExecTarget(t, ctx, sink, "CREATE SCHEMA rc_it")
	mustExecTarget(t, ctx, sink, ddl)

	// Varied rows incl. NULLs, quotes, and fractional numerics/timestamps.
	mustExec(t, ctx, src.conn, `INSERT INTO rc_it.orders
		SELECT g,
		       CASE WHEN g%5=0 THEN NULL ELSE 'note '''||g END,
		       (g * 1.25)::numeric(12,2),
		       timestamptz '2020-01-01 00:00:00Z' + (g || ' hours')::interval
		FROM generate_series(1, 47) g`)

	ref := engine.TableRef{Schema: "rc_it", Name: "orders"}
	table, err := src.tableMeta(ctx, ref)
	if err != nil {
		t.Fatalf("tableMeta: %v", err)
	}
	cols := transportColumns(table)

	chunks, err := src.PlanChunks(ctx, ref, engine.ChunkOptions{TargetRows: 10})
	if err != nil {
		t.Fatalf("PlanChunks: %v", err)
	}
	if len(chunks) < 3 {
		t.Fatalf("expected several chunks, got %d", len(chunks))
	}
	for _, c := range chunks {
		copyOneChunk(t, ctx, src, sink, ref, cols, c)
	}

	// Faithful comparison: every column, as text, ordered by PK.
	sel := "SELECT id::text, coalesce(note,'<n>'), amount::text, created::text FROM rc_it.orders ORDER BY id"
	srcDump := dumpText(t, ctx, src.conn, sel)
	tgtDump := dumpText(t, ctx, sink.conn, sel)
	if len(srcDump) != 47 {
		t.Fatalf("source has %d rows, want 47", len(srcDump))
	}
	if !eqLines(srcDump, tgtDump) {
		t.Errorf("target is not byte-faithful to source\nfirst source: %v\nfirst target: %v", first(srcDump), first(tgtDump))
	}
}

// TestTransportColumns is a pure unit test: GENERATED STORED columns are
// excluded from transport (the target regenerates them) while identity and
// ordinary columns are transported. (Generated columns are a PG12+ feature, so
// this cannot be exercised against the 9.6 source.)
func TestTransportColumns(t *testing.T) {
	table := engine.Table{
		Ref: ref("public.g"),
		Columns: []engine.Column{
			{Name: "id", DataType: "integer", Identity: true},
			{Name: "w", DataType: "integer"},
			{Name: "area", DataType: "integer", Generated: true},
		},
	}
	got := transportColumns(table)
	want := []string{"id", "w"}
	if !eqLines(got, want) {
		t.Errorf("transportColumns = %v, want %v (generated excluded, identity kept)", got, want)
	}
}

func first(s []string) string {
	if len(s) == 0 {
		return "<empty>"
	}
	return s[0]
}
