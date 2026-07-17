package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

func TestBuildCtidRanges(t *testing.T) {
	ref := engine.TableRef{Schema: "public", Name: "t"}

	// No boundaries -> single unbounded chunk.
	if chunks := buildCtidRanges(ref, nil); len(chunks) != 1 || chunks[0].Lo != nil || chunks[0].Hi != nil {
		t.Fatalf("no-boundary case = %+v", chunks)
	}

	// Boundaries [10,20] -> [nil,10), [10,20), [20,nil).
	chunks := buildCtidRanges(ref, []int{10, 20})
	if len(chunks) != 3 {
		t.Fatalf("expected 3 chunks, got %d", len(chunks))
	}
	if chunks[0].Lo != nil || chunks[2].Hi != nil {
		t.Error("ends should be unbounded")
	}
	if chunks[0].Method != engine.ChunkCTID {
		t.Errorf("method = %q", chunks[0].Method)
	}
	if b, _ := chunks[1].Lo[0].(int); b != 10 {
		t.Errorf("middle Lo = %v, want 10", chunks[1].Lo[0])
	}
	if b, _ := chunks[1].Hi[0].(int); b != 20 {
		t.Errorf("middle Hi = %v, want 20", chunks[1].Hi[0])
	}
}

func TestCtidPredicate(t *testing.T) {
	if p, _ := ctidPredicate(nil, nil); p != "TRUE" {
		t.Errorf("unbounded = %q", p)
	}
	if p, _ := ctidPredicate(nil, engine.KeyValues{10}); p != "ctid < '(10,0)'::tid" {
		t.Errorf("upper-only = %q", p)
	}
	if p, _ := ctidPredicate(engine.KeyValues{10}, nil); p != "ctid >= '(10,0)'::tid" {
		t.Errorf("lower-only = %q", p)
	}
	if p, _ := ctidPredicate(engine.KeyValues{10}, engine.KeyValues{20}); p != "ctid >= '(10,0)'::tid AND ctid < '(20,0)'::tid" {
		t.Errorf("bounded = %q", p)
	}
}

func TestPlanCtidChunksIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	src := captureSource(t, ctx)
	setupSourceTables(t, ctx, src, "CREATE TABLE rc_it.log (a int, b text)") // keyless
	mustExec(t, ctx, src.conn, "INSERT INTO rc_it.log SELECT g, 'v'||g FROM generate_series(1,5000) g")
	mustExec(t, ctx, src.conn, "ANALYZE rc_it.log")

	ref := engine.TableRef{Schema: "rc_it", Name: "log"}
	chunks, err := src.PlanChunks(ctx, ref, engine.ChunkOptions{Method: engine.ChunkCTID, TargetRows: 1000})
	if err != nil {
		t.Fatalf("PlanChunks ctid: %v", err)
	}
	if len(chunks) < 2 {
		t.Fatalf("expected multiple ctid chunks for 5000 rows, got %d", len(chunks))
	}
	for _, c := range chunks {
		if c.Method != engine.ChunkCTID {
			t.Errorf("chunk method = %q, want ctid", c.Method)
		}
	}
	if chunks[0].Lo != nil || chunks[len(chunks)-1].Hi != nil {
		t.Error("ends should be unbounded")
	}
}

func TestPlanChunksAutoFallbackToCtid(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	src := captureSource(t, ctx)
	setupSourceTables(t, ctx, src, "CREATE TABLE rc_it.log (a int, b text)") // keyless
	mustExec(t, ctx, src.conn, "INSERT INTO rc_it.log SELECT g, 'v'||g FROM generate_series(1,3000) g")
	mustExec(t, ctx, src.conn, "ANALYZE rc_it.log")

	ref := engine.TableRef{Schema: "rc_it", Name: "log"}
	// Default (keyset) method must auto-fall back to ctid for a keyless table.
	chunks, err := src.PlanChunks(ctx, ref, engine.ChunkOptions{TargetRows: 1000})
	if err != nil {
		t.Fatalf("PlanChunks default on keyless: %v", err)
	}
	if len(chunks) == 0 || chunks[0].Method != engine.ChunkCTID {
		t.Fatalf("keyless table should fall back to ctid, got %+v", chunks)
	}
}

func TestCopyChunkCtidFaithful(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	src := captureSource(t, ctx)
	sink := sinkTarget(t, ctx)

	ddl := "CREATE TABLE rc_it.log (a int, b text)"
	setupSourceTables(t, ctx, src, ddl)
	mustExecTarget(t, ctx, sink, "CREATE SCHEMA rc_it")
	mustExecTarget(t, ctx, sink, ddl)

	mustExec(t, ctx, src.conn, "INSERT INTO rc_it.log SELECT g, 'v'||g FROM generate_series(1,5000) g")
	mustExec(t, ctx, src.conn, "ANALYZE rc_it.log")

	ref := engine.TableRef{Schema: "rc_it", Name: "log"}
	table, _ := src.tableMeta(ctx, ref)
	cols := transportColumns(table)

	chunks, err := src.PlanChunks(ctx, ref, engine.ChunkOptions{Method: engine.ChunkCTID, TargetRows: 800})
	if err != nil {
		t.Fatalf("PlanChunks: %v", err)
	}
	for _, c := range chunks {
		copyOneChunk(t, ctx, src, sink, ref, cols, c)
	}

	// Exact coverage: every row copied once. Order by (a) for comparison.
	sel := "SELECT a::text, b FROM rc_it.log ORDER BY a"
	srcDump := dumpText(t, ctx, src.conn, sel)
	tgtDump := dumpText(t, ctx, sink.conn, sel)
	if len(srcDump) != 5000 {
		t.Fatalf("source rows = %d, want 5000", len(srcDump))
	}
	if !eqLines(srcDump, tgtDump) {
		t.Errorf("ctid copy not faithful: %d source vs %d target rows", len(srcDump), len(tgtDump))
	}
}
