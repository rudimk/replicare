package postgres

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

// TestRunChunksSharesOutWork: every chunk runs exactly once, the work is spread over
// more than one connection pair, and an error stops further chunks from starting.
func TestRunChunksSharesOutWork(t *testing.T) {
	conns := []copyConn{{sink: &Sink{}}, {sink: &Sink{}}, {sink: &Sink{}}}
	chunks := make([]engine.Chunk, 40)
	for i := range chunks {
		chunks[i] = engine.Chunk{Table: engine.TableRef{Name: strconv.Itoa(i)}}
	}
	var mu sync.Mutex
	ran := map[string]int{}
	used := map[*Sink]bool{}
	err := runChunks(context.Background(), conns, chunks, func(_ context.Context, c copyConn, ch engine.Chunk) error {
		time.Sleep(time.Millisecond)
		mu.Lock()
		defer mu.Unlock()
		ran[ch.Table.Name]++
		used[c.sink] = true
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := range chunks {
		if n := ran[strconv.Itoa(i)]; n != 1 {
			t.Errorf("chunk %d ran %d times, want 1", i, n)
		}
	}
	if len(used) < 2 {
		t.Errorf("only %d connection pair(s) used, want the work shared out", len(used))
	}

	boom := errors.New("boom")
	started := 0
	err = runChunks(context.Background(), conns, chunks, func(_ context.Context, _ copyConn, ch engine.Chunk) error {
		mu.Lock()
		started++
		mu.Unlock()
		if ch.Table.Name == "0" {
			return boom
		}
		time.Sleep(5 * time.Millisecond)
		return nil
	})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want boom", err)
	}
	if started > 5 {
		t.Errorf("%d chunks started after the first one failed, want new chunks to stop", started)
	}
}

// helperPair opens one more source/target connection pair onto the test databases,
// without resetting their schemas (the test's own captureSource/sinkTarget do that).
func helperPair(t *testing.T, ctx context.Context) engine.ConnPair {
	t.Helper()
	src := &Source{cfg: harnessConn(t, "source")}
	if err := src.Connect(ctx); err != nil {
		t.Fatalf("connect helper source: %v", err)
	}
	sink := &Sink{cfg: harnessConn(t, "target")}
	if err := sink.Connect(ctx); err != nil {
		t.Fatalf("connect helper target: %v", err)
	}
	t.Cleanup(func() {
		_ = src.Close(context.Background())
		_ = sink.Close(context.Background())
	})
	return engine.ConnPair{Src: src, Sink: sink}
}

// TestCyclicCopyParallelMixedTarget: a chunked NULL-then-fill copy spread over four
// connection pairs, onto a target that already holds stale rows for part of the hub's
// key range and cannot be cleared first (a table outside the component points into it).
// Empty ranges take the plain COPY, occupied ones the staged upsert, and the result
// matches the source, with every row reported copied once.
func TestCyclicCopyParallelMixedTarget(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	src := captureSource(t, ctx)
	sink := sinkTarget(t, ctx)
	orig := copyChunkRows
	copyChunkRows = 25
	t.Cleanup(func() { copyChunkRows = orig })

	hubDDL := "CREATE TABLE rc_it.hub (id int PRIMARY KEY, buddy_id int REFERENCES rc_it.hub(id), name text)"
	childDDL := "CREATE TABLE rc_it.child (id int PRIMARY KEY, hub_id int NOT NULL REFERENCES rc_it.hub(id), note text)"
	setupSourceTables(t, ctx, src, hubDDL, childDDL)
	mustExecTarget(t, ctx, sink, "CREATE SCHEMA rc_it")
	mustExecTarget(t, ctx, sink, hubDDL)
	mustExecTarget(t, ctx, sink, childDDL)
	mustExecTarget(t, ctx, sink, "CREATE TABLE rc_it.note (id int PRIMARY KEY, hub_id int REFERENCES rc_it.hub(id))")
	mustExec(t, ctx, src.conn, "INSERT INTO rc_it.hub SELECT g, NULLIF(g-1,0), 'h'||g FROM generate_series(1,600) g")
	mustExec(t, ctx, src.conn, "INSERT INTO rc_it.child SELECT g, 1+((g*7)%600), 'c'||g FROM generate_series(1,600) g")
	mustExecTarget(t, ctx, sink, "INSERT INTO rc_it.hub SELECT g, NULL, 'stale' FROM generate_series(1,100) g")
	mustExecTarget(t, ctx, sink, "INSERT INTO rc_it.note VALUES (1, 50)")

	helpers := []engine.ConnPair{helperPair(t, ctx), helperPair(t, ctx), helperPair(t, ctx)}
	var mu sync.Mutex
	copied := map[engine.TableRef]int64{}
	progress := func(t engine.TableRef, n int64) {
		mu.Lock()
		copied[t] += n
		mu.Unlock()
	}
	if err := sink.CopyCyclicComponentWith(ctx, src, helpers, []engine.TableRef{ref("rc_it.hub"), ref("rc_it.child")}, progress); err != nil {
		t.Fatalf("parallel cyclic copy: %v", err)
	}
	if copied[ref("rc_it.hub")] != 600 || copied[ref("rc_it.child")] != 600 {
		t.Errorf("reported rows copied = %v, want hub=600 child=600", copied)
	}
	for _, sel := range []string{
		"SELECT id::text, coalesce(buddy_id::text,'<n>'), name FROM rc_it.hub ORDER BY id",
		"SELECT id::text, hub_id::text, note FROM rc_it.child ORDER BY id",
	} {
		if !eqLines(dumpText(t, ctx, src.conn, sel), dumpText(t, ctx, sink.conn, sel)) {
			t.Errorf("did not converge: %s", sel)
		}
	}
	if n := tgtCount(t, ctx, sink, "SELECT count(*) FROM rc_it.note"); n != 1 {
		t.Errorf("the row outside the component was touched: %d left, want 1", n)
	}
}

// TestCyclicCopyParallelParentSkew: the parent lags far behind on the target, so most
// child chunks, running side by side, fail on a missing parent and re-copy it. The
// re-copies take turns, and the child still loads completely.
func TestCyclicCopyParallelParentSkew(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	src := captureSource(t, ctx)
	sink := sinkTarget(t, ctx)
	orig := copyChunkRows
	copyChunkRows = 25
	t.Cleanup(func() { copyChunkRows = orig })

	hubDDL := "CREATE TABLE rc_it.hub (id int PRIMARY KEY, name text)"
	childDDL := "CREATE TABLE rc_it.child (id int PRIMARY KEY, hub_id int NOT NULL REFERENCES rc_it.hub(id), note text)"
	setupSourceTables(t, ctx, src, hubDDL, childDDL)
	mustExecTarget(t, ctx, sink, "CREATE SCHEMA rc_it")
	mustExecTarget(t, ctx, sink, hubDDL)
	mustExecTarget(t, ctx, sink, childDDL)
	mustExec(t, ctx, src.conn, "INSERT INTO rc_it.hub SELECT g, 'h'||g FROM generate_series(1,300) g")
	mustExec(t, ctx, src.conn, "INSERT INTO rc_it.child SELECT g, 1+((g*13)%300), 'c'||g FROM generate_series(1,400) g")
	mustExecTarget(t, ctx, sink, "INSERT INTO rc_it.hub SELECT g, 'h'||g FROM generate_series(1,3) g")

	schema, err := src.Introspect(ctx, engine.Selection{Include: []string{"rc_it.hub", "rc_it.child"}})
	if err != nil {
		t.Fatal(err)
	}
	byRef := map[engine.TableRef]engine.Table{}
	for _, tb := range schema.Tables {
		byRef[tb.Ref] = tb
	}
	hubRef, childRef := ref("rc_it.hub"), ref("rc_it.child")
	conns := []copyConn{{src: src, sink: sink}}
	for _, h := range []engine.ConnPair{helperPair(t, ctx), helperPair(t, ctx), helperPair(t, ctx)} {
		conns = append(conns, copyConn{src: h.Src.(*Source), sink: h.Sink.(*Sink)})
	}
	var recopyMu sync.Mutex
	recopyOn := func(c copyConn) parentRecopier {
		one := recopyOmitting(c.src, c.sink, byRef, map[engine.TableRef][]string{})
		return func(ctx context.Context, p engine.TableRef) error {
			recopyMu.Lock()
			defer recopyMu.Unlock()
			return one(ctx, p)
		}
	}
	if err := copyTableChunkedWithParentRetry(ctx, conns, childRef, transportColumns(byRef[childRef]), []engine.TableRef{hubRef}, recopyOn); err != nil {
		t.Fatalf("parallel chunked copy with parent skew: %v", err)
	}
	for _, sel := range []string{
		"SELECT id::text, name FROM rc_it.hub ORDER BY id",
		"SELECT id::text, hub_id::text, note FROM rc_it.child ORDER BY id",
	} {
		if !eqLines(dumpText(t, ctx, src.conn, sel), dumpText(t, ctx, sink.conn, sel)) {
			t.Errorf("did not converge: %s", sel)
		}
	}
}
