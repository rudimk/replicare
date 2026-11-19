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
	if err := copyTableChunkedWithParentRetry(ctx, conns, childRef, transportColumns(byRef[childRef]), []engine.TableRef{hubRef}, recopyOn, nil); err != nil {
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

// TestCyclicCopyFetchesOnlyReferencedParents: when a child chunk fails on parent rows the
// target lacks, only the parent rows that chunk points at are brought across, not the
// whole parent table, and rows already on the target are left as they are. It walks up a
// level when the fetched parents point at grandparent rows the target lacks too.
func TestCyclicCopyFetchesOnlyReferencedParents(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	src := captureSource(t, ctx)
	sink := sinkTarget(t, ctx)
	orig := copyChunkRows
	copyChunkRows = 25
	t.Cleanup(func() { copyChunkRows = orig })

	usrDDL := "CREATE TABLE rc_it.usr (id int PRIMARY KEY, name text)"
	hubDDL := "CREATE TABLE rc_it.hub (id int PRIMARY KEY, usr_id int NOT NULL REFERENCES rc_it.usr(id), name text)"
	childDDL := "CREATE TABLE rc_it.child (id int PRIMARY KEY, hub_id int NOT NULL REFERENCES rc_it.hub(id), note text)"
	setupSourceTables(t, ctx, src, usrDDL, hubDDL, childDDL)
	mustExecTarget(t, ctx, sink, "CREATE SCHEMA rc_it")
	for _, d := range []string{usrDDL, hubDDL, childDDL} {
		mustExecTarget(t, ctx, sink, d)
	}
	// Source: 300 users, 300 hubs (hub g belongs to user g), and 200 children that point
	// only at hubs 1..100.
	mustExec(t, ctx, src.conn, "INSERT INTO rc_it.usr SELECT g, 'u'||g FROM generate_series(1,300) g")
	mustExec(t, ctx, src.conn, "INSERT INTO rc_it.hub SELECT g, g, 'h'||g FROM generate_series(1,300) g")
	mustExec(t, ctx, src.conn, "INSERT INTO rc_it.child SELECT g, 1+((g-1)%100), 'c'||g FROM generate_series(1,200) g")
	// Target: users and hubs lag far behind (only 1..3, with stale names).
	mustExecTarget(t, ctx, sink, "INSERT INTO rc_it.usr SELECT g, 'stale' FROM generate_series(1,3) g")
	mustExecTarget(t, ctx, sink, "INSERT INTO rc_it.hub SELECT g, g, 'stale' FROM generate_series(1,3) g")

	schema, err := src.Introspect(ctx, engine.Selection{Include: []string{"rc_it.usr", "rc_it.hub", "rc_it.child"}})
	if err != nil {
		t.Fatal(err)
	}
	members := schema.Tables
	byRef := map[engine.TableRef]engine.Table{}
	for _, tb := range members {
		byRef[tb.Ref] = tb
	}
	usrRef, hubRef, childRef := ref("rc_it.usr"), ref("rc_it.hub"), ref("rc_it.child")
	parents := nonCyclicParents(members, map[string]bool{})
	var mu sync.Mutex
	wholeRecopies := 0
	recopyOn := func(c copyConn) parentRecopier {
		return func(ctx context.Context, p engine.TableRef) error {
			wholeRecopies++
			return recopyOmitting(c.src, c.sink, byRef, map[engine.TableRef][]string{})(ctx, p)
		}
	}
	pf := newParentFetch(members, map[string]bool{}, parents, map[engine.TableRef][]string{}, map[engine.TableRef][]string{}, recopyOn, &mu)
	fetchOn := func(c copyConn) referencedFetcher {
		return func(ctx context.Context, w string) error { return pf.fetch(ctx, c, childRef, w) }
	}
	conns := []copyConn{{src: src, sink: sink}}
	for _, h := range []engine.ConnPair{helperPair(t, ctx), helperPair(t, ctx)} {
		conns = append(conns, copyConn{src: h.Src.(*Source), sink: h.Sink.(*Sink)})
	}
	if err := copyTableChunkedWithParentRetry(ctx, conns, childRef, transportColumns(byRef[childRef]), parents[childRef], recopyOn, fetchOn); err != nil {
		t.Fatalf("copy with targeted parent fetch: %v", err)
	}

	sel := "SELECT id::text, hub_id::text, note FROM rc_it.child ORDER BY id"
	if !eqLines(dumpText(t, ctx, src.conn, sel), dumpText(t, ctx, sink.conn, sel)) {
		t.Error("child did not converge")
	}
	if wholeRecopies != 0 {
		t.Errorf("fell back to %d whole-table parent re-copies, want 0", wholeRecopies)
	}
	if n := tgtCount(t, ctx, sink, "SELECT count(*) FROM rc_it.hub"); n != 100 {
		t.Errorf("target hub has %d rows, want 100 (only the hubs the children point at)", n)
	}
	if n := tgtCount(t, ctx, sink, "SELECT count(*) FROM rc_it.usr"); n != 100 {
		t.Errorf("target usr has %d rows, want 100 (only the users those hubs point at)", n)
	}
	if n := tgtCount(t, ctx, sink, "SELECT count(*) FROM rc_it.hub WHERE name = 'stale'"); n != 3 {
		t.Errorf("%d stale hub rows left, want 3 (rows already there are left alone)", n)
	}
	_ = usrRef
	_ = hubRef
}

// TestCyclicCopyRetryReadsOneSnapshot: a busy app keeps adding parents and children
// that point at them while a failed chunk is being fixed. The fetch and the retried chunk
// read the source at the same instant, so the retry succeeds first time: every chunk is
// fixed at most once, even though a new child (pointing at a new parent) lands in the
// chunk's range between the fetch and the retry.
func TestCyclicCopyRetryReadsOneSnapshot(t *testing.T) {
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
	// Even child ids only, so the "app" can add odd ids inside any chunk's range.
	mustExec(t, ctx, src.conn, "INSERT INTO rc_it.hub SELECT g, 'h'||g FROM generate_series(1,100) g")
	mustExec(t, ctx, src.conn, "INSERT INTO rc_it.child SELECT g*2, 1+((g-1)%100), 'c'||g FROM generate_series(1,200) g")
	// The target has none of the hubs, so every chunk fails once.

	schema, err := src.Introspect(ctx, engine.Selection{Include: []string{"rc_it.hub", "rc_it.child"}})
	if err != nil {
		t.Fatal(err)
	}
	byRef := map[engine.TableRef]engine.Table{}
	for _, tb := range schema.Tables {
		byRef[tb.Ref] = tb
	}
	hubRef, childRef := ref("rc_it.hub"), ref("rc_it.child")
	parents := nonCyclicParents(schema.Tables, map[string]bool{})
	var mu sync.Mutex
	recopyOn := func(c copyConn) parentRecopier {
		return recopyOmitting(c.src, c.sink, byRef, map[engine.TableRef][]string{})
	}
	pf := newParentFetch(schema.Tables, map[string]bool{}, parents, map[engine.TableRef][]string{}, map[engine.TableRef][]string{}, recopyOn, &mu)

	app := helperPair(t, ctx).Src.(*Source) // the busy app, on its own connection
	var fixMu sync.Mutex
	fixes := map[string]int{}
	newHub := 1000
	fetchOn := func(c copyConn) referencedFetcher {
		return func(ctx context.Context, w string) error {
			if err := pf.fetch(ctx, c, childRef, w); err != nil {
				return err
			}
			fixMu.Lock()
			defer fixMu.Unlock()
			fixes[w]++
			newHub++
			// Between the fetch and the retry: a new hub, and a new child inside this
			// chunk's range pointing at it.
			var lo int
			if err := app.conn.QueryRow(ctx, "SELECT min(id) FROM rc_it.child WHERE "+w).Scan(&lo); err != nil {
				return err
			}
			if _, err := app.conn.Exec(ctx, "INSERT INTO rc_it.hub VALUES ($1, 'new')", newHub); err != nil {
				return err
			}
			_, err := app.conn.Exec(ctx, "INSERT INTO rc_it.child VALUES ($1, $2, 'new')", lo+1, newHub)
			return err
		}
	}
	conns := []copyConn{{src: src, sink: sink}}
	if err := copyTableChunkedWithParentRetry(ctx, conns, childRef, transportColumns(byRef[childRef]), parents[childRef], recopyOn, fetchOn); err != nil {
		t.Fatalf("copy: %v", err)
	}
	for w, n := range fixes {
		if n != 1 {
			t.Errorf("chunk %s was fixed %d times, want 1 (the retry should read the same snapshot as the fetch)", w, n)
		}
	}
	if len(fixes) == 0 {
		t.Fatal("no chunk needed a fix; the test did not exercise the retry")
	}
	sel := "SELECT id::text, hub_id::text, note FROM rc_it.child WHERE id % 2 = 0 ORDER BY id"
	if !eqLines(dumpText(t, ctx, src.conn, sel), dumpText(t, ctx, sink.conn, sel)) {
		t.Error("the original children did not converge")
	}
	_ = hubRef
}

// TestEstimateTableBytes: table sizes come from the catalogue, include out-of-line
// (TOAST) values, and leave indexes out, so a table of wide rows weighs more than a table
// of narrow ones with more rows.
func TestEstimateTableBytes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	src := captureSource(t, ctx)
	setupSourceTables(t, ctx, src,
		"CREATE TABLE rc_it.narrow (id int PRIMARY KEY, n int)",
		"CREATE TABLE rc_it.wide (id int PRIMARY KEY, body text)")
	mustExec(t, ctx, src.conn, "INSERT INTO rc_it.narrow SELECT g, g FROM generate_series(1,20000) g")
	// Incompressible 20 kB values, stored out of line in the TOAST table.
	mustExec(t, ctx, src.conn, "INSERT INTO rc_it.wide SELECT g, (SELECT string_agg(md5(random()::text), '') FROM generate_series(1,625)) FROM generate_series(1,200) g")
	got, err := src.EstimateTableBytes(ctx, []engine.TableRef{ref("rc_it.narrow"), ref("rc_it.wide"), ref("rc_it.missing")})
	if err != nil {
		t.Fatal(err)
	}
	narrow, wide := got[ref("rc_it.narrow")], got[ref("rc_it.wide")]
	if narrow <= 0 || wide <= 0 {
		t.Fatalf("sizes = %v, want both tables", got)
	}
	if wide < 3_000_000 {
		t.Errorf("wide = %d bytes, want its out-of-line values (~4 MB) counted", wide)
	}
	if wide <= narrow {
		t.Errorf("wide (%d bytes, 200 rows) should outweigh narrow (%d bytes, 20,000 rows)", wide, narrow)
	}
	if _, ok := got[ref("rc_it.missing")]; ok {
		t.Error("a table that doesn't exist should be left out")
	}
}
