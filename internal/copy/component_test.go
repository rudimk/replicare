package copy

import (
	"context"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

// newWorkers builds n connected worker pairs and registers cleanup.
func (f *fixture) newWorkers(t *testing.T, ctx context.Context, n int) []Worker {
	t.Helper()
	eng, err := engine.Get("postgres")
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	workers := make([]Worker, n)
	for i := 0; i < n; i++ {
		src, err := eng.NewSource(srcCfg())
		if err != nil {
			t.Fatalf("new source: %v", err)
		}
		if err := src.Connect(ctx); err != nil {
			t.Fatalf("connect source: %v", err)
		}
		sink, err := eng.NewSink(tgtCfg())
		if err != nil {
			t.Fatalf("new sink: %v", err)
		}
		if err := sink.Connect(ctx); err != nil {
			t.Fatalf("connect sink: %v", err)
		}
		workers[i] = Worker{Src: src, Sink: sink}
		t.Cleanup(func() {
			bg := context.Background()
			_ = src.Close(bg)
			_ = sink.Close(bg)
		})
	}
	return workers
}

func TestComponentParentsFirstFKHolds(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	f := newFixture(t, ctx)

	// parent <- child with an enforced FK on BOTH source and target. Copying
	// child before parent would violate the target FK; Component copies parents
	// first so it holds.
	parentDDL := "CREATE TABLE rc_it.parent (id int PRIMARY KEY, label text)"
	childDDL := "CREATE TABLE rc_it.child (id int PRIMARY KEY, parent_id int NOT NULL REFERENCES rc_it.parent(id), note text)"
	exec(t, ctx, f.rawSrc, parentDDL)
	exec(t, ctx, f.rawSrc, childDDL)
	exec(t, ctx, f.rawTgt, parentDDL)
	exec(t, ctx, f.rawTgt, childDDL)
	exec(t, ctx, f.rawSrc, "INSERT INTO rc_it.parent SELECT g, 'p'||g FROM generate_series(1,60) g")
	exec(t, ctx, f.rawSrc, "INSERT INTO rc_it.child SELECT g, (g%60)+1, 'c'||g FROM generate_series(1,200) g")

	parent := engine.TableRef{Schema: "rc_it", Name: "parent"}
	child := engine.TableRef{Schema: "rc_it", Name: "child"}
	workers := f.newWorkers(t, ctx, 3)

	// Topological order: parent then child.
	if err := Component(ctx, workers, f.store, f.syncName, []engine.TableRef{parent, child}, engine.ChunkOptions{TargetRows: 15}); err != nil {
		t.Fatalf("Component: %v", err)
	}

	var np, nc int
	if err := f.rawTgt.QueryRow(ctx, "SELECT count(*) FROM rc_it.parent").Scan(&np); err != nil {
		t.Fatal(err)
	}
	if err := f.rawTgt.QueryRow(ctx, "SELECT count(*) FROM rc_it.child").Scan(&nc); err != nil {
		t.Fatal(err)
	}
	if np != 60 || nc != 200 {
		t.Fatalf("target counts parent=%d child=%d, want 60/200", np, nc)
	}
	// No orphaned children (FK integrity held).
	var orphans int
	if err := f.rawTgt.QueryRow(ctx,
		"SELECT count(*) FROM rc_it.child c LEFT JOIN rc_it.parent p ON p.id=c.parent_id WHERE p.id IS NULL").Scan(&orphans); err != nil {
		t.Fatal(err)
	}
	if orphans != 0 {
		t.Errorf("%d orphaned child rows — parents-first ordering violated", orphans)
	}
}

func TestComponentParallelChunksFaithful(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	f := newFixture(t, ctx)
	exec(t, ctx, f.rawSrc, ordersDDL)
	exec(t, ctx, f.rawTgt, ordersDDL)
	exec(t, ctx, f.rawSrc, "INSERT INTO rc_it.orders SELECT g, 'note '||g FROM generate_series(1,5000) g")

	ref := engine.TableRef{Schema: "rc_it", Name: "orders"}
	workers := f.newWorkers(t, ctx, 4) // 4-way parallel chunks

	if err := Component(ctx, workers, f.store, f.syncName, []engine.TableRef{ref}, engine.ChunkOptions{TargetRows: 300}); err != nil {
		t.Fatalf("Component: %v", err)
	}
	if got := f.rowCount(t, ctx, f.rawTgt); got != 5000 {
		t.Fatalf("target has %d rows, want 5000 (parallel chunks, no gaps/dupes)", got)
	}
	f.faithful(t, ctx)
}

func TestComponentResumeParallel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	f := newFixture(t, ctx)
	exec(t, ctx, f.rawSrc, ordersDDL)
	exec(t, ctx, f.rawTgt, ordersDDL)
	exec(t, ctx, f.rawSrc, "INSERT INTO rc_it.orders SELECT g, 'note '||g FROM generate_series(1,2000) g")

	ref := engine.TableRef{Schema: "rc_it", Name: "orders"}

	// Crash one worker partway.
	workers := f.newWorkers(t, ctx, 3)
	flaky := []Worker{{Src: &flakySource{Source: workers[0].Src, failAfter: 2}, Sink: workers[0].Sink}}
	err := Component(ctx, flaky, f.store, f.syncName, []engine.TableRef{ref}, engine.ChunkOptions{TargetRows: 200})
	if err == nil {
		t.Fatal("expected injected crash")
	}

	// Resume with healthy parallel workers -> converges exactly.
	if err := Component(ctx, workers, f.store, f.syncName, []engine.TableRef{ref}, engine.ChunkOptions{TargetRows: 200}); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if got := f.rowCount(t, ctx, f.rawTgt); got != 2000 {
		t.Fatalf("after resume target has %d rows, want 2000", got)
	}
	f.faithful(t, ctx)
}
