package mysql

import (
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

// TestConcurrentLoadNoCollision runs several LOAD DATA copies concurrently, each
// on its own connection pair, to prove the unique-per-load reader-handler names
// don't collide (Momus M2 / 2nd-pass m3) — the exact condition the parallel copy
// driver creates.
func TestConcurrentLoadNoCollision(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// Reset the shared rc_it database once, and seed N tables.
	setup := connectSource(t, ctx)
	setupSink := connectSink(t, ctx)
	mustExec(t, ctx, setupSink.db, "DROP DATABASE IF EXISTS rc_it", "CREATE DATABASE rc_it")
	t.Cleanup(func() { _, _ = setupSink.db.Exec("DROP DATABASE IF EXISTS rc_it") })

	const n = 4
	for i := 0; i < n; i++ {
		ddl := fmt.Sprintf("CREATE TABLE rc_it.t%d (id INT PRIMARY KEY, v VARCHAR(40)) ENGINE=InnoDB", i)
		mustExec(t, ctx, setup.db, ddl)
		mustExec(t, ctx, setupSink.db, ddl)
		mustExec(t, ctx, setup.db, fmt.Sprintf(
			"INSERT INTO rc_it.t%d SELECT s, CONCAT('row',s) FROM (SELECT 1 s UNION SELECT 2 UNION SELECT 3) x", i))
	}

	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// Each goroutine gets its OWN Source/Sink (own *sql.DB connection).
			src := &Source{cfg: srcCfg()}
			sink := &Sink{cfg: tgtCfg()}
			if err := src.Connect(ctx); err != nil {
				errs[i] = err
				return
			}
			defer src.Close(context.Background())
			if err := sink.Connect(ctx); err != nil {
				errs[i] = err
				return
			}
			defer sink.Close(context.Background())

			ref := engine.TableRef{Schema: "rc_it", Name: fmt.Sprintf("t%d", i)}
			tbl, err := src.tableMeta(ctx, ref)
			if err != nil {
				errs[i] = err
				return
			}
			cols := transportCols(tbl)
			chunks, err := src.PlanChunks(ctx, ref, engine.ChunkOptions{TargetRows: 100})
			if err != nil {
				errs[i] = err
				return
			}
			for _, c := range chunks {
				pr, pw := io.Pipe()
				cerr := make(chan error, 1)
				go func(c engine.Chunk) {
					e := src.CopyChunk(ctx, c, pw)
					_ = pw.CloseWithError(e)
					cerr <- e
				}(c)
				if _, e := sink.BulkLoad(ctx, ref, cols, pr, engine.LoadDirect); e != nil {
					_ = pr.CloseWithError(e)
					<-cerr
					errs[i] = e
					return
				}
				_ = pr.CloseWithError(nil)
				if e := <-cerr; e != nil {
					errs[i] = e
					return
				}
			}
		}(i)
	}
	wg.Wait()

	for i, e := range errs {
		if e != nil {
			t.Errorf("concurrent copy %d failed: %v", i, e)
		}
		var c int
		if err := setupSink.db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM rc_it.t%d", i)).Scan(&c); err != nil {
			t.Fatalf("count t%d: %v", i, err)
		}
		if c != 3 {
			t.Errorf("table t%d has %d rows, want 3 (concurrent load collided?)", i, c)
		}
	}
}
