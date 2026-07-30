// Package copy is the engine-neutral chunked initial-copy driver (CLAUDE.md
// §4.1, §8.1): it plans chunks on a Source, streams each source→target through
// an io.Pipe, and checkpoints per-chunk progress to the StateStore so a restart
// resumes from the last committed watermark rather than re-copying. Chunks within
// a table copy in parallel across a pool of worker connections; within an FK
// component, tables copy parents-first so target FK constraints hold during load.
// Built entirely on the engine.Source / engine.Sink / state.StateStore interfaces.
package copy

import (
	"context"
	"fmt"
	"io"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/state"
)

// Worker is one connection pair used to copy chunks. A Source/Sink is not safe
// for concurrent use, so parallelism comes from having several Workers.
type Worker struct {
	Src  engine.Source
	Sink engine.Sink
}

// Option configures a copy run. Options are variadic so existing callers stay
// source-compatible; the only one today is WithProgress.
type Option func(*config)

type config struct {
	// progress, if set, is called after each chunk with the table and the number
	// of rows loaded, for the rows-copied progress metric. It may be called
	// concurrently from multiple copy workers, so it must be safe for that.
	progress func(engine.TableRef, int64)
}

func newConfig(options []Option) config {
	var c config
	for _, o := range options {
		o(&c)
	}
	return c
}

// WithProgress reports per-chunk rows-loaded counts as the copy proceeds. The
// callback may fire concurrently across workers and must be goroutine-safe.
func WithProgress(fn func(engine.TableRef, int64)) Option {
	return func(c *config) { c.progress = fn }
}

// Table performs a resumable initial copy of one table using a single worker
// (serial chunks). Equivalent to Component with one table and one worker.
func Table(ctx context.Context, src engine.Source, sink engine.Sink, store state.StateStore,
	syncName string, ref engine.TableRef, opts engine.ChunkOptions, options ...Option) error {
	return copyTable(ctx, []Worker{{Src: src, Sink: sink}}, store, syncName, ref, opts, newConfig(options))
}

// Component copies an FK component's tables in the given topological order
// (parents first), each table's chunks parallelized across the workers. Distinct
// components are independent and may be run concurrently by the caller, each with
// its own worker pool (CLAUDE.md §8.1).
func Component(ctx context.Context, workers []Worker, store state.StateStore,
	syncName string, tablesTopoOrder []engine.TableRef, opts engine.ChunkOptions, options ...Option) error {
	if len(workers) == 0 {
		return fmt.Errorf("copy: component needs at least one worker")
	}
	cfg := newConfig(options)
	for _, ref := range tablesTopoOrder {
		if err := copyTable(ctx, workers, store, syncName, ref, opts, cfg); err != nil {
			return err
		}
	}
	return nil
}

// copyTable copies one table's chunks in parallel across the workers, advancing
// the copy-progress watermark over the contiguous completed prefix so a restart
// resumes fine-grained. On resume it clears the incomplete tail (>= watermark)
// then re-copies from there.
func copyTable(ctx context.Context, workers []Worker, store state.StateStore,
	syncName string, ref engine.TableRef, opts engine.ChunkOptions, cfg config) error {

	prog, err := store.LoadCopyProgress(ctx, syncName, ref)
	if err != nil {
		return fmt.Errorf("copy %s: load progress: %w", ref, err)
	}
	if prog.Done {
		return nil
	}
	if prog.Watermark != nil {
		if err := workers[0].Sink.DeleteRange(ctx, ref, prog.Watermark, nil); err != nil {
			return fmt.Errorf("copy %s: clear resume tail: %w", ref, err)
		}
	}

	planOpts := opts
	planOpts.Lo = prog.Watermark
	chunks, err := workers[0].Src.PlanChunks(ctx, ref, planOpts)
	if err != nil {
		return fmt.Errorf("copy %s: plan chunks: %w", ref, err)
	}
	cols, err := transportColumns(ctx, workers[0].Src, ref)
	if err != nil {
		return err
	}

	// Parallel copy: a feeder emits chunk indices; each worker drains them. The
	// contiguous completed prefix advances the persisted watermark.
	done := make([]bool, len(chunks))
	prefix := 0
	var mu sync.Mutex

	g, gctx := errgroup.WithContext(ctx)
	idxCh := make(chan int)
	g.Go(func() error {
		defer close(idxCh)
		for i := range chunks {
			select {
			case idxCh <- i:
			case <-gctx.Done():
				return gctx.Err()
			}
		}
		return nil
	})
	for w := range workers {
		wk := workers[w]
		g.Go(func() error {
			for i := range idxCh {
				n, err := copyChunk(gctx, wk.Src, wk.Sink, ref, cols, chunks[i])
				if err != nil {
					return fmt.Errorf("copy %s: chunk %d: %w", ref, i, err)
				}
				if cfg.progress != nil {
					cfg.progress(ref, n)
				}
				// Advance the contiguous prefix; persist the watermark only when it
				// moves (serialized under mu so the watermark never regresses).
				mu.Lock()
				done[i] = true
				moved := false
				for prefix < len(chunks) && done[prefix] {
					prefix++
					moved = true
				}
				var snap state.CopyProgress
				if moved && prefix < len(chunks) {
					snap = state.CopyProgress{Table: ref, Watermark: chunks[prefix-1].Hi}
				}
				if moved && prefix < len(chunks) {
					if err := store.SaveCopyProgress(gctx, syncName, snap); err != nil {
						mu.Unlock()
						return fmt.Errorf("copy %s: checkpoint: %w", ref, err)
					}
				}
				mu.Unlock()
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	// All chunks done: mark the table complete.
	if err := store.SaveCopyProgress(ctx, syncName, state.CopyProgress{Table: ref, Done: true}); err != nil {
		return fmt.Errorf("copy %s: mark done: %w", ref, err)
	}
	return nil
}

// copyChunk pipes one chunk source→target via io.Pipe so it never fully buffers.
// It returns the number of rows loaded into the target.
func copyChunk(ctx context.Context, src engine.Source, sink engine.Sink,
	ref engine.TableRef, cols []string, c engine.Chunk) (int64, error) {

	pr, pw := io.Pipe()
	errc := make(chan error, 1)
	go func() {
		err := src.CopyChunk(ctx, c, pw)
		_ = pw.CloseWithError(err)
		errc <- err
	}()
	n, loadErr := sink.BulkLoad(ctx, ref, cols, pr, engine.LoadDirect)
	_ = pr.CloseWithError(loadErr)
	copyErr := <-errc
	if copyErr != nil {
		return 0, fmt.Errorf("read side: %w", copyErr)
	}
	if loadErr != nil {
		return 0, fmt.Errorf("write side: %w", loadErr)
	}
	return n, nil
}

// transportColumns returns the name-matched column list to copy, in SOURCE
// column order (the order CopyChunk emits), excluding GENERATED STORED columns.
func transportColumns(ctx context.Context, src engine.Source, ref engine.TableRef) ([]string, error) {
	schema, err := src.Introspect(ctx, engine.Selection{Include: []string{ref.String()}})
	if err != nil {
		return nil, fmt.Errorf("copy %s: introspect columns: %w", ref, err)
	}
	for _, t := range schema.Tables {
		if t.Ref != ref {
			continue
		}
		cols := make([]string, 0, len(t.Columns))
		for _, col := range t.Columns {
			if col.Generated {
				continue
			}
			cols = append(cols, col.Name)
		}
		return cols, nil
	}
	return nil, fmt.Errorf("copy %s: table not found on source", ref)
}
