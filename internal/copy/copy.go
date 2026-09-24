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
	// loadMode selects the per-chunk target write strategy. The default ("") is the
	// empty-target direct COPY with DELETE-range resume (CLAUDE.md §4.1). Cluster
	// (active-active) edges pass LoadMerge: every member is simultaneously a source
	// being copied FROM and a target being written TO by its reciprocal edges, so a
	// member's live table transiently carries rows the other edge just applied; a
	// direct COPY of those bounced rows would collide on the target PK. The merge
	// path (INSERT ... ON CONFLICT DO UPDATE) is idempotent against them and needs no
	// DELETE-range resume.
	loadMode engine.LoadMode
}

// WithLoadMode overrides the per-chunk target write strategy (default: direct COPY).
// Cluster edges pass engine.LoadMerge so the concurrent cross-edge copy is idempotent.
func WithLoadMode(m engine.LoadMode) Option {
	return func(c *config) { c.loadMode = m }
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

// Table performs a resumable initial copy of one table into one target using a
// single worker (serial chunks). Equivalent to Component with one table and one
// worker. Progress is checkpointed per (sync, target, table).
func Table(ctx context.Context, src engine.Source, sink engine.Sink, store state.StateStore,
	syncName string, target engine.TargetID, ref engine.TableRef, opts engine.ChunkOptions, options ...Option) error {
	return copyTable(ctx, []Worker{{Src: src, Sink: sink}}, store, syncName, target, ref, opts, newConfig(options))
}

// Component copies an FK component's tables into one target in the given
// topological order (parents first), each table's chunks parallelized across the
// workers. Distinct components are independent and may be run concurrently by the
// caller, each with its own worker pool (CLAUDE.md §8.1). Progress is per
// (sync, target, table), so fan-out to several targets never shares a watermark.
func Component(ctx context.Context, workers []Worker, store state.StateStore,
	syncName string, target engine.TargetID, tablesTopoOrder []engine.TableRef, opts engine.ChunkOptions, options ...Option) error {
	if len(workers) == 0 {
		return fmt.Errorf("copy: component needs at least one worker")
	}
	cfg := newConfig(options)
	for _, ref := range tablesTopoOrder {
		if err := copyTable(ctx, workers, store, syncName, target, ref, opts, cfg); err != nil {
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
	syncName string, target engine.TargetID, ref engine.TableRef, opts engine.ChunkOptions, cfg config) error {

	prog, err := store.LoadCopyProgress(ctx, syncName, target, ref)
	if err != nil {
		return fmt.Errorf("copy %s: load progress: %w", ref, err)
	}
	if prog.Done {
		return nil
	}
	// Idempotent direct copy: each chunk is DELETE-range-then-COPY (see copyChunk),
	// so a fresh copy into a target that ALREADY holds previously-replicated rows —
	// a restart / pod reschedule / node refresh after the state store was reset or
	// lost, or simply re-running against a populated target — re-copies cleanly
	// instead of colliding on the PK. This subsumes the old watermark-gated
	// tail-delete: the resume watermark below still fast-forwards past completed
	// chunks, and the per-chunk delete clears whatever those re-planned chunks cover
	// (CLAUDE.md §4.1: during initial copy the copier is the table's sole writer, so
	// range-delete + re-copy is exclusive). The merge path (cluster edges) is
	// idempotent via ON CONFLICT and must NOT delete — a mesh tail row may be
	// legitimately present from the reciprocal edge.

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
				n, err := copyChunk(gctx, wk.Src, wk.Sink, ref, cols, chunks[i], cfg.loadMode)
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
					snap = state.CopyProgress{Target: target, Table: ref, Watermark: chunks[prefix-1].Hi}
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
	if err := store.SaveCopyProgress(ctx, syncName, state.CopyProgress{Target: target, Table: ref, Done: true}); err != nil {
		return fmt.Errorf("copy %s: mark done: %w", ref, err)
	}
	return nil
}

// copyChunk pipes one chunk source→target via io.Pipe so it never fully buffers.
// It returns the number of rows loaded into the target.
func copyChunk(ctx context.Context, src engine.Source, sink engine.Sink,
	ref engine.TableRef, cols []string, c engine.Chunk, mode engine.LoadMode) (int64, error) {

	// Direct COPY errors on a row that already exists (duplicate PK). Clear the
	// chunk's key range first so the load is idempotent — safe on restart / node
	// refresh when the target already holds previously-replicated rows, and a
	// no-op on a genuinely empty target. Keyset only: chunks then carry real key
	// bounds and are disjoint, so parallel per-chunk deletes never overlap. The
	// ctid fallback has no key bounds to delete by (a nil-nil range would wipe the
	// whole table per chunk), and the merge path (cluster) is already idempotent
	// via ON CONFLICT — both skip the delete.
	if mode != engine.LoadMerge && c.Method != engine.ChunkCTID {
		if err := sink.DeleteRange(ctx, ref, c.Lo, c.Hi); err != nil {
			return 0, fmt.Errorf("clear range: %w", err)
		}
	}

	pr, pw := io.Pipe()
	errc := make(chan error, 1)
	go func() {
		err := src.CopyChunk(ctx, c, pw)
		_ = pw.CloseWithError(err)
		errc <- err
	}()
	n, loadErr := sink.BulkLoad(ctx, ref, cols, pr, mode)
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
