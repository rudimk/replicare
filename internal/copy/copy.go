// Package copy is the engine-neutral chunked initial-copy driver (CLAUDE.md
// §4.1): it plans chunks on a Source, streams each source→target through an
// io.Pipe, and checkpoints per-chunk progress to the StateStore so a restart
// resumes from the last committed watermark rather than re-copying. It is built
// entirely on the engine.Source / engine.Sink / state.StateStore interfaces.
package copy

import (
	"context"
	"fmt"
	"io"

	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/state"
)

// Table performs a resumable initial copy of one table for a sync. It loads any
// saved progress, resumes from the watermark (clearing the interrupted tail on
// the target first), copies the remaining chunks in key order, and checkpoints
// after each. Safe to re-run after a crash: the DELETE-then-COPY of the resumed
// range is idempotent (CLAUDE.md §4.1).
func Table(ctx context.Context, src engine.Source, sink engine.Sink, store state.StateStore,
	sync string, ref engine.TableRef, opts engine.ChunkOptions) error {

	prog, err := store.LoadCopyProgress(ctx, sync, ref)
	if err != nil {
		return fmt.Errorf("copy %s: load progress: %w", ref, err)
	}
	if prog.Done {
		return nil // already fully copied
	}

	// Resume: clear the incomplete tail (>= watermark) on the target so the
	// re-copy is exclusive and idempotent. Fresh copies (nil watermark) skip
	// this — the direct COPY path assumes an empty range.
	if prog.Watermark != nil {
		if err := sink.DeleteRange(ctx, ref, prog.Watermark, nil); err != nil {
			return fmt.Errorf("copy %s: clear resume tail: %w", ref, err)
		}
	}

	planOpts := opts
	planOpts.Lo = prog.Watermark
	chunks, err := src.PlanChunks(ctx, ref, planOpts)
	if err != nil {
		return fmt.Errorf("copy %s: plan chunks: %w", ref, err)
	}

	cols, err := transportColumns(ctx, src, ref)
	if err != nil {
		return err
	}

	for _, c := range chunks {
		if err := copyChunk(ctx, src, sink, ref, cols, c); err != nil {
			return fmt.Errorf("copy %s: chunk: %w", ref, err)
		}
		// Checkpoint: everything strictly below this chunk's upper bound is now
		// on the target. The final chunk (unbounded above) completes the table.
		prog.Table = ref
		if c.Hi == nil {
			prog.Done = true
		} else {
			prog.Watermark = c.Hi
		}
		if err := store.SaveCopyProgress(ctx, sync, prog); err != nil {
			return fmt.Errorf("copy %s: checkpoint: %w", ref, err)
		}
	}
	return nil
}

// copyChunk pipes one chunk source→target via io.Pipe so it never fully buffers.
func copyChunk(ctx context.Context, src engine.Source, sink engine.Sink,
	ref engine.TableRef, cols []string, c engine.Chunk) error {

	pr, pw := io.Pipe()
	errc := make(chan error, 1)
	go func() {
		err := src.CopyChunk(ctx, c, pw)
		_ = pw.CloseWithError(err)
		errc <- err
	}()
	loadErr := sink.BulkLoad(ctx, ref, cols, pr, engine.LoadDirect)
	_ = pr.CloseWithError(loadErr)
	copyErr := <-errc
	if copyErr != nil {
		return fmt.Errorf("read side: %w", copyErr)
	}
	if loadErr != nil {
		return fmt.Errorf("write side: %w", loadErr)
	}
	return nil
}

// transportColumns returns the name-matched column list to copy, in SOURCE
// column order (the order CopyChunk emits), excluding GENERATED STORED columns.
// Pre-flight guarantees each of these exists on the target.
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
				continue // target regenerates it
			}
			cols = append(cols, col.Name)
		}
		return cols, nil
	}
	return nil, fmt.Errorf("copy %s: table not found on target", ref)
}
