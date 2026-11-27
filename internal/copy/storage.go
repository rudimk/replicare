package copy

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/state"
)

// Storage-order copy (CLAUDE.md §4.1 "storage-order copy"). A table whose rows are not
// stored in key order is read block range by block range instead of key range by key
// range, so the source reads it sequentially. What makes that safe on a live source:
//   - capture is installed before the copy, so a row that moves while it runs (an
//     UPDATE writes a new version elsewhere) was changed, is queued, and is re-read
//     by streaming, whether the copy saw it before the move, after it, or not at all;
//   - a row seen twice (before and after a move) makes a direct load of the second
//     chunk fail on the duplicate key, and the chunk is loaded again with the merge
//     load, which is idempotent;
//   - a rewrite of the whole table (VACUUM FULL, CLUSTER, pg_repack, TRUNCATE) moves
//     every row without changing it, so nothing is queued: the copy notices the
//     table's storage changed and starts it again from the first block.

// Event names logged by the storage-order copy.
const (
	EventStorageOrder     = "copy.storage_order"
	EventStorageRewritten = "copy.storage_rewritten"
	EventStorageRetry     = "copy.storage_chunk_retry"
)

var errRewritten = errors.New("table rewritten during its storage-order copy")

const (
	// storageRewriteRestarts bounds how many times one table's copy starts again
	// because the table was rewritten under it, before it fails loud.
	storageRewriteRestarts = 5
	// storageChunkAttempts is how many times a storage-order chunk is loaded before
	// its error stops the copy: the first load, then merge loads (a duplicate key from
	// a row that moved, or a deadlock between two chunks holding the same row).
	storageChunkAttempts = 3
	storageRetryBackoff  = time.Second
)

// WithEvents reports the copy's notable decisions (which order a table is read in, a
// table rewritten under its copy) as named events; without it they go to slog. It may
// be called concurrently.
func WithEvents(fn func(ctx context.Context, level, event string, table engine.TableRef, msg string)) Option {
	return func(c *config) { c.event = fn }
}

func (c config) emit(ctx context.Context, level, event string, table engine.TableRef, msg string) {
	if c.event != nil {
		c.event(ctx, level, event, table, msg)
		return
	}
	lvl := slog.LevelInfo
	switch level {
	case "WARN":
		lvl = slog.LevelWarn
	case "ERROR":
		lvl = slog.LevelError
	case "DEBUG":
		lvl = slog.LevelDebug
	}
	slog.Log(ctx, lvl, msg, "event", event, "table", table.String())
}

// chooseStorageOrder decides whether ref is copied in storage order. A copy that
// already started in storage order keeps it (unless key order is now asked for), so a
// change in the table's statistics never switches order halfway through.
func chooseStorageOrder(ctx context.Context, src engine.Source, ref engine.TableRef, order engine.CopyOrder,
	prog state.CopyProgress, cfg config) (engine.StorageOrderChoice, bool) {
	if order != engine.CopyOrderAuto && order != engine.CopyOrderStorage {
		return engine.StorageOrderChoice{}, false
	}
	sr, ok := src.(engine.StorageOrderReader)
	if !ok {
		return engine.StorageOrderChoice{}, false
	}
	pref := order
	if prog.StorageOrder {
		pref = engine.CopyOrderStorage
	}
	choice, err := sr.StorageOrder(ctx, ref, pref)
	if err != nil {
		cfg.emit(ctx, "WARN", EventStorageOrder, ref,
			fmt.Sprintf("%s: could not tell how the table is stored, so it is read in key order: %v", ref, err))
		return engine.StorageOrderChoice{}, false
	}
	if !choice.Use {
		level := "DEBUG"
		if pref == engine.CopyOrderStorage {
			level = "WARN"
		}
		cfg.emit(ctx, level, EventStorageOrder, ref, fmt.Sprintf("%s: read in key order: %s", ref, choice.Reason))
		return choice, false
	}
	msg := fmt.Sprintf("%s: read in storage order: %s", ref, choice.Reason)
	switch {
	case prog.StorageOrder:
		msg += " (carrying on from where the copy got to)"
	case len(prog.Watermark) > 0 || len(prog.Completed) > 0:
		msg += " (the copy started in key order and starts again; rows already copied are written again)"
	}
	cfg.emit(ctx, "INFO", EventStorageOrder, ref, msg)
	return choice, true
}

// copyTableStorage copies one table in storage order, resuming from its block
// watermark when the table's storage is the one the copy started on.
func copyTableStorage(ctx context.Context, workers []Worker, store state.StateStore, syncName string,
	target engine.TargetID, ref engine.TableRef, opts engine.ChunkOptions, cfg config,
	prog state.CopyProgress, choice engine.StorageOrderChoice, mode engine.LoadMode) error {

	sr := workers[0].Src.(engine.StorageOrderReader)
	cols, err := transportColumns(ctx, workers[0].Src, ref)
	if err != nil {
		return err
	}
	from, file := int64(0), choice.File
	if prog.StorageOrder {
		if prog.RelFile == choice.File {
			from = prog.BlockWatermark
		} else {
			cfg.emit(ctx, "WARN", EventStorageRewritten, ref,
				fmt.Sprintf("%s: the table was rewritten since its copy started, so it is copied again from the start", ref))
		}
	}
	for restarts := 0; ; restarts++ {
		if err := store.SaveCopyProgress(ctx, syncName, state.CopyProgress{
			Target: target, Table: ref, StorageOrder: true, BlockWatermark: from, RelFile: file,
		}); err != nil {
			return fmt.Errorf("copy %s: checkpoint: %w", ref, err)
		}
		err := copyStorageRun(ctx, workers, store, syncName, target, ref, cols, opts.TargetRows, cfg, from, file, mode)
		if err == nil {
			break
		}
		if !errors.Is(err, errRewritten) {
			return err
		}
		if restarts+1 >= storageRewriteRestarts {
			return fmt.Errorf("copy %s: the table was rewritten %d times while it was being copied in storage order (VACUUM FULL, CLUSTER, pg_repack or TRUNCATE); wait for that to finish, or set the sync's copy_order to key", ref, restarts+1)
		}
		if file, err = sr.StorageFile(ctx, ref); err != nil {
			return fmt.Errorf("copy %s: %w", ref, err)
		}
		cfg.emit(ctx, "WARN", EventStorageRewritten, ref,
			fmt.Sprintf("%s: the table was rewritten while it was being copied (VACUUM FULL, CLUSTER, pg_repack or TRUNCATE), so it is copied again from the start", ref))
		from = 0
		mode = engine.LoadMerge // the target now holds rows from the pass before
	}
	if err := store.SaveCopyProgress(ctx, syncName, state.CopyProgress{Target: target, Table: ref, Done: true}); err != nil {
		return fmt.Errorf("copy %s: mark done: %w", ref, err)
	}
	return nil
}

// copyStorageRun copies the block ranges from `from` to the end in parallel, advancing
// the block watermark over the contiguous completed prefix. It returns errRewritten
// when the table's storage stops being `file`; chunks in flight then finish (a query
// is never cancelled midway), and no new one starts.
func copyStorageRun(ctx context.Context, workers []Worker, store state.StateStore, syncName string,
	target engine.TargetID, ref engine.TableRef, cols []string, targetRows int, cfg config,
	from, file int64, mode engine.LoadMode) error {

	sr := workers[0].Src.(engine.StorageOrderReader)
	chunks, err := sr.PlanStorageChunks(ctx, ref, from, targetRows)
	if err != nil {
		return fmt.Errorf("copy %s: plan chunks: %w", ref, err)
	}

	done := make([]bool, len(chunks))
	prefix := 0
	var mu sync.Mutex
	var rewritten atomic.Bool

	g, gctx := errgroup.WithContext(ctx)
	idxCh := make(chan int)
	g.Go(func() error {
		defer close(idxCh)
		for i := range chunks {
			if rewritten.Load() {
				return nil
			}
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
		wsr, _ := wk.Src.(engine.StorageOrderReader)
		g.Go(func() error {
			for i := range idxCh {
				if rewritten.Load() {
					continue
				}
				n, err := copyStorageChunk(gctx, wk, ref, cols, chunks[i], mode, cfg)
				if err != nil {
					return fmt.Errorf("copy %s: chunk %d (blocks %s): %w", ref, i, blockSpan(chunks[i]), err)
				}
				if cfg.progress != nil {
					cfg.progress(ref, n)
				}
				mu.Lock()
				done[i] = true
				moved := false
				for prefix < len(chunks) && done[prefix] {
					prefix++
					moved = true
				}
				if moved && prefix < len(chunks) && wsr != nil {
					// Check the storage on this worker's own connection before saying the
					// blocks below are copied: after a rewrite they are other rows.
					now, err := wsr.StorageFile(gctx, ref)
					if err != nil {
						mu.Unlock()
						return fmt.Errorf("copy %s: %w", ref, err)
					}
					if now != file {
						rewritten.Store(true)
						mu.Unlock()
						continue
					}
					if err := store.SaveCopyProgress(gctx, syncName, state.CopyProgress{
						Target: target, Table: ref, StorageOrder: true,
						BlockWatermark: chunkBlock(chunks[prefix-1].Hi), RelFile: file,
					}); err != nil {
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
	if rewritten.Load() {
		return errRewritten
	}
	// Every block range is in. A rewrite after the last check would leave rows behind,
	// so check once more before the table counts as copied.
	now, err := sr.StorageFile(ctx, ref)
	if err != nil {
		return fmt.Errorf("copy %s: %w", ref, err)
	}
	if now != file {
		return errRewritten
	}
	return nil
}

// copyStorageChunk loads one block range, and loads it again with the merge load
// when the load fails with both connections up: a row that moved while the copy runs
// can be in two chunks, which a direct load refuses as a duplicate key, and two
// chunks holding the same row can deadlock. A real data error fails every attempt and
// stops the copy, loud, as in key order.
func copyStorageChunk(ctx context.Context, wk Worker, ref engine.TableRef, cols []string, c engine.Chunk,
	mode engine.LoadMode, cfg config) (int64, error) {
	n, err := copyChunkResilient(ctx, wk, ref, cols, c, mode)
	for attempt := 1; err != nil && attempt < storageChunkAttempts && ctx.Err() == nil; attempt++ {
		cfg.emit(ctx, "INFO", EventStorageRetry, ref,
			fmt.Sprintf("%s: loading blocks %s again, merging (attempt %d of %d): %v", ref, blockSpan(c), attempt+1, storageChunkAttempts, err))
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(time.Duration(attempt) * storageRetryBackoff):
		}
		n, err = copyChunkResilient(ctx, wk, ref, cols, c, engine.LoadMerge)
	}
	return n, err
}

// chunkBlock returns a block-range bound's block number (0 for an unbounded one).
func chunkBlock(kv engine.KeyValues) int64 {
	if len(kv) != 1 {
		return 0
	}
	switch b := kv[0].(type) {
	case int:
		return int64(b)
	case int64:
		return b
	}
	return 0
}

// blockSpan renders a chunk's block range for messages.
func blockSpan(c engine.Chunk) string {
	lo := fmt.Sprint(chunkBlock(c.Lo))
	if c.Hi == nil {
		return lo + " to the end"
	}
	return fmt.Sprintf("%s to %d", lo, chunkBlock(c.Hi)-1)
}
