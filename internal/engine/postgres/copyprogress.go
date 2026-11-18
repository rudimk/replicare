package postgres

import (
	"context"
	"sync/atomic"

	"github.com/rudimk/replicare/internal/engine"
)

// Row-count reporting for the cyclic component copy (the dashboard's initial-copy
// progress). The cyclic copy is a tree of helpers (chunked ranges, staged upserts,
// whole-table loads, parent re-copies, the fill pass), so rather than threading a
// count through every signature, the COPY pipes add the rows they move to a counter
// carried in the context, and only a table's FIRST-PASS load installs one:
//
//   - a load's rows are reported only once the load has committed, so a failed or
//     rolled-back attempt never counts;
//   - parent re-copies (live-source skew recovery) and the pass-2 fill run without a
//     counter, so the reported rows never exceed what the table really holds.

type (
	copyProgressKey struct{}
	rowCounterKey   struct{}
)

// withCopyProgress attaches the caller's per-table progress callback (nil = none).
func withCopyProgress(ctx context.Context, progress func(engine.TableRef, int64)) context.Context {
	if progress == nil {
		return ctx
	}
	return context.WithValue(ctx, copyProgressKey{}, progress)
}

// reportCopied hands n committed rows for ref to the progress callback, if any.
func reportCopied(ctx context.Context, ref engine.TableRef, n int64) {
	if fn, ok := ctx.Value(copyProgressKey{}).(func(engine.TableRef, int64)); ok && n > 0 {
		fn(ref, n)
	}
}

// countCopiedRows adds n rows moved by a COPY pipe to the enclosing counter, if any.
func countCopiedRows(ctx context.Context, n int64) {
	if c, ok := ctx.Value(rowCounterKey{}).(*atomic.Int64); ok {
		c.Add(n)
	}
}

// countAttempt runs one attempt with a fresh counter; only if it succeeds are its rows
// added to the enclosing counter. Used where an operation retries internally, so a
// failed attempt's rows are dropped.
func countAttempt(ctx context.Context, attempt func(context.Context) error) (int64, error) {
	c := new(atomic.Int64)
	if err := attempt(context.WithValue(ctx, rowCounterKey{}, c)); err != nil {
		return 0, err
	}
	n := c.Load()
	countCopiedRows(ctx, n)
	return n, nil
}

// countedLoad runs a table's first-pass load and, once it has committed, reports the
// rows it moved. Anything the load does outside the context it is handed (parent
// re-copies run with the caller's context) is not counted.
func countedLoad(ctx context.Context, ref engine.TableRef, load func(context.Context) error) error {
	c := new(atomic.Int64)
	if err := load(context.WithValue(ctx, rowCounterKey{}, c)); err != nil {
		return err
	}
	reportCopied(ctx, ref, c.Load())
	return nil
}
