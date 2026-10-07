package apply

import (
	"context"
	"sync"

	"github.com/rudimk/replicare/internal/engine"
)

// appliedCounts accumulates, per table, the number of deltas CONFIRMED-consumed in a
// drain pass — the "rows last synced" signal the status surface reports (CLAUDE.md §10).
// It is pass-scoped OBSERVABILITY ONLY: it never affects what the drain applies, the FK
// ordering, the confirm/purge, or any error path. It is carried in the context so the
// drain records into it with no change to any drain signature, and it is nil-safe (no
// accumulator installed → recordApplied is a no-op), so every existing caller and test
// is byte-for-byte unchanged. The mutex makes it safe for the concurrent (pooled) drain
// paths.
type appliedCounts struct {
	mu      sync.Mutex
	byTable map[engine.TableRef]int
}

type appliedCountsKey struct{}

// WithAppliedCounts installs a fresh per-pass accumulator on ctx and returns the derived
// context plus a reader that snapshots the accumulated per-table confirmed-consumed
// counts. The streaming loop wraps one pass with it, drains every component, then reads
// the snapshot to record each table's "rows last synced". Reading is safe at any time
// (it copies under the lock).
func WithAppliedCounts(ctx context.Context) (context.Context, func() map[engine.TableRef]int) {
	ac := &appliedCounts{byTable: map[engine.TableRef]int{}}
	ctx = context.WithValue(ctx, appliedCountsKey{}, ac)
	return ctx, func() map[engine.TableRef]int {
		ac.mu.Lock()
		defer ac.mu.Unlock()
		out := make(map[engine.TableRef]int, len(ac.byTable))
		for k, v := range ac.byTable {
			out[k] = v
		}
		return out
	}
}

// recordApplied adds n confirmed-consumed deltas for table t to the pass accumulator on
// ctx, if one is installed (a no-op otherwise, and a no-op for n==0). It is called at
// each drain confirm point, right where the pass's running total is incremented.
func recordApplied(ctx context.Context, t engine.TableRef, n int) {
	if n == 0 {
		return
	}
	ac, ok := ctx.Value(appliedCountsKey{}).(*appliedCounts)
	if !ok || ac == nil {
		return
	}
	ac.mu.Lock()
	ac.byTable[t] += n
	ac.mu.Unlock()
}
