package pipeline

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/rudimk/replicare/internal/apply"
	"github.com/rudimk/replicare/internal/engine"
)

// drainComponents drains every FK component once, over the apply pool (decision, 0.8.4,
// from a production catch-up). Components are FK-independent (CLAUDE.md §8.1), so they
// run side by side, one connection pair each; a component of several tables also takes
// whatever other pairs are free when it starts and applies its tables in parallel on them
// (apply_concurrency's original meaning). With a pool of one pair (apply_concurrency 1,
// the default) this is exactly the old order: one component after another.
//
// A failing component is noted and the others carry on (0.8.3). When the failure comes
// with a database that doesn't answer, no new component starts: every one would fail.
// Running components are never cancelled mid-query (that closes the connection); they
// finish.
func (s *Syncer) drainComponents(ctx context.Context, pool []apply.Conn) (int, error) {
	free := make(chan apply.Conn, len(pool))
	for _, c := range pool {
		free <- c
	}
	var (
		mu       sync.Mutex
		consumed int
		firstErr error
		failed   []string
		halted   bool
		wg       sync.WaitGroup
	)
	stopped := func() bool {
		mu.Lock()
		defer mu.Unlock()
		return halted
	}

	// Groups whose tables have nothing queued have nothing to drain: skip them instead of
	// reading each empty queue (engine.ChangePresence, asked once per pass; on a large
	// schema most groups). A change queued after the check is drained next pass, exactly
	// as one queued just after its table was read. If the check fails, every group runs.
	var present map[engine.TableRef]bool
	if len(pool) > 0 {
		if cp, ok := pool[0].Src.(engine.ChangePresence); ok {
			var tables []engine.TableRef
			for _, comp := range s.Components {
				tables = append(tables, comp.Order...)
			}
			if m, err := cp.TablesWithChanges(ctx, tables); err == nil {
				present = m
			}
		}
	}
	nothingQueued := func(comp engine.Component) bool {
		if present == nil {
			return false
		}
		for _, t := range comp.Order {
			if has, known := present[t]; !known || has {
				return false
			}
		}
		return true
	}

schedule:
	for _, comp := range s.Components {
		if ctx.Err() != nil || stopped() {
			break
		}
		if nothingQueued(comp) {
			continue
		}
		var conns []apply.Conn
		select {
		case c := <-free:
			conns = append(conns, c)
		case <-ctx.Done():
			break schedule
		}
		if stopped() { // a component that finished while this one waited failed loud
			free <- conns[0]
			break
		}
	more:
		for len(conns) < min(len(comp.Order), len(pool)) {
			select {
			case c := <-free:
				conns = append(conns, c)
			default:
				break more
			}
		}
		wg.Add(1)
		go func(comp engine.Component, conns []apply.Conn) {
			defer wg.Done()
			n, err := apply.DrainComponentRetryingPool(ctx, conns, len(conns),
				comp.Order, s.Target, s.DrainBatch, comp.HasCycle(), apply.DefaultRetryPolicy)
			up := true
			if err != nil && ctx.Err() == nil {
				up = connsUp(ctx, conns) // these pairs are this goroutine's alone until released
			}
			for _, c := range conns {
				free <- c
			}
			mu.Lock()
			defer mu.Unlock()
			consumed += n
			if err == nil {
				return
			}
			if firstErr == nil {
				firstErr = err
			}
			failed = append(failed, comp.Order[0].String())
			if !up || ctx.Err() != nil {
				halted = true
			}
		}(comp, conns)
	}
	wg.Wait()

	if len(failed) > 1 {
		shown := failed
		if len(shown) > 5 {
			shown = shown[:5]
		}
		return consumed, fmt.Errorf("%d groups of tables failed this pass (from %s); the first: %w",
			len(failed), strings.Join(shown, ", "), firstErr)
	}
	return consumed, firstErr
}

// connsUp reports whether every source and target of the given pairs answers a
// bounded health-check.
func connsUp(ctx context.Context, conns []apply.Conn) bool {
	for _, c := range conns {
		hctx, cancel := context.WithTimeout(ctx, healthTimeout)
		ok := c.Src.HealthCheck(hctx) == nil && c.Sink.HealthCheck(hctx) == nil
		cancel()
		if !ok {
			return false
		}
	}
	return true
}
