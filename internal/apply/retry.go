package apply

import (
	"context"
	"fmt"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

// Retry fallback (CLAUDE.md §3.3, §8.1): topo ordering cannot resolve FK
// cycles, self-references, or cross-pass dependencies (a child whose parent's
// delta has not committed on the source yet, or a target whose FK graph differs
// from the source's). When a pass hits a transient FK violation the target
// transaction rolls back and the deltas stay dirty (unconfirmed), so a fresh
// pass — with a fresh dirty-key read and re-read — retries once the dependency
// lands. Retries are bounded; exhaustion halts the component loud (§4.2 error
// policy): nothing is skipped, and the dirty deltas re-apply automatically
// after the operator fixes the cause.

// RetryPolicy bounds the transient-FK retry loop.
type RetryPolicy struct {
	// MaxAttempts is the total number of passes tried (including the first).
	MaxAttempts int
	// BaseBackoff is the wait before the first retry; it doubles per retry.
	BaseBackoff time.Duration
	// MaxBackoff caps the exponential growth.
	MaxBackoff time.Duration
}

// DefaultRetryPolicy is a conservative production default.
var DefaultRetryPolicy = RetryPolicy{MaxAttempts: 5, BaseBackoff: 200 * time.Millisecond, MaxBackoff: 5 * time.Second}

// DrainComponentRetrying runs one component drain pass, retrying transient FK
// violations per the policy. Each retry is a full fresh pass (dirty-key read +
// re-read), so dependencies that landed since the last attempt resolve. Any
// non-transient error fails immediately; exhausting the policy returns a loud
// halt error with the deltas still dirty.
func DrainComponentRetrying(ctx context.Context, src engine.Source, sink engine.Sink,
	tablesTopoOrder []engine.TableRef, target engine.TargetID, batch int, cyclic bool, policy RetryPolicy) (int, error) {

	backoff := policy.BaseBackoff
	var lastErr error
	for attempt := 1; attempt <= policy.MaxAttempts; attempt++ {
		n, err := DrainComponent(ctx, src, sink, tablesTopoOrder, target, batch, cyclic)
		if err == nil {
			return n, nil
		}
		if !engine.IsTransientConstraint(err) {
			return 0, err
		}
		lastErr = err
		if attempt == policy.MaxAttempts {
			break
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > policy.MaxBackoff {
			backoff = policy.MaxBackoff
		}
	}
	return 0, fmt.Errorf("apply component HALTED: FK dependency unresolved after %d attempts; "+
		"deltas remain dirty and will re-apply once the dependency lands: %w",
		policy.MaxAttempts, lastErr)
}
