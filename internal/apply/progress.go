package apply

import "context"

// The streaming liveness check (`/healthz`, observability.stall_timeout) must tell a
// pass that is slow but moving from one that is stuck. A pass on a busy source can
// run for minutes: many tables, large batches, a component retrying a transient FK
// violation with backoff. Counting only finished passes made such a pass look stuck,
// and Kubernetes restarted a healthy daemon over and over. So the drain reports
// progress whenever a unit of work against the databases completes: a table's apply
// committing, or a component attempt returning (even with a transient error, which
// still proves both databases answered). A query that never returns reports nothing,
// so a true hang still trips the check.

type progressKey struct{}

// WithProgress installs fn on ctx as the drain's progress signal. It must be cheap and
// safe to call from several goroutines (the pooled drain applies tables concurrently).
func WithProgress(ctx context.Context, fn func()) context.Context {
	if fn == nil {
		return ctx
	}
	return context.WithValue(ctx, progressKey{}, fn)
}

// reportProgress calls the progress signal on ctx, if one is installed.
func reportProgress(ctx context.Context) {
	if fn, ok := ctx.Value(progressKey{}).(func()); ok && fn != nil {
		fn()
	}
}
