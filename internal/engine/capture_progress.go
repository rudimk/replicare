package engine

import "context"

// Capture installs one table at a time, and on a large schema over a slow link that takes
// minutes. An engine reports its progress through the context, so the caller can log it
// without the Source interface changing.

type captureProgressKey struct{}

// WithCaptureProgress returns a context whose capture install reports, after each table,
// how many of the tables it has done.
func WithCaptureProgress(ctx context.Context, fn func(done, total int)) context.Context {
	return context.WithValue(ctx, captureProgressKey{}, fn)
}

// ReportCaptureProgress calls the context's capture-progress func, if any.
func ReportCaptureProgress(ctx context.Context, done, total int) {
	if fn, ok := ctx.Value(captureProgressKey{}).(func(done, total int)); ok && fn != nil {
		fn(done, total)
	}
}
