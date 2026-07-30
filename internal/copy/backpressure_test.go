package copy

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

// slowSink wraps a Sink and throttles the read side of every BulkLoad, simulating
// a slow-but-reachable target. Because CopyChunk pipes through an unbuffered
// io.Pipe, a slow reader backpressures the source write directly — a chunk never
// buffers in memory regardless of target speed (CLAUDE.md §4.1 backpressure).
type slowSink struct {
	engine.Sink
	delay time.Duration
}

func (s *slowSink) BulkLoad(ctx context.Context, t engine.TableRef, cols []string, r io.Reader, mode engine.LoadMode) (int64, error) {
	return s.Sink.BulkLoad(ctx, t, cols, &slowReader{r: r, delay: s.delay}, mode)
}

type slowReader struct {
	r     io.Reader
	delay time.Duration
}

func (s *slowReader) Read(p []byte) (int, error) {
	time.Sleep(s.delay)
	if len(p) > 128 {
		p = p[:128] // read in small bites to force many throttled reads
	}
	return s.r.Read(p)
}

// TestBackpressureSlowTarget copies against a throttled target and asserts the
// copy still converges faithfully — the pipe's backpressure holds without
// deadlock or unbounded buffering.
func TestBackpressureSlowTarget(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	f := newFixture(t, ctx)
	exec(t, ctx, f.rawSrc, ordersDDL)
	exec(t, ctx, f.rawTgt, ordersDDL)
	exec(t, ctx, f.rawSrc, "INSERT INTO rc_it.orders SELECT g, 'note '||g FROM generate_series(1,1000) g")

	ref := engine.TableRef{Schema: "rc_it", Name: "orders"}
	slow := &slowSink{Sink: f.sink, delay: 500 * time.Microsecond}

	if err := Table(ctx, f.src, slow, f.store, f.syncName, ref, engine.ChunkOptions{TargetRows: 200}); err != nil {
		t.Fatalf("copy under throttled target: %v", err)
	}
	if got := f.rowCount(t, ctx, f.rawTgt); got != 1000 {
		t.Fatalf("target has %d rows, want 1000", got)
	}
	f.faithful(t, ctx)
}
