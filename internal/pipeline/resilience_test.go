package pipeline

import (
	"context"
	"errors"
	"io"
	"runtime"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

// --- slow-but-reachable target ---

// slowSink wraps a Sink and throttles the staging COPY of every apply pass,
// simulating a slow-but-reachable target. Because the streaming apply pipes the
// source re-read into the target staging through an unbuffered io.Pipe, a slow
// target backpressures the source read directly — a pass never buffers the whole
// batch in Go memory regardless of target speed (CLAUDE.md §4.1 backpressure).
type slowSink struct {
	engine.Sink
	delay time.Duration
}

func (s slowSink) BeginApply(ctx context.Context, cyclic bool, componentTables []engine.TableRef) (engine.ApplyTx, error) {
	tx, err := s.Sink.BeginApply(ctx, cyclic, componentTables)
	if err != nil {
		return nil, err
	}
	return slowTx{ApplyTx: tx, delay: s.delay}, nil
}

type slowTx struct {
	engine.ApplyTx
	delay time.Duration
}

func (t slowTx) StageUpsert(ctx context.Context, ref engine.TableRef, cols []string, reread io.Reader) error {
	return t.ApplyTx.StageUpsert(ctx, ref, cols, &slowReader{r: reread, delay: t.delay})
}

type slowReader struct {
	r     io.Reader
	delay time.Duration
}

func (s *slowReader) Read(p []byte) (int, error) {
	time.Sleep(s.delay)
	return s.r.Read(p)
}

// TestStreamSlowTargetBackpressure: a large backlog drains to convergence against
// a deliberately slow target, and goroutine count returns to baseline afterward —
// evidence the pipe-based apply backpressures rather than buffering unboundedly.
func TestStreamSlowTargetBackpressure(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	_, rawTgt := freshFKSchema(t, ctx)
	syncer := buildSyncer(t, ctx)
	if err := syncer.Bringup(ctx); err != nil {
		t.Fatalf("Bringup: %v", err)
	}
	// Slow the target only for the streaming phase (bring-up copy stays fast).
	rawSrc := rawConn(t, ctx, srcCfg())
	defer rawSrc.Close(context.Background())
	mustExecC(t, ctx, rawSrc, "INSERT INTO rc_it.orders SELECT g, (g%20)+1, 'o'||g FROM generate_series(1000, 1000+1999) g")

	syncer.Sink = slowSink{Sink: syncer.Sink, delay: 200 * time.Microsecond}
	syncer.DrainBatch = 5000

	base := stableGoroutines()
	if err := syncer.StreamOnce(ctx); err != nil {
		t.Fatalf("StreamOnce under slow target: %v", err)
	}
	if got := countRows(t, ctx, rawTgt, "rc_it.orders"); got != 50+2000 {
		t.Fatalf("target orders = %d, want %d after backpressured drain", got, 2050)
	}
	after := stableGoroutines()
	if after > base+8 {
		t.Errorf("goroutine count grew from ~%d to ~%d — possible unbounded buffering/leak", base, after)
	}
}

// stableGoroutines lets transient goroutines settle, then samples the count.
func stableGoroutines() int {
	for i := 0; i < 5; i++ {
		runtime.GC()
		time.Sleep(50 * time.Millisecond)
	}
	return runtime.NumGoroutine()
}

// --- target down then recovery ---

// togglableSink fails every apply while down is true, simulating a target that
// goes away mid-stream and later returns.
type togglableSink struct {
	engine.Sink
	down *bool
}

func (s togglableSink) BeginApply(ctx context.Context, cyclic bool, componentTables []engine.TableRef) (engine.ApplyTx, error) {
	if *s.down {
		return nil, errors.New("simulated target down: connection refused")
	}
	return s.Sink.BeginApply(ctx, cyclic, componentTables)
}

func (s togglableSink) ServerVersion(ctx context.Context) (int, error) {
	if *s.down {
		return 0, errors.New("simulated target down")
	}
	return s.Sink.ServerVersion(ctx)
}

// TestStreamTargetDownThenRecovers: a drain pass while the target is down fails
// (surfacing the down signal) but does not lose data; once the target returns, a
// later pass converges. The streaming loop is resilient — a transient outage is
// retried, never silently dropped.
func TestStreamTargetDownThenRecovers(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	rawSrc, rawTgt := freshFKSchema(t, ctx)
	syncer := buildSyncer(t, ctx)
	if err := syncer.Bringup(ctx); err != nil {
		t.Fatalf("Bringup: %v", err)
	}

	down := true
	syncer.Sink = togglableSink{Sink: syncer.Sink, down: &down}

	// Changes arrive while the target is down.
	mustExecC(t, ctx, rawSrc, "INSERT INTO rc_it.orders VALUES (9001, 1, 'while-down'), (9002, 2, 'while-down2')")

	// A pass while down must fail (not silently succeed) and lose nothing.
	if err := syncer.StreamOnce(ctx); err == nil {
		t.Fatalf("StreamOnce should fail while the target is down")
	}
	if got := countRows(t, ctx, rawTgt, "rc_it.orders WHERE id IN (9001,9002)"); got != 0 {
		t.Fatalf("nothing should have been applied while down, target has %d", got)
	}

	// Target recovers; a later pass converges.
	down = false
	if err := syncer.StreamOnce(ctx); err != nil {
		t.Fatalf("StreamOnce after recovery: %v", err)
	}
	if got := countRows(t, ctx, rawTgt, "rc_it.orders WHERE id IN (9001,9002)"); got != 2 {
		t.Errorf("target did not converge after recovery: %d of 2 rows applied", got)
	}
}
