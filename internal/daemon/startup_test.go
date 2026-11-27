package daemon

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	stdsync "sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/observability/health"
	"github.com/rudimk/replicare/internal/observability/prom"
	"github.com/rudimk/replicare/internal/observability/telemetry"
	"github.com/rudimk/replicare/internal/state"
)

// eventStore is a state store that only records events; everything else is unused here.
type eventStore struct {
	state.StateStore
	mu     stdsync.Mutex
	events []state.Event
}

func (s *eventStore) RecordEvent(_ context.Context, e state.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, e)
	return nil
}

func (s *eventStore) eventsFor(job string) []state.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []state.Event
	for _, e := range s.events {
		if e.Sync == job {
			out = append(out, e)
		}
	}
	return out
}

type logBuf struct {
	mu stdsync.Mutex
	b  bytes.Buffer
}

func (l *logBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *logBuf) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

func testDaemon(t *testing.T) (*Daemon, *eventStore, *logBuf) {
	t.Helper()
	store := &eventStore{}
	logs := &logBuf{}
	log := slog.New(slog.NewTextHandler(logs, nil))
	reg := prom.New()
	return &Daemon{store: store, metrics: reg, tel: telemetry.New(reg, nil, log, nil), log: log, beat: health.New(time.Minute)}, store, logs
}

func fastRetries(t *testing.T) {
	t.Helper()
	oldMin, oldMax := startRetryMin, startRetryMax
	startRetryMin, startRetryMax = 5*time.Millisecond, 20*time.Millisecond
	t.Cleanup(func() { startRetryMin, startRetryMax = oldMin, oldMax })
}

// TestRunWithRetryIsolatesAFailingJob: a job that cannot start is logged and retried,
// while a healthy job next to it keeps running untouched.
func TestRunWithRetryIsolatesAFailingJob(t *testing.T) {
	fastRetries(t)
	d, store, logs := testDaemon(t)
	ctx, cancel := context.WithCancel(context.Background())

	var badAttempts, goodRuns atomic.Int32
	var wg stdsync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		d.runWithRetry(ctx, "bad", "dst", func(context.Context, func()) error {
			badAttempts.Add(1)
			return errors.New("source unreachable")
		})
	}()
	go func() {
		defer wg.Done()
		d.runWithRetry(ctx, "good", "dst", func(ctx context.Context, started func()) error {
			goodRuns.Add(1)
			started()
			<-ctx.Done()
			return ctx.Err()
		})
	}()
	time.Sleep(150 * time.Millisecond)
	cancel()
	wg.Wait()

	if n := badAttempts.Load(); n < 3 {
		t.Fatalf("failing job tried %d times, want it retried", n)
	}
	if n := goodRuns.Load(); n != 1 {
		t.Fatalf("healthy job ran %d times, want once (untouched by the other's failures)", n)
	}
	evs := store.eventsFor("bad")
	if len(evs) == 0 || evs[0].Level != "ERROR" || !strings.Contains(evs[0].Message, "could not start") ||
		!strings.Contains(evs[0].Message, "source unreachable") {
		t.Fatalf("failing job's events = %+v, want an ERROR naming the cause", evs)
	}
	if evs := store.eventsFor("good"); len(evs) != 0 {
		t.Fatalf("healthy job recorded %+v, want nothing", evs)
	}
	if !strings.Contains(logs.String(), "source unreachable") {
		t.Fatalf("the failure is not in the log:\n%s", logs.String())
	}
}

// TestRunWithRetryAfterStreamingStops: a job that started and then stopped is reported as
// stopped (not as failing to start), and its wait starts from the shortest again.
func TestRunWithRetryAfterStreamingStops(t *testing.T) {
	fastRetries(t)
	d, store, _ := testDaemon(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var runs atomic.Int32
	done := make(chan struct{})
	go func() {
		defer close(done)
		d.runWithRetry(ctx, "s", "dst", func(_ context.Context, started func()) error {
			if runs.Add(1) >= 2 {
				cancel()
				return nil
			}
			started()
			return errors.New("apply halted")
		})
	}()
	<-done
	evs := store.eventsFor("s")
	if len(evs) != 1 || !strings.Contains(evs[0].Message, "stopped streaming") || !strings.Contains(evs[0].Message, "attempt 1") {
		t.Fatalf("events = %+v, want one 'stopped streaming (attempt 1 …)'", evs)
	}
}

func TestBeatUnregisterStopsTheClock(t *testing.T) {
	b := health.New(time.Nanosecond)
	b.Register("k")
	time.Sleep(time.Millisecond)
	if _, stale := b.Stale(); !stale {
		t.Fatal("registered key not stale")
	}
	b.Unregister("k")
	if k, stale := b.Stale(); stale {
		t.Fatalf("unregistered key %q still counts", k)
	}
}

// TestLogPreflightSaysWhyNothingReplicates: a sync with nothing to replicate says so at
// ERROR, with the reason: no table matched, or none has a key.
func TestLogPreflightSaysWhyNothingReplicates(t *testing.T) {
	d, _, logs := testDaemon(t)
	sel := engine.Selection{Include: []string{"public.*"}}
	d.logPreflight(context.Background(), "a", "dst", 0, sel, &engine.PreflightReport{})
	d.logPreflight(context.Background(), "b", "dst", 2, sel, &engine.PreflightReport{Skipped: []engine.SkippedTable{
		{Table: engine.TableRef{Schema: "public", Name: "x"}}, {Table: engine.TableRef{Schema: "public", Name: "y"}}}})
	d.logPreflight(context.Background(), "c", "dst", 3, sel, &engine.PreflightReport{
		Replicable: []engine.TableRef{{Schema: "public", Name: "p"}, {Schema: "public", Name: "q"}},
		Skipped:    []engine.SkippedTable{{Table: engine.TableRef{Schema: "public", Name: "z"}}}})
	out := logs.String()
	for _, want := range []string{
		`level=ERROR msg="replicates nothing: the selection (include [public.*]`,
		"matches no table on the source",
		"none has a primary key or unique key",
		"level=WARN msg=\"replicating 2 tables",
		"public.z",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("log lacks %q:\n%s", want, out)
		}
	}
}

// TestRunWithRetryClearsFailingWhileTryingAgain: replicare_sync_start_failed is 1 only
// while a job waits to be retried. An attempt that gets past the failure is still in its
// initial copy, long before it streams, and must not keep reading as failing.
func TestRunWithRetryClearsFailingWhileTryingAgain(t *testing.T) {
	fastRetries(t)
	d, _, _ := testDaemon(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const failing = `replicare_sync_start_failed{sync="s",target="dst"}`
	gauge := func() string {
		for _, line := range strings.Split(scrapeMetrics(t, d), "\n") {
			if strings.HasPrefix(line, failing+" ") {
				return strings.TrimPrefix(line, failing+" ")
			}
		}
		return "absent"
	}
	copying := make(chan struct{})
	var attempts atomic.Int32
	done := make(chan struct{})
	go func() {
		defer close(done)
		d.runWithRetry(ctx, "s", "dst", func(ctx context.Context, _ func()) error {
			if attempts.Add(1) == 1 {
				return errors.New("pre-flight blocked")
			}
			close(copying) // the second attempt is copying: not streaming yet
			<-ctx.Done()
			return ctx.Err()
		})
	}()
	<-copying
	if v := gauge(); v != "0" {
		t.Errorf("failing gauge while the next attempt copies = %s, want 0", v)
	}
	cancel()
	<-done
}
