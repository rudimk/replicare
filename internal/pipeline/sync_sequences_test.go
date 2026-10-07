package pipeline

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/rudimk/replicare/internal/engine"
)

// fakeReseedSink is a Sink that also implements engine.SequenceReseeder, counting how
// many times each table's counter was advanced. The embedded (nil) engine.Sink satisfies
// the rest of the interface's method set; syncSequences only ever calls ReseedSequences,
// so the nil embed is never dereferenced.
type fakeReseedSink struct {
	engine.Sink
	mu    sync.Mutex
	calls map[engine.TableRef]int
	err   error // if set, every ReseedSequences returns it (best-effort error path)
}

func (f *fakeReseedSink) ReseedSequences(_ context.Context, t engine.TableRef, dryRun bool) (engine.SequenceReseedResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.calls == nil {
		f.calls = map[engine.TableRef]int{}
	}
	f.calls[t]++
	return engine.SequenceReseedResult{}, f.err
}

func (f *fakeReseedSink) total() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.calls {
		n += c
	}
	return n
}

// plainSink (declared in delete_metrics_test.go) is a Sink that does NOT implement
// SequenceReseeder — e.g. Redis — so syncSequences must be a safe no-op against it.

func newSeqSyncer(sink engine.Sink, enabled, cluster bool) *Syncer {
	return &Syncer{
		Name:          "s1",
		Target:        "dst",
		Sink:          sink,
		Replicable:    []engine.TableRef{{Schema: "public", Name: "a"}, {Schema: "public", Name: "b"}},
		SyncSequences: enabled,
		ClusterMode:   cluster,
		// Store is nil: recordEvent/log are nil-store safe, so the best-effort error
		// path logs without panicking.
	}
}

func TestSyncSequencesEnabledAdvancesEachTable(t *testing.T) {
	f := &fakeReseedSink{}
	s := newSeqSyncer(f, true, false)
	s.syncSequences(context.Background())
	if f.total() != 2 || f.calls[s.Replicable[0]] != 1 || f.calls[s.Replicable[1]] != 1 {
		t.Fatalf("expected one advance per table, got %v", f.calls)
	}

	// Throttled: an immediate second call does nothing (lastSeqSync is within the interval).
	s.syncSequences(context.Background())
	if f.total() != 2 {
		t.Errorf("throttle failed: expected still 2 calls, got %d", f.total())
	}
}

func TestSyncSequencesDisabledIsNoop(t *testing.T) {
	f := &fakeReseedSink{}
	s := newSeqSyncer(f, false, false)
	s.syncSequences(context.Background())
	if f.total() != 0 {
		t.Errorf("disabled sync_sequences should not reseed, got %d calls", f.total())
	}
}

func TestSyncSequencesClusterModeIsNoop(t *testing.T) {
	// Defence in depth: even if somehow enabled on a cluster edge, syncSequences must not
	// touch a mesh node's counter (config load already refuses a mesh-member target).
	f := &fakeReseedSink{}
	s := newSeqSyncer(f, true, true)
	s.syncSequences(context.Background())
	if f.total() != 0 {
		t.Errorf("cluster mode must never sync sequences, got %d calls", f.total())
	}
}

func TestSyncSequencesEngineWithoutCapabilityIsNoop(t *testing.T) {
	// A Sink that does not implement SequenceReseeder (Redis) must be a safe no-op.
	s := newSeqSyncer(&plainSink{}, true, false)
	s.syncSequences(context.Background()) // must not panic
}

func TestSyncSequencesBestEffortOnError(t *testing.T) {
	// A reseed error (e.g. the DR-only grant is missing) is logged, not fatal, and does
	// not stop the remaining tables from being attempted.
	f := &fakeReseedSink{err: errors.New("permission denied for sequence")}
	s := newSeqSyncer(f, true, false)
	s.syncSequences(context.Background())
	if f.total() != 2 {
		t.Errorf("an error on one table must not stop the others: got %v", f.calls)
	}
}
