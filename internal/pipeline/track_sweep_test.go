package pipeline

import (
	"context"
	"sync"
	"testing"

	"github.com/rudimk/replicare/internal/engine"
)

// sweepingSource hands out a fixed number of leftover windows per table and records
// the calls.
type sweepingSource struct {
	engine.Source
	mu      sync.Mutex
	left    map[engine.TableRef]int
	calls   int
	closed  bool
	targets []engine.TargetID
}

func (s *sweepingSource) SweepLeftoverTrack(_ context.Context, t engine.TableRef, targets []engine.TargetID) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	s.targets = targets
	if s.left[t] == 0 {
		return 0, nil
	}
	s.left[t]--
	return 100, nil
}

func (s *sweepingSource) Close(context.Context) error { s.closed = true; return nil }

// TestTrackSweepRoundClearsEveryTable: a round sweeps each table window by window
// until nothing is left, for this syncer's target only, on a connection it opens for
// the round and closes after.
func TestTrackSweepRoundClearsEveryTable(t *testing.T) {
	old := trackSweepPause
	trackSweepPause = 0
	t.Cleanup(func() { trackSweepPause = old })

	a := engine.TableRef{Schema: "public", Name: "a"}
	b := engine.TableRef{Schema: "public", Name: "b"}
	fresh := &sweepingSource{left: map[engine.TableRef]int{a: 3, b: 1}}
	s := &Syncer{
		Name: "s1", Target: "dst", Replicable: []engine.TableRef{a, b},
		Source:     &sweepingSource{},
		OpenSource: func(context.Context) (engine.Source, error) { return fresh, nil },
	}
	removed, err := s.sweepTrackRound(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if removed != 400 {
		t.Fatalf("removed %d rows, want 400 (3 windows of a, 1 of b)", removed)
	}
	if fresh.calls != 6 {
		t.Fatalf("%d sweep calls, want 6 (each table until it returns 0)", fresh.calls)
	}
	if len(fresh.targets) != 1 || fresh.targets[0] != "dst" {
		t.Fatalf("swept for targets %v, want only this syncer's target", fresh.targets)
	}
	if !fresh.closed {
		t.Fatal("the round's source connection was not closed")
	}
}
