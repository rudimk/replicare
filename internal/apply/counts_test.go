package apply

import (
	"context"
	"sync"
	"testing"

	"github.com/rudimk/replicare/internal/engine"
)

func TestAppliedCountsAccumulate(t *testing.T) {
	ctx, read := WithAppliedCounts(context.Background())
	a := engine.TableRef{Schema: "public", Name: "a"}
	b := engine.TableRef{Schema: "public", Name: "b"}

	recordApplied(ctx, a, 3)
	recordApplied(ctx, b, 5)
	recordApplied(ctx, a, 2) // same table across passes/components sums
	recordApplied(ctx, a, 0) // zero is a no-op

	got := read()
	if got[a] != 5 {
		t.Errorf("table a = %d, want 5", got[a])
	}
	if got[b] != 5 {
		t.Errorf("table b = %d, want 5", got[b])
	}
	if len(got) != 2 {
		t.Errorf("expected 2 tables, got %d: %v", len(got), got)
	}
}

func TestRecordAppliedNoAccumulatorIsNoop(t *testing.T) {
	// No accumulator installed on the context: recordApplied must be a safe no-op, so
	// every existing drain caller (which passes a plain context) is unaffected.
	recordApplied(context.Background(), engine.TableRef{Name: "x"}, 7)
}

func TestAppliedCountsConcurrent(t *testing.T) {
	// The concurrent (pooled) drain paths call recordApplied from several goroutines;
	// the accumulator must be race-free.
	ctx, read := WithAppliedCounts(context.Background())
	tbl := engine.TableRef{Schema: "public", Name: "t"}
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); recordApplied(ctx, tbl, 1) }()
	}
	wg.Wait()
	if got := read()[tbl]; got != 50 {
		t.Errorf("concurrent accumulate = %d, want 50", got)
	}
}
