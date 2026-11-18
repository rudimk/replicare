package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/rudimk/replicare/internal/engine"
)

func collectProgress() (context.Context, map[engine.TableRef]int64) {
	got := map[engine.TableRef]int64{}
	return withCopyProgress(context.Background(), func(t engine.TableRef, n int64) { got[t] += n }), got
}

func TestCountedLoadReportsOnlyCommittedRows(t *testing.T) {
	ctx, got := collectProgress()
	tbl := ref("public.orders")

	if err := countedLoad(ctx, tbl, func(ctx context.Context) error {
		countCopiedRows(ctx, 40)
		return errors.New("rolled back")
	}); err == nil {
		t.Fatal("want the load error back")
	}
	if got[tbl] != 0 {
		t.Fatalf("a failed load must report nothing, got %d", got[tbl])
	}

	if err := countedLoad(ctx, tbl, func(ctx context.Context) error {
		countCopiedRows(ctx, 40)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got[tbl] != 40 {
		t.Fatalf("got %d, want 40", got[tbl])
	}
}

func TestCountAttemptDropsFailedAttempts(t *testing.T) {
	ctx, got := collectProgress()
	tbl := ref("public.orders")
	attempts := 0
	err := countedLoad(ctx, tbl, func(ctx context.Context) error {
		for {
			attempts++
			_, err := countAttempt(ctx, func(ctx context.Context) error {
				countCopiedRows(ctx, 25) // each attempt moves the same 25 rows
				if attempts < 3 {
					return errors.New("connection dropped")
				}
				return nil
			})
			if err == nil {
				return nil
			}
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if got[tbl] != 25 {
		t.Fatalf("three attempts at the same chunk must count once: got %d, want 25", got[tbl])
	}
}

func TestParentRecopyIsNotCounted(t *testing.T) {
	// withParentRetry hands the re-copy the caller's context, outside the child's
	// counted load — so re-copied parent rows never inflate any table's total.
	ctx, got := collectProgress()
	child, parent := ref("public.child"), ref("public.parent")
	fail := true
	load := func() error {
		return countedLoad(ctx, child, func(ctx context.Context) error {
			countCopiedRows(ctx, 10)
			if fail {
				fail = false
				return fkErr("public.child")
			}
			return nil
		})
	}
	recopy := func(ctx context.Context, _ engine.TableRef) error {
		countCopiedRows(ctx, 500) // the parent's rows, copied again
		return nil
	}
	if err := withParentRetry(ctx, load, []engine.TableRef{parent}, recopy); err != nil {
		t.Fatal(err)
	}
	if got[child] != 10 || got[parent] != 0 || len(got) != 1 {
		t.Fatalf("got %v, want only child=10", got)
	}
}
