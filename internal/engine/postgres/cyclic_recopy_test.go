package postgres

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/rudimk/replicare/internal/engine"
)

func fkErr(table string) error {
	return fmt.Errorf("upsert %s: %w", table, &pgconn.PgError{Code: "23503", Message: "violates foreign key constraint"})
}

// The production failure: CreditTransaction's chunk hits an FK miss, so its parent Project
// is re-copied — but the fresh Project snapshot references a User created after User was
// copied. One-hop retry gave up there and failed the whole component. The re-copy must
// walk up: re-copy User, then Project again, then the child converges.
func TestTransitiveRecopierWalksUpTheChain(t *testing.T) {
	user, project, credit := ref("public.User"), ref("public.Project"), ref("public.CreditTransaction")
	parents := map[engine.TableRef][]engine.TableRef{
		credit:  {project},
		project: {user},
	}

	userCopies := 0
	var calls []string
	one := func(_ context.Context, p engine.TableRef) error {
		calls = append(calls, p.String())
		switch p {
		case user:
			userCopies++
			return nil
		case project:
			if userCopies == 0 {
				return fkErr("public.Project") // references a User the target does not have yet
			}
			return nil
		}
		return fmt.Errorf("unexpected re-copy of %s", p)
	}

	childOK := false
	load := func() error {
		if childOK {
			return nil
		}
		return fkErr("public.CreditTransaction")
	}
	recopy := transitiveRecopier(one, parents)
	wrapped := func(ctx context.Context, p engine.TableRef) error {
		err := recopy(ctx, p)
		if err == nil && p == project {
			childOK = true // the parent now holds every row the child references
		}
		return err
	}

	if err := withParentRetry(context.Background(), load, parents[credit], wrapped); err != nil {
		t.Fatalf("expected convergence after re-copying the grandparent, got %v (calls %v)", err, calls)
	}
	want := []string{"public.Project", "public.User", "public.Project"}
	if fmt.Sprint(calls) != fmt.Sprint(want) {
		t.Errorf("re-copy order = %v, want %v", calls, want)
	}
}

func TestTransitiveRecopierHaltsOnNonFKError(t *testing.T) {
	p, gp := ref("public.p"), ref("public.gp")
	boom := errors.New("disk full")
	calls := 0
	one := func(_ context.Context, r engine.TableRef) error {
		calls++
		if r == p {
			return boom
		}
		return nil
	}
	err := transitiveRecopier(one, map[engine.TableRef][]engine.TableRef{p: {gp}})(context.Background(), p)
	if !errors.Is(err, boom) {
		t.Fatalf("got %v, want the non-FK error passed straight through", err)
	}
	if calls != 1 {
		t.Errorf("a non-FK error must not trigger grandparent re-copies; calls = %d", calls)
	}
}

func TestTransitiveRecopierGuardsAgainstLoops(t *testing.T) {
	// `parents` is acyclic by construction, but a malformed map must not recurse forever.
	a, b := ref("public.a"), ref("public.b")
	one := func(_ context.Context, r engine.TableRef) error { return fkErr(r.String()) }
	err := transitiveRecopier(one, map[engine.TableRef][]engine.TableRef{a: {b}, b: {a}})(context.Background(), a)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23503" {
		t.Fatalf("got %v, want the FK error back (terminated, not recursed forever)", err)
	}
}
