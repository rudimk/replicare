package reseed

import (
	"context"
	"errors"
	"testing"

	"github.com/rudimk/replicare/internal/engine"
)

// presenceSource records which tables Purge was called for, and answers
// TablesWithChanges from a fixed set (or fails).
type presenceSource struct {
	engine.Source
	queued  map[engine.TableRef]bool
	failAsk bool
	purged  []engine.TableRef
}

func (p *presenceSource) TablesWithChanges(_ context.Context, tables []engine.TableRef) (map[engine.TableRef]bool, error) {
	if p.failAsk {
		return nil, errors.New("down")
	}
	out := map[engine.TableRef]bool{}
	for _, t := range tables {
		out[t] = p.queued[t]
	}
	return out, nil
}

func (p *presenceSource) Purge(_ context.Context, t engine.TableRef, _ []engine.TargetID, _ engine.RetentionPolicy) (engine.PurgeStats, error) {
	p.purged = append(p.purged, t)
	return engine.PurgeStats{DeltasPurged: 1}, nil
}

// Enforce purges only the tables with something queued: on 820 tables, purging each
// one in turn took minutes per pass. If the source cannot say, it purges every table.
func TestEnforceSkipsTablesWithNothingQueued(t *testing.T) {
	a, b, c := engine.TableRef{Schema: "s", Name: "a"}, engine.TableRef{Schema: "s", Name: "b"}, engine.TableRef{Schema: "s", Name: "c"}
	tables := []engine.TableRef{a, b, c}

	src := &presenceSource{queued: map[engine.TableRef]bool{b: true}}
	res, err := Enforce(context.Background(), Deps{Src: src}, "sync", tables, []engine.TargetID{"dst"}, engine.RetentionPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if len(src.purged) != 1 || src.purged[0] != b || res.Purged[b] != 1 {
		t.Fatalf("purged %v (%v), want only %v", src.purged, res.Purged, b)
	}

	src = &presenceSource{failAsk: true}
	if _, err := Enforce(context.Background(), Deps{Src: src}, "sync", tables, []engine.TargetID{"dst"}, engine.RetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	if len(src.purged) != 3 {
		t.Fatalf("with the check failing, purged %v, want every table", src.purged)
	}
}
