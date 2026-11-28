package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/apply"
	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/observability/prom"
	"github.com/rudimk/replicare/internal/observability/telemetry"
)

// presenceSrc answers TablesWithChanges from a fixed set and counts DeltaBacklog reads.
type presenceSrc struct {
	engine.Source
	queued map[engine.TableRef]bool
	asked  []engine.TableRef
}

func (p *presenceSrc) TablesWithChanges(_ context.Context, tables []engine.TableRef) (map[engine.TableRef]bool, error) {
	out := map[engine.TableRef]bool{}
	for _, t := range tables {
		out[t] = p.queued[t]
	}
	return out, nil
}

func (p *presenceSrc) DeltaBacklog(_ context.Context, t engine.TableRef, _ engine.TargetID) (engine.DeltaBacklog, error) {
	p.asked = append(p.asked, t)
	return engine.DeltaBacklog{Rows: 7, HasBacklog: true, OldestAge: time.Hour}, nil
}

// The backlog round reads only the tables with something queued, and reports a zero
// backlog for the rest: on 820 tables, reading each one took the first round 5 minutes.
func TestBacklogRoundAsksOnlyQueuedTables(t *testing.T) {
	a, b, c := engine.TableRef{Schema: "s", Name: "a"}, engine.TableRef{Schema: "s", Name: "b"}, engine.TableRef{Schema: "s", Name: "c"}
	src := &presenceSrc{queued: map[engine.TableRef]bool{b: true}}
	s := &Syncer{Name: "s", Target: "dst", Replicable: []engine.TableRef{a, b, c}, Tel: telemetry.New(prom.New(), nil, nil, nil)}
	got, err := s.publishBacklogsFrom(context.Background(), src)
	if err != nil {
		t.Fatal(err)
	}
	if len(src.asked) != 1 || src.asked[0] != b {
		t.Fatalf("read the backlog of %v, want only %v", src.asked, b)
	}
	if len(got) != 3 || got[a] != (engine.DeltaBacklog{}) || got[c] != (engine.DeltaBacklog{}) || got[b].Rows != 7 {
		t.Fatalf("backlogs = %v, want all three, a and c zero", got)
	}
}

func (p *presenceSrc) ReadDirtyKeys(_ context.Context, t engine.TableRef, _ engine.TargetID, _ int) ([]engine.DirtyKey, error) {
	p.asked = append(p.asked, t)
	return nil, nil
}

// A pass drains only the groups with something queued: the others have nothing to apply,
// and reading each empty queue cost round trips on every pass.
func TestPassSkipsGroupsWithNothingQueued(t *testing.T) {
	a, b, c := engine.TableRef{Schema: "s", Name: "a"}, engine.TableRef{Schema: "s", Name: "b"}, engine.TableRef{Schema: "s", Name: "c"}
	src := &presenceSrc{queued: map[engine.TableRef]bool{c: true}}
	s := &Syncer{Name: "s", Target: "dst", DrainBatch: 10, Components: []engine.Component{
		{Tables: []engine.TableRef{a}, Order: []engine.TableRef{a}},
		{Tables: []engine.TableRef{b, c}, Order: []engine.TableRef{b, c}}, // c is queued: the whole group runs
	}}
	if _, err := s.drainComponents(context.Background(), []apply.Conn{{Src: src}}); err != nil {
		t.Fatal(err)
	}
	for _, x := range src.asked {
		if x == a {
			t.Fatalf("read the queue of %v, which has nothing queued (read %v)", a, src.asked)
		}
	}
	if len(src.asked) != 2 {
		t.Fatalf("read %v, want the queued group's two tables", src.asked)
	}
}
