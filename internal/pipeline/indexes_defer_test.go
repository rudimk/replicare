package pipeline

import (
	"context"
	"errors"
	"testing"

	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/state"
)

// progressStore answers LoadCopyProgress from a table, or with an error for tables in
// failing; everything else of the StateStore is unused here.
type progressStore struct {
	state.StateStore
	done    map[engine.TableRef]bool
	failing map[engine.TableRef]bool
	saved   []engine.DeferredIndex
}

func (p *progressStore) LoadCopyProgress(_ context.Context, _ string, _ engine.TargetID, t engine.TableRef) (state.CopyProgress, error) {
	if p.failing[t] {
		return state.CopyProgress{}, errors.New("state store unreachable")
	}
	return state.CopyProgress{Table: t, Done: p.done[t]}, nil
}
func (p *progressStore) RecordEvent(context.Context, state.Event) error { return nil }
func (p *progressStore) SaveIndexes(_ context.Context, _ string, _ engine.TargetID, idx []engine.DeferredIndex) error {
	p.saved = append(p.saved, idx...)
	return nil
}
func (p *progressStore) ListIndexes(context.Context, string) ([]state.SavedIndex, error) {
	return nil, nil
}
func (p *progressStore) MarkIndexBuilt(context.Context, string, engine.TargetID, engine.TableRef, string) error {
	return nil
}

// deferringSink records which tables it was asked to take indexes down for.
type deferringSink struct {
	engine.Sink
	engine.IndexDeferrer
	asked   []engine.TableRef
	dropped []string
}

func (d *deferringSink) DeferrableIndexes(_ context.Context, tables []engine.TableRef) ([]engine.DeferredIndex, []engine.IndexSkip, error) {
	d.asked = append(d.asked, tables...)
	out := make([]engine.DeferredIndex, 0, len(tables))
	for _, t := range tables {
		out = append(out, engine.DeferredIndex{Table: t, Name: t.Name + "_idx"})
	}
	return out, nil, nil
}
func (d *deferringSink) DropDeferredIndex(_ context.Context, ix engine.DeferredIndex) error {
	d.dropped = append(d.dropped, ix.Name)
	return nil
}

// TestDeferIndexesKeepsIndexesWhenProgressUnreadable: a table whose copy progress
// cannot be read keeps its indexes (it may well be copied already), a table known
// not to be copied has them taken down, and a cyclic component is taken down whole
// only when one of its tables is known not to be copied.
func TestDeferIndexesKeepsIndexesWhenProgressUnreadable(t *testing.T) {
	a := engine.TableRef{Schema: "public", Name: "a"}
	b := engine.TableRef{Schema: "public", Name: "b"}
	c := engine.TableRef{Schema: "public", Name: "c"}

	cases := []struct {
		name    string
		comp    engine.Component
		done    map[engine.TableRef]bool
		failing map[engine.TableRef]bool
		want    []string
	}{
		{
			name:    "copied table with unreadable progress keeps its indexes",
			comp:    engine.Component{Tables: []engine.TableRef{a, b}, Order: []engine.TableRef{a, b}},
			done:    map[engine.TableRef]bool{b: true},
			failing: map[engine.TableRef]bool{a: true},
			want:    nil,
		},
		{
			name:    "only the table known not to be copied is taken down",
			comp:    engine.Component{Tables: []engine.TableRef{a, b, c}, Order: []engine.TableRef{a, b, c}},
			done:    map[engine.TableRef]bool{b: true},
			failing: map[engine.TableRef]bool{a: true},
			want:    []string{"c_idx"},
		},
		{
			name:    "cyclic component with only unreadable progress is left alone",
			comp:    engine.Component{Tables: []engine.TableRef{a, b}, Order: []engine.TableRef{a, b}, Cyclic: []engine.TableRef{a, b}},
			done:    map[engine.TableRef]bool{b: true},
			failing: map[engine.TableRef]bool{a: true},
			want:    nil,
		},
		{
			name:    "cyclic component with a table not copied is taken down whole",
			comp:    engine.Component{Tables: []engine.TableRef{a, b}, Order: []engine.TableRef{a, b}, Cyclic: []engine.TableRef{a, b}},
			done:    map[engine.TableRef]bool{a: true},
			failing: nil,
			want:    []string{"a_idx", "b_idx"},
		},
		{
			name: "fully copied component is left alone",
			comp: engine.Component{Tables: []engine.TableRef{a, b}, Order: []engine.TableRef{a, b}},
			done: map[engine.TableRef]bool{a: true, b: true},
			want: nil,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := &progressStore{done: tc.done, failing: tc.failing}
			sink := &deferringSink{}
			s := &Syncer{Name: "s1", Target: "dst", Store: store, Sink: sink, DeferIndexes: true}
			s.deferIndexesForComponent(context.Background(), tc.comp)
			if len(sink.dropped) != len(tc.want) {
				t.Fatalf("dropped %v, want %v", sink.dropped, tc.want)
			}
			for i := range tc.want {
				if sink.dropped[i] != tc.want[i] {
					t.Fatalf("dropped %v, want %v", sink.dropped, tc.want)
				}
			}
		})
	}
}
