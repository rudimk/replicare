package pipeline

import (
	"testing"

	"github.com/rudimk/replicare/internal/engine"
)

// TestPurgeTargetsIncludesOwn: a table's purge waits for every listed consumer, and
// always for the syncer's own target, even when the list leaves it out or is empty.
func TestPurgeTargetsIncludesOwn(t *testing.T) {
	a := engine.TableRef{Schema: "s", Name: "a"}
	b := engine.TableRef{Schema: "s", Name: "b"}
	s := &Syncer{Target: "mine", PurgeTargets: map[engine.TableRef][]engine.TargetID{
		a: {"other", "mine"},
		b: {"other"},
	}}
	if got := s.purgeTargets(a); len(got) != 2 {
		t.Errorf("a: %v, want [other mine]", got)
	}
	if got := s.purgeTargets(b); len(got) != 2 || got[1] != "mine" {
		t.Errorf("b: %v, want [other mine]", got)
	}
	if got := s.purgeTargets(engine.TableRef{Schema: "s", Name: "c"}); len(got) != 1 || got[0] != "mine" {
		t.Errorf("unlisted table: %v, want [mine]", got)
	}
	// The list for b is not modified by adding the own target.
	if len(s.PurgeTargets[b]) != 1 {
		t.Errorf("PurgeTargets[b] modified: %v", s.PurgeTargets[b])
	}
}
