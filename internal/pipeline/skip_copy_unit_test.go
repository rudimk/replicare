package pipeline

import (
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

// TestAllCaughtUp: caught up only when every table's backlog was read and its oldest
// queued change is under a minute old.
func TestAllCaughtUp(t *testing.T) {
	a := engine.TableRef{Schema: "s", Name: "a"}
	b := engine.TableRef{Schema: "s", Name: "b"}
	tables := []engine.TableRef{a, b}
	fresh := engine.DeltaBacklog{Rows: 3, OldestAge: 5 * time.Second}
	old := engine.DeltaBacklog{Rows: 3, OldestAge: 2 * time.Minute}
	if !allCaughtUp(map[engine.TableRef]engine.DeltaBacklog{a: fresh, b: {}}, tables) {
		t.Error("fresh backlogs: want caught up")
	}
	if allCaughtUp(map[engine.TableRef]engine.DeltaBacklog{a: fresh, b: old}, tables) {
		t.Error("one old backlog: want not caught up")
	}
	if allCaughtUp(map[engine.TableRef]engine.DeltaBacklog{a: fresh}, tables) {
		t.Error("an unread backlog: want not caught up")
	}
}
