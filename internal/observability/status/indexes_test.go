package status

import (
	"testing"

	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/state"
)

func TestIndexStatuses(t *testing.T) {
	tb := engine.TableRef{Schema: "public", Name: "msg"}
	saved := []state.SavedIndex{
		{Target: "a", Index: engine.DeferredIndex{Table: tb, Name: "x_idx"}, Built: true},
		{Target: "a", Index: engine.DeferredIndex{Table: tb, Name: "y_idx"}},
		{Target: "b", Index: engine.DeferredIndex{Table: tb, Name: "x_idx"}, Built: true},
	}
	got := indexStatuses(saved)
	if len(got) != 2 {
		t.Fatalf("got %+v, want one entry per target", got)
	}
	if got[0].Target != "a" || got[0].Total != 2 || got[0].Built != 1 || len(got[0].Pending) != 1 || got[0].Pending[0] != "public.msg.y_idx" {
		t.Errorf("target a = %+v", got[0])
	}
	if got[1].Target != "b" || got[1].Total != 1 || got[1].Built != 1 || len(got[1].Pending) != 0 {
		t.Errorf("target b = %+v", got[1])
	}
}
