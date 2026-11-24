package daemon

import (
	"strings"
	"testing"

	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/observability/live"
)

// TestSkipCheckResults: a verify report becomes one result per table, and a report that
// does not cover the target never passes anything.
func TestSkipCheckResults(t *testing.T) {
	a := engine.TableRef{Schema: "public", Name: "a"}
	b := engine.TableRef{Schema: "public", Name: "b"}
	c := engine.TableRef{Schema: "public", Name: "c"}
	d := engine.TableRef{Schema: "public", Name: "d"}
	e := engine.TableRef{Schema: "public", Name: "e"}
	rep := live.VerifyReport{Targets: []live.VerifyTarget{{Target: "tgt", Tables: []live.VerifyTable{
		{Table: "public.a", Converged: true, Status: "ok", Mode: "chunked"},
		{Table: "public.b", Status: "drift-checksum", Mode: "chunked", Missing: 2, Different: 1,
			Keys: []live.VerifyKey{{Key: "(7)", Problem: "missing"}}},
		{Table: "public.c", Status: "error", Error: "timeout"},
		{Table: "public.e", Converged: true, Status: "ok", Mode: "sampled"},
	}}}}
	got, err := skipCheckResults(rep, "tgt", []engine.TableRef{a, b, c, d, e})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 5 {
		t.Fatalf("got %d results, want 5", len(got))
	}
	if !got[0].Match || got[0].Failed {
		t.Errorf("a: %+v, want a match", got[0])
	}
	if got[1].Match || got[1].Failed || !strings.Contains(got[1].Detail, "2 rows missing") || !strings.Contains(got[1].Detail, "(7)") {
		t.Errorf("b: %+v, want drift naming the missing rows and a key", got[1])
	}
	if !got[2].Failed || got[2].Detail != "timeout" {
		t.Errorf("c: %+v, want a failed comparison", got[2])
	}
	if !got[3].Failed {
		t.Errorf("d (not in the report): %+v, want a failed comparison", got[3])
	}
	if got[4].Match {
		t.Errorf("e (only sampled): %+v, a sample must never confirm a table", got[4])
	}

	for name, bad := range map[string]live.VerifyReport{
		"whole-report error": {Error: "source unreachable"},
		"no targets":         {},
		"another target":     {Targets: []live.VerifyTarget{{Target: "other"}}},
		"target error":       {Targets: []live.VerifyTarget{{Target: "tgt", Error: "unreachable"}}},
	} {
		if _, err := skipCheckResults(bad, "tgt", []engine.TableRef{a}); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
}
