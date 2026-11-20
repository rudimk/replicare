package telemetry

import (
	"testing"

	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/observability"
	"github.com/rudimk/replicare/internal/observability/prom"
)

// TestIndexBuildSeries: one build-info series per target, replaced as the build moves
// through its phases and onto the next index, and cleared when nothing is building.
func TestIndexBuildSeries(t *testing.T) {
	reg := prom.New()
	tel := New(reg, nil, nil, nil)
	tb := engine.TableRef{Schema: "public", Name: "msg"}
	info := func(index, phase string) (float64, bool) {
		return gaugeValue(t, reg, observability.MetricIndexBuildInfo, map[string]string{
			"sync": "s", "target": "dst", "table": "public.msg", "index": index, "phase": phase})
	}
	ratio := func() (float64, bool) {
		return gaugeValue(t, reg, observability.MetricIndexBuildPhaseRatio, map[string]string{"sync": "s", "target": "dst"})
	}

	tel.SetDeferredIndexes("s", "dst", 3, 1)
	if v, ok := gaugeValue(t, reg, observability.MetricIndexesBuilt, map[string]string{"sync": "s", "target": "dst"}); !ok || v != 1 {
		t.Fatalf("built = %v (%v), want 1", v, ok)
	}

	tel.SetIndexBuild("s", "dst", tb, "a_idx", engine.IndexBuildProgress{Phase: "building index: scanning table", Done: 25, Total: 100})
	if v, ok := ratio(); !ok || v != 0.25 {
		t.Fatalf("phase ratio = %v (%v), want 0.25", v, ok)
	}
	tel.SetIndexBuild("s", "dst", tb, "a_idx", engine.IndexBuildProgress{Phase: "building index: sorting live tuples"})
	if _, ok := info("a_idx", "building index: scanning table"); ok {
		t.Error("the previous phase's series is still there")
	}
	if v, ok := info("a_idx", "building index: sorting live tuples"); !ok || v != 1 {
		t.Errorf("current phase series = %v (%v), want 1", v, ok)
	}
	if v, _ := ratio(); v != 0 {
		t.Errorf("phase ratio with no progress reported = %v, want 0", v)
	}
	tel.SetIndexBuild("s", "dst", tb, "", engine.IndexBuildProgress{})
	if _, ok := info("a_idx", "building index: sorting live tuples"); ok {
		t.Error("build series left behind after the build ended")
	}
	if _, ok := ratio(); ok {
		t.Error("phase ratio left behind after the build ended")
	}
}
