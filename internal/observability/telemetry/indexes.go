package telemetry

import (
	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/observability"
)

// SetDeferredIndexes publishes how many of a target's plain indexes were taken down
// for the initial copy and how many are built again.
func (t *Telemetry) SetDeferredIndexes(sync string, target engine.TargetID, total, built int) {
	if t.reg == nil {
		return
	}
	t.reg.Gauge(observability.MetricIndexesDeferred).WithLabelValues(sync, string(target)).Set(float64(total))
	t.reg.Gauge(observability.MetricIndexesBuilt).WithLabelValues(sync, string(target)).Set(float64(built))
}

// SetIndexBuild publishes the index build in flight on a target: its table, name and
// phase as an info series (exactly one per target), and how far it is through that
// phase. An empty index clears both: nothing is building.
func (t *Telemetry) SetIndexBuild(sync string, target engine.TargetID, table engine.TableRef, index string, p engine.IndexBuildProgress) {
	if t.reg == nil {
		return
	}
	info := t.reg.Gauge(observability.MetricIndexBuildInfo)
	ratio := t.reg.Gauge(observability.MetricIndexBuildPhaseRatio)
	t.mu.Lock()
	defer t.mu.Unlock()
	key := sync + "\x00" + string(target)
	if prev, ok := t.indexBuild[key]; ok {
		info.DeleteLabelValues(prev...)
		delete(t.indexBuild, key)
	}
	if index == "" {
		ratio.DeleteLabelValues(sync, string(target))
		return
	}
	labels := []string{sync, string(target), table.String(), index, p.Phase}
	info.WithLabelValues(labels...).Set(1)
	if t.indexBuild == nil {
		t.indexBuild = map[string][]string{}
	}
	t.indexBuild[key] = labels
	r := 0.0
	if p.Total > 0 {
		r = float64(p.Done) / float64(p.Total)
		if r > 1 {
			r = 1
		}
	}
	ratio.WithLabelValues(sync, string(target)).Set(r)
}
