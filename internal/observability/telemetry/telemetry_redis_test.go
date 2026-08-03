package telemetry

import (
	"testing"

	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/observability"
	"github.com/rudimk/replicare/internal/observability/prom"
)

// TestRedisUnitLabelsThroughReusedMetrics is the RM8 "table -> unit" verification:
// a Redis replication unit (TableRef{Schema:"redis", Name:"db0"}) flows through the
// REUSED F2 emitters and lands on the scrape with a well-formed `table="redis.db0"`
// label — no Redis-specific metric code, the neutral series just work.
func TestRedisUnitLabelsThroughReusedMetrics(t *testing.T) {
	reg := prom.New()
	tel := New(reg, nil, nil, nil)
	unit := engine.TableRef{Schema: "redis", Name: "db0"}
	const sync, target = "redis-sync", engine.TargetID("dst")

	tel.SetBacklog(sync, target, unit, engine.DeltaBacklog{Rows: 5, Bytes: 40, OldestAge: 0})
	tel.SetReplicationLag(sync, target, unit, 2.5) // reconciliation age
	tel.AddRowsCopied(sync, unit, 100)             // keys copied
	tel.SetPhase(sync, unit, "streaming")
	tel.SetDeleteLag(sync, target, unit, 1.0)
	tel.AddDeletes(sync, target, unit, 3)

	fams, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	labelOf := func(name, label string) (string, bool) {
		for _, f := range fams {
			if f.GetName() != name || len(f.Metric) == 0 {
				continue
			}
			for _, lp := range f.Metric[0].Label {
				if lp.GetName() == label {
					return lp.GetValue(), true
				}
			}
		}
		return "", false
	}

	for _, name := range []string{
		observability.MetricDeltaBacklog,
		observability.MetricReplicationLagSeconds,
		observability.MetricRowsCopiedTotal,
		observability.MetricPhaseInfo,
		observability.MetricDeleteReconcileLag,
		observability.MetricDeletesReconciled,
	} {
		got, ok := labelOf(name, "table")
		if !ok {
			t.Errorf("%s: not populated / no table label", name)
			continue
		}
		if got != "redis.db0" {
			t.Errorf("%s: table label = %q, want redis.db0", name, got)
		}
	}
}
