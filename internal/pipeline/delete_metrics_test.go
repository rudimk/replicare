package pipeline

import (
	"context"
	"testing"

	dto "github.com/prometheus/client_model/go"

	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/observability"
	"github.com/rudimk/replicare/internal/observability/prom"
	"github.com/rudimk/replicare/internal/observability/telemetry"
)

// --- fakes: a capture-less engine (implements KeyLister + KeyExister) ---

type fakeDelSink struct {
	engine.Sink
	batches [][]engine.KeyValues // one ScanTargetKeys batch per call, then empty
	call    int
}

func (f *fakeDelSink) ScanTargetKeys(_ context.Context, _ engine.TableRef, _ uint64, _ int) ([]engine.KeyValues, uint64, error) {
	if f.call < len(f.batches) {
		b := f.batches[f.call]
		f.call++
		return b, 1, nil // more to come
	}
	return nil, 0, nil // pass complete
}

func (f *fakeDelSink) BeginApply(context.Context, bool, []engine.TableRef) (engine.ApplyTx, error) {
	return &fakeDelTx{}, nil
}

type fakeDelTx struct{ engine.ApplyTx }

func (tx *fakeDelTx) DeleteAbsent(context.Context, engine.TableRef, []engine.KeyValues) error {
	return nil
}
func (tx *fakeDelTx) Commit(context.Context) error   { return nil }
func (tx *fakeDelTx) Rollback(context.Context) error { return nil }

type fakeDelSource struct{ engine.Source }

// MissingAtSource reports every passed key as missing (all get deleted).
func (fakeDelSource) MissingAtSource(_ context.Context, _ engine.TableRef, keys []engine.KeyValues) ([]engine.KeyValues, error) {
	return keys, nil
}

// labelValue returns the value of label on the first sample of metric `name`.
func labelValue(t *testing.T, reg *prom.Registry, name, label string) (string, bool) {
	t.Helper()
	fams, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
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

func metricValue(t *testing.T, reg *prom.Registry, name string) (float64, bool) {
	t.Helper()
	fams, _ := reg.Gather()
	for _, f := range fams {
		if f.GetName() != name || len(f.Metric) == 0 {
			continue
		}
		m := f.Metric[0]
		switch f.GetType() {
		case dto.MetricType_COUNTER:
			return m.Counter.GetValue(), true
		case dto.MetricType_GAUGE:
			return m.Gauge.GetValue(), true
		}
	}
	return 0, false
}

// TestDeleteReconcileMetrics is the RM8 verification for the capture-less delete
// signals: driving the delete-reconciliation sweep to completion publishes the
// deletes-reconciled counter AND the delete-reconciliation-lag gauge, both labelled
// with the Redis unit ref (table -> "redis.db0"). It uses fakes, so it verifies the
// pipeline wiring without a live engine.
func TestDeleteReconcileMetrics(t *testing.T) {
	ctx := context.Background()
	reg := prom.New()
	ref := engine.TableRef{Schema: "redis", Name: "db0"}
	sink := &fakeDelSink{batches: [][]engine.KeyValues{{{"a"}, {"b"}, {"c"}}}}
	s := &Syncer{
		Name:       "redis-sync",
		Source:     fakeDelSource{},
		Sink:       sink,
		Target:     "dst",
		Replicable: []engine.TableRef{ref},
		DrainBatch: 64,
		Tel:        telemetry.New(reg, nil, nil, nil),
	}

	// Step 1: ScanTargetKeys returns 3 keys (all missing) → 3 deletes, next=1.
	// Step 2: ScanTargetKeys returns empty → next=0 → pass complete → lag published.
	if err := s.deleteReconcile(ctx); err != nil {
		t.Fatalf("deleteReconcile step 1: %v", err)
	}
	if err := s.deleteReconcile(ctx); err != nil {
		t.Fatalf("deleteReconcile step 2: %v", err)
	}

	if v, ok := metricValue(t, reg, observability.MetricDeletesReconciled); !ok || v != 3 {
		t.Errorf("deletes_reconciled = %v (present=%v), want 3", v, ok)
	}
	if _, ok := metricValue(t, reg, observability.MetricDeleteReconcileLag); !ok {
		t.Errorf("delete_reconciliation_lag gauge not published after a completed sweep pass")
	}
	// Labels carry the Redis unit ref and the target.
	if tbl, _ := labelValue(t, reg, observability.MetricDeletesReconciled, "table"); tbl != "redis.db0" {
		t.Errorf("deletes_reconciled table label = %q, want redis.db0", tbl)
	}
	if tgt, _ := labelValue(t, reg, observability.MetricDeleteReconcileLag, "target"); tgt != "dst" {
		t.Errorf("delete_reconciliation_lag target label = %q, want dst", tgt)
	}
}

// TestDeleteReconcileSkippedForCaptureEngines: a Sink/Source that do NOT implement
// KeyLister/KeyExister (Postgres/MySQL) make the delete-reconciliation step a
// complete no-op — no sweep, no metrics — so capture-driven engines are unaffected.
func TestDeleteReconcileSkippedForCaptureEngines(t *testing.T) {
	ctx := context.Background()
	reg := prom.New()
	s := &Syncer{
		Name:       "pg-sync",
		Source:     plainSource{},
		Sink:       plainSink{},
		Target:     "dst",
		Replicable: []engine.TableRef{{Schema: "public", Name: "orders"}},
		DrainBatch: 64,
		Tel:        telemetry.New(reg, nil, nil, nil),
	}
	if err := s.deleteReconcile(ctx); err != nil {
		t.Fatalf("deleteReconcile: %v", err)
	}
	if _, ok := metricValue(t, reg, observability.MetricDeletesReconciled); ok {
		t.Error("delete metrics emitted for a capture-driven engine — the step must be a no-op")
	}
}

// plainSource/plainSink implement only the base interfaces (no KeyLister/KeyExister).
type plainSource struct{ engine.Source }
type plainSink struct{ engine.Sink }
