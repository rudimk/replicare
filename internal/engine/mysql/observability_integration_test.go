package mysql

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/observability"
	"github.com/rudimk/replicare/internal/observability/prom"
	"github.com/rudimk/replicare/internal/observability/telemetry"
	"github.com/rudimk/replicare/internal/observability/tracing"
	"github.com/rudimk/replicare/internal/pipeline"
	"github.com/rudimk/replicare/internal/state"
)

// MM6 verifies the REUSED F2 observability contract for the MySQL engine: the
// neutral instrumented drain path (pipeline.DrainTable) drives the two
// engine-specific inputs — MySQL's DeltaBacklog and sink reachability — and must
// emit the same named metric series, span, log, and durable-event channels as the
// Postgres engine does. Everything below the engine boundary (telemetry/prom/
// tracing/status) is engine-neutral and already covered; these tests prove the
// MySQL engine wires into it correctly end to end. (Storeless: DrainTable needs no
// StateStore, so this runs against the MySQL-only harness.)

// evCaptureSink records durable events for assertions.
type evCaptureSink struct{ events []state.Event }

func (c *evCaptureSink) RecordEvent(_ context.Context, e state.Event) error {
	c.events = append(c.events, e)
	return nil
}

// gaugeVal returns the value of the gauge series `name` with exactly the label set
// `want`, or (0,false) if absent.
func gaugeVal(t *testing.T, reg *prom.Registry, name string, want map[string]string) (float64, bool) {
	t.Helper()
	fams, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range fams {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.Metric {
			got := map[string]string{}
			for _, l := range m.Label {
				got[l.GetName()] = l.GetValue()
			}
			if len(got) != len(want) {
				continue
			}
			ok := true
			for k, v := range want {
				if got[k] != v {
					ok = false
					break
				}
			}
			if ok {
				return m.GetGauge().GetValue(), true
			}
		}
	}
	return 0, false
}

// TestObservabilityHealthyTargetNamedSeries asserts that a successful instrumented
// drain of a live MySQL sync publishes the named series fed by the MySQL engine:
// the reachability gauge (up) and the delta-backlog trio (rows/bytes/oldest-age),
// each with the expected labels.
func TestObservabilityHealthyTargetNamedSeries(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	src := connectSource(t, ctx)
	sink := connectSink(t, ctx)
	t.Cleanup(func() { _, _ = src.db.Exec("DROP DATABASE IF EXISTS replicare") })

	ddl := "CREATE TABLE rc_it.orders (id INT PRIMARY KEY, note VARCHAR(50)) ENGINE=InnoDB"
	mustExec(t, ctx, src.db, ddl)
	mustExec(t, ctx, sink.db, "DROP DATABASE IF EXISTS rc_it", "CREATE DATABASE rc_it", ddl)
	t.Cleanup(func() { _, _ = sink.db.Exec("DROP DATABASE IF EXISTS rc_it") })

	ref := engine.TableRef{Schema: "rc_it", Name: "orders"}
	if err := src.InstallCapture(ctx, []engine.TableRef{ref}); err != nil {
		t.Fatalf("install capture: %v", err)
	}
	mustExec(t, ctx, src.db, "INSERT INTO rc_it.orders SELECT n, CONCAT('v', n) FROM "+
		"(SELECT 1 n UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7) s")
	time.Sleep(1100 * time.Millisecond) // give the oldest-age gauge a non-zero value

	reg := prom.New()
	tm := telemetry.New(reg, nil, nil, nil)

	n, err := pipeline.DrainTable(ctx, tm, src, sink, "s1", "dst", ref, 100, engine.RetentionPolicy{MaxAgeSeconds: 3600})
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if n != 7 {
		t.Fatalf("consumed = %d, want 7", n)
	}

	labels := map[string]string{"sync": "s1", "target": "dst", "table": "rc_it.orders"}
	if v, ok := gaugeVal(t, reg, observability.MetricTargetUp, map[string]string{"sync": "s1", "target": "dst"}); !ok || v != 1 {
		t.Errorf("target_up = %v (present=%v), want 1", v, ok)
	}
	if v, ok := gaugeVal(t, reg, observability.MetricDeltaBacklog, labels); !ok || v != 7 {
		t.Errorf("delta_backlog_rows = %v (present=%v), want 7 (pre-drain snapshot)", v, ok)
	}
	if _, ok := gaugeVal(t, reg, observability.MetricDeltaBacklogBytes, labels); !ok {
		t.Errorf("delta_backlog_bytes series missing for a live MySQL sync")
	}
	if v, ok := gaugeVal(t, reg, observability.MetricDeltaOldestAgeSeconds, labels); !ok || v <= 0 {
		t.Errorf("delta_oldest_unconsumed_age_seconds = %v (present=%v), want > 0", v, ok)
	}
}

// TestObservabilityDownedTargetTrifecta is the MM6 headline acceptance: a drain
// pass against a DOWNED MySQL target surfaces across all channels at once — an
// errored span with backlog blast-radius, the reachability gauge at 0 plus the
// delta-backlog series, an ERROR target.unreachable log, and a durable event.
func TestObservabilityDownedTargetTrifecta(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	src := connectSource(t, ctx)
	sink := connectSink(t, ctx)
	t.Cleanup(func() { _, _ = src.db.Exec("DROP DATABASE IF EXISTS replicare") })

	mustExec(t, ctx, src.db, "CREATE TABLE rc_it.orders (id INT PRIMARY KEY, note VARCHAR(50)) ENGINE=InnoDB")
	ref := engine.TableRef{Schema: "rc_it", Name: "orders"}
	if err := src.InstallCapture(ctx, []engine.TableRef{ref}); err != nil {
		t.Fatalf("install capture: %v", err)
	}
	// Queue an unconsumed backlog so the drain has work and the backlog is visible.
	mustExec(t, ctx, src.db, "INSERT INTO rc_it.orders SELECT n, CONCAT('v', n) FROM "+
		"(SELECT 1 n UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7) s")

	// Close the sink to simulate the target going down: the drain's apply and the
	// reachability probe both fail.
	if err := sink.Close(ctx); err != nil {
		t.Fatalf("close sink (simulate down): %v", err)
	}

	reg := prom.New()
	rec := tracetest.NewSpanRecorder()
	tracer := tracing.New(tracing.NewProvider(rec))
	var logBuf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	evSink := &evCaptureSink{}
	tm := telemetry.New(reg, tracer, log, evSink)

	n, err := pipeline.DrainTable(ctx, tm, src, sink, "s1", "dst", ref, 100, engine.RetentionPolicy{MaxAgeSeconds: 3600})
	if err == nil {
		t.Fatalf("expected the drain to fail against a downed target")
	}
	if n != 0 {
		t.Errorf("consumed = %d, want 0 on failure", n)
	}

	// Trace channel: exactly one errored drain span.
	ended := rec.Ended()
	if len(ended) != 1 || ended[0].Status().Code != codes.Error {
		t.Fatalf("expected one errored drain span, got %+v", ended)
	}

	// Metrics channel: reachability down + backlog published.
	if v, ok := gaugeVal(t, reg, observability.MetricTargetUp,
		map[string]string{"sync": "s1", "target": "dst"}); !ok || v != 0 {
		t.Errorf("target_up = %v (present=%v), want 0", v, ok)
	}
	if v, ok := gaugeVal(t, reg, observability.MetricDeltaBacklog,
		map[string]string{"sync": "s1", "target": "dst", "table": "rc_it.orders"}); !ok || v != 7 {
		t.Errorf("delta_backlog_rows = %v (present=%v), want 7", v, ok)
	}

	// Log channel: ERROR target.unreachable.
	logs := logBuf.String()
	if !strings.Contains(logs, `"event":"`+observability.EventTargetUnreachable+`"`) || !strings.Contains(logs, `"level":"ERROR"`) {
		t.Errorf("expected ERROR target.unreachable log, got %s", logs)
	}

	// Durable channel: the event was recorded.
	if len(evSink.events) != 1 || evSink.events[0].Event != observability.EventTargetUnreachable {
		t.Errorf("durable events = %+v", evSink.events)
	}
}
