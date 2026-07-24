package telemetry

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/observability"
	"github.com/rudimk/replicare/internal/observability/prom"
	"github.com/rudimk/replicare/internal/observability/tracing"
	"github.com/rudimk/replicare/internal/state"
)

// captureSink records events in memory (stands in for the StateStore).
type captureSink struct{ events []state.Event }

func (c *captureSink) RecordEvent(_ context.Context, e state.Event) error {
	c.events = append(c.events, e)
	return nil
}

// gaugeValue finds a gauge sample by metric name and an exact label name->value
// match (order-independent: Prometheus sorts gathered labels by name).
func gaugeValue(t *testing.T, reg *prom.Registry, name string, want map[string]string) (float64, bool) {
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
			match := true
			for k, v := range want {
				if got[k] != v {
					match = false
					break
				}
			}
			if match {
				return m.GetGauge().GetValue(), true
			}
		}
	}
	return 0, false
}

// TestTargetUnreachableAllChannels is the M6 cross-channel acceptance at the unit
// level: one call fires metrics (reachability + backlog), a trace (error status +
// attributes), an escalated log, and a durable event — the §3.4 rule that a
// degrading target is never visible in only one channel.
func TestTargetUnreachableAllChannels(t *testing.T) {
	reg := prom.New()
	rec := tracetest.NewSpanRecorder()
	tracer := tracing.New(tracing.NewProvider(rec))
	var logBuf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	sink := &captureSink{}

	tm := New(reg, tracer, log, sink)

	ref := engine.TableRef{Schema: "public", Name: "orders"}
	bl := engine.DeltaBacklog{Rows: 128, Bytes: 4096, OldestAge: 2 * time.Hour, HasBacklog: true}

	ctx, span := tm.StartDrainSpan(context.Background(), "s1", "dst", ref, bl, 0.95)
	tm.TargetUnreachable(ctx, span, "s1", "dst", ref, bl, 0.95, errors.New("connection refused"))
	span.End()

	// Metrics: reachability down + backlog published.
	if v, ok := gaugeValue(t, reg, observability.MetricTargetUp,
		map[string]string{"sync": "s1", "target": "dst"}); !ok || v != 0 {
		t.Errorf("target_up = %v (present=%v), want 0", v, ok)
	}
	if v, ok := gaugeValue(t, reg, observability.MetricDeltaBacklog,
		map[string]string{"sync": "s1", "target": "dst", "table": "public.orders"}); !ok || v != 128 {
		t.Errorf("delta_backlog = %v (present=%v), want 128", v, ok)
	}

	// Trace: span ended with error status and backlog attribute.
	ended := rec.Ended()
	if len(ended) != 1 {
		t.Fatalf("recorded %d spans, want 1", len(ended))
	}
	if ended[0].Status().Code != codes.Error {
		t.Errorf("span status = %v, want Error", ended[0].Status().Code)
	}

	// Log: ERROR-level target.unreachable event.
	logs := logBuf.String()
	if !strings.Contains(logs, `"event":"`+observability.EventTargetUnreachable+`"`) {
		t.Errorf("log missing target.unreachable event: %s", logs)
	}
	if !strings.Contains(logs, `"level":"ERROR"`) {
		t.Errorf("log not at ERROR level: %s", logs)
	}

	// Durable event.
	if len(sink.events) != 1 || sink.events[0].Event != observability.EventTargetUnreachable {
		t.Errorf("durable events = %+v, want one target.unreachable", sink.events)
	}
}

// TestRetentionApproachingEscalates asserts the log level climbs with proximity
// and durable events fire only at WARN and above.
func TestRetentionApproachingEscalates(t *testing.T) {
	ref := engine.TableRef{Schema: "public", Name: "orders"}
	cases := []struct {
		prox      float64
		wantLevel string
		wantEvent bool
	}{
		{0.2, `"level":"INFO"`, false},
		{0.6, `"level":"WARN"`, true},
		{0.95, `"level":"ERROR"`, true},
	}
	for _, c := range cases {
		var buf bytes.Buffer
		log := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
		sink := &captureSink{}
		tm := New(prom.New(), nil, log, sink)
		bl := engine.DeltaBacklog{Rows: 10, OldestAge: time.Hour, HasBacklog: true}

		tm.RetentionApproaching(context.Background(), "s1", "dst", ref, bl, c.prox)

		if !strings.Contains(buf.String(), c.wantLevel) {
			t.Errorf("prox %.2f: log missing %s: %s", c.prox, c.wantLevel, buf.String())
		}
		if got := len(sink.events) > 0; got != c.wantEvent {
			t.Errorf("prox %.2f: durable event = %v, want %v", c.prox, got, c.wantEvent)
		}
	}
}

func TestRetentionProximity(t *testing.T) {
	bl := engine.DeltaBacklog{Bytes: 500, OldestAge: 90 * time.Minute}
	// age 5400s / 3600 cap = 1.5 -> clamped to 1; bytes 500/2000 = 0.25; max=1.
	if p := RetentionProximity(bl, engine.RetentionPolicy{MaxAgeSeconds: 3600, MaxBytes: 2000}); p != 1 {
		t.Errorf("proximity = %v, want 1 (age over cap, clamped)", p)
	}
	// bytes only: 500/2000 = 0.25.
	if p := RetentionProximity(bl, engine.RetentionPolicy{MaxBytes: 2000}); p != 0.25 {
		t.Errorf("proximity = %v, want 0.25", p)
	}
	// no caps -> 0.
	if p := RetentionProximity(bl, engine.RetentionPolicy{}); p != 0 {
		t.Errorf("proximity = %v, want 0", p)
	}
}
