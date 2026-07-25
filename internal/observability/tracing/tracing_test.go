package tracing

import (
	"context"
	"errors"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/rudimk/replicare/internal/observability"
)

// TestDrainSpanErrorAndBacklogAttrs is the trace half of the §3.4 cross-channel
// acceptance: a drain span against a downed target carries error status and the
// backlog/age attributes.
func TestDrainSpanErrorAndBacklogAttrs(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tr := New(NewProvider(rec))

	_, span := tr.Start(context.Background(), observability.SpanDeltaDrain,
		AttrSync("s1"), AttrTarget("dst"), AttrTable("public.orders"),
		AttrDeltaBacklog(42), AttrOldestAgeSeconds(3600), AttrRetentionProximity(0.9))
	Fail(span, errors.New("dial tcp 10.0.0.5:5432: connection refused"))
	span.End()

	ended := rec.Ended()
	if len(ended) != 1 {
		t.Fatalf("recorded %d spans, want 1", len(ended))
	}
	s := ended[0]
	if s.Name() != observability.SpanDeltaDrain {
		t.Errorf("span name = %q, want %q", s.Name(), observability.SpanDeltaDrain)
	}
	if s.Status().Code != codes.Error {
		t.Errorf("span status = %v, want Error", s.Status().Code)
	}

	attrs := attrMap(s.Attributes())
	if v, ok := attrs[observability.AttrDeltaBacklog]; !ok || v.AsInt64() != 42 {
		t.Errorf("delta_backlog attr = %v (present=%v), want 42", v.AsInt64(), ok)
	}
	if v, ok := attrs[observability.AttrTarget]; !ok || v.AsString() != "dst" {
		t.Errorf("target attr = %q (present=%v), want dst", v.AsString(), ok)
	}
	if v, ok := attrs[observability.AttrOldestAgeSeconds]; !ok || v.AsFloat64() != 3600 {
		t.Errorf("oldest_age attr = %v (present=%v), want 3600", v.AsFloat64(), ok)
	}
	// The error was recorded as a span event.
	if n := len(s.Events()); n == 0 {
		t.Errorf("expected a recorded error event on the span")
	}
}

// TestNoopDoesNotRecord confirms the default tracer discards spans (no exporter,
// no overhead when OTLP is unconfigured).
func TestNoopDoesNotRecord(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	_ = rec
	tr := Noop()
	_, span := tr.Start(context.Background(), observability.SpanDeltaDrain)
	span.End()
	// A no-op span is never recording.
	if span.IsRecording() {
		t.Errorf("noop span should not be recording")
	}
}

func attrMap(kvs []attribute.KeyValue) map[string]attribute.Value {
	out := make(map[string]attribute.Value, len(kvs))
	for _, kv := range kvs {
		out[string(kv.Key)] = kv.Value
	}
	return out
}
