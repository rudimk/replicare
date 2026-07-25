// Package tracing is replicare's OpenTelemetry tracing surface (CLAUDE.md §10).
// It starts spans by their F2 contract name (internal/observability) and carries
// the F2 attribute keys, so the trace channel stays consistent with the metric
// and log channels — in particular the cross-channel target-unreachable signal
// (CLAUDE.md §3.4): a drain span against a downed target must carry error status
// plus backlog/age attributes.
//
// The default is a no-op provider (zero overhead when OTLP is not configured);
// the daemon installs an SDK provider with an OTLP exporter at startup (M7). The
// OTLP endpoint is already in the config (observability.otlp_endpoint).
package tracing

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/rudimk/replicare/internal/observability"
)

// scopeName is the instrumentation scope reported on every span.
const scopeName = "github.com/rudimk/replicare"

// Tracer starts replicare spans on an injected TracerProvider.
type Tracer struct{ tr trace.Tracer }

// New wraps a TracerProvider. A nil provider yields a no-op tracer.
func New(tp trace.TracerProvider) *Tracer {
	if tp == nil {
		tp = noop.NewTracerProvider()
	}
	return &Tracer{tr: tp.Tracer(scopeName)}
}

// Noop returns a tracer that discards spans — the default when OTLP is unset.
func Noop() *Tracer { return New(noop.NewTracerProvider()) }

// Start begins a span by its F2 contract name with the given attributes.
func (t *Tracer) Start(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return t.tr.Start(ctx, name, trace.WithAttributes(attrs...))
}

// NewProvider builds an always-sampling SDK TracerProvider feeding the given
// span processors. Tests pass a recorder; the daemon passes a batch processor
// over an OTLP exporter (wired at startup, M7).
func NewProvider(procs ...sdktrace.SpanProcessor) *sdktrace.TracerProvider {
	opts := []sdktrace.TracerProviderOption{sdktrace.WithSampler(sdktrace.AlwaysSample())}
	for _, p := range procs {
		opts = append(opts, sdktrace.WithSpanProcessor(p))
	}
	return sdktrace.NewTracerProvider(opts...)
}

// Fail marks a span as errored and records the error, so a failing span (e.g. a
// drain against an unreachable target) carries error status and the error event
// (CLAUDE.md §3.4).
func Fail(span trace.Span, err error) {
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}

// F2 attribute builders — map the shared attribute keys to typed OTel attributes
// so span attributes and structured-log fields use the same vocabulary.

func AttrSync(v string) attribute.KeyValue   { return attribute.String(observability.AttrSync, v) }
func AttrTarget(v string) attribute.KeyValue { return attribute.String(observability.AttrTarget, v) }
func AttrTable(v string) attribute.KeyValue  { return attribute.String(observability.AttrTable, v) }
func AttrPhase(v string) attribute.KeyValue  { return attribute.String(observability.AttrPhase, v) }

func AttrDeltaBacklog(rows int64) attribute.KeyValue {
	return attribute.Int64(observability.AttrDeltaBacklog, rows)
}

func AttrOldestAgeSeconds(sec float64) attribute.KeyValue {
	return attribute.Float64(observability.AttrOldestAgeSeconds, sec)
}

func AttrRetentionProximity(frac float64) attribute.KeyValue {
	return attribute.Float64(observability.AttrRetentionProx, frac)
}
