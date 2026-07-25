package tracing

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// NewOTLP builds an SDK TracerProvider that exports spans to an OTLP/gRPC
// endpoint (e.g. an OpenTelemetry Collector, or Honeycomb/Tempo), tagged with the
// service name and version. The exporter connects lazily, so this does not block
// on the collector being up. The connection is insecure (plaintext gRPC) — point
// it at a local collector / sidecar, or terminate TLS there. The caller owns
// Shutdown (to flush buffered spans on stop).
func NewOTLP(ctx context.Context, endpoint, version string) (*sdktrace.TracerProvider, error) {
	exp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("tracing: build OTLP exporter for %q: %w", endpoint, err)
	}
	res := resource.NewSchemaless(
		attribute.String("service.name", "replicare"),
		attribute.String("service.version", version),
	)
	return sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	), nil
}
