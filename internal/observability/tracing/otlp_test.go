package tracing

import (
	"context"
	"testing"
	"time"
)

// TestNewOTLPBuilds confirms the OTLP provider constructs without a live
// collector (the gRPC exporter connects lazily), yields a working tracer, and
// shuts down cleanly. Actual export to a collector is out of unit-test scope.
func TestNewOTLPBuilds(t *testing.T) {
	tp, err := NewOTLP(context.Background(), "127.0.0.1:4317", "test")
	if err != nil {
		t.Fatalf("NewOTLP: %v", err)
	}
	if tr := New(tp); tr == nil {
		t.Fatal("expected a tracer from the OTLP provider")
	}
	// No spans are queued, so shutdown does not block on the (absent) collector.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := tp.Shutdown(ctx); err != nil {
		t.Errorf("Shutdown: %v", err)
	}
}
