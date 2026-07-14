package postgres

import (
	"context"
	"testing"

	"github.com/rudimk/replicare/internal/engine"
)

func TestEngineRegistered(t *testing.T) {
	e, err := engine.Get(EngineName)
	if err != nil {
		t.Fatalf("engine %q not registered: %v", EngineName, err)
	}
	if e.Name() != EngineName {
		t.Errorf("Name() = %q, want %q", e.Name(), EngineName)
	}
}

func TestNewSourceAndSink(t *testing.T) {
	e, err := engine.Get(EngineName)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	cc := engine.ConnConfig{Host: "h", Port: 5432, Database: "d", User: "u"}

	src, err := e.NewSource(cc)
	if err != nil {
		t.Fatalf("NewSource: %v", err)
	}
	if src == nil {
		t.Fatal("NewSource returned nil source")
	}
	sink, err := e.NewSink(cc)
	if err != nil {
		t.Fatalf("NewSink: %v", err)
	}
	if sink == nil {
		t.Fatal("NewSink returned nil sink")
	}
}

func TestSourceOpsRequireConnect(t *testing.T) {
	src := &Source{cfg: engine.ConnConfig{Host: "h", Port: 5432, Database: "d", User: "u"}}
	ctx := context.Background()

	if _, err := src.ServerVersion(ctx); err == nil {
		t.Error("ServerVersion before Connect should error")
	}
	// Close on an unconnected source is a safe no-op.
	if err := src.Close(ctx); err != nil {
		t.Errorf("Close on unconnected source: %v", err)
	}
}

func TestSinkOpsRequireConnect(t *testing.T) {
	sink := &Sink{cfg: engine.ConnConfig{Host: "h", Port: 5432, Database: "d", User: "u"}}
	ctx := context.Background()

	if _, err := sink.ServerVersion(ctx); err == nil {
		t.Error("ServerVersion before Connect should error")
	}
	if err := sink.Close(ctx); err != nil {
		t.Errorf("Close on unconnected sink: %v", err)
	}
}
