package engine

import (
	"context"
	"io"
	"testing"
)

// fakeEngine is a minimal Engine used to exercise the registry.
type fakeEngine struct{ name string }

func (f fakeEngine) Name() string                         { return f.name }
func (f fakeEngine) NewSource(ConnConfig) (Source, error) { return nil, nil }
func (f fakeEngine) NewSink(ConnConfig) (Sink, error)     { return nil, nil }

func TestRegisterAndGet(t *testing.T) {
	// Use a fresh registry to avoid cross-test contamination.
	registryMu.Lock()
	registry = map[string]Engine{}
	registryMu.Unlock()

	Register(fakeEngine{name: "fake"})

	got, err := Get("fake")
	if err != nil {
		t.Fatalf("Get(fake): unexpected error: %v", err)
	}
	if got.Name() != "fake" {
		t.Fatalf("Get(fake).Name() = %q, want fake", got.Name())
	}

	if _, err := Get("missing"); err == nil {
		t.Fatal("Get(missing): expected error, got nil")
	}

	if names := List(); len(names) != 1 || names[0] != "fake" {
		t.Fatalf("List() = %v, want [fake]", names)
	}
}

func TestRegisterDuplicatePanics(t *testing.T) {
	registryMu.Lock()
	registry = map[string]Engine{}
	registryMu.Unlock()

	Register(fakeEngine{name: "dup"})
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on duplicate registration")
		}
	}()
	Register(fakeEngine{name: "dup"})
}

// Ensure the supporting types satisfy their interfaces at compile time via a
// no-op reference (keeps the io import meaningful for future stub impls).
var _ = io.Discard
var _ = context.Background
