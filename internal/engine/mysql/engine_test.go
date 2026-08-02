package mysql

import (
	"testing"

	"github.com/rudimk/replicare/internal/engine"
)

// TestEngineRegistered confirms the MySQL engine registers itself (via init) and
// is retrievable through the neutral registry — the MM0 acceptance that
// Get("mysql") returns the engine and the factory builds a Source/Sink.
func TestEngineRegistered(t *testing.T) {
	eng, err := engine.Get(EngineName)
	if err != nil {
		t.Fatalf("Get(%q): %v", EngineName, err)
	}
	if eng.Name() != EngineName {
		t.Fatalf("Name() = %q, want %q", eng.Name(), EngineName)
	}
	if _, err := eng.NewSource(engine.ConnConfig{}); err != nil {
		t.Errorf("NewSource: %v", err)
	}
	if _, err := eng.NewSink(engine.ConnConfig{}); err != nil {
		t.Errorf("NewSink: %v", err)
	}
}

func TestServerVersionNum(t *testing.T) {
	cases := []struct {
		in   string
		want int
	}{
		{"5.6.51", 50651},
		{"5.7.44", 50744},
		{"8.0.36", 80036},
		{"8.4.0", 80400},
		{"8.0.36-0ubuntu0.22.04.1", 80036}, // distro build suffix stripped
		{"8.0", 80000},                     // missing patch -> 0
	}
	for _, c := range cases {
		got, err := serverVersionNum(c.in)
		if err != nil {
			t.Errorf("serverVersionNum(%q): %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("serverVersionNum(%q) = %d, want %d", c.in, got, c.want)
		}
	}
	if _, err := serverVersionNum("garbage"); err == nil {
		t.Error("serverVersionNum(garbage): expected error")
	}
}

func TestIsMariaDB(t *testing.T) {
	if !isMariaDB("10.11.6-MariaDB-1:10.11.6+maria~ubu2204") {
		t.Error("expected MariaDB detection on a MariaDB version string")
	}
	if isMariaDB("8.4.0") {
		t.Error("false positive: 8.4.0 is not MariaDB")
	}
}
