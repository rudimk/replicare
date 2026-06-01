package buildinfo

import (
	"strings"
	"testing"
)

func TestStringContainsVersion(t *testing.T) {
	s := String()
	if !strings.HasPrefix(s, "replicare ") {
		t.Fatalf("String() = %q, want it to start with 'replicare '", s)
	}
	if !strings.Contains(s, Version) {
		t.Fatalf("String() = %q, want it to contain version %q", s, Version)
	}
}
