package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateNoArgs(t *testing.T) {
	var out, errb bytes.Buffer
	if code := run([]string{"validate"}, &out, &errb); code != 2 {
		t.Fatalf("validate without a path: exit = %d, want 2", code)
	}
	if !strings.Contains(errb.String(), "usage:") {
		t.Errorf("expected usage on stderr, got %q", errb.String())
	}
}

func TestValidateMissingConfig(t *testing.T) {
	var out, errb bytes.Buffer
	if code := run([]string{"validate", "/no/such/config.yml"}, &out, &errb); code != 2 {
		t.Fatalf("validate with a missing config: exit = %d, want 2", code)
	}
}

func TestValidateInvalidConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.yml")
	// Structurally invalid: a sync referencing an undefined source.
	if err := os.WriteFile(path, []byte(`
sources: {}
targets: {}
syncs:
  - name: s1
    source: nope
    targets: [nope]
`), 0o600); err != nil {
		t.Fatal(err)
	}
	var out, errb bytes.Buffer
	if code := run([]string{"validate", path}, &out, &errb); code != 2 {
		t.Fatalf("validate with an invalid config: exit = %d, want 2", code)
	}
}
