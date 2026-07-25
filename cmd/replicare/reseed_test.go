package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestReseedUsageMissingFlags(t *testing.T) {
	var out, errb bytes.Buffer
	// Missing --sync/--target.
	if code := run([]string{"reseed", "cfg.yml"}, &out, &errb); code != 2 {
		t.Fatalf("reseed missing-flags exit = %d, want 2", code)
	}
	if !strings.Contains(errb.String(), "usage: replicare reseed") {
		t.Fatalf("expected usage, got %q", errb.String())
	}
}

func TestReseedTargetFlagNeedsValue(t *testing.T) {
	var out, errb bytes.Buffer
	if code := run([]string{"reseed", "cfg.yml", "--sync", "s1", "--target"}, &out, &errb); code != 2 {
		t.Fatalf("reseed --target no-value exit = %d, want 2", code)
	}
	if !strings.Contains(errb.String(), "--target needs a value") {
		t.Fatalf("expected --target error, got %q", errb.String())
	}
}

func TestReseedListedInUsage(t *testing.T) {
	var out, errb bytes.Buffer
	run([]string{"help"}, &out, &errb)
	if !strings.Contains(out.String(), "reseed <config>") {
		t.Fatalf("help should list the reseed command, got %q", out.String())
	}
}
