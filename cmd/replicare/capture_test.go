package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestCaptureUsage(t *testing.T) {
	var out, errb bytes.Buffer
	if code := run([]string{"capture"}, &out, &errb); code != 2 {
		t.Fatalf("capture no-action exit = %d, want 2", code)
	}
	if !strings.Contains(errb.String(), "usage: replicare capture") {
		t.Fatalf("expected usage, got %q", errb.String())
	}
}

func TestCaptureUnknownAction(t *testing.T) {
	var out, errb bytes.Buffer
	if code := run([]string{"capture", "frobnicate", "cfg.yml"}, &out, &errb); code != 2 {
		t.Fatalf("capture bad-action exit = %d, want 2", code)
	}
	if !strings.Contains(errb.String(), "unknown action") {
		t.Fatalf("expected unknown-action error, got %q", errb.String())
	}
}

func TestCaptureListedInUsage(t *testing.T) {
	var out, errb bytes.Buffer
	run([]string{"help"}, &out, &errb)
	if !strings.Contains(out.String(), "capture ") {
		t.Fatalf("help should list the capture command, got %q", out.String())
	}
}
