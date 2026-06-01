package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestRunVersion(t *testing.T) {
	var out, errb bytes.Buffer
	code := run([]string{"version"}, &out, &errb)
	if code != 0 {
		t.Fatalf("version exit code = %d, want 0", code)
	}
	if !strings.Contains(out.String(), "replicare ") {
		t.Fatalf("version output = %q, want it to contain 'replicare '", out.String())
	}
}

func TestRunNoArgs(t *testing.T) {
	var out, errb bytes.Buffer
	if code := run(nil, &out, &errb); code != 2 {
		t.Fatalf("no-args exit code = %d, want 2", code)
	}
	if !strings.Contains(errb.String(), "Usage:") {
		t.Fatalf("expected usage on stderr, got %q", errb.String())
	}
}

func TestRunUnknownCommand(t *testing.T) {
	var out, errb bytes.Buffer
	if code := run([]string{"frobnicate"}, &out, &errb); code != 2 {
		t.Fatalf("unknown-command exit code = %d, want 2", code)
	}
	if !strings.Contains(errb.String(), "unknown command") {
		t.Fatalf("expected unknown-command error, got %q", errb.String())
	}
}

func TestRunHelp(t *testing.T) {
	var out, errb bytes.Buffer
	if code := run([]string{"help"}, &out, &errb); code != 0 {
		t.Fatalf("help exit code = %d, want 0", code)
	}
	if !strings.Contains(out.String(), "Usage:") {
		t.Fatalf("expected usage on stdout, got %q", out.String())
	}
}
