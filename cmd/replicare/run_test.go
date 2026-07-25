package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestRunUsageNoConfig(t *testing.T) {
	var out, errb bytes.Buffer
	if code := run([]string{"run"}, &out, &errb); code != 2 {
		t.Fatalf("run no-config exit = %d, want 2", code)
	}
	if !strings.Contains(errb.String(), "usage: replicare run") {
		t.Fatalf("expected usage, got %q", errb.String())
	}
}

func TestRunListedInUsage(t *testing.T) {
	var out, errb bytes.Buffer
	run([]string{"help"}, &out, &errb)
	if !strings.Contains(out.String(), "run <config>") {
		t.Fatalf("help should list the run command, got %q", out.String())
	}
}
