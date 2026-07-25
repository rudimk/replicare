package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/rudimk/replicare/internal/observability/status"
)

func TestStatusUsageNoConfig(t *testing.T) {
	var out, errb bytes.Buffer
	if code := run([]string{"status"}, &out, &errb); code != 2 {
		t.Fatalf("status no-config exit = %d, want 2", code)
	}
	if !strings.Contains(errb.String(), "usage: replicare status") {
		t.Fatalf("expected usage, got %q", errb.String())
	}
}

func TestStatusSyncFlagNeedsValue(t *testing.T) {
	var out, errb bytes.Buffer
	if code := run([]string{"status", "--sync"}, &out, &errb); code != 2 {
		t.Fatalf("status --sync (no value) exit = %d, want 2", code)
	}
	if !strings.Contains(errb.String(), "--sync needs a value") {
		t.Fatalf("expected --sync error, got %q", errb.String())
	}
}

func TestStatusListedInUsage(t *testing.T) {
	var out, errb bytes.Buffer
	run([]string{"help"}, &out, &errb)
	if !strings.Contains(out.String(), "status <config>") {
		t.Fatalf("help should list the status command, got %q", out.String())
	}
}

func TestRenderReports(t *testing.T) {
	reports := []status.Report{{
		Sync: "s1",
		Tables: []status.TableStatus{{
			Table:    "public.orders",
			CopyDone: true,
			Targets: []status.TargetStatus{{
				Target: "dst", Phase: "streaming", LastDelta: 42,
				NeedsReseed: true, CursorAgeSeconds: 30,
			}},
		}},
		Events: []status.EventView{
			{Level: "ERROR", Event: "target.unreachable", Target: "dst", Table: "public.orders"},
		},
	}}
	var out bytes.Buffer
	renderReports(&out, reports)
	got := out.String()
	for _, want := range []string{"sync: s1", "public.orders", "streaming", "NEEDS-RESEED", "42", "target.unreachable"} {
		if !strings.Contains(got, want) {
			t.Errorf("rendered output missing %q:\n%s", want, got)
		}
	}
}
