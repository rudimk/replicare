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
	renderReports(&out, reports, false)
	got := out.String()
	for _, want := range []string{"sync: s1", "public.orders", "streaming", "NEEDS-RESEED", "42", "target.unreachable"} {
		if !strings.Contains(got, want) {
			t.Errorf("rendered output missing %q:\n%s", want, got)
		}
	}
	// The non-live render must NOT include the live-only columns.
	if strings.Contains(got, "SRC_ROWS") || strings.Contains(got, "BACKLOG") {
		t.Errorf("non-live render leaked live columns:\n%s", got)
	}
}

func TestRenderReportsLive(t *testing.T) {
	srcRows := int64(1000)
	tgtRows := int64(950)
	reports := []status.Report{{
		Sync:      "s1",
		LiveError: "target \"slow\" unreachable: dial tcp: timeout",
		Tables: []status.TableStatus{{
			Table:      "public.orders",
			CopyDone:   true,
			SourceRows: &srcRows,
			Targets: []status.TargetStatus{{
				Target: "dst", Phase: "streaming", LastDelta: 42, CursorAgeSeconds: 5,
				TargetRows: &tgtRows,
				Backlog:    &status.Backlog{Rows: 50, Bytes: 8192, OldestAgeSeconds: 12},
			}},
		}},
	}}
	var out bytes.Buffer
	renderReports(&out, reports, true)
	got := out.String()
	for _, want := range []string{
		"SRC_ROWS", "TGT_ROWS", "BACKLOG", "1000", "950", "50 (12s)",
		"live: partial", "unreachable",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("live render missing %q:\n%s", want, got)
		}
	}
}

func TestVerifyListedInUsage(t *testing.T) {
	var out, errb bytes.Buffer
	run([]string{"help"}, &out, &errb)
	if !strings.Contains(out.String(), "verify <config>") {
		t.Fatalf("help should list the verify command, got %q", out.String())
	}
}

func TestStatusWatchInvalidDuration(t *testing.T) {
	var out, errb bytes.Buffer
	if code := run([]string{"status", "cfg.yml", "--watch", "nope"}, &out, &errb); code != 2 {
		t.Fatalf("status --watch nope exit = %d, want 2", code)
	}
	if !strings.Contains(errb.String(), "invalid --watch duration") {
		t.Fatalf("expected --watch error, got %q", errb.String())
	}
}
