package main

import (
	"bytes"
	"strings"
	"testing"
	"time"

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
	renderReports(&out, reports, false, time.Now())
	got := out.String()
	for _, want := range []string{
		"sync: s1", "public.orders", "streaming", "NEEDS-RESEED", "target.unreachable",
		"SEEN", "LAST_SYNC", "ROWS", // the new columns
		"last pass",           // headline liveness
		"no data applied yet", // headline: no LastAppliedAt set on this cursor
	} {
		if !strings.Contains(got, want) {
			t.Errorf("rendered output missing %q:\n%s", want, got)
		}
	}
	// The non-live render must NOT include the live-only columns or the raw delta seq.
	if strings.Contains(got, "SRC_ROWS") || strings.Contains(got, "BACKLOG") || strings.Contains(got, "LAST_DELTA") {
		t.Errorf("non-live render leaked live/internal columns:\n%s", got)
	}
}

func TestRenderReportsLive(t *testing.T) {
	srcRows := int64(1000)
	tgtRows := int64(950)
	now := time.Date(2026, 10, 7, 12, 0, 40, 0, time.UTC)
	applied := now.Add(-40 * time.Second) // last data-moving pass 40s before `now`
	reports := []status.Report{{
		Sync:      "s1",
		LiveError: "target \"slow\" unreachable: dial tcp: timeout",
		Tables: []status.TableStatus{{
			Table:      "public.orders",
			CopyDone:   true,
			SourceRows: &srcRows,
			Targets: []status.TargetStatus{{
				Target: "dst", Phase: "streaming", LastDelta: 42, CursorAgeSeconds: 5,
				LastAppliedAt: &applied, LastAppliedRows: 1240,
				TargetRows: &tgtRows,
				Backlog:    &status.Backlog{Rows: 50, Bytes: 8192, OldestAgeSeconds: 12},
			}},
		}},
	}}
	var out bytes.Buffer
	renderReports(&out, reports, true, now)
	got := out.String()
	for _, want := range []string{
		"SRC_ROWS", "TGT_ROWS", "BACKLOG", "1000", "950", "50 (12s)",
		"LAST_SYNC", "ROWS", "40s ago", "1240", // last-applied column + rows
		"last applied 40s ago (1240 rows total)", // headline data-movement
		"backlog 50 (oldest 12s)",                // headline backlog rollup
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
