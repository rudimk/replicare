package status

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/state"
)

// fakeReader serves canned state for the Reporter without a database.
type fakeReader struct {
	progs   []state.CopyProgress
	cursors []state.Cursor
	events  []state.Event
}

func (f *fakeReader) ListCopyProgress(_ context.Context, _ string) ([]state.CopyProgress, error) {
	return f.progs, nil
}
func (f *fakeReader) ListCursors(_ context.Context, _ string) ([]state.Cursor, error) {
	return f.cursors, nil
}
func (f *fakeReader) RecentEvents(_ context.Context, _ string, _ int) ([]state.Event, error) {
	return f.events, nil
}

func orders() engine.TableRef { return engine.TableRef{Schema: "public", Name: "orders"} }

func TestReporterAssemblesStatus(t *testing.T) {
	now := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	reader := &fakeReader{
		progs: []state.CopyProgress{{Table: orders(), Done: true, UpdatedAt: now.Add(-time.Hour)}},
		cursors: []state.Cursor{{
			Target: "dst", Table: orders(), Phase: state.PhaseStreaming,
			LastDelta: 42, NeedsReseed: false, UpdatedAt: now.Add(-30 * time.Second),
		}},
		events: []state.Event{
			{Level: "ERROR", Event: "target.unreachable", Target: "dst", Table: orders(), CreatedAt: now.Add(-10 * time.Second)},
		},
	}
	r := NewReporter(reader)
	r.now = func() time.Time { return now }

	rep, err := r.Report(context.Background(), "s1")
	if err != nil {
		t.Fatalf("Report: %v", err)
	}
	if rep.Sync != "s1" || len(rep.Tables) != 1 {
		t.Fatalf("report = %+v", rep)
	}
	tbl := rep.Tables[0]
	if tbl.Table != "public.orders" || !tbl.CopyDone {
		t.Errorf("table status = %+v", tbl)
	}
	if len(tbl.Targets) != 1 {
		t.Fatalf("targets = %d, want 1", len(tbl.Targets))
	}
	tg := tbl.Targets[0]
	if tg.Target != "dst" || tg.Phase != "streaming" || tg.LastDelta != 42 {
		t.Errorf("target status = %+v", tg)
	}
	if tg.CursorAgeSeconds != 30 {
		t.Errorf("cursor age = %v, want 30 (lag proxy)", tg.CursorAgeSeconds)
	}
	if len(rep.Events) != 1 || rep.Events[0].Event != "target.unreachable" {
		t.Errorf("events = %+v", rep.Events)
	}
}

// TestReporterFoldsCursorOnlyTable: a table with a cursor but no copy-progress
// row still appears (and vice versa).
func TestReporterFoldsCursorOnlyTable(t *testing.T) {
	reader := &fakeReader{
		cursors: []state.Cursor{{Target: "dst", Table: orders(), Phase: state.PhaseInitialCopy, NeedsReseed: true}},
	}
	rep, err := NewReporter(reader).Report(context.Background(), "s1")
	if err != nil {
		t.Fatalf("Report: %v", err)
	}
	if len(rep.Tables) != 1 || !rep.Tables[0].Targets[0].NeedsReseed {
		t.Errorf("expected the cursor-only table with needs_reseed, got %+v", rep.Tables)
	}
}

func TestServerRoutes(t *testing.T) {
	reader := &fakeReader{
		cursors: []state.Cursor{{Target: "dst", Table: orders(), Phase: state.PhaseStreaming}},
	}
	metrics := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("# HELP replicare_up test\n"))
	})
	srv := NewServer(NewReporter(reader), metrics,
		func(_ context.Context) ([]string, error) { return []string{"s1"}, nil })
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// /healthz
	resp, err := http.Get(ts.URL + "/healthz")
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("healthz: %v status=%v", err, resp.StatusCode)
	}
	resp.Body.Close()

	// /metrics is delegated to the injected handler.
	resp, _ = http.Get(ts.URL + "/metrics")
	body := readAll(t, resp)
	if resp.StatusCode != 200 || !contains(body, "replicare_up") {
		t.Errorf("metrics route = %d %q", resp.StatusCode, body)
	}

	// /status returns a JSON array of reports.
	resp, _ = http.Get(ts.URL + "/status")
	var reports []Report
	if err := json.NewDecoder(resp.Body).Decode(&reports); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	resp.Body.Close()
	if len(reports) != 1 || reports[0].Sync != "s1" {
		t.Errorf("status reports = %+v", reports)
	}

	// /status?sync=name scopes to one sync.
	resp, _ = http.Get(ts.URL + "/status?sync=only")
	_ = json.NewDecoder(resp.Body).Decode(&reports)
	resp.Body.Close()
	if len(reports) != 1 || reports[0].Sync != "only" {
		t.Errorf("scoped status = %+v", reports)
	}
}

func readAll(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()
	buf := make([]byte, 4096)
	n, _ := resp.Body.Read(buf)
	return string(buf[:n])
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
