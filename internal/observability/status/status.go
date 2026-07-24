// Package status is the M6 operator surface: a health/status HTTP API plus the
// Reporter that assembles a sync's live status (phase, lag, initial-copy
// progress, recent events) from the StateStore, and a Server that serves it
// alongside the Prometheus /metrics endpoint (CLAUDE.md §10). The CLI `status`
// command renders the same Report.
package status

import (
	"context"
	"time"

	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/state"
)

// Reader is the slice of the StateStore the status surface needs (the aggregate
// reads from M6). state.StateStore satisfies it.
type Reader interface {
	ListCopyProgress(ctx context.Context, sync string) ([]state.CopyProgress, error)
	ListCursors(ctx context.Context, sync string) ([]state.Cursor, error)
	RecentEvents(ctx context.Context, sync string, limit int) ([]state.Event, error)
}

// Report is a sync's operator-facing status snapshot.
type Report struct {
	Sync        string        `json:"sync"`
	Tables      []TableStatus `json:"tables"`
	Events      []EventView   `json:"recent_events"`
	GeneratedAt time.Time     `json:"generated_at"`
}

// TableStatus is one table's initial-copy state plus its per-target streaming
// state within the sync.
type TableStatus struct {
	Table         string         `json:"table"`
	CopyDone      bool           `json:"copy_done"`
	CopyUpdatedAt *time.Time     `json:"copy_updated_at,omitempty"`
	Targets       []TargetStatus `json:"targets"`
}

// TargetStatus is one (target, table) streaming cursor's status. CursorAgeSeconds
// (now - last cursor write) is the lag proxy surfaced to operators.
type TargetStatus struct {
	Target           string  `json:"target"`
	Phase            string  `json:"phase"`
	LastDelta        int64   `json:"last_delta"`
	NeedsReseed      bool    `json:"needs_reseed"`
	CursorAgeSeconds float64 `json:"cursor_age_seconds"`
}

// EventView is a recent operational event for the "last error" / audit view.
type EventView struct {
	Level     string    `json:"level"`
	Event     string    `json:"event"`
	Target    string    `json:"target,omitempty"`
	Table     string    `json:"table,omitempty"`
	Message   string    `json:"message,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

// Reporter builds Reports from a Reader. now is injectable for deterministic
// tests; it defaults to time.Now.
type Reporter struct {
	reader     Reader
	now        func() time.Time
	eventLimit int
}

// NewReporter constructs a Reporter over the given Reader.
func NewReporter(reader Reader) *Reporter {
	return &Reporter{reader: reader, now: time.Now, eventLimit: 20}
}

// Report assembles the status for one sync.
func (r *Reporter) Report(ctx context.Context, sync string) (Report, error) {
	progs, err := r.reader.ListCopyProgress(ctx, sync)
	if err != nil {
		return Report{}, err
	}
	cursors, err := r.reader.ListCursors(ctx, sync)
	if err != nil {
		return Report{}, err
	}
	events, err := r.reader.RecentEvents(ctx, sync, r.eventLimit)
	if err != nil {
		return Report{}, err
	}

	now := r.now()

	// Index tables by name, seeding from copy progress then folding in cursors so
	// a table with a cursor but no progress row (or vice versa) still appears.
	type entry struct {
		st  TableStatus
		idx int
	}
	order := []string{}
	byTable := map[string]*entry{}
	ensure := func(name string) *entry {
		if e, ok := byTable[name]; ok {
			return e
		}
		e := &entry{st: TableStatus{Table: name}, idx: len(order)}
		byTable[name] = e
		order = append(order, name)
		return e
	}

	for _, p := range progs {
		e := ensure(p.Table.String())
		e.st.CopyDone = p.Done
		if !p.UpdatedAt.IsZero() {
			ts := p.UpdatedAt
			e.st.CopyUpdatedAt = &ts
		}
	}
	for _, c := range cursors {
		e := ensure(c.Table.String())
		age := 0.0
		if !c.UpdatedAt.IsZero() {
			age = now.Sub(c.UpdatedAt).Seconds()
		}
		e.st.Targets = append(e.st.Targets, TargetStatus{
			Target:           string(c.Target),
			Phase:            string(c.Phase),
			LastDelta:        int64(c.LastDelta),
			NeedsReseed:      c.NeedsReseed,
			CursorAgeSeconds: age,
		})
	}

	tables := make([]TableStatus, 0, len(order))
	for _, name := range order {
		tables = append(tables, byTable[name].st)
	}

	views := make([]EventView, 0, len(events))
	for _, ev := range events {
		views = append(views, EventView{
			Level: ev.Level, Event: ev.Event, Target: ev.Target,
			Table: tableName(ev.Table), Message: ev.Message, CreatedAt: ev.CreatedAt,
		})
	}

	return Report{Sync: sync, Tables: tables, Events: views, GeneratedAt: now}, nil
}

// tableName renders a TableRef for display, empty when unset (sync-level events).
func tableName(t engine.TableRef) string {
	if t.Schema == "" && t.Name == "" {
		return ""
	}
	return t.String()
}
