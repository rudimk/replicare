// Package state defines the StateStore contract: the daemon's own operational
// state (sync definitions, initial-copy progress, per-target cursors, ownership)
// plus a migration runner. The v1 backend is Postgres (CLAUDE.md §9); the
// interface is pluggable so other backends can be added later.
//
// This is distinct from the trigger-CDC delta/track tables, which live on the
// SOURCE database (CLAUDE.md §9 "do not conflate"). The StateStore holds only
// the daemon's progress/ownership, never per-row change state.
package state

import (
	"context"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

// CopyProgress is a table's initial-copy progress: a completed-range high-water
// mark plus a sparse set of out-of-order completed ranges, so a restart skips
// finished chunks (CLAUDE.md §4.1).
type CopyProgress struct {
	Table     engine.TableRef
	Done      bool
	Watermark engine.KeyValues // everything strictly below this key is copied
	Completed []CompletedRange // out-of-order completed ranges above the watermark
	UpdatedAt time.Time        // last checkpoint time (read-only; ignored on write)
}

// CompletedRange is one finished chunk's half-open key range.
type CompletedRange struct {
	Lo engine.KeyValues
	Hi engine.KeyValues
}

// Cursor is the per-(target, table) streaming position used for lag/observability
// (it is NOT the correctness mechanism — that is the source-side track table,
// CLAUDE.md §3.3). Phase tracks copy vs streaming for cutover.
type Cursor struct {
	Target      engine.TargetID
	Table       engine.TableRef
	Phase       Phase
	LastDelta   engine.DeltaID
	NeedsReseed bool      // set when retention cap forces a reseed (CLAUDE.md §3.4)
	UpdatedAt   time.Time // last cursor write (read-only; the lag/age signal)
}

// Phase is a table's lifecycle phase within a sync.
type Phase string

const (
	PhaseInitialCopy Phase = "initial_copy"
	PhaseStreaming   Phase = "streaming"
)

// SyncDef is a stored sync definition (source, targets, selection, tuning). The
// concrete shape is finalized with the config schema (M0.5); this is the stored
// projection the daemon resumes from.
type SyncDef struct {
	Name      string
	Source    string // engine name + endpoint reference (resolved at runtime)
	Targets   []engine.TargetID
	Selection engine.Selection
}

// Event is an operational event persisted for the status API / audit trail. It
// mirrors the F2 observability vocabulary (Level is a slog level, Event is an
// F2 event name) so the durable record and the live logs/metrics agree
// (CLAUDE.md §10).
type Event struct {
	Sync      string
	Target    string
	Table     engine.TableRef
	Level     string // INFO | WARN | ERROR
	Event     string // F2 event name, e.g. "target.needs_reseed"
	Message   string
	Attrs     map[string]any
	CreatedAt time.Time // set on read (RecentEvents); ignored on write
}

// StateStore persists daemon state and provides single-active ownership. The
// Postgres implementation lands in M2; this is the contract.
type StateStore interface {
	// Open connects and runs migrations to the current schema version (F4).
	Open(ctx context.Context) error
	Close(ctx context.Context) error

	// Acquire takes the single-active ownership lock for a sync (pg_advisory_lock
	// in the Postgres backend). Returns held=false if another process owns it.
	// The interface is shaped so leader election can be added later (CLAUDE.md §9).
	Acquire(ctx context.Context, sync string) (held bool, release func() error, err error)

	// Sync definitions.
	PutSync(ctx context.Context, def SyncDef) error
	GetSync(ctx context.Context, name string) (SyncDef, error)
	ListSyncs(ctx context.Context) ([]SyncDef, error)

	// Initial-copy progress (per sync, per table).
	SaveCopyProgress(ctx context.Context, sync string, p CopyProgress) error
	LoadCopyProgress(ctx context.Context, sync string, t engine.TableRef) (CopyProgress, error)

	// Streaming cursors (per sync, per target, per table).
	SaveCursor(ctx context.Context, sync string, c Cursor) error
	LoadCursor(ctx context.Context, sync string, target engine.TargetID, t engine.TableRef) (Cursor, error)

	// RecordEvent persists an operational event for the status API / audit trail.
	RecordEvent(ctx context.Context, e Event) error

	// Read aggregates for the status surface (M6). ListCopyProgress and
	// ListCursors return every row for a sync; RecentEvents returns the newest
	// events (most recent first, up to limit) for "last error" reporting.
	ListCopyProgress(ctx context.Context, sync string) ([]CopyProgress, error)
	ListCursors(ctx context.Context, sync string) ([]Cursor, error)
	RecentEvents(ctx context.Context, sync string, limit int) ([]Event, error)
}
