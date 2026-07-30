package engine

import (
	"context"
	"errors"
	"io"
	"time"
)

// TransientConstraintError marks an apply failure — typically an FK violation —
// that can resolve on a later drain pass once the missing dependency lands
// (CLAUDE.md §3.3 retry fallback: cycles, self-references, cross-pass deps).
// Engines wrap the underlying driver error; the drain loop retries these with
// bounded backoff and halts loud on anything else (or on exhaustion).
type TransientConstraintError struct{ Err error }

func (e *TransientConstraintError) Error() string { return e.Err.Error() }
func (e *TransientConstraintError) Unwrap() error { return e.Err }

// IsTransientConstraint reports whether err is (or wraps) a transient
// constraint violation eligible for the retry fallback.
func IsTransientConstraint(err error) bool {
	var t *TransientConstraintError
	return errors.As(err, &t)
}

// TLSMode mirrors the libpq sslmode spectrum (CLAUDE.md §11). Connections are
// configurable per endpoint, disable -> verify-full.
type TLSMode string

const (
	TLSDisable    TLSMode = "disable"
	TLSAllow      TLSMode = "allow"
	TLSPrefer     TLSMode = "prefer"
	TLSRequire    TLSMode = "require"
	TLSVerifyCA   TLSMode = "verify-ca"
	TLSVerifyFull TLSMode = "verify-full"
)

// ConnConfig is a resolved connection descriptor for one endpoint (source or
// target). Secrets are already resolved here (inline or env override happens in
// the config layer, CLAUDE.md §11) — this struct carries the concrete password.
type ConnConfig struct {
	Host     string
	Port     int
	Database string
	User     string
	Password string
	TLS      TLSMode
	// Params carries extra driver/connection parameters. Session GUC
	// canonicalization (DateStyle, TimeZone, extra_float_digits, ...) is applied
	// by the implementation, not configured here (CLAUDE.md §4.2).
	Params map[string]string
}

// Engine is a database engine implementation (postgres, mysql, redis). It is the
// factory for that engine's Source and Sink. Registered via the package registry.
type Engine interface {
	// Name is the engine identifier used in config, e.g. "postgres".
	Name() string
	// NewSource constructs (does not yet connect) a Source for the endpoint.
	NewSource(cfg ConnConfig) (Source, error)
	// NewSink constructs (does not yet connect) a Sink for the endpoint.
	NewSink(cfg ConnConfig) (Sink, error)
	// Preflight classifies an introspected source against an introspected
	// target into a neutral report (M1). Engine-specific type rules live in the
	// implementation; inputs and output are engine-neutral (CLAUDE.md §4.2).
	Preflight(syncName string, srcVersion, tgtVersion int, source, target *Schema) *PreflightReport
}

// Source is the read side of replication: introspection, trigger-based capture,
// chunked initial copy, and dirty-key delta consumption (CLAUDE.md §3, §4).
//
// This interface is the contract; concrete methods are implemented per milestone
// (introspection M1, capture M3, copy M4, delta M5). It will grow as those land.
type Source interface {
	// Connect opens the connection and applies session-GUC canonicalization.
	Connect(ctx context.Context) error
	Close(ctx context.Context) error
	// ServerVersion returns the source server version number (e.g. 90600 for
	// 9.6) so version-tolerant code paths can branch (CLAUDE.md §1.6).
	ServerVersion(ctx context.Context) (int, error)

	// Introspect returns the schema for the selected tables (M1).
	Introspect(ctx context.Context, sel Selection) (*Schema, error)

	// InstallCapture / RemoveCapture manage the replicare schema, per-table delta
	// tables, track tables, and triggers on the source (M3).
	InstallCapture(ctx context.Context, tables []TableRef) error
	RemoveCapture(ctx context.Context, tables []TableRef) error

	// PlanChunks computes balanced copy chunks for a table (keyset/ctid; M4).
	PlanChunks(ctx context.Context, t TableRef, opts ChunkOptions) ([]Chunk, error)
	// CopyChunk streams one chunk as text COPY into w (M4).
	CopyChunk(ctx context.Context, c Chunk, w io.Writer) error

	// ReadDirtyKeys returns unconsumed delta keys for (target, table) via
	// set-difference against the track table (M5, CLAUDE.md §3.3).
	ReadDirtyKeys(ctx context.Context, t TableRef, target TargetID, max int) ([]DirtyKey, error)
	// RereadCurrent streams the current source values for the given keys as text
	// COPY into w (faithful re-read; M5, CLAUDE.md §4.2).
	RereadCurrent(ctx context.Context, t TableRef, keys []KeyValues, w io.Writer) error
	// ConfirmConsumed records the delta ids as consumed for (target, table) and
	// is what later enables purge. Delete-by-delta_id discipline (CLAUDE.md §3.3).
	ConfirmConsumed(ctx context.Context, t TableRef, target TargetID, ids []DeltaID) error
	// Purge removes deltas consumed by all configured targets, subject to
	// retention (M5c). targets is the FULL configured target set for the sync:
	// "consumed by all" is only decidable against the complete set (a target that
	// has consumed nothing has no track rows yet still pins the queue). A target
	// pushing the queue past the retention cap is sacrificed — its track is reset
	// and it is returned in PurgeStats.TargetsReseeded for the orchestration to
	// mark needs-reseed (Purge itself never touches the StateStore, CLAUDE.md §9).
	Purge(ctx context.Context, t TableRef, targets []TargetID, ret RetentionPolicy) (PurgeStats, error)

	// DeltaBacklog reports a target's unconsumed-delta backlog for a table (the
	// delta MINUS track_target set) for retention decisions and F2 telemetry
	// (CLAUDE.md §3.4, §10): row count, estimated on-disk bytes, and the age of
	// the oldest unconsumed delta.
	DeltaBacklog(ctx context.Context, t TableRef, target TargetID) (DeltaBacklog, error)
}

// Sink is the write side: introspection, bulk load, and faithful, FK-ordered,
// per-component-transactional apply (CLAUDE.md §3.3, §4.2, §8.1).
type Sink interface {
	Connect(ctx context.Context) error
	Close(ctx context.Context) error
	ServerVersion(ctx context.Context) (int, error)

	// Introspect returns the (pre-existing) target schema for pre-flight (M1).
	Introspect(ctx context.Context, sel Selection) (*Schema, error)

	// BulkLoad streams a text COPY (r) into the target table for initial copy
	// (M4). cols is the explicit, name-matched column list (CLAUDE.md §4.2). It
	// returns the number of rows loaded (for the rows-copied progress metric).
	BulkLoad(ctx context.Context, t TableRef, cols []string, r io.Reader, mode LoadMode) (int64, error)

	// DeleteRange deletes target rows in the half-open key range [lo, hi) (a nil
	// bound is unbounded on that side), used to clear an incomplete chunk's rows
	// before re-COPY on resume (CLAUDE.md §4.1). Keys are faithful text values.
	DeleteRange(ctx context.Context, t TableRef, lo, hi KeyValues) error

	// ApplyPass applies one streaming drain pass for a table (CLAUDE.md §3.3): in
	// one transaction, upsert the re-read present rows and delete the dirtyKeys
	// now absent at the source. reread is the faithful text COPY of current
	// source rows for the dirty keys; cols is the name-matched column list.
	ApplyPass(ctx context.Context, t TableRef, cols []string, dirtyKeys []KeyValues, reread io.Reader) error

	// BeginApply starts a transaction scoped to one FK component so a drain
	// pass applies atomically (CLAUDE.md §8.1). Upserts and deletes within it are
	// ordered parent->child / child->parent by the caller. The transaction defers
	// FK checks (SET CONSTRAINTS ALL DEFERRED) so deferrable cyclic FKs commit.
	BeginApply(ctx context.Context) (ApplyTx, error)
}

// ApplyTx is a target transaction for one FK component's drain pass (M5b). The
// caller stages+upserts every dirty table (parent->child), then deletes-absent
// every dirty table (child->parent), then commits — so the component is
// referentially consistent within the pass.
type ApplyTx interface {
	// StageUpsert stages the faithful text re-read (present rows) for a table
	// into a per-table TEMP table and upserts it (INSERT ... ON CONFLICT DO
	// UPDATE). The staging is kept for a later DeleteAbsent in the same tx.
	StageUpsert(ctx context.Context, t TableRef, cols []string, reread io.Reader) error
	// DeleteAbsent deletes the given dirty keys that are absent from the table's
	// staging (i.e. deleted at the source). Requires a prior StageUpsert for t.
	DeleteAbsent(ctx context.Context, t TableRef, dirtyKeys []KeyValues) error
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

// ChunkMethod is how a table is split for parallel copy (CLAUDE.md §4.1).
type ChunkMethod string

const (
	ChunkKeyset ChunkMethod = "keyset" // PK/unique-key range (default)
	ChunkCTID   ChunkMethod = "ctid"   // physical block range (fallback)
)

// ChunkOptions tunes chunk planning.
type ChunkOptions struct {
	Method     ChunkMethod
	TargetRows int // approximate rows per chunk
	// Lo, when non-nil, restricts planning to keys >= Lo — the resume lower
	// bound (the copy-progress watermark). Keyset only (CLAUDE.md §4.1).
	Lo KeyValues
}

// KeyValues is one row's key column values, in key-column order.
type KeyValues []any

// Chunk is one unit of parallel initial copy: a half-open key or block range.
type Chunk struct {
	Table  TableRef
	Method ChunkMethod
	Lo     KeyValues // inclusive lower bound (nil = unbounded)
	Hi     KeyValues // exclusive upper bound (nil = unbounded)
}

// DeltaID is the unique id of a delta row; consumption deletes by id, never by
// PK, to survive the read-and-clear race (CLAUDE.md §3.3).
type DeltaID int64

// Op is the change operation recorded by a trigger.
type Op string

const (
	OpInsert Op = "I"
	OpUpdate Op = "U"
	OpDelete Op = "D"
)

// DirtyKey is one unconsumed change: which key changed and how. For PK-only
// capture the value is re-read at sync time (CLAUDE.md §3.1).
type DirtyKey struct {
	DeltaID DeltaID
	Op      Op
	Key     KeyValues
}

// LoadMode selects the initial-copy target-write strategy (CLAUDE.md §4.1).
type LoadMode string

const (
	// LoadDirect: COPY straight into an empty target; resume via DELETE-range.
	LoadDirect LoadMode = "direct"
	// LoadMerge: COPY to TEMP staging then INSERT ... ON CONFLICT (non-empty target).
	LoadMerge LoadMode = "merge"
)

// RetentionPolicy bounds delta retention to protect the source (CLAUDE.md §3.4).
// A target exceeding either cap is marked needs-reseed.
type RetentionPolicy struct {
	MaxAgeSeconds int64 // 0 = unbounded by age
	MaxBytes      int64 // 0 = unbounded by size
}

// PurgeStats reports the outcome of a purge pass.
type PurgeStats struct {
	DeltasPurged    int64
	TargetsReseeded []TargetID
}

// DeltaBacklog is a target's unconsumed-delta footprint for one table, the
// headline "is the source healthy?" signal (CLAUDE.md §3.4, §10). Bytes is an
// estimate (backlog fraction of the delta table's total on-disk size); OldestAge
// is the wall-clock age of the oldest unconsumed delta, or 0 when the backlog is
// empty. These feed retention decisions and the F2 delta-backlog metrics.
type DeltaBacklog struct {
	Rows       int64
	Bytes      int64
	OldestAge  time.Duration
	HasBacklog bool
}
