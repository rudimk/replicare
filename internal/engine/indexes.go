package engine

import "context"

// DeferredIndex is one plain secondary index on a target table that replicare takes
// down for the initial copy and builds again once the sync is streaming (CLAUDE.md
// §4.1 "deferred target indexes"). Only indexes replicare may safely remove qualify:
// never a primary key, a unique index, or an index behind a constraint — apply needs
// those, and they define the data, not just its speed.
type DeferredIndex struct {
	Table TableRef
	// Name is the index's name in the table's schema.
	Name string
	// Definition is the index's own CREATE INDEX statement, exactly as the target
	// reports it (Postgres: pg_get_indexdef). The builder adds the non-blocking form.
	Definition string
	// Tablespace is the index's tablespace when it is not the database default ("" =
	// default), so the rebuilt index lands where the original was.
	Tablespace string
	// After holds statements that restore what the CREATE statement alone does not
	// carry (a comment, the table's CLUSTER marker, per-column statistics targets).
	After []string
	// Link marks an index whose leading column is a foreign-key column of its table.
	// Deleting a parent row scans the child for references, which is slow without
	// such an index, so these are rebuilt first.
	Link bool
}

// IndexSkip is a table whose plain indexes replicare leaves in place, and why
// (for example: the target role does not own the table, so it may not drop them).
type IndexSkip struct {
	Table  TableRef
	Reason string
}

// IndexPresence is what the target currently holds under a deferred index's name.
type IndexPresence int

const (
	// IndexMissing: no index of that name exists.
	IndexMissing IndexPresence = iota
	// IndexValid: the index exists and is usable.
	IndexValid
	// IndexInvalid: the index exists but an interrupted build left it unusable.
	IndexInvalid
)

// IndexBuildOptions tunes one index build's session. Zero values leave the target's
// own settings in place.
type IndexBuildOptions struct {
	// MaintenanceWorkMem is the memory each build may sort in (Postgres
	// maintenance_work_mem), as the server accepts it (e.g. "1GB"). "" = server default.
	MaintenanceWorkMem string
	// ParallelWorkers is how many extra workers a build may use (Postgres
	// max_parallel_maintenance_workers). 0 = server default.
	ParallelWorkers int
}

// IndexBuildProgress is a snapshot of an index build in flight.
type IndexBuildProgress struct {
	Phase string
	// Done / Total are the current phase's own units (blocks or rows); Total 0 when
	// the phase reports none (a sort, a wait).
	Done, Total int64
}

// IndexDeferrer is an optional Sink capability: take a table's plain indexes down
// before its initial copy and build them again afterwards, so a large first copy
// does not maintain every index row by row. Postgres implements it; an engine that
// does not simply copies with its indexes in place.
//
// A Sink is not safe for concurrent use, so a build and its progress reads run on
// two different Sinks: BuildDeferredIndex blocks on one while IndexBuildProgress
// polls the other, given the builder's BuildSession.
type IndexDeferrer interface {
	// DeferrableIndexes lists the valid plain indexes on these tables that this role
	// may drop, plus the tables it must leave alone and why.
	DeferrableIndexes(ctx context.Context, tables []TableRef) ([]DeferredIndex, []IndexSkip, error)
	// DropDeferredIndex removes the index if it exists, never queueing the
	// application behind a waiting lock: a short lock wait, backoff and retries, then
	// an error.
	DropDeferredIndex(ctx context.Context, idx DeferredIndex) error
	// IndexPresence reports what the target holds under the index's name.
	IndexPresence(ctx context.Context, idx DeferredIndex) (IndexPresence, error)
	// BuildDeferredIndex builds the index without blocking writes to its table, then
	// runs its After statements. It drops a left-over invalid index of that name first,
	// and treats an existing valid one as already built.
	BuildDeferredIndex(ctx context.Context, idx DeferredIndex, opts IndexBuildOptions) error
	// BuildSession identifies this Sink's connection, for IndexBuildProgress on another.
	BuildSession(ctx context.Context) (int64, error)
	// IndexBuildProgress reads the progress of the build running on session; ok=false
	// when that session is not building an index.
	IndexBuildProgress(ctx context.Context, session int64) (p IndexBuildProgress, ok bool, err error)
	// AnalyzeTables refreshes the planner statistics of tables whose indexes are back.
	AnalyzeTables(ctx context.Context, tables []TableRef) error
}
