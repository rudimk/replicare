// Package statepg is the Postgres implementation of state.StateStore (CLAUDE.md
// §9): the daemon's own operational state — sync definitions, initial-copy
// progress, per-target cursors, events — plus single-active ownership via
// pg_advisory_lock. It lives in a dedicated, F4-versioned schema on a
// user-pointed Postgres (which may be the target, the source, or a separate DB).
//
// This is distinct from the trigger-CDC delta/track tables, which live on the
// SOURCE database (CLAUDE.md §9 "do not conflate").
package statepg

import "github.com/rudimk/replicare/internal/migrate"

// stateSchema is the dedicated schema holding all state-store tables.
const stateSchema = "replicare_state"

// versionTable records applied schema versions (F4).
const versionTable = stateSchema + ".schema_version"

// schemaSet is the F4 migration set for the state-store schema. The schema and
// the schema_version table are created by the runner (EnsureVersionTable); each
// migration here is applied once, atomically with its version bump, and must
// preserve existing state on upgrade (never drop-recreate).
var schemaSet = migrate.Set{
	Name:         "statestore",
	VersionTable: versionTable,
	Migrations: []migrate.Migration{
		{
			Version: 1,
			Name:    "initial",
			Statements: []string{
				// Sync definitions the daemon resumes from.
				`CREATE TABLE replicare_state.syncs (
					name       text PRIMARY KEY,
					source     text NOT NULL,
					targets    text[] NOT NULL DEFAULT '{}',
					include    text[] NOT NULL DEFAULT '{}',
					exclude    text[] NOT NULL DEFAULT '{}',
					updated_at timestamptz NOT NULL DEFAULT now()
				)`,
				// Per-table initial-copy progress: a completed-range watermark plus
				// a sparse set of out-of-order completed ranges (CLAUDE.md §4.1).
				`CREATE TABLE replicare_state.copy_progress (
					sync        text NOT NULL REFERENCES replicare_state.syncs(name) ON DELETE CASCADE,
					schema_name text NOT NULL,
					table_name  text NOT NULL,
					done        boolean NOT NULL DEFAULT false,
					watermark   jsonb,
					completed   jsonb NOT NULL DEFAULT '[]',
					updated_at  timestamptz NOT NULL DEFAULT now(),
					PRIMARY KEY (sync, schema_name, table_name)
				)`,
				// Per-(target, table) streaming cursors: lag/observability position
				// and phase for cutover (NOT the correctness mechanism — §3.3).
				`CREATE TABLE replicare_state.cursors (
					sync         text NOT NULL REFERENCES replicare_state.syncs(name) ON DELETE CASCADE,
					target       text NOT NULL,
					schema_name  text NOT NULL,
					table_name   text NOT NULL,
					phase        text NOT NULL DEFAULT 'initial_copy',
					last_delta   bigint NOT NULL DEFAULT 0,
					needs_reseed boolean NOT NULL DEFAULT false,
					updated_at   timestamptz NOT NULL DEFAULT now(),
					PRIMARY KEY (sync, target, schema_name, table_name)
				)`,
				// Operational events for the status API / audit trail.
				`CREATE TABLE replicare_state.events (
					id          bigserial PRIMARY KEY,
					sync        text,
					target      text,
					schema_name text,
					table_name  text,
					level       text NOT NULL,
					event       text NOT NULL,
					message     text,
					attrs       jsonb,
					created_at  timestamptz NOT NULL DEFAULT now()
				)`,
			},
		},
		{
			Version: 2,
			Name:    "copy_progress_per_target",
			// Fan-out (and, later, an active-active mesh) copies each target
			// independently, so initial-copy progress must be keyed per
			// (sync, TARGET, table) — the original schema keyed only
			// (sync, table), so two targets of one sync collided on a single row
			// (fan-out was never hardened; CLAUDE.md §6 / multi-master plan MM2).
			//
			// In-place migration, progress-preserving (§14 — never drop-recreate):
			// backfill the new target dimension from the cursors table (already
			// target-keyed), so every (target, table) that has a cursor keeps its
			// watermark and re-copies nothing. A copy_progress row with NO matching
			// cursor can only exist mid-initial-copy before cutover; it has no target
			// to attribute to and is dropped, so that one table re-copies once —
			// idempotent and safe.
			Statements: []string{
				`ALTER TABLE replicare_state.copy_progress ADD COLUMN target text`,
				// Drop the old PK so transient duplicate (sync,schema,table) rows are
				// allowed while we fan the single row out to one row per target.
				`ALTER TABLE replicare_state.copy_progress DROP CONSTRAINT copy_progress_pkey`,
				`INSERT INTO replicare_state.copy_progress
					(sync, target, schema_name, table_name, done, watermark, completed, updated_at)
				 SELECT cp.sync, c.target, cp.schema_name, cp.table_name,
				        cp.done, cp.watermark, cp.completed, cp.updated_at
				 FROM replicare_state.copy_progress cp
				 JOIN (SELECT DISTINCT sync, target, schema_name, table_name
				       FROM replicare_state.cursors) c
				   ON c.sync = cp.sync
				  AND c.schema_name = cp.schema_name
				  AND c.table_name = cp.table_name
				 WHERE cp.target IS NULL`,
				`DELETE FROM replicare_state.copy_progress WHERE target IS NULL`,
				`ALTER TABLE replicare_state.copy_progress ALTER COLUMN target SET NOT NULL`,
				`ALTER TABLE replicare_state.copy_progress
				   ADD CONSTRAINT copy_progress_pkey PRIMARY KEY (sync, target, schema_name, table_name)`,
			},
		},
	},
}
