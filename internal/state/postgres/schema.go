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
	},
}
