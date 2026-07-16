package postgres

import (
	"context"
	"fmt"

	"github.com/rudimk/replicare/internal/migrate"
	"github.com/rudimk/replicare/internal/pgmigrate"
)

// Source-side trigger-CDC capture schema (CLAUDE.md §3). This is the `replicare`
// schema we install on the SOURCE database: an F4-versioned schema holding a
// registry of captured tables plus, created lazily per table, delta tables,
// track tables, and trigger functions. It is distinct from the daemon's own
// StateStore (CLAUDE.md §9 "do not conflate").
//
// The source schema lives on a database we may not own, so it is migrated in
// place and never drop-recreated; upgrades preserve queued deltas.

// captureSchema is the dedicated schema on the source holding all capture objects.
const captureSchema = "replicare"

// captureVersionTable records the source-schema version (F4).
const captureVersionTable = captureSchema + ".schema_version"

// sourceSchemaSet is the F4 migration set for the source `replicare` schema. The
// schema and schema_version table are created by the runner (EnsureVersionTable);
// migration 1 adds the shared delta sequence and the captured-tables registry.
var sourceSchemaSet = migrate.Set{
	Name:         "source",
	VersionTable: captureVersionTable,
	Migrations: []migrate.Migration{
		{
			Version: 1,
			Name:    "initial",
			Statements: []string{
				// Shared sequence for the rc_seq ordering hint (lag metric only;
				// never the resume cursor — CLAUDE.md §3.3).
				`CREATE SEQUENCE replicare.delta_seq`,
				// Registry mapping a captured source table to its stable rel_id and
				// ordered primary-key column names. Object names (delta/track/
				// trigger) are derived from rel_id.
				`CREATE TABLE replicare.captured (
					rel_id      serial PRIMARY KEY,
					schema_name text NOT NULL,
					table_name  text NOT NULL,
					pk_cols     text[] NOT NULL,
					created_at  timestamptz NOT NULL DEFAULT now(),
					UNIQUE (schema_name, table_name)
				)`,
			},
		},
	},
}

// ensureCaptureSchema brings the source `replicare` schema up to the current
// version (F4). It is idempotent and safe to call before every install.
func (s *Source) ensureCaptureSchema(ctx context.Context) error {
	if err := s.requireConn(); err != nil {
		return err
	}
	if _, err := migrate.Apply(ctx, pgmigrate.New(s.conn), sourceSchemaSet); err != nil {
		return fmt.Errorf("postgres: ensure capture schema: %w", err)
	}
	return nil
}
