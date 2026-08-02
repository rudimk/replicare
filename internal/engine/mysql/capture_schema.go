package mysql

import (
	"context"
	"fmt"

	"github.com/rudimk/replicare/internal/migrate"
	"github.com/rudimk/replicare/internal/mysqlmigrate"
)

// Source-side trigger-CDC capture schema (CLAUDE.md §3): the `replicare` DATABASE
// installed on the SOURCE, holding a registry of captured tables plus, created
// lazily per table, delta and track tables. It is distinct from the daemon's
// StateStore (§9). It lives on a database we may not own, so it is migrated in
// place, never drop-recreated (F4), preserving queued deltas across upgrades.

// captureVersionTable records the source-schema version (F4).
const captureVersionTable = captureDB + ".schema_version"

// sourceSchemaSet is the F4 migration set for the source `replicare` database.
// Every statement is idempotent (IF NOT EXISTS) because MySQL auto-commits DDL,
// so a crash mid-apply recovers by re-running (mysql-plan MF4 / Momus M1). pk_cols
// is stored JSON-encoded to survive any column name.
var sourceSchemaSet = migrate.Set{
	Name:         "source-mysql",
	VersionTable: captureVersionTable,
	Migrations: []migrate.Migration{
		{
			Version: 1,
			Name:    "initial",
			Statements: []string{
				"CREATE TABLE IF NOT EXISTS " + captureRef("captured") + ` (
					rel_id      INT AUTO_INCREMENT PRIMARY KEY,
					schema_name VARCHAR(64) NOT NULL,
					table_name  VARCHAR(64) NOT NULL,
					pk_cols     TEXT NOT NULL,
					created_at  DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
					UNIQUE KEY uq_schema_table (schema_name, table_name)
				) ENGINE=InnoDB`,
			},
		},
	},
}

// ensureCaptureSchema brings the source `replicare` database up to the current
// version (F4). Idempotent; safe to call before every install. Serialized by the
// migrate runner's advisory lock so concurrent syncs sharing a source don't race.
func (s *Source) ensureCaptureSchema(ctx context.Context) error {
	if s.db == nil {
		return errNotConnected
	}
	if _, err := migrate.Apply(ctx, mysqlmigrate.New(s.db), sourceSchemaSet); err != nil {
		return fmt.Errorf("mysql: ensure capture schema: %w", err)
	}
	return nil
}
