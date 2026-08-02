// Package mysqlmigrate implements the migrate.DB / migrate.Tx ports over
// database/sql for MySQL, the MySQL adapter for the F4 shared migration runner
// (mysql-plan MF4). Unlike Postgres, MySQL auto-commits every DDL statement, so a
// migration's statements and its version bump CANNOT be one atomic transaction.
// The adapter therefore runs statements without a wrapping transaction and relies
// on migrations being idempotent (CREATE ... IF NOT EXISTS): a crash mid-apply is
// safely recovered by re-running (the DDL is a no-op, then the version records).
// This is the "idempotent per-step checkpointing" MF4 calls for (Momus M1).
package mysqlmigrate

import (
	"context"
	"database/sql"
	"fmt"
	"hash/fnv"

	"github.com/rudimk/replicare/internal/migrate"
)

// DB adapts a *sql.DB to migrate.DB + migrate.Locker.
type DB struct{ db *sql.DB }

// New returns a migrate.DB backed by the given *sql.DB.
func New(db *sql.DB) *DB { return &DB{db: db} }

var (
	_ migrate.DB     = (*DB)(nil)
	_ migrate.Locker = (*DB)(nil)
)

// Lock serializes concurrent migrators of the same schema behind a MySQL named
// lock (GET_LOCK), so two syncs bringing up the same source `replicare` schema
// don't race on non-idempotent CREATE TRIGGER (M7). GET_LOCK is session-scoped;
// the single-connection *sql.DB (MaxOpenConns=1) keeps lock+unlock on one session.
func (d *DB) Lock(ctx context.Context, key string) (func(context.Context) error, error) {
	name := lockName(key)
	var ok sql.NullInt64
	// 10s acquire timeout; a stuck holder should fail loudly, not hang forever.
	if err := d.db.QueryRowContext(ctx, "SELECT GET_LOCK(?, 10)", name).Scan(&ok); err != nil {
		return nil, fmt.Errorf("mysqlmigrate: GET_LOCK: %w", err)
	}
	if !ok.Valid || ok.Int64 != 1 {
		return nil, fmt.Errorf("mysqlmigrate: could not acquire migration lock %q (timeout)", name)
	}
	return func(ctx context.Context) error {
		if _, err := d.db.ExecContext(ctx, "SELECT RELEASE_LOCK(?)", name); err != nil {
			return fmt.Errorf("mysqlmigrate: RELEASE_LOCK: %w", err)
		}
		return nil
	}, nil
}

// EnsureVersionTable creates the capture database and the version table if
// absent. versionTable is "<db>.<table>"; the database is created too.
func (d *DB) EnsureVersionTable(ctx context.Context, versionTable string) error {
	dbName, tbl := splitQualified(versionTable)
	if _, err := d.db.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", dbName)); err != nil {
		return fmt.Errorf("mysqlmigrate: create database %s: %w", dbName, err)
	}
	stmt := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		version    INT PRIMARY KEY,
		name       VARCHAR(255) NOT NULL,
		applied_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
	) ENGINE=InnoDB`, backtick(dbName, tbl))
	if _, err := d.db.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("mysqlmigrate: ensure version table: %w", err)
	}
	return nil
}

// CurrentVersion returns the highest applied version (0 if none).
func (d *DB) CurrentVersion(ctx context.Context, versionTable string) (int, error) {
	dbName, tbl := splitQualified(versionTable)
	var v sql.NullInt64
	err := d.db.QueryRowContext(ctx, fmt.Sprintf("SELECT COALESCE(MAX(version), 0) FROM %s", backtick(dbName, tbl))).Scan(&v)
	if err != nil {
		return 0, fmt.Errorf("mysqlmigrate: current version: %w", err)
	}
	return int(v.Int64), nil
}

// InTx runs a migration's statements. MySQL auto-commits DDL, so this is NOT a
// real transaction — it executes each statement directly. Correctness rests on
// the migration statements being idempotent (see the package doc).
func (d *DB) InTx(ctx context.Context, fn func(migrate.Tx) error) error {
	return fn(&tx{db: d.db})
}

type tx struct{ db *sql.DB }

func (t *tx) Exec(ctx context.Context, sql string) error {
	if _, err := t.db.ExecContext(ctx, sql); err != nil {
		return fmt.Errorf("mysqlmigrate: exec: %w", err)
	}
	return nil
}

func (t *tx) RecordVersion(ctx context.Context, versionTable string, version int, name string) error {
	dbName, tbl := splitQualified(versionTable)
	// Idempotent: re-recording the same version after a recovered crash is a no-op.
	_, err := t.db.ExecContext(ctx,
		fmt.Sprintf("INSERT INTO %s (version, name) VALUES (?, ?) ON DUPLICATE KEY UPDATE name = VALUES(name)", backtick(dbName, tbl)),
		version, name)
	if err != nil {
		return fmt.Errorf("mysqlmigrate: record version %d: %w", version, err)
	}
	return nil
}

// --- helpers ---

func splitQualified(q string) (dbName, tbl string) {
	for i := 0; i < len(q); i++ {
		if q[i] == '.' {
			return q[:i], q[i+1:]
		}
	}
	return "", q
}

func backtick(dbName, tbl string) string {
	if dbName == "" {
		return "`" + tbl + "`"
	}
	return "`" + dbName + "`.`" + tbl + "`"
}

// lockName derives a stable, short MySQL lock name from a key (MySQL lock names
// are capped at 64 chars). FNV-1a keeps it collision-resistant and bounded.
func lockName(key string) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	return fmt.Sprintf("replicare_mig_%x", h.Sum64())
}
