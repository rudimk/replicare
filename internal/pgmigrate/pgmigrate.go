// Package pgmigrate implements the migrate.DB / migrate.Tx ports over pgx. It is
// the single pgx adapter for the F4 shared migration runner, reused by both the
// state-store schema (M2) and the source `replicare` schema (M3) — the runner
// logic lives in package migrate; this is only the database plumbing.
//
// It works over any pgx querier (a *pgx.Conn or a *pgxpool.Pool), so a
// single-connection caller and a pooled caller share the same adapter.
package pgmigrate

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/rudimk/replicare/internal/migrate"
)

// Querier is the subset of pgx used by the adapter; satisfied by *pgx.Conn and
// *pgxpool.Pool alike.
type Querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Begin(ctx context.Context) (pgx.Tx, error)
}

// DB adapts a Querier to migrate.DB.
type DB struct {
	q Querier
}

// New returns a migrate.DB backed by the given pgx querier.
func New(q Querier) *DB { return &DB{q: q} }

// Compile-time assertions.
var (
	_ migrate.DB     = (*DB)(nil)
	_ migrate.Locker = (*DB)(nil)
)

// Lock takes a session-level Postgres advisory lock keyed on the (hashed) schema
// name, so concurrent migrators of the same schema serialize instead of racing on
// non-idempotent DDL (M7). It returns an unlock func. Advisory locks are
// old-PG-safe (since 8.2) and need no special privilege. On a single connection
// (the source capture path) lock and unlock land on the same session; the state
// store migrates once at startup, so pooling is not a concern there.
func (d *DB) Lock(ctx context.Context, key string) (func(context.Context) error, error) {
	k := advisoryKey(key)
	if _, err := d.q.Exec(ctx, "SELECT pg_advisory_lock($1)", k); err != nil {
		return nil, fmt.Errorf("pgmigrate: advisory lock: %w", err)
	}
	return func(ctx context.Context) error {
		if _, err := d.q.Exec(ctx, "SELECT pg_advisory_unlock($1)", k); err != nil {
			return fmt.Errorf("pgmigrate: advisory unlock: %w", err)
		}
		return nil
	}, nil
}

// advisoryKey derives a stable 64-bit advisory-lock key from a schema name via
// FNV-1a, so distinct schemas (state store vs source) use distinct locks.
func advisoryKey(s string) int64 {
	var h uint64 = 1469598103934665603
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 1099511628211
	}
	return int64(h)
}

// EnsureVersionTable idempotently creates the version table's schema and the
// table itself. versionTable is a trusted, schema-qualified identifier supplied
// by our own migration Sets (never user input).
//
// CREATE SCHEMA runs only when the schema is actually absent: Postgres checks the
// CREATE-on-database privilege before the IF NOT EXISTS short-circuit, so blindly
// issuing CREATE SCHEMA IF NOT EXISTS would fail for a least-privilege role even
// when the schema was pre-created for us (CLAUDE.md §12 "a pre-created schema we
// own"). When the schema is present we skip straight to the table.
func (d *DB) EnsureVersionTable(ctx context.Context, versionTable string) error {
	schema, table, err := splitIdent(versionTable)
	if err != nil {
		return err
	}
	if schema != "" {
		exists, err := d.schemaExists(ctx, schema)
		if err != nil {
			return err
		}
		if !exists {
			if _, err := d.q.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, quoteIdent(schema))); err != nil {
				return fmt.Errorf("pgmigrate: create schema %q: %w", schema, err)
			}
		}
	}
	stmt := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		version    integer PRIMARY KEY,
		name       text NOT NULL,
		applied_at timestamptz NOT NULL DEFAULT now()
	)`, qualified(schema, table))
	if _, err := d.q.Exec(ctx, stmt); err != nil {
		return fmt.Errorf("pgmigrate: create version table %q: %w", versionTable, err)
	}
	return nil
}

// schemaExists reports whether a schema is already present. Reading
// information_schema.schemata needs no special privilege and is old-PG-safe.
func (d *DB) schemaExists(ctx context.Context, schema string) (bool, error) {
	var exists bool
	err := d.q.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)`, schema).
		Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("pgmigrate: check schema %q: %w", schema, err)
	}
	return exists, nil
}

// CurrentVersion returns the highest recorded version, or 0 if none.
func (d *DB) CurrentVersion(ctx context.Context, versionTable string) (int, error) {
	schema, table, err := splitIdent(versionTable)
	if err != nil {
		return 0, err
	}
	var v int
	q := fmt.Sprintf(`SELECT COALESCE(MAX(version), 0) FROM %s`, qualified(schema, table))
	if err := d.q.QueryRow(ctx, q).Scan(&v); err != nil {
		return 0, fmt.Errorf("pgmigrate: read current version from %q: %w", versionTable, err)
	}
	return v, nil
}

// InTx runs fn inside a transaction, committing on success and rolling back on
// error, so a migration's statements and its version bump are atomic.
func (d *DB) InTx(ctx context.Context, fn func(migrate.Tx) error) error {
	tx, err := d.q.Begin(ctx)
	if err != nil {
		return fmt.Errorf("pgmigrate: begin: %w", err)
	}
	if err := fn(&txAdapter{tx: tx}); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("pgmigrate: commit: %w", err)
	}
	return nil
}

// txAdapter adapts a pgx.Tx to migrate.Tx.
type txAdapter struct {
	tx pgx.Tx
}

func (t *txAdapter) Exec(ctx context.Context, sql string) error {
	if _, err := t.tx.Exec(ctx, sql); err != nil {
		return err
	}
	return nil
}

func (t *txAdapter) RecordVersion(ctx context.Context, versionTable string, version int, name string) error {
	schema, table, err := splitIdent(versionTable)
	if err != nil {
		return err
	}
	q := fmt.Sprintf(`INSERT INTO %s (version, name) VALUES ($1, $2)`, qualified(schema, table))
	if _, err := t.tx.Exec(ctx, q, version, name); err != nil {
		return fmt.Errorf("pgmigrate: record version %d: %w", version, err)
	}
	return nil
}

// splitIdent parses a "schema.table" (or bare "table") identifier and validates
// that each part is a safe SQL identifier (our Sets only ever pass constants, but
// we validate to fail loudly on a malformed VersionTable rather than emit unsafe
// SQL).
func splitIdent(ident string) (schema, table string, err error) {
	parts := strings.Split(ident, ".")
	switch len(parts) {
	case 1:
		table = parts[0]
	case 2:
		schema, table = parts[0], parts[1]
	default:
		return "", "", fmt.Errorf("pgmigrate: invalid version table identifier %q", ident)
	}
	if schema != "" && !isSafeIdent(schema) {
		return "", "", fmt.Errorf("pgmigrate: unsafe schema %q in %q", schema, ident)
	}
	if !isSafeIdent(table) { // rejects empty table too
		return "", "", fmt.Errorf("pgmigrate: unsafe or empty table %q in %q", table, ident)
	}
	return schema, table, nil
}

// isSafeIdent allows lowercase letters, digits, and underscores, starting with a
// letter or underscore — the shape of all identifiers our migration Sets use.
func isSafeIdent(s string) bool {
	if s == "" {
		return false
	}
	for i, r := range s {
		switch {
		case r == '_':
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9' && i > 0:
		default:
			return false
		}
	}
	return true
}

func quoteIdent(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

func qualified(schema, table string) string {
	if schema == "" {
		return quoteIdent(table)
	}
	return quoteIdent(schema) + "." + quoteIdent(table)
}
