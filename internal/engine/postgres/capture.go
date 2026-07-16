package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/rudimk/replicare/internal/engine"
)

// InstallCapture installs trigger-based CDC on the given source tables
// (CLAUDE.md §3.1): it ensures the F4 capture schema, then per table registers a
// stable rel_id and lazily creates the delta table, track table, SECURITY
// DEFINER trigger function, and AFTER trigger, and sets aggressive autovacuum on
// the delta table. It is idempotent — re-installing preserves already-queued
// deltas (delta/track tables use IF NOT EXISTS; the function/trigger are
// replaced). Tables without a usable key are skipped (they cannot be captured;
// pre-flight already warns).
func (s *Source) InstallCapture(ctx context.Context, tables []engine.TableRef) error {
	if err := s.requireConn(); err != nil {
		return err
	}
	if err := s.ensureCaptureSchema(ctx); err != nil {
		return err
	}
	introspected, err := s.introspectTables(ctx, tables)
	if err != nil {
		return err
	}
	for _, ref := range tables {
		tbl, ok := introspected[ref]
		if !ok {
			return fmt.Errorf("postgres: install capture: table %s not found on source", ref)
		}
		cols := captureColsFor(tbl)
		if len(cols) == 0 {
			// No usable key — cannot capture; skip (pre-flight warns).
			continue
		}
		if err := s.installOne(ctx, ref, cols); err != nil {
			return fmt.Errorf("postgres: install capture on %s: %w", ref, err)
		}
	}
	return nil
}

// installOne installs capture for a single table within one transaction, so a
// partial failure leaves no half-built capture objects.
func (s *Source) installOne(ctx context.Context, ref engine.TableRef, cols []captureCol) error {
	tx, err := s.conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	relID, err := upsertRegistry(ctx, tx, ref, cols)
	if err != nil {
		return err
	}

	stmts := []string{
		deltaTableDDL(relID, cols),
		trackTableDDL(relID),
		deltaAutovacuumDDL(relID),
		triggerFunctionDDL(relID, cols),
		dropTriggerDDL(relID, qualifyTable(ref)),
		createTriggerDDL(relID, qualifyTable(ref)),
	}
	for _, stmt := range stmts {
		if _, err := tx.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("exec: %w\n---\n%s", err, stmt)
		}
	}
	return tx.Commit(ctx)
}

// RemoveCapture tears down capture for the given tables, leaving the source
// clean: it drops the trigger, trigger function, delta and track tables, and the
// registry row. The replicare schema framework (schema_version, registry,
// sequence) remains for other captured tables. Unregistered tables are ignored.
func (s *Source) RemoveCapture(ctx context.Context, tables []engine.TableRef) error {
	if err := s.requireConn(); err != nil {
		return err
	}
	for _, ref := range tables {
		relID, ok, err := lookupRegistry(ctx, s.conn, ref)
		if err != nil {
			return fmt.Errorf("postgres: remove capture on %s: %w", ref, err)
		}
		if !ok {
			continue // not captured
		}
		if err := s.removeOne(ctx, ref, relID); err != nil {
			return fmt.Errorf("postgres: remove capture on %s: %w", ref, err)
		}
	}
	return nil
}

func (s *Source) removeOne(ctx context.Context, ref engine.TableRef, relID int) error {
	tx, err := s.conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	stmts := []string{
		dropTriggerDDL(relID, qualifyTable(ref)),
		fmt.Sprintf("DROP FUNCTION IF EXISTS %s()", qualifiedCapture(triggerFnName(relID))),
		fmt.Sprintf("DROP TABLE IF EXISTS %s", qualifiedCapture(deltaTableName(relID))),
		fmt.Sprintf("DROP TABLE IF EXISTS %s", qualifiedCapture(trackTableName(relID))),
		fmt.Sprintf("DELETE FROM %s.captured WHERE rel_id = %d", quoteIdentifier(captureSchema), relID),
	}
	for _, stmt := range stmts {
		if _, err := tx.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("exec: %w\n---\n%s", err, stmt)
		}
	}
	return tx.Commit(ctx)
}

// upsertRegistry registers (or updates) a table and returns its stable rel_id.
func upsertRegistry(ctx context.Context, q pgx.Tx, ref engine.TableRef, cols []captureCol) (int, error) {
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.Name
	}
	var relID int
	err := q.QueryRow(ctx, fmt.Sprintf(`
		INSERT INTO %s.captured (schema_name, table_name, pk_cols)
		VALUES ($1, $2, $3)
		ON CONFLICT (schema_name, table_name) DO UPDATE SET pk_cols = EXCLUDED.pk_cols
		RETURNING rel_id`, quoteIdentifier(captureSchema)),
		ref.Schema, ref.Name, names).Scan(&relID)
	if err != nil {
		return 0, fmt.Errorf("register table: %w", err)
	}
	return relID, nil
}

// lookupRegistry returns a table's rel_id, or ok=false if it is not captured.
func lookupRegistry(ctx context.Context, q *pgx.Conn, ref engine.TableRef) (int, bool, error) {
	var relID int
	err := q.QueryRow(ctx, fmt.Sprintf(
		`SELECT rel_id FROM %s.captured WHERE schema_name = $1 AND table_name = $2`,
		quoteIdentifier(captureSchema)), ref.Schema, ref.Name).Scan(&relID)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return relID, true, nil
}

// introspectTables introspects exactly the given tables (matched by exact name)
// and returns them keyed by ref.
func (s *Source) introspectTables(ctx context.Context, refs []engine.TableRef) (map[engine.TableRef]engine.Table, error) {
	version, err := serverVersion(ctx, s.conn)
	if err != nil {
		return nil, err
	}
	inc := make([]string, len(refs))
	for i, r := range refs {
		inc[i] = r.String()
	}
	schema, err := introspectConn(ctx, s.conn, version, engine.Selection{Include: inc})
	if err != nil {
		return nil, err
	}
	out := make(map[engine.TableRef]engine.Table, len(schema.Tables))
	for _, t := range schema.Tables {
		out[t.Ref] = t
	}
	return out, nil
}

// captureColsFor returns the replication-identity columns (name + type) for a
// table, or nil if it has no usable key. The identity is the primary key, else
// the first usable unique key (CLAUDE.md §3.1).
func captureColsFor(t engine.Table) []captureCol {
	key := replicationKey(t)
	if key == nil {
		return nil
	}
	typeByName := make(map[string]string, len(t.Columns))
	for _, c := range t.Columns {
		typeByName[c.Name] = c.DataType
	}
	cols := make([]captureCol, 0, len(key.Columns))
	for _, name := range key.Columns {
		cols = append(cols, captureCol{Name: name, Type: typeByName[name]})
	}
	return cols
}

// qualifyTable returns a schema-qualified, quoted source table name.
func qualifyTable(ref engine.TableRef) string {
	return quoteIdentifier(ref.Schema) + "." + quoteIdentifier(ref.Name)
}
