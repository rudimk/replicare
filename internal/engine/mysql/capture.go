package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/rudimk/replicare/internal/engine"
)

// captureColsFor returns the replication-key columns (PK, else first unique key)
// with their MySQL types, or nil if the table has no usable key.
func captureColsFor(t engine.Table) []captureCol {
	key := replicationKey(t)
	if key == nil {
		return nil
	}
	byName := map[string]engine.Column{}
	for _, c := range t.Columns {
		byName[c.Name] = c
	}
	cols := make([]captureCol, 0, len(key.Columns))
	for _, name := range key.Columns {
		cols = append(cols, captureCol{Name: name, Type: byName[name].DataType})
	}
	return cols
}

// InstallCapture installs trigger-based CDC on the given source tables (§3.1). It
// ensures the F4 `replicare` database, then per table registers a stable rel_id
// and creates the delta table, track table, and three AFTER triggers. It is
// idempotent and crash-safe despite MySQL auto-committing DDL: delta/track use
// IF NOT EXISTS and each trigger is DROP-then-CREATE, so a re-run after a partial
// install converges (Momus M1). Tables without a usable key are skipped
// (pre-flight warns). A pre-existing conflicting trigger blocks on the 5.7 floor
// (the block moved here from MM1b, which lacks a connection).
func (s *Source) InstallCapture(ctx context.Context, tables []engine.TableRef) error {
	if s.db == nil {
		return errNotConnected
	}
	if err := s.ensureCaptureSchema(ctx); err != nil {
		return err
	}
	ver, err := serverVersion(ctx, s.db)
	if err != nil {
		return err
	}
	introspected, err := s.introspectByRef(ctx, tables)
	if err != nil {
		return err
	}
	for _, ref := range tables {
		tbl, ok := introspected[ref]
		if !ok {
			return fmt.Errorf("mysql: install capture: table %s not found on source", ref)
		}
		cols := captureColsFor(tbl)
		if len(cols) == 0 {
			continue // no usable key; pre-flight warns
		}
		if err := s.blockConflictingTrigger(ctx, ref, ver); err != nil {
			return err
		}
		if err := s.installOne(ctx, ref, cols); err != nil {
			return fmt.Errorf("mysql: install capture on %s: %w", ref, err)
		}
	}
	return nil
}

// installOne installs capture for one table. Each statement is individually
// idempotent (MySQL auto-commits DDL, so there is no wrapping transaction).
func (s *Source) installOne(ctx context.Context, ref engine.TableRef, cols []captureCol) error {
	relID, err := s.upsertRegistry(ctx, ref, cols)
	if err != nil {
		return err
	}
	stmts := []string{
		deltaTableDDL(relID, cols),
		trackTableDDL(relID),
	}
	for _, op := range []byte{'I', 'U', 'D'} {
		stmts = append(stmts, dropTriggerDDL(relID, ref.Schema, op), triggerDDL(relID, ref.Schema, ref.Name, op, cols))
	}
	for _, stmt := range stmts {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("exec: %w\n---\n%s", err, stmt)
		}
	}
	return nil
}

// RemoveCapture tears down capture for the given tables, leaving the source
// clean: it drops the three triggers, the delta and track tables, and the
// registry row. The `replicare` database and registry remain for other tables.
// MySQL DROP TRIGGER needs only the TRIGGER privilege (no table ownership, unlike
// Postgres), so the daemon role can fully uninstall.
func (s *Source) RemoveCapture(ctx context.Context, tables []engine.TableRef) error {
	if s.db == nil {
		return errNotConnected
	}
	for _, ref := range tables {
		relID, _, ok, err := s.lookupRegistry(ctx, ref)
		if err != nil {
			return fmt.Errorf("mysql: remove capture on %s: %w", ref, err)
		}
		if !ok {
			continue
		}
		stmts := []string{
			dropTriggerDDL(relID, ref.Schema, 'I'),
			dropTriggerDDL(relID, ref.Schema, 'U'),
			dropTriggerDDL(relID, ref.Schema, 'D'),
			fmt.Sprintf("DROP TABLE IF EXISTS %s", captureRef(deltaTableName(relID))),
			fmt.Sprintf("DROP TABLE IF EXISTS %s", captureRef(trackTableName(relID))),
			fmt.Sprintf("DELETE FROM %s WHERE rel_id = %d", captureRef("captured"), relID),
		}
		for _, stmt := range stmts {
			if _, err := s.db.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("mysql: remove capture on %s: exec: %w\n---\n%s", ref, err, stmt)
			}
		}
	}
	return nil
}

// blockConflictingTrigger enforces the pre-existing-trigger rule (mysql-plan
// §0.5). On MySQL < 8.0 only one trigger per (timing, event) is allowed, so a
// pre-existing non-replicare AFTER trigger on a selected table blocks capture
// install with an actionable error. On 8.0+ multiple triggers per event are
// allowed, so it is permitted.
func (s *Source) blockConflictingTrigger(ctx context.Context, ref engine.TableRef, serverVer int) error {
	if serverVer >= 80000 {
		return nil
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT TRIGGER_NAME, EVENT_MANIPULATION
		FROM information_schema.TRIGGERS
		WHERE EVENT_OBJECT_SCHEMA = ? AND EVENT_OBJECT_TABLE = ? AND ACTION_TIMING = 'AFTER'`,
		ref.Schema, ref.Name)
	if err != nil {
		return fmt.Errorf("mysql: check existing triggers on %s: %w", ref, err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var name, event string
		if err := rows.Scan(&name, &event); err != nil {
			return err
		}
		if strings.HasPrefix(name, "rc_trg_") {
			continue // one of ours (a prior install)
		}
		return fmt.Errorf("mysql: table %s already has an AFTER %s trigger %q; MySQL %d allows only one trigger per event, "+
			"so replicare cannot install capture — drop/merge the existing trigger or upgrade the source to 8.0+ (mysql-plan §0.5)",
			ref, strings.ToLower(event), name, serverVer)
	}
	return rows.Err()
}

// --- registry ---

func (s *Source) upsertRegistry(ctx context.Context, ref engine.TableRef, cols []captureCol) (int, error) {
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.Name
	}
	pkJSON, _ := json.Marshal(names)
	_, err := s.db.ExecContext(ctx, fmt.Sprintf(
		"INSERT INTO %s (schema_name, table_name, pk_cols) VALUES (?, ?, ?) "+
			"ON DUPLICATE KEY UPDATE pk_cols = VALUES(pk_cols)", captureRef("captured")),
		ref.Schema, ref.Name, string(pkJSON))
	if err != nil {
		return 0, fmt.Errorf("mysql: register %s: %w", ref, err)
	}
	relID, _, ok, err := s.lookupRegistry(ctx, ref)
	if err != nil || !ok {
		return 0, fmt.Errorf("mysql: register %s: lookup after upsert failed: %w", ref, err)
	}
	return relID, nil
}

func (s *Source) lookupRegistry(ctx context.Context, ref engine.TableRef) (relID int, pkCols []string, ok bool, err error) {
	var pkJSON string
	row := s.db.QueryRowContext(ctx,
		fmt.Sprintf("SELECT rel_id, pk_cols FROM %s WHERE schema_name = ? AND table_name = ?", captureRef("captured")),
		ref.Schema, ref.Name)
	if err := row.Scan(&relID, &pkJSON); err != nil {
		if err == sql.ErrNoRows {
			return 0, nil, false, nil
		}
		return 0, nil, false, err
	}
	if err := json.Unmarshal([]byte(pkJSON), &pkCols); err != nil {
		return 0, nil, false, fmt.Errorf("mysql: decode pk_cols for %s: %w", ref, err)
	}
	return relID, pkCols, true, nil
}

// introspectByRef introspects the given tables (whole-database introspection
// filtered to the requested refs), returning them keyed by ref.
func (s *Source) introspectByRef(ctx context.Context, refs []engine.TableRef) (map[engine.TableRef]engine.Table, error) {
	patterns := make([]string, len(refs))
	for i, r := range refs {
		patterns[i] = r.String()
	}
	schema, err := introspectDB(ctx, s.db, engine.Selection{Include: patterns})
	if err != nil {
		return nil, err
	}
	out := map[engine.TableRef]engine.Table{}
	for _, t := range schema.Tables {
		out[t.Ref] = t
	}
	return out, nil
}
