package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

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
	return s.installCapture(ctx, tables, false, "")
}

// InstallOriginCapture implements engine.OriginAwareCapturer: it installs capture
// with the loop-suppression WHEN guard on the trigger, so replicare's own applies
// (which set the applyMarkerGUC) are not re-captured on a cluster member (CLAUDE.md
// §6, docs/multi-master.md §5.4). Everything else is identical to InstallCapture;
// the only difference is the guarded trigger, so a table's delta/track/function DDL
// is unchanged and the one-way path (InstallCapture) is byte-identical.
func (s *Source) InstallOriginCapture(ctx context.Context, tables []engine.TableRef, nodeID string) error {
	return s.installCapture(ctx, tables, true, nodeID)
}

var _ engine.OriginAwareCapturer = (*Source)(nil)

func (s *Source) installCapture(ctx context.Context, tables []engine.TableRef, origin bool, nodeID string) error {
	if err := s.requireConn(); err != nil {
		return err
	}
	if err := s.ensureCaptureSchema(ctx); err != nil {
		return err
	}
	// Cluster members additionally get the mesh version register + HLC (CLAUDE.md §6);
	// a one-way source gets none of it, so its schema is unchanged.
	if origin {
		if err := ensureMeshState(ctx, s.conn, nodeID); err != nil {
			return err
		}
	}
	introspected, err := s.introspectTables(ctx, tables)
	if err != nil {
		return err
	}
	version, err := serverVersion(ctx, s.conn)
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
		if err := s.installOneRetrying(ctx, ref, cols, origin, version); err != nil {
			return fmt.Errorf("postgres: install capture on %s: %w", ref, err)
		}
	}
	return nil
}

// Capture install touches the USER's table (the trigger), and a source we may not own
// is busy: a VACUUM, a migration, or an app session left "idle in transaction" can hold a
// lock on it for minutes. Waiting for such a lock is dangerous, not just slow: while
// replicare's lock request waits, Postgres queues every NEW query on that table behind it,
// so the application's reads and writes on the table stall too (a production incident:
// DROP TRIGGER on "Message" waited on a 34-minute manual VACUUM and froze every Message
// query). So every install transaction runs with a short lock wait; on timeout it gives
// up, backs off, and retries, and after captureLockAttempts it fails loud instead of
// ever parking a lock request in front of the application.
const captureLockWait = 2 * time.Second

var (
	captureLockAttempts = 15
	captureLockBackoff  = func(attempt int) time.Duration {
		d := time.Duration(1<<attempt) * time.Second
		if d > 30*time.Second {
			d = 30 * time.Second
		}
		return d
	}
)

// lockWaitSQL bounds how long any statement in the install transaction may wait for a
// lock. lock_timeout exists from 9.3; older servers fall back to statement_timeout, which
// also bounds the lock wait (the install statements themselves are instant).
func lockWaitSQL(version int) string {
	ms := captureLockWait.Milliseconds()
	if version >= 90300 {
		return fmt.Sprintf("SET LOCAL lock_timeout = '%dms'", ms)
	}
	return fmt.Sprintf("SET LOCAL statement_timeout = '%dms'", ms)
}

// isLockWaitTimeout reports a statement given up by lockWaitSQL: lock_not_available
// (lock_timeout) or query_canceled (the statement_timeout fallback).
func isLockWaitTimeout(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && (pgErr.Code == "55P03" || pgErr.Code == "57014")
}

// installOneRetrying runs installOne, retrying with backoff when a lock on the table is
// not available within captureLockWait. Between attempts replicare holds and requests no
// lock, so the application is never queued behind it for more than captureLockWait.
func (s *Source) installOneRetrying(ctx context.Context, ref engine.TableRef, cols []captureCol, origin bool, version int) error {
	var err error
	for attempt := 0; attempt < captureLockAttempts; attempt++ {
		err = s.installOne(ctx, ref, cols, origin, version)
		if err == nil || !isLockWaitTimeout(err) {
			return err
		}
		if attempt == captureLockAttempts-1 {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(captureLockBackoff(attempt)):
		}
	}
	return fmt.Errorf("could not get a lock on %s within %s on any of %d attempts; another session is holding it "+
		"(for example a VACUUM, a migration, or an application session left idle in transaction). replicare gave up "+
		"rather than queue the application's queries on that table behind its own lock request; it retries on restart: %w",
		ref, captureLockWait, captureLockAttempts, err)
}

// installOne installs capture for a single table within one transaction, so a
// partial failure leaves no half-built capture objects. origin selects the
// loop-suppression trigger variant (cluster member) vs the byte-identical one-way
// trigger. Every statement's lock wait is bounded (lockWaitSQL), and the trigger on the
// user's table is only dropped and re-created when it is missing or not the one
// expected (captureTriggerCurrent) — so a restart of an already-installed source takes
// no lock on any user table at all.
func (s *Source) installOne(ctx context.Context, ref engine.TableRef, cols []captureCol, origin bool, version int) error {
	tx, err := s.conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, lockWaitSQL(version)); err != nil {
		return fmt.Errorf("bound lock wait: %w", err)
	}

	relID, err := upsertRegistry(ctx, tx, ref, cols)
	if err != nil {
		return err
	}

	// The trigger function differs by mode: a cluster member additionally stamps the
	// version register (and needs a per-table register table), while a one-way source
	// keeps the byte-identical PK-only function. Everything else is shared.
	triggerFn := triggerFunctionDDL(relID, cols)
	stmts := []string{
		deltaTableDDL(relID, cols),
		trackTableDDL(relID),
		deltaAutovacuumDDL(relID),
	}
	if origin {
		triggerFn = meshTriggerFunctionDDL(relID, ref, cols)
		stmts = append(stmts, registerTableDDL(ref, cols))
	}
	// CREATE OR REPLACE FUNCTION keeps the function's identity, so an existing trigger
	// picks up the new body without being touched.
	stmts = append(stmts, triggerFn)
	for _, stmt := range stmts {
		if _, err := tx.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("exec: %w\n---\n%s", err, stmt)
		}
	}

	current, err := captureTriggerCurrent(ctx, tx, ref, relID, origin)
	if err != nil {
		return err
	}
	if !current {
		for _, stmt := range []string{
			dropTriggerDDL(relID, qualifyTable(ref)),
			createTriggerDDL(relID, qualifyTable(ref), origin),
		} {
			if _, err := tx.Exec(ctx, stmt); err != nil {
				return fmt.Errorf("exec: %w\n---\n%s", err, stmt)
			}
		}
	}
	return tx.Commit(ctx)
}

// captureTriggerTypeAfterRowIUD is pg_trigger.tgtype for AFTER INSERT OR UPDATE OR
// DELETE ... FOR EACH ROW: ROW(1) | INSERT(4) | DELETE(8) | UPDATE(16), no BEFORE bit.
const captureTriggerTypeAfterRowIUD = 29

// captureTriggerCurrent reports whether the table already carries exactly the capture
// trigger createTriggerDDL would create: our name, our function, AFTER ROW on all three
// events, enabled, no column list, and the loop-suppression WHEN guard present iff
// origin. Reading pg_trigger takes no lock on the user's table.
func captureTriggerCurrent(ctx context.Context, tx pgx.Tx, ref engine.TableRef, relID int, origin bool) (bool, error) {
	var (
		fnMatch  bool
		tgType   int
		enabled  string
		hasGuard bool
		attrs    string
	)
	err := tx.QueryRow(ctx, `
		SELECT t.tgfoid = $2::regprocedure, t.tgtype::int, t.tgenabled::text,
		       t.tgqual IS NOT NULL, t.tgattr::text
		FROM pg_trigger t
		WHERE t.tgrelid = $1::regclass AND t.tgname = $3`,
		qualifyTable(ref), qualifiedCapture(triggerFnName(relID))+"()", triggerName(relID),
	).Scan(&fnMatch, &tgType, &enabled, &hasGuard, &attrs)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("inspect capture trigger: %w", err)
	}
	return fnMatch && tgType == captureTriggerTypeAfterRowIUD && enabled == "O" && hasGuard == origin && attrs == "", nil
}

// RemoveCapture tears down capture for the given tables, leaving the source
// clean: it drops the trigger, trigger function, delta and track tables, and the
// registry row. The replicare schema framework (schema_version, registry,
// sequence) remains for other captured tables. Unregistered tables are ignored.
func (s *Source) RemoveCapture(ctx context.Context, tables []engine.TableRef) error {
	if err := s.requireConn(); err != nil {
		return err
	}
	version, err := serverVersion(ctx, s.conn)
	if err != nil {
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
		if err := s.removeOne(ctx, ref, relID, version); err != nil {
			return fmt.Errorf("postgres: remove capture on %s: %w", ref, err)
		}
	}
	return nil
}

func (s *Source) removeOne(ctx context.Context, ref engine.TableRef, relID int, version int) error {
	tx, err := s.conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Same rule as install: never wait on the user's table long enough to queue the
	// application behind us. A busy table fails loud; re-run once it is free.
	if _, err := tx.Exec(ctx, lockWaitSQL(version)); err != nil {
		return fmt.Errorf("bound lock wait: %w", err)
	}

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
