package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	driver "github.com/go-sql-driver/mysql"

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
	return s.installCapture(ctx, tables, false, "")
}

// InstallOriginCapture implements engine.OriginAwareCapturer: it installs cluster
// (multi-master) capture — the trigger bodies are wrapped in `IF @replicare_apply IS
// NULL` (loop suppression) and stamp the HLC version register (CLAUDE.md §6, §5.3).
// One-way InstallCapture installs the byte-identical unguarded triggers and no
// register, so the one-way path is unchanged.
func (s *Source) InstallOriginCapture(ctx context.Context, tables []engine.TableRef, nodeID string) error {
	return s.installCapture(ctx, tables, true, nodeID)
}

var _ engine.OriginAwareCapturer = (*Source)(nil)

func (s *Source) installCapture(ctx context.Context, tables []engine.TableRef, origin bool, nodeID string) error {
	if s.db == nil {
		return errNotConnected
	}
	if err := s.ensureCaptureSchema(ctx); err != nil {
		return err
	}
	// Cluster members additionally get the mesh HLC state (CLAUDE.md §6); a one-way
	// source gets none of it, so its schema is unchanged.
	if origin {
		if err := ensureMeshState(ctx, s.db, nodeID); err != nil {
			return err
		}
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
		if err := s.installOneRetrying(ctx, ref, cols, origin); err != nil {
			return fmt.Errorf("mysql: install capture on %s: %w", ref, err)
		}
	}
	return nil
}

// Trigger DDL takes an exclusive metadata lock on the USER's table. If an application
// transaction that touched the table is still open (or "idle in transaction"), that lock
// request waits — and while it waits MySQL queues every NEW query on the table behind it,
// stalling the application (the same incident class seen on Postgres, §3.1). MySQL's
// default lock_wait_timeout is a YEAR. So trigger DDL runs on a pinned connection with a
// short lock wait, backs off and retries, and finally fails loud; and a restart leaves an
// already-correct trigger untouched, so it needs no lock on the user's table at all.
const captureLockWaitSeconds = 2

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

// isLockWaitTimeout reports MySQL ER_LOCK_WAIT_TIMEOUT (1205): lock_wait_timeout elapsed.
func isLockWaitTimeout(err error) bool {
	var myErr *driver.MySQLError
	return errors.As(err, &myErr) && myErr.Number == 1205
}

// installOneRetrying runs installOne, retrying with backoff when the table's metadata
// lock is not available within captureLockWaitSeconds; between attempts replicare holds
// and requests no lock, so the application is never queued behind it for longer.
func (s *Source) installOneRetrying(ctx context.Context, ref engine.TableRef, cols []captureCol, origin bool) error {
	var err error
	for attempt := 0; attempt < captureLockAttempts; attempt++ {
		err = s.installOne(ctx, ref, cols, origin)
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
	return fmt.Errorf("could not get a lock on %s within %ds on any of %d attempts; another session is holding it "+
		"(for example an open or idle transaction that touched the table, or a running ALTER). replicare gave up "+
		"rather than queue the application's queries on that table behind its own lock request; it retries on restart: %w",
		ref, captureLockWaitSeconds, captureLockAttempts, err)
}

// installOne installs capture for one table. Each statement is individually
// idempotent (MySQL auto-commits DDL, so there is no wrapping transaction). origin
// selects the loop-suppressing register-stamping trigger variant (cluster member) and
// creates the per-table version register. The capture tables live in our own database;
// only the triggers touch the user's table (see installTriggers).
func (s *Source) installOne(ctx context.Context, ref engine.TableRef, cols []captureCol, origin bool) error {
	relID, err := s.upsertRegistry(ctx, ref, cols)
	if err != nil {
		return err
	}
	stmts := []string{
		deltaTableDDL(relID, cols),
		trackTableDDL(relID),
	}
	if origin {
		stmts = append(stmts, registerTableDDL(ref, cols))
	}
	for _, stmt := range stmts {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("exec: %w\n---\n%s", err, stmt)
		}
	}
	return s.installTriggers(ctx, ref, relID, cols, origin)
}

// installTriggers (re)creates the three capture triggers on a pinned connection with a
// bounded lock wait, skipping any trigger that already matches exactly.
//
// MySQL cannot replace a trigger atomically: DROP and CREATE each auto-commit, so between
// them that event is not captured. Skipping current triggers means that window only
// opens when a trigger really changed (an upgrade), never on a plain restart.
func (s *Source) installTriggers(ctx context.Context, ref engine.TableRef, relID int, cols []captureCol, origin bool) (err error) {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET SESSION lock_wait_timeout = %d", captureLockWaitSeconds)); err != nil {
		return fmt.Errorf("bound lock wait: %w", err)
	}
	// The pinned connection goes back to the pool afterwards: restore the server default.
	defer func() {
		if _, rerr := conn.ExecContext(context.Background(), "SET SESSION lock_wait_timeout = DEFAULT"); rerr != nil && err == nil {
			err = fmt.Errorf("restore lock_wait_timeout: %w", rerr)
		}
	}()

	for _, op := range []byte{'I', 'U', 'D'} {
		ddl := triggerDDL(relID, ref.Schema, ref.Name, op, cols)
		if origin {
			ddl = meshTriggerDDL(relID, ref.Schema, ref.Name, op, cols)
		}
		current, err := triggerCurrent(ctx, conn, ref, relID, op, ddl)
		if err != nil {
			return err
		}
		if current {
			continue
		}
		for _, stmt := range []string{dropTriggerDDL(relID, ref.Schema, op), ddl} {
			if _, err := conn.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("exec: %w\n---\n%s", err, stmt)
			}
		}
	}
	return nil
}

// triggerCurrent reports whether the named capture trigger already exists on the table
// with exactly the timing, event and body that ddl would create. MySQL stores the body
// verbatim (information_schema.TRIGGERS.ACTION_STATEMENT), so comparing it detects any
// change to the trigger, including the one-way vs cluster variant.
func triggerCurrent(ctx context.Context, conn *sql.Conn, ref engine.TableRef, relID int, op byte, ddl string) (bool, error) {
	_, wantBody, ok := strings.Cut(ddl, " FOR EACH ROW\n")
	if !ok {
		return false, nil // unexpected shape: never skip, always (re)create
	}
	var table, timing, event, body string
	err := conn.QueryRowContext(ctx, `
		SELECT EVENT_OBJECT_TABLE, ACTION_TIMING, EVENT_MANIPULATION, ACTION_STATEMENT
		FROM information_schema.TRIGGERS
		WHERE TRIGGER_SCHEMA = ? AND TRIGGER_NAME = ?`,
		ref.Schema, triggerName(relID, op)).Scan(&table, &timing, &event, &body)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("inspect capture trigger: %w", err)
	}
	wantEvent := map[byte]string{'I': "INSERT", 'U': "UPDATE", 'D': "DELETE"}[op]
	return table == ref.Name && timing == "AFTER" && event == wantEvent && body == wantBody, nil
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
	// Look up every registry row BEFORE pinning a connection: the source pool holds a
	// single connection, so a lookup through s.db while it is pinned would deadlock.
	type captured struct {
		ref   engine.TableRef
		relID int
	}
	var todo []captured
	for _, ref := range tables {
		relID, _, ok, err := s.lookupRegistry(ctx, ref)
		if err != nil {
			return fmt.Errorf("mysql: remove capture on %s: %w", ref, err)
		}
		if ok {
			todo = append(todo, captured{ref, relID})
		}
	}
	if len(todo) == 0 {
		return nil
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	// Same rule as install: never wait on the user's table long enough to queue the
	// application behind us. A busy table fails loud; re-run once it is free.
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET SESSION lock_wait_timeout = %d", captureLockWaitSeconds)); err != nil {
		return fmt.Errorf("mysql: remove capture: bound lock wait: %w", err)
	}
	defer func() { _, _ = conn.ExecContext(context.Background(), "SET SESSION lock_wait_timeout = DEFAULT") }()
	for _, c := range todo {
		ref, relID := c.ref, c.relID
		stmts := []string{
			dropTriggerDDL(relID, ref.Schema, 'I'),
			dropTriggerDDL(relID, ref.Schema, 'U'),
			dropTriggerDDL(relID, ref.Schema, 'D'),
			fmt.Sprintf("DROP TABLE IF EXISTS %s", captureRef(deltaTableName(relID))),
			fmt.Sprintf("DROP TABLE IF EXISTS %s", captureRef(trackTableName(relID))),
			fmt.Sprintf("DELETE FROM %s WHERE rel_id = %d", captureRef("captured"), relID),
		}
		for _, stmt := range stmts {
			if _, err := conn.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("mysql: remove capture on %s: exec: %w\n---\n%s", ref, err, stmt)
			}
		}
	}
	return nil
}

// multiTriggerMinVersion is the first MySQL release that allows more than one trigger
// per (timing, event) on a table (5.7.2). Below it — in practice 5.6, best-effort
// only — a table's existing AFTER trigger occupies the slot replicare needs.
const multiTriggerMinVersion = 50702

// blockConflictingTrigger enforces the pre-existing-trigger rule (mysql-plan §0.5)
// where it actually applies: before MySQL 5.7.2 a table can hold only one trigger per
// (timing, event), so a non-replicare AFTER trigger on a selected table blocks capture
// install with an actionable error. From 5.7.2 on (every GA 5.7 and all 8.x) several
// triggers per event are allowed, so replicare's triggers simply sit alongside the
// application's own and both fire. (The original plan assumed the limit lasted until
// 8.0; that was wrong and needlessly refused tables with their own triggers on 5.7.)
func (s *Source) blockConflictingTrigger(ctx context.Context, ref engine.TableRef, serverVer int) error {
	if serverVer >= multiTriggerMinVersion {
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
			"so replicare cannot install capture — drop/merge the existing trigger or upgrade the source to 5.7.2+ (mysql-plan §0.5)",
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
