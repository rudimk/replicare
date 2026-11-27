package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/rudimk/replicare/internal/engine"
)

// *Sink keeps the target's own triggers from firing on replicare's writes (CLAUDE.md
// §4.2 "the target's own triggers").
var _ engine.TriggerGuard = (*Sink)(nil)

// targetTriggersSQL lists the triggers on the given tables that are not part of a
// constraint (a foreign key's own triggers are internal and never listed). owned says
// whether this role may ALTER the table to switch one off or on.
const targetTriggersSQL = `
SELECT n.nspname, c.relname, t.tgname, t.tgenabled::text, pg_has_role(c.relowner, 'USAGE')
FROM pg_trigger t
JOIN pg_class c     ON c.oid = t.tgrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN unnest($1::text[], $2::text[]) AS sel(s, r) ON sel.s = n.nspname AND sel.r = c.relname
WHERE NOT t.tgisinternal
ORDER BY n.nspname, c.relname, t.tgname`

// replicareTriggerPrefix is the name prefix of every trigger replicare installs.
const replicareTriggerPrefix = "replicare_"

// TargetTriggers implements engine.TriggerGuard.
func (s *Sink) TargetTriggers(ctx context.Context, tables []engine.TableRef) ([]engine.TargetTrigger, error) {
	if s.conn == nil {
		return nil, errNotConnected("sink")
	}
	if len(tables) == 0 {
		return nil, nil
	}
	schemas := make([]string, len(tables))
	names := make([]string, len(tables))
	for i, t := range tables {
		schemas[i], names[i] = t.Schema, t.Name
	}
	rows, err := s.conn.Query(ctx, targetTriggersSQL, schemas, names)
	if err != nil {
		return nil, fmt.Errorf("postgres: list target triggers: %w", err)
	}
	defer rows.Close()
	var out []engine.TargetTrigger
	for rows.Next() {
		var t engine.TargetTrigger
		if err := rows.Scan(&t.Table.Schema, &t.Table.Name, &t.Name, &t.Mode, &t.Owned); err != nil {
			return nil, fmt.Errorf("postgres: scan target trigger: %w", err)
		}
		t.Replicare = len(t.Name) >= len(replicareTriggerPrefix) && t.Name[:len(replicareTriggerPrefix)] == replicareTriggerPrefix
		out = append(out, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: list target triggers: %w", err)
	}
	return out, nil
}

// ReplicaModeAllowed implements engine.TriggerGuard. Setting session_replication_role
// needs superuser, a managed service's admin role (RDS rds_superuser), or on 15+ a
// GRANT SET ON PARAMETER; the check sets it inside a transaction it rolls back.
func (s *Sink) ReplicaModeAllowed(ctx context.Context) (bool, error) {
	if s.conn == nil {
		return false, errNotConnected("sink")
	}
	tx, err := s.conn.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("postgres: check replica mode: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, "SET LOCAL session_replication_role = replica"); err != nil {
		if isInsufficientPrivilege(err) {
			return false, nil
		}
		return false, fmt.Errorf("postgres: check replica mode: %w", err)
	}
	return true, nil
}

// UseReplicaMode implements engine.TriggerGuard. The setting is session-wide, so it
// covers every statement this Sink runs (copy, apply, reseed), and Connect applies it
// again after a reconnect.
func (s *Sink) UseReplicaMode(ctx context.Context, on bool) error {
	s.replicaMode = on
	if s.conn == nil {
		return nil
	}
	return applyReplicaMode(ctx, s.conn, on)
}

func applyReplicaMode(ctx context.Context, conn *pgx.Conn, on bool) error {
	role := "origin"
	if on {
		role = "replica"
	}
	if _, err := conn.Exec(ctx, "SET session_replication_role = "+role); err != nil {
		return fmt.Errorf("postgres: set session_replication_role = %s: %w", role, err)
	}
	return nil
}

// DisableTrigger implements engine.TriggerGuard.
func (s *Sink) DisableTrigger(ctx context.Context, t engine.TargetTrigger) error {
	stmt := fmt.Sprintf("ALTER TABLE %s DISABLE TRIGGER %s", qualifyTable(t.Table), quoteIdentifier(t.Name))
	return s.lockedDDL(ctx, stmt, fmt.Sprintf("switch off trigger %s", t.Name), t.Table)
}

// RestoreStatement implements engine.TriggerGuard.
func (s *Sink) RestoreStatement(t engine.TargetTrigger) string { return triggerRestoreStatement(t) }

// triggerRestoreStatement is the statement that switches a trigger back on in t.Mode.
func triggerRestoreStatement(t engine.TargetTrigger) string {
	how := "ENABLE"
	switch t.Mode {
	case "A":
		how = "ENABLE ALWAYS"
	case "R":
		how = "ENABLE REPLICA"
	case "D":
		how = "DISABLE"
	}
	return fmt.Sprintf("ALTER TABLE %s %s TRIGGER %s", qualifyTable(t.Table), how, quoteIdentifier(t.Name))
}

// RestoreTrigger implements engine.TriggerGuard.
func (s *Sink) RestoreTrigger(ctx context.Context, t engine.TargetTrigger) (bool, error) {
	if s.conn == nil {
		return false, errNotConnected("sink")
	}
	var exists bool
	err := s.conn.QueryRow(ctx, `
SELECT true FROM pg_trigger t
JOIN pg_class c     ON c.oid = t.tgrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = $1 AND c.relname = $2 AND t.tgname = $3`, t.Table.Schema, t.Table.Name, t.Name).Scan(&exists)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("postgres: look up trigger %s on %s: %w", t.Name, t.Table, err)
	}
	return true, s.lockedDDL(ctx, triggerRestoreStatement(t), fmt.Sprintf("switch trigger %s back on", t.Name), t.Table)
}

// lockedDDL runs one statement that needs a brief lock on a user table. Like installing
// capture, it waits at most captureLockWait for the lock, backs off and retries, and
// after captureLockAttempts fails rather than ever queueing the application's queries
// behind a waiting lock request.
func (s *Sink) lockedDDL(ctx context.Context, stmt, what string, table engine.TableRef) error {
	if s.conn == nil {
		return errNotConnected("sink")
	}
	var err error
	for attempt := 0; attempt < captureLockAttempts; attempt++ {
		err = s.dropIndexOnce(ctx, stmt)
		if err == nil || !isLockWaitTimeout(err) {
			break
		}
		if attempt == captureLockAttempts-1 {
			return fmt.Errorf("postgres: %s: could not get a lock on %s within %s on any of %d attempts; "+
				"another session is holding it: %w", what, table, captureLockWait, captureLockAttempts, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(captureLockBackoff(attempt)):
		}
	}
	if err != nil {
		return fmt.Errorf("postgres: %s on %s: %w", what, table, withPgDetail(err))
	}
	return nil
}
