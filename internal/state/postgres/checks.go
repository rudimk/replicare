package statepg

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/state"
)

var _ state.CheckStore = (*Store)(nil)

// TriggerGuardStarted implements state.CheckStore.
func (s *Store) TriggerGuardStarted(ctx context.Context, sync string, target engine.TargetID) (bool, error) {
	if err := s.requirePool(); err != nil {
		return false, err
	}
	var on bool
	if err := s.pool.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM replicare_state.trigger_guard WHERE sync = $1 AND target = $2)`,
		sync, string(target)).Scan(&on); err != nil {
		return false, fmt.Errorf("statepg: read trigger guard for %s/%s: %w", sync, target, err)
	}
	return on, nil
}

// SetTriggerGuardStarted implements state.CheckStore.
func (s *Store) SetTriggerGuardStarted(ctx context.Context, sync string, target engine.TargetID, on bool) error {
	if err := s.requirePool(); err != nil {
		return err
	}
	q := `INSERT INTO replicare_state.trigger_guard (sync, target) VALUES ($1, $2) ON CONFLICT DO NOTHING`
	if !on {
		q = `DELETE FROM replicare_state.trigger_guard WHERE sync = $1 AND target = $2`
	}
	if _, err := s.pool.Exec(ctx, q, sync, string(target)); err != nil {
		return fmt.Errorf("statepg: record trigger guard for %s/%s: %w", sync, target, err)
	}
	return nil
}

// MarkForCheck implements state.CheckStore in one transaction.
func (s *Store) MarkForCheck(ctx context.Context, sync string, target engine.TargetID, tables []engine.TableRef, reason string) error {
	if err := s.requirePool(); err != nil {
		return err
	}
	if len(tables) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, t := range tables {
		batch.Queue(`
			INSERT INTO replicare_state.pending_checks (sync, target, schema_name, table_name, reason)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (sync, target, schema_name, table_name) DO NOTHING`,
			sync, string(target), t.Schema, t.Name, reason)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("statepg: mark tables for check: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := tx.SendBatch(ctx, batch).Close(); err != nil {
		return fmt.Errorf("statepg: mark tables for check for %s/%s: %w", sync, target, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("statepg: mark tables for check: %w", err)
	}
	return nil
}

// ListChecks implements state.CheckStore.
func (s *Store) ListChecks(ctx context.Context, sync string) ([]state.PendingCheck, error) {
	if err := s.requirePool(); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `
		SELECT target, schema_name, table_name, reason, created_at
		FROM replicare_state.pending_checks
		WHERE sync = $1
		ORDER BY target, schema_name, table_name`, sync)
	if err != nil {
		return nil, fmt.Errorf("statepg: list checks for %s: %w", sync, err)
	}
	defer rows.Close()
	var out []state.PendingCheck
	for rows.Next() {
		var (
			pc     state.PendingCheck
			target string
		)
		if err := rows.Scan(&target, &pc.Table.Schema, &pc.Table.Name, &pc.Reason, &pc.CreatedAt); err != nil {
			return nil, fmt.Errorf("statepg: scan check: %w", err)
		}
		pc.Target = engine.TargetID(target)
		out = append(out, pc)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("statepg: list checks for %s: %w", sync, err)
	}
	return out, nil
}

// ClearCheck implements state.CheckStore.
func (s *Store) ClearCheck(ctx context.Context, sync string, target engine.TargetID, t engine.TableRef) error {
	if err := s.requirePool(); err != nil {
		return err
	}
	if _, err := s.pool.Exec(ctx, `
		DELETE FROM replicare_state.pending_checks
		WHERE sync = $1 AND target = $2 AND schema_name = $3 AND table_name = $4`,
		sync, string(target), t.Schema, t.Name); err != nil {
		return fmt.Errorf("statepg: clear check of %s: %w", t, err)
	}
	return nil
}
