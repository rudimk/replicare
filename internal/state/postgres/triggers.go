package statepg

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/state"
)

var _ state.TriggerStore = (*Store)(nil)

// SaveTriggers implements state.TriggerStore in one transaction.
func (s *Store) SaveTriggers(ctx context.Context, sync string, target engine.TargetID, ts []engine.TargetTrigger) error {
	if err := s.requirePool(); err != nil {
		return err
	}
	if len(ts) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, t := range ts {
		batch.Queue(`
			INSERT INTO replicare_state.disabled_triggers
				(sync, target, schema_name, table_name, trigger_name, restore_mode, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, now())
			ON CONFLICT (sync, target, schema_name, table_name, trigger_name) DO NOTHING`,
			sync, string(target), t.Table.Schema, t.Table.Name, t.Name, t.Mode)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("statepg: save triggers: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := tx.SendBatch(ctx, batch).Close(); err != nil {
		return fmt.Errorf("statepg: save triggers for %s/%s: %w", sync, target, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("statepg: save triggers: %w", err)
	}
	return nil
}

// ListTriggers implements state.TriggerStore.
func (s *Store) ListTriggers(ctx context.Context, sync string) ([]state.SavedTrigger, error) {
	if err := s.requirePool(); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `
		SELECT target, schema_name, table_name, trigger_name, restore_mode, updated_at
		FROM replicare_state.disabled_triggers
		WHERE sync = $1
		ORDER BY target, schema_name, table_name, trigger_name`, sync)
	if err != nil {
		return nil, fmt.Errorf("statepg: list triggers for %s: %w", sync, err)
	}
	defer rows.Close()
	var out []state.SavedTrigger
	for rows.Next() {
		var (
			st     state.SavedTrigger
			target string
		)
		if err := rows.Scan(&target, &st.Trigger.Table.Schema, &st.Trigger.Table.Name, &st.Trigger.Name,
			&st.Trigger.Mode, &st.UpdatedAt); err != nil {
			return nil, fmt.Errorf("statepg: scan trigger: %w", err)
		}
		st.Target = engine.TargetID(target)
		out = append(out, st)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("statepg: list triggers for %s: %w", sync, err)
	}
	return out, nil
}

// ForgetTrigger implements state.TriggerStore.
func (s *Store) ForgetTrigger(ctx context.Context, sync string, target engine.TargetID, t engine.TableRef, name string) error {
	if err := s.requirePool(); err != nil {
		return err
	}
	if _, err := s.pool.Exec(ctx, `
		DELETE FROM replicare_state.disabled_triggers
		WHERE sync = $1 AND target = $2 AND schema_name = $3 AND table_name = $4 AND trigger_name = $5`,
		sync, string(target), t.Schema, t.Name, name); err != nil {
		return fmt.Errorf("statepg: forget trigger %s: %w", name, err)
	}
	return nil
}
