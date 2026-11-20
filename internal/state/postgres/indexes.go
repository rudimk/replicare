package statepg

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/state"
)

var _ state.IndexStore = (*Store)(nil)

// SaveIndexes implements state.IndexStore in one transaction.
func (s *Store) SaveIndexes(ctx context.Context, sync string, target engine.TargetID, idx []engine.DeferredIndex) error {
	if err := s.requirePool(); err != nil {
		return err
	}
	if len(idx) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, d := range idx {
		after := d.After
		if after == nil {
			after = []string{}
		}
		batch.Queue(`
			INSERT INTO replicare_state.deferred_indexes
				(sync, target, schema_name, table_name, index_name, definition, tablespace, after_sql, link, built, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, false, now())
			ON CONFLICT (sync, target, schema_name, index_name) DO UPDATE SET
				table_name = EXCLUDED.table_name,
				definition = EXCLUDED.definition,
				tablespace = EXCLUDED.tablespace,
				after_sql  = EXCLUDED.after_sql,
				link       = EXCLUDED.link,
				built      = false,
				updated_at = now()`,
			sync, string(target), d.Table.Schema, d.Table.Name, d.Name, d.Definition, d.Tablespace, after, d.Link)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("statepg: save indexes: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := tx.SendBatch(ctx, batch).Close(); err != nil {
		return fmt.Errorf("statepg: save indexes for %s/%s: %w", sync, target, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("statepg: save indexes: %w", err)
	}
	return nil
}

// ListIndexes implements state.IndexStore.
func (s *Store) ListIndexes(ctx context.Context, sync string) ([]state.SavedIndex, error) {
	if err := s.requirePool(); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `
		SELECT target, schema_name, table_name, index_name, definition, tablespace, after_sql, link, built, updated_at
		FROM replicare_state.deferred_indexes
		WHERE sync = $1
		ORDER BY target, link DESC, schema_name, table_name, index_name`, sync)
	if err != nil {
		return nil, fmt.Errorf("statepg: list indexes for %s: %w", sync, err)
	}
	defer rows.Close()
	var out []state.SavedIndex
	for rows.Next() {
		var (
			si     state.SavedIndex
			target string
		)
		if err := rows.Scan(&target, &si.Index.Table.Schema, &si.Index.Table.Name, &si.Index.Name,
			&si.Index.Definition, &si.Index.Tablespace, &si.Index.After, &si.Index.Link, &si.Built, &si.UpdatedAt); err != nil {
			return nil, fmt.Errorf("statepg: scan index: %w", err)
		}
		si.Target = engine.TargetID(target)
		out = append(out, si)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("statepg: list indexes for %s: %w", sync, err)
	}
	return out, nil
}

// MarkIndexBuilt implements state.IndexStore.
func (s *Store) MarkIndexBuilt(ctx context.Context, sync string, target engine.TargetID, t engine.TableRef, name string) error {
	if err := s.requirePool(); err != nil {
		return err
	}
	if _, err := s.pool.Exec(ctx, `
		UPDATE replicare_state.deferred_indexes SET built = true, updated_at = now()
		WHERE sync = $1 AND target = $2 AND schema_name = $3 AND index_name = $4`,
		sync, string(target), t.Schema, name); err != nil {
		return fmt.Errorf("statepg: mark index %s built: %w", name, err)
	}
	return nil
}
