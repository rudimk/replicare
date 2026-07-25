package statepg

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/state"
)

// Aggregate reads for the M6 status surface. These are per-sync list queries over
// the copy-progress, cursor, and event tables — the daemon's operator view of
// where each table/target stands (phase, lag, progress, last error).

// ListCopyProgress returns every table's initial-copy progress for a sync.
func (s *Store) ListCopyProgress(ctx context.Context, sync string) ([]state.CopyProgress, error) {
	if err := s.requirePool(); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `
		SELECT schema_name, table_name, done, watermark, completed, updated_at
		FROM replicare_state.copy_progress
		WHERE sync = $1
		ORDER BY schema_name, table_name`, sync)
	if err != nil {
		return nil, fmt.Errorf("statepg: list copy progress for %s: %w", sync, err)
	}
	defer rows.Close()

	var out []state.CopyProgress
	for rows.Next() {
		var (
			p             state.CopyProgress
			watermarkJSON []byte
			completedJSON []byte
		)
		if err := rows.Scan(&p.Table.Schema, &p.Table.Name, &p.Done, &watermarkJSON, &completedJSON, &p.UpdatedAt); err != nil {
			return nil, fmt.Errorf("statepg: scan copy progress: %w", err)
		}
		if p.Watermark, err = decodeKeyValues(watermarkJSON); err != nil {
			return nil, fmt.Errorf("statepg: decode watermark: %w", err)
		}
		if p.Completed, err = decodeRanges(completedJSON); err != nil {
			return nil, fmt.Errorf("statepg: decode completed ranges: %w", err)
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("statepg: list copy progress for %s: %w", sync, err)
	}
	return out, nil
}

// ListCursors returns every per-(target, table) streaming cursor for a sync,
// including UpdatedAt (the lag/age signal).
func (s *Store) ListCursors(ctx context.Context, sync string) ([]state.Cursor, error) {
	if err := s.requirePool(); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `
		SELECT target, schema_name, table_name, phase, last_delta, needs_reseed, updated_at
		FROM replicare_state.cursors
		WHERE sync = $1
		ORDER BY target, schema_name, table_name`, sync)
	if err != nil {
		return nil, fmt.Errorf("statepg: list cursors for %s: %w", sync, err)
	}
	defer rows.Close()

	var out []state.Cursor
	for rows.Next() {
		var (
			c         state.Cursor
			target    string
			phase     string
			lastDelta int64
		)
		if err := rows.Scan(&target, &c.Table.Schema, &c.Table.Name, &phase, &lastDelta, &c.NeedsReseed, &c.UpdatedAt); err != nil {
			return nil, fmt.Errorf("statepg: scan cursor: %w", err)
		}
		c.Target = engine.TargetID(target)
		c.Phase = state.Phase(phase)
		c.LastDelta = engine.DeltaID(lastDelta)
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("statepg: list cursors for %s: %w", sync, err)
	}
	return out, nil
}

// RecentEvents returns the newest events for a sync (most recent first, capped at
// limit), for the status API's "last error" / audit view. A limit <= 0 defaults
// to a small page.
func (s *Store) RecentEvents(ctx context.Context, sync string, limit int) ([]state.Event, error) {
	if err := s.requirePool(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.pool.Query(ctx, `
		SELECT COALESCE(target,''), COALESCE(schema_name,''), COALESCE(table_name,''),
		       level, event, COALESCE(message,''), attrs, created_at
		FROM replicare_state.events
		WHERE sync = $1
		ORDER BY id DESC
		LIMIT $2`, sync, limit)
	if err != nil {
		return nil, fmt.Errorf("statepg: recent events for %s: %w", sync, err)
	}
	defer rows.Close()

	var out []state.Event
	for rows.Next() {
		var (
			e         state.Event
			attrsJSON []byte
		)
		e.Sync = sync
		if err := rows.Scan(&e.Target, &e.Table.Schema, &e.Table.Name, &e.Level, &e.Event, &e.Message, &attrsJSON, &e.CreatedAt); err != nil {
			return nil, fmt.Errorf("statepg: scan event: %w", err)
		}
		if len(attrsJSON) > 0 {
			if err := json.Unmarshal(attrsJSON, &e.Attrs); err != nil {
				return nil, fmt.Errorf("statepg: decode event attrs: %w", err)
			}
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("statepg: recent events for %s: %w", sync, err)
	}
	return out, nil
}
