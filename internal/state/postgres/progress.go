package statepg

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/state"
)

// Initial-copy progress and streaming cursors. Each write is a single upsert, so
// a checkpoint is crash-atomic and a restart resumes from exactly the last
// committed state (CLAUDE.md §4.1, §9). Absence of a row means "no progress yet"
// (fresh start), never an error.
//
// Key values (watermark/range bounds) are stored as JSON and decoded with
// json.Number so integer keys round-trip without float precision loss. The final
// faithful key encoding is settled when the copy planner produces them (M4); this
// preserves exactness in the meantime.

// SaveCopyProgress upserts a table's initial-copy progress within a sync.
func (s *Store) SaveCopyProgress(ctx context.Context, sync string, p state.CopyProgress) error {
	if err := s.requirePool(); err != nil {
		return err
	}
	watermark, err := json.Marshal(p.Watermark)
	if err != nil {
		return fmt.Errorf("statepg: encode watermark: %w", err)
	}
	completed, err := json.Marshal(p.Completed)
	if err != nil {
		return fmt.Errorf("statepg: encode completed ranges: %w", err)
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO replicare_state.copy_progress
			(sync, schema_name, table_name, done, watermark, completed, updated_at)
		VALUES ($1, $2, $3, $4, $5::jsonb, $6::jsonb, now())
		ON CONFLICT (sync, schema_name, table_name) DO UPDATE SET
			done       = EXCLUDED.done,
			watermark  = EXCLUDED.watermark,
			completed  = EXCLUDED.completed,
			updated_at = now()`,
		sync, p.Table.Schema, p.Table.Name, p.Done, string(watermark), string(completed))
	if err != nil {
		return fmt.Errorf("statepg: save copy progress for %s/%s: %w", sync, p.Table, err)
	}
	return nil
}

// LoadCopyProgress returns a table's stored progress, or a fresh (not-done,
// no-watermark) value if none is recorded yet.
func (s *Store) LoadCopyProgress(ctx context.Context, sync string, t engine.TableRef) (state.CopyProgress, error) {
	if err := s.requirePool(); err != nil {
		return state.CopyProgress{}, err
	}
	var (
		done          bool
		watermarkJSON []byte
		completedJSON []byte
	)
	err := s.pool.QueryRow(ctx, `
		SELECT done, watermark, completed
		FROM replicare_state.copy_progress
		WHERE sync = $1 AND schema_name = $2 AND table_name = $3`,
		sync, t.Schema, t.Name).Scan(&done, &watermarkJSON, &completedJSON)
	if errors.Is(err, pgx.ErrNoRows) {
		return state.CopyProgress{Table: t}, nil
	}
	if err != nil {
		return state.CopyProgress{}, fmt.Errorf("statepg: load copy progress for %s/%s: %w", sync, t, err)
	}

	p := state.CopyProgress{Table: t, Done: done}
	if p.Watermark, err = decodeKeyValues(watermarkJSON); err != nil {
		return state.CopyProgress{}, fmt.Errorf("statepg: decode watermark: %w", err)
	}
	if p.Completed, err = decodeRanges(completedJSON); err != nil {
		return state.CopyProgress{}, fmt.Errorf("statepg: decode completed ranges: %w", err)
	}
	return p, nil
}

// SaveCursor upserts a per-(target, table) streaming cursor within a sync.
func (s *Store) SaveCursor(ctx context.Context, sync string, c state.Cursor) error {
	if err := s.requirePool(); err != nil {
		return err
	}
	phase := c.Phase
	if phase == "" {
		phase = state.PhaseInitialCopy
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO replicare_state.cursors
			(sync, target, schema_name, table_name, phase, last_delta, needs_reseed, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, now())
		ON CONFLICT (sync, target, schema_name, table_name) DO UPDATE SET
			phase        = EXCLUDED.phase,
			last_delta   = EXCLUDED.last_delta,
			needs_reseed = EXCLUDED.needs_reseed,
			updated_at   = now()`,
		sync, string(c.Target), c.Table.Schema, c.Table.Name, string(phase), int64(c.LastDelta), c.NeedsReseed)
	if err != nil {
		return fmt.Errorf("statepg: save cursor for %s/%s/%s: %w", sync, c.Target, c.Table, err)
	}
	return nil
}

// LoadCursor returns a per-(target, table) cursor, or a fresh initial-copy cursor
// if none is recorded yet.
func (s *Store) LoadCursor(ctx context.Context, sync string, target engine.TargetID, t engine.TableRef) (state.Cursor, error) {
	if err := s.requirePool(); err != nil {
		return state.Cursor{}, err
	}
	var (
		phase       string
		lastDelta   int64
		needsReseed bool
	)
	err := s.pool.QueryRow(ctx, `
		SELECT phase, last_delta, needs_reseed
		FROM replicare_state.cursors
		WHERE sync = $1 AND target = $2 AND schema_name = $3 AND table_name = $4`,
		sync, string(target), t.Schema, t.Name).Scan(&phase, &lastDelta, &needsReseed)
	if errors.Is(err, pgx.ErrNoRows) {
		return state.Cursor{Target: target, Table: t, Phase: state.PhaseInitialCopy}, nil
	}
	if err != nil {
		return state.Cursor{}, fmt.Errorf("statepg: load cursor for %s/%s/%s: %w", sync, target, t, err)
	}
	return state.Cursor{
		Target:      target,
		Table:       t,
		Phase:       state.Phase(phase),
		LastDelta:   engine.DeltaID(lastDelta),
		NeedsReseed: needsReseed,
	}, nil
}

// decodeKeyValues decodes a JSON key tuple, using json.Number so integer keys do
// not lose precision to float64.
func decodeKeyValues(b []byte) (engine.KeyValues, error) {
	if len(b) == 0 {
		return nil, nil
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var kv engine.KeyValues
	if err := dec.Decode(&kv); err != nil {
		return nil, err
	}
	return kv, nil
}

// decodeRanges decodes the JSON array of completed copy ranges.
func decodeRanges(b []byte) ([]state.CompletedRange, error) {
	if len(b) == 0 {
		return nil, nil
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var rs []state.CompletedRange
	if err := dec.Decode(&rs); err != nil {
		return nil, err
	}
	return rs, nil
}
