package statepg

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"

	"github.com/rudimk/replicare/internal/state"
)

// Acquire takes the single-active ownership lock for a sync via a Postgres
// advisory lock (CLAUDE.md §9). The lock is held on a dedicated pooled
// connection for its whole lifetime; releasing it unlocks and returns the
// connection to the pool.
//
// held=false means another session — in this or another process — already owns
// the sync, so a restarted/rescheduled pod stands by until the previous owner's
// session ends (its advisory lock is released automatically on disconnect). The
// signature is shaped so leader election can be layered on later without a
// redesign.
func (s *Store) Acquire(ctx context.Context, sync string) (held bool, release func() error, err error) {
	if err := s.requirePool(); err != nil {
		return false, nil, err
	}
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return false, nil, fmt.Errorf("statepg: acquire connection for lock: %w", err)
	}

	key := advisoryKey(sync)
	var locked bool
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", key).Scan(&locked); err != nil {
		conn.Release()
		return false, nil, fmt.Errorf("statepg: try advisory lock for %q: %w", sync, err)
	}
	if !locked {
		conn.Release()
		return false, nil, nil
	}

	release = func() error {
		defer conn.Release()
		// Use a background context so release still runs during shutdown even if
		// the caller's context is already cancelled.
		if _, err := conn.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", key); err != nil {
			return fmt.Errorf("statepg: advisory unlock for %q: %w", sync, err)
		}
		return nil
	}
	return true, release, nil
}

// advisoryKey derives a stable 64-bit advisory-lock key from a sync name via
// FNV-1a. Collisions across differently-named syncs are astronomically unlikely
// at 64 bits; a collision would merely serialize two syncs' ownership, never
// corrupt state.
func advisoryKey(sync string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(sync))
	return int64(h.Sum64())
}

// RecordEvent persists an operational event for the status API / audit trail.
func (s *Store) RecordEvent(ctx context.Context, e state.Event) error {
	if err := s.requirePool(); err != nil {
		return err
	}
	var attrs []byte
	if e.Attrs != nil {
		b, err := json.Marshal(e.Attrs)
		if err != nil {
			return fmt.Errorf("statepg: encode event attrs: %w", err)
		}
		attrs = b
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO replicare_state.events
			(sync, target, schema_name, table_name, level, event, message, attrs)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb)`,
		nullIfEmpty(e.Sync), nullIfEmpty(e.Target), nullIfEmpty(e.Table.Schema), nullIfEmpty(e.Table.Name),
		e.Level, e.Event, nullIfEmpty(e.Message), attrsParam(attrs))
	if err != nil {
		return fmt.Errorf("statepg: record event %q: %w", e.Event, err)
	}
	return nil
}

// nullIfEmpty maps "" to a SQL NULL so optional event columns stay null rather
// than empty strings.
func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// attrsParam maps nil JSON to a SQL NULL (rather than an empty jsonb).
func attrsParam(b []byte) any {
	if b == nil {
		return nil
	}
	return string(b)
}
