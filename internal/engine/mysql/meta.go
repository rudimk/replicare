package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/rudimk/replicare/internal/engine"
)

// errNoBoundary signals that a keyset-boundary query returned no row (fewer than
// N rows remain — the final chunk is unbounded above).
var errNoBoundary = errors.New("no boundary")

// tableMeta returns cached introspection for one table (Source side), populating
// the cache on first use. A Source is single-goroutine, so no lock is needed.
func (s *Source) tableMeta(ctx context.Context, t engine.TableRef) (engine.Table, error) {
	if s.meta == nil {
		s.meta = map[engine.TableRef]engine.Table{}
	}
	if m, ok := s.meta[t]; ok {
		return m, nil
	}
	m, err := introspectOne(ctx, s.db, t)
	if err != nil {
		return engine.Table{}, err
	}
	s.meta[t] = m
	return m, nil
}

// tableMeta returns cached introspection for one table (Sink side).
func (s *Sink) tableMeta(ctx context.Context, t engine.TableRef) (engine.Table, error) {
	if s.meta == nil {
		s.meta = map[engine.TableRef]engine.Table{}
	}
	if m, ok := s.meta[t]; ok {
		return m, nil
	}
	m, err := introspectOne(ctx, s.db, t)
	if err != nil {
		return engine.Table{}, err
	}
	s.meta[t] = m
	return m, nil
}

// introspectOne introspects a single table (via the selection-filtered path).
func introspectOne(ctx context.Context, db *sql.DB, t engine.TableRef) (engine.Table, error) {
	schema, err := introspectDB(ctx, db, engine.Selection{Include: []string{t.String()}})
	if err != nil {
		return engine.Table{}, err
	}
	for _, tt := range schema.Tables {
		if tt.Ref == t {
			return tt, nil
		}
	}
	return engine.Table{}, fmt.Errorf("mysql: table %s not found", t)
}

// transportCols returns the name-matched column list to copy, in source ordinal
// order, excluding GENERATED columns (the target regenerates them). This MUST
// match the neutral copy driver's transportColumns for the same table, so the
// serialized stream and the LOAD DATA column list align.
func transportCols(t engine.Table) []string {
	cols := make([]string, 0, len(t.Columns))
	for _, c := range t.Columns {
		if c.Generated {
			continue
		}
		cols = append(cols, c.Name)
	}
	return cols
}

// scanKey scans n key columns from a single row as raw bytes into KeyValues,
// returning errNoBoundary when the row does not exist.
func scanKey(row *sql.Row, n int) (engine.KeyValues, error) {
	raw := make([][]byte, n)
	dest := make([]any, n)
	for i := range raw {
		dest[i] = &raw[i]
	}
	if err := row.Scan(dest...); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errNoBoundary
		}
		return nil, err
	}
	key := make(engine.KeyValues, n)
	for i := range raw {
		if raw[i] == nil {
			key[i] = nil
		} else {
			key[i] = string(raw[i])
		}
	}
	return key, nil
}
