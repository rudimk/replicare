package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/rudimk/replicare/internal/engine"
)

var _ engine.ChangePresence = (*Source)(nil)

// presencePerStatement bounds how many queues one statement checks. Each is one column
// (EXISTS) in a flat select list, so the statement grows wide, not deep (a UNION of
// hundreds of branches is a deep tree, which risks Postgres's stack depth limit).
var presencePerStatement = 200

// TablesWithChanges implements engine.ChangePresence: one read of the capture registry,
// then one statement per presencePerStatement tables, each an EXISTS on the table's
// delta table (the first row is enough). Old-PG safe.
func (s *Source) TablesWithChanges(ctx context.Context, tables []engine.TableRef) (map[engine.TableRef]bool, error) {
	if err := s.requireConn(); err != nil {
		return nil, err
	}
	out := make(map[engine.TableRef]bool, len(tables))
	if len(tables) == 0 {
		return out, nil
	}
	rows, err := s.conn.Query(ctx, fmt.Sprintf(
		`SELECT schema_name, table_name, rel_id FROM %s.captured`, quoteIdentifier(captureSchema)))
	if err != nil {
		return nil, fmt.Errorf("postgres: which tables have changes: %w", err)
	}
	relIDs := map[engine.TableRef]int{}
	for rows.Next() {
		var ref engine.TableRef
		var id int
		if err := rows.Scan(&ref.Schema, &ref.Name, &id); err != nil {
			rows.Close()
			return nil, fmt.Errorf("postgres: which tables have changes: %w", err)
		}
		relIDs[ref] = id
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: which tables have changes: %w", err)
	}

	var captured []engine.TableRef
	for _, t := range tables {
		if _, ok := relIDs[t]; ok {
			captured = append(captured, t)
		} else {
			out[t] = true // not ours to answer: the caller does what it did before
		}
	}
	for _, piece := range engine.Pieces(captured, presencePerStatement) {
		cols := make([]string, len(piece))
		for i, t := range piece {
			cols[i] = fmt.Sprintf("EXISTS (SELECT 1 FROM %s)", qualifiedCapture(deltaTableName(relIDs[t])))
		}
		has := make([]bool, len(piece))
		dest := make([]any, len(piece))
		for i := range has {
			dest[i] = &has[i]
		}
		if err := s.conn.QueryRow(ctx, "SELECT "+strings.Join(cols, ", ")).Scan(dest...); err != nil {
			return nil, fmt.Errorf("postgres: which tables have changes: %w", err)
		}
		for i, t := range piece {
			out[t] = has[i]
		}
	}
	return out, nil
}
