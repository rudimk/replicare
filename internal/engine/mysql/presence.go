package mysql

import (
	"context"
	"fmt"
	"strings"

	"github.com/rudimk/replicare/internal/engine"
)

var _ engine.ChangePresence = (*Source)(nil)

// presencePerStatement bounds how many queues one statement checks (one EXISTS column
// each, in a flat select list).
var presencePerStatement = 200

// TablesWithChanges implements engine.ChangePresence: one read of the capture registry,
// then one statement per presencePerStatement tables, each an EXISTS on the table's
// delta table. 5.7-safe.
func (s *Source) TablesWithChanges(ctx context.Context, tables []engine.TableRef) (map[engine.TableRef]bool, error) {
	if s.db == nil {
		return nil, errNotConnected
	}
	out := make(map[engine.TableRef]bool, len(tables))
	if len(tables) == 0 {
		return out, nil
	}
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf("SELECT schema_name, table_name, rel_id FROM %s", captureRef("captured")))
	if err != nil {
		return nil, fmt.Errorf("mysql: which tables have changes: %w", err)
	}
	relIDs := map[engine.TableRef]int{}
	for rows.Next() {
		var ref engine.TableRef
		var id int
		if err := rows.Scan(&ref.Schema, &ref.Name, &id); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("mysql: which tables have changes: %w", err)
		}
		relIDs[ref] = id
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("mysql: which tables have changes: %w", err)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("mysql: which tables have changes: %w", err)
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
			cols[i] = fmt.Sprintf("EXISTS (SELECT 1 FROM %s)", captureRef(deltaTableName(relIDs[t])))
		}
		has := make([]int64, len(piece))
		dest := make([]any, len(piece))
		for i := range has {
			dest[i] = &has[i]
		}
		if err := s.db.QueryRowContext(ctx, "SELECT "+strings.Join(cols, ", ")).Scan(dest...); err != nil {
			return nil, fmt.Errorf("mysql: which tables have changes: %w", err)
		}
		for i, t := range piece {
			out[t] = has[i] != 0
		}
	}
	return out, nil
}
