package postgres

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/rudimk/replicare/internal/engine"
)

// Streaming apply (CLAUDE.md §3.3, §4.2). ApplyPass applies one drain pass's
// dirty keys to a table in a single transaction: it re-reads current source rows
// (present keys) into a TEMP staging table, upserts them, then deletes the dirty
// keys that are absent from the staging (deleted at the source). Everything moves
// as text, so apply is as faithful as copy. Idempotent — re-applying the same
// pass re-reads current values and converges to the same state.

// ApplyPass implements engine.Sink.
func (s *Sink) ApplyPass(ctx context.Context, t engine.TableRef, cols []string, dirtyKeys []engine.KeyValues, reread io.Reader) error {
	if s.conn == nil {
		return errNotConnected("sink")
	}
	if len(dirtyKeys) == 0 {
		return nil
	}
	table, err := s.tableMeta(ctx, t)
	if err != nil {
		return err
	}
	keyCols := captureColsFor(table)
	if len(keyCols) == 0 {
		return fmt.Errorf("postgres: apply: target %s has no usable key", t)
	}
	typeByName := make(map[string]string, len(table.Columns))
	identity := false
	colSet := make(map[string]bool, len(cols))
	for _, c := range cols {
		colSet[c] = true
	}
	for _, c := range table.Columns {
		typeByName[c.Name] = c.DataType
		if c.Identity && colSet[c.Name] {
			identity = true
		}
	}

	const stg = "replicare_apply"
	stgCols := make([]string, len(cols))
	for i, c := range cols {
		stgCols[i] = quoteIdentifier(c) + " " + typeByName[c]
	}

	inPred, err := keysetInPredicate(keyCols, dirtyKeys)
	if err != nil {
		return err
	}
	keyTuple := "(" + quotedKeyList(keyCols) + ")"

	if _, err := s.conn.Exec(ctx, "BEGIN"); err != nil {
		return fmt.Errorf("postgres: apply: begin: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = s.conn.Exec(context.Background(), "ROLLBACK")
		}
	}()

	if _, err := s.conn.Exec(ctx, fmt.Sprintf("CREATE TEMP TABLE %s (%s) ON COMMIT DROP",
		quoteIdentifier(stg), strings.Join(stgCols, ", "))); err != nil {
		return fmt.Errorf("postgres: apply: create staging: %w", err)
	}
	copySQL := fmt.Sprintf("COPY %s (%s) FROM STDIN", quoteIdentifier(stg), quotedColumnList(cols))
	if _, err := s.conn.PgConn().CopyFrom(ctx, reread, copySQL); err != nil {
		return fmt.Errorf("postgres: apply: stage re-read: %w", err)
	}

	// Present keys -> upsert.
	if _, err := s.conn.Exec(ctx, mergeInsertSQL(t, stg, cols, keyCols, colSetOf(keyCols), identity)); err != nil {
		return fmt.Errorf("postgres: apply: upsert %s: %w", t, err)
	}

	// Dirty keys that are absent from the staging were deleted at the source.
	del := fmt.Sprintf("DELETE FROM %s WHERE %s AND %s NOT IN (SELECT %s FROM %s)",
		qualifyTable(t), inPred, keyTuple, quotedKeyList(keyCols), quoteIdentifier(stg))
	if _, err := s.conn.Exec(ctx, del); err != nil {
		return fmt.Errorf("postgres: apply: delete absent %s: %w", t, err)
	}

	if _, err := s.conn.Exec(ctx, "COMMIT"); err != nil {
		return fmt.Errorf("postgres: apply: commit: %w", err)
	}
	committed = true
	return nil
}
