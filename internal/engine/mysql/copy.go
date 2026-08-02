package mysql

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/rudimk/replicare/internal/engine"
)

// CopyChunk streams one chunk of a table as byte-faithful LOAD DATA text into w
// (§4.1, §0.1). It selects the transport columns (name-matched, generated
// excluded) for the chunk's key range, reads every value as raw bytes
// (character_set_results=binary), and serializes each row with the full escape
// set so the paired BulkLoad (LOAD DATA LOCAL INFILE) reconstructs it byte-for-
// byte. The column set/order matches the neutral copy driver's transportColumns.
func (s *Source) CopyChunk(ctx context.Context, c engine.Chunk, w io.Writer) error {
	if s.db == nil {
		return errNotConnected
	}
	tbl, err := s.tableMeta(ctx, c.Table)
	if err != nil {
		return err
	}
	cols := transportCols(tbl)
	if len(cols) == 0 {
		return fmt.Errorf("mysql: copy chunk: table %s has no transportable columns", c.Table)
	}
	keyCols := captureColsFor(tbl)
	pred, args := keysetPredicate(keyCols, c.Lo, c.Hi)

	sel := make([]string, len(cols))
	for i, name := range cols {
		sel[i] = bq(name)
	}
	q := fmt.Sprintf("SELECT %s FROM %s", strings.Join(sel, ", "), qualify(c.Table.Schema, c.Table.Name))
	if pred != "" {
		q += " WHERE " + pred
	}

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return fmt.Errorf("mysql: copy chunk %s: %w", c.Table, err)
	}
	defer func() { _ = rows.Close() }()

	bw := newRowWriter(w)
	raw := make([][]byte, len(cols))
	dest := make([]any, len(cols))
	for i := range raw {
		dest[i] = &raw[i]
	}
	for rows.Next() {
		for i := range raw {
			raw[i] = nil
		}
		if err := rows.Scan(dest...); err != nil {
			return fmt.Errorf("mysql: copy chunk scan %s: %w", c.Table, err)
		}
		writeRow(bw, raw)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("mysql: copy chunk %s: %w", c.Table, err)
	}
	return bw.Flush()
}
