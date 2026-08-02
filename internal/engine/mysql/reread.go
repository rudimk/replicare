package mysql

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/rudimk/replicare/internal/engine"
)

// RereadCurrent streams the CURRENT source values for the given dirty keys as
// byte-faithful LOAD DATA text into w (§3.3, §4.2). This is the self-healing
// re-read: we always copy the latest value for a dirty PK, so overlapping
// copy/stream windows and intermediate states reconcile. Keys absent from the
// result were deleted at the source (the apply pass deletes them on the target).
// Column set/order matches the neutral drain's transportColumns.
func (s *Source) RereadCurrent(ctx context.Context, t engine.TableRef, keys []engine.KeyValues, w io.Writer) error {
	if s.db == nil {
		return errNotConnected
	}
	tbl, err := s.tableMeta(ctx, t)
	if err != nil {
		return err
	}
	cols := transportCols(tbl)
	keyCols := captureColsFor(tbl)
	pred, args := keyInPredicate(keyCols, keys)

	sel := make([]string, len(cols))
	for i, c := range cols {
		sel[i] = bq(c)
	}
	q := fmt.Sprintf("SELECT %s FROM %s WHERE %s", strings.Join(sel, ", "), qualify(t.Schema, t.Name), pred)

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return fmt.Errorf("mysql: reread %s: %w", t, err)
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
			return fmt.Errorf("mysql: reread scan %s: %w", t, err)
		}
		writeRow(bw, raw)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return bw.Flush()
}

// keyInPredicate builds a membership predicate `key IN (...)` for the given keys
// plus its args. Single-column keys use `k IN (?, ?)`; composite keys use
// row-value `(k1,k2) IN ((?,?),(?,?))`. Empty keys match nothing (`1=0`).
func keyInPredicate(keyCols []captureCol, keys []engine.KeyValues) (string, []any) {
	if len(keys) == 0 {
		return "1=0", nil
	}
	names := make([]string, len(keyCols))
	for i, c := range keyCols {
		names[i] = bq(c.Name)
	}
	var args []any
	if len(keyCols) == 1 {
		ph := make([]string, len(keys))
		for i, k := range keys {
			ph[i] = "?"
			args = append(args, k...)
		}
		return fmt.Sprintf("%s IN (%s)", names[0], strings.Join(ph, ", ")), args
	}
	tuples := make([]string, len(keys))
	for i, k := range keys {
		tuples[i] = "(" + placeholders(len(keyCols)) + ")"
		args = append(args, k...)
	}
	return fmt.Sprintf("(%s) IN (%s)", strings.Join(names, ", "), strings.Join(tuples, ", ")), args
}
