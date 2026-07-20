package postgres

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/rudimk/replicare/internal/engine"
)

// Faithful re-read (CLAUDE.md §3.3, §4.2). At sync time the daemon re-reads the
// CURRENT source values for a set of dirty primary keys and applies them
// idempotently — this is what makes capture self-healing (we always copy the
// latest value; transient intermediate states don't matter). Keys absent from
// the re-read result have been deleted at the source (derived by the apply).

// RereadCurrent streams the current source rows for the given keys as text COPY
// into w — the same verbatim text transport as the initial copy, so re-read is
// as faithful as copy (§4.2). Keys are faithful text values inlined as literals
// (COPY's simple protocol has no bind parameters).
func (s *Source) RereadCurrent(ctx context.Context, t engine.TableRef, keys []engine.KeyValues, w io.Writer) error {
	if err := s.requireConn(); err != nil {
		return err
	}
	table, err := s.tableMeta(ctx, t)
	if err != nil {
		return err
	}
	cols := transportColumns(table)
	keyCols := captureColsFor(table)
	if len(keyCols) == 0 {
		return fmt.Errorf("postgres: reread: table %s has no usable key", t)
	}
	pred, err := keysetInPredicate(keyCols, keys)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf("COPY (SELECT %s FROM %s WHERE %s) TO STDOUT",
		quotedColumnList(cols), qualifyTable(t), pred)
	if _, err := s.conn.PgConn().CopyTo(ctx, w, sql); err != nil {
		return fmt.Errorf("postgres: reread %s: %w", t, err)
	}
	return nil
}

// keysetInPredicate builds a membership predicate over a set of keys, casting
// each faithful text value back to its column type. A single-column key uses
// `k IN (v, ...)`; a composite key uses row-value form `(k1,k2) IN ((..),(..))`.
// An empty key set yields FALSE (matches nothing).
func keysetInPredicate(keyCols []captureCol, keys []engine.KeyValues) (string, error) {
	if len(keys) == 0 {
		return "FALSE", nil
	}
	if len(keyCols) == 1 {
		vals := make([]string, len(keys))
		for i, k := range keys {
			if len(k) != 1 {
				return "", fmt.Errorf("postgres: key has %d values, want 1", len(k))
			}
			sv, ok := k[0].(string)
			if !ok {
				return "", fmt.Errorf("postgres: key value is %T, want text", k[0])
			}
			vals[i] = sqlTextLiteral(sv) + "::" + keyCols[0].Type
		}
		return fmt.Sprintf("%s IN (%s)", quoteIdentifier(keyCols[0].Name), strings.Join(vals, ", ")), nil
	}

	colTuple := "(" + quotedKeyList(keyCols) + ")"
	tuples := make([]string, len(keys))
	for i, k := range keys {
		vt, err := valueTuple(keyCols, k)
		if err != nil {
			return "", err
		}
		tuples[i] = vt
	}
	return fmt.Sprintf("%s IN (%s)", colTuple, strings.Join(tuples, ", ")), nil
}
