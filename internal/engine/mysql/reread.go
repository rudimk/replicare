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

	q := fmt.Sprintf("SELECT %s FROM %s WHERE %s", quotedCols(cols), qualify(t.Schema, t.Name), pred)
	nScan := len(cols)
	if s.cluster {
		q, args = s.versionedRereadQuery(t, cols, keyCols, keys)
		nScan = len(cols) + 4
	}

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return fmt.Errorf("mysql: reread %s: %w", t, err)
	}
	defer func() { _ = rows.Close() }()

	bw := newRowWriter(w)
	raw := make([][]byte, nScan)
	dest := make([]any, nScan)
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

// meshVersionCols are the four version columns a cluster re-read appends after the
// transport columns (same order the apply's cluster staging expects).
var meshVersionCols = []string{"rc_hlc_phys", "rc_hlc_log", "rc_node", "rc_deleted"}

func quotedCols(cols []string) string {
	q := make([]string, len(cols))
	for i, c := range cols {
		q[i] = bq(c)
	}
	return strings.Join(q, ", ")
}

// versionedRereadQuery builds the cluster (multi-master) re-read: it drives from the
// version register (LEFT JOIN the user table) so BOTH live rows AND tombstones for the
// dirty keys are returned, each carrying its (hlc, node, deleted). Key columns come
// from the register (present even for a tombstone); non-key value columns from the
// user table (NULL when deleted). The four version columns are appended.
func (s *Source) versionedRereadQuery(t engine.TableRef, cols []string, keyCols []captureCol, keys []engine.KeyValues) (string, []any) {
	reg := captureRef(registerTableName(t))
	keySet := map[string]bool{}
	for _, c := range keyCols {
		keySet[c.Name] = true
	}
	sel := make([]string, len(cols))
	for i, c := range cols {
		if keySet[c] {
			sel[i] = "r." + bq(c)
		} else {
			sel[i] = "u." + bq(c)
		}
	}
	joins := make([]string, len(keyCols))
	regNames := make([]string, len(keyCols))
	for i, kc := range keyCols {
		q := bq(kc.Name)
		joins[i] = "u." + q + " = r." + q
		regNames[i] = "r." + q
	}
	pred, args := keyInPredicateAliased(keyCols, keys, "r")
	q := fmt.Sprintf(
		"SELECT %s, r.rc_hlc_phys, r.rc_hlc_log, r.rc_node, r.rc_deleted "+
			"FROM %s r LEFT JOIN %s u ON %s WHERE %s",
		strings.Join(sel, ", "), reg, qualify(t.Schema, t.Name), strings.Join(joins, " AND "), pred)
	return q, args
}

// keyInPredicateAliased is keyInPredicate with each key column qualified by a table
// alias (so the predicate is unambiguous against the register in a JOIN).
func keyInPredicateAliased(keyCols []captureCol, keys []engine.KeyValues, alias string) (string, []any) {
	if len(keys) == 0 {
		return "1=0", nil
	}
	names := make([]string, len(keyCols))
	for i, c := range keyCols {
		names[i] = alias + "." + bq(c.Name)
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
