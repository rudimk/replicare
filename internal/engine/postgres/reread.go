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
	if s.cluster {
		return s.rereadVersioned(ctx, t, cols, keyCols, pred, w)
	}
	sql := fmt.Sprintf("COPY (SELECT %s FROM %s WHERE %s) TO STDOUT",
		quotedColumnList(cols), qualifyTable(t), pred)
	if _, err := s.conn.PgConn().CopyTo(ctx, w, sql); err != nil {
		return fmt.Errorf("postgres: reread %s: %w", t, err)
	}
	return nil
}

// meshVersionCols are the four version columns a cluster re-read appends after the
// table's transport columns, so the version travels with the row through the same
// text COPY channel the apply already uses. Kept in this fixed order because the
// paired sink's cluster staging expects exactly these, in this order.
var meshVersionCols = []string{"rc_hlc_phys", "rc_hlc_log", "rc_node", "rc_deleted"}

// rereadVersioned is the cluster (multi-master) re-read: it drives from the version
// register (LEFT JOIN the user table) so BOTH live rows AND tombstones for the dirty
// keys are returned, each carrying its (hlc, node, deleted). A live key yields its
// user-table values; a tombstone yields NULL value columns (the apply deletes it,
// version-guarded). Key columns come from the register so a tombstone still carries
// its key. Emitted as text COPY, appending meshVersionCols after the transport cols.
func (s *Source) rereadVersioned(ctx context.Context, t engine.TableRef, cols []string,
	keyCols []captureCol, pred string, w io.Writer) error {
	reg := qualifiedCapture(registerTableName(t))
	keySet := colSetOf(keyCols)
	// Value columns: key columns from the register r (always present, even for a
	// tombstone); non-key columns from the user table u (NULL when deleted).
	sel := make([]string, len(cols))
	for i, c := range cols {
		if keySet[c] {
			sel[i] = "r." + quoteIdentifier(c)
		} else {
			sel[i] = "u." + quoteIdentifier(c)
		}
	}
	joinConds := make([]string, len(keyCols))
	for i, kc := range keyCols {
		q := quoteIdentifier(kc.Name)
		joinConds[i] = "u." + q + " = r." + q
	}
	// The dirty-key predicate is over bare key columns; qualify it to the register.
	regPred := qualifyPredicate(pred, keyCols, "r")
	sql := fmt.Sprintf(
		"COPY (SELECT %s, r.rc_hlc_phys, r.rc_hlc_log, r.rc_node, r.rc_deleted "+
			"FROM %s r LEFT JOIN %s u ON %s WHERE %s) TO STDOUT",
		strings.Join(sel, ", "), reg, qualifyTable(t), strings.Join(joinConds, " AND "), regPred)
	if _, err := s.conn.PgConn().CopyTo(ctx, w, sql); err != nil {
		return fmt.Errorf("postgres: versioned reread %s: %w", t, err)
	}
	return nil
}

// qualifyPredicate rewrites a keyset predicate built on bare key-column identifiers
// to reference them via a table alias (e.g. `"id"` -> `r."id"`), so it is unambiguous
// in a JOIN. It replaces each quoted key-column identifier with alias.identifier.
func qualifyPredicate(pred string, keyCols []captureCol, alias string) string {
	out := pred
	for _, kc := range keyCols {
		q := quoteIdentifier(kc.Name)
		out = strings.ReplaceAll(out, q, alias+"."+q)
	}
	return out
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
