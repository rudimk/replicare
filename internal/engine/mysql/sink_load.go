package mysql

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync/atomic"

	driver "github.com/go-sql-driver/mysql"

	"github.com/rudimk/replicare/internal/engine"
)

// loadCounter mints unique LOAD DATA reader-handler names. go-sql-driver's
// RegisterReaderHandler keys handlers in a process-global (mutex-guarded) map, so
// concurrent chunk loads MUST use distinct names and deregister when done, or
// they clobber each other (Momus M2 / 2nd-pass m3).
var loadCounter atomic.Uint64

// BulkLoad streams a byte-faithful LOAD DATA LOCAL INFILE into the target table
// (§0.1). The reader r is the CopyChunk pipe; it is exposed to the driver via a
// uniquely-named, deregistered-on-return reader handler. The target validates the
// bytes under the table's column charset (write-side faithfulness, §0.1/2nd-pass
// B2): a single-charset table loads with CHARACTER SET that charset so an invalid
// byte halts loud; a mixed-per-column-charset table is refused (per-charset-group
// loading is a future path — pre-flight flags it, MM1b). Returns rows loaded.
func (s *Sink) BulkLoad(ctx context.Context, t engine.TableRef, cols []string, r io.Reader, mode engine.LoadMode) (int64, error) {
	if s.db == nil {
		return 0, errNotConnected
	}
	if len(cols) == 0 {
		return 0, fmt.Errorf("mysql: bulk load: no columns for %s", t)
	}
	if mode == engine.LoadMerge {
		return 0, errNotImplemented // merge path: MM4b
	}

	charset, err := s.loadCharset(ctx, t)
	if err != nil {
		return 0, err
	}

	name := fmt.Sprintf("rc_load_%d", loadCounter.Add(1))
	driver.RegisterReaderHandler(name, func() io.Reader { return r })
	defer driver.DeregisterReaderHandler(name)

	colList := make([]string, len(cols))
	for i, c := range cols {
		colList[i] = bq(c)
	}
	q := fmt.Sprintf("LOAD DATA LOCAL INFILE 'Reader::%s' INTO TABLE %s CHARACTER SET %s %s (%s)",
		name, qualify(t.Schema, t.Name), charset, loadDataClause, strings.Join(colList, ", "))

	res, err := s.db.ExecContext(ctx, q)
	if err != nil {
		return 0, fmt.Errorf("mysql: bulk load into %s: %w", t, err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// loadCharset returns the CHARACTER SET clause value for a target table: the
// single charset shared by its character columns, "binary" if it has none, or an
// error if columns use more than one charset (mixed-charset needs per-charset-
// group loading, deferred; pre-flight flags such tables).
func (s *Sink) loadCharset(ctx context.Context, t engine.TableRef) (string, error) {
	tbl, err := s.tableMeta(ctx, t)
	if err != nil {
		return "", err
	}
	seen := map[string]bool{}
	for _, c := range tbl.Columns {
		if c.Charset != "" {
			seen[c.Charset] = true
		}
	}
	switch len(seen) {
	case 0:
		return "binary", nil
	case 1:
		for cs := range seen {
			return cs, nil
		}
	}
	return "", fmt.Errorf("mysql: table %s has mixed per-column charsets; per-charset-group LOAD DATA is not yet implemented (pre-flight flags this, mysql-plan §0.1)", t)
}

// DeleteRange deletes target rows in the half-open key range [lo, hi), used to
// clear an incomplete chunk before re-copy on resume (§4.1). During a table's
// initial-copy phase the copier is the only writer, so this is exclusive and
// idempotent. Uses the key's own collation, consistent with keyset chunking.
func (s *Sink) DeleteRange(ctx context.Context, t engine.TableRef, lo, hi engine.KeyValues) error {
	if s.db == nil {
		return errNotConnected
	}
	tbl, err := s.tableMeta(ctx, t)
	if err != nil {
		return err
	}
	keyCols := captureColsFor(tbl)
	if len(keyCols) == 0 {
		return fmt.Errorf("mysql: delete range: table %s has no usable key", t)
	}
	pred, args := keysetPredicate(keyCols, lo, hi)
	q := "DELETE FROM " + qualify(t.Schema, t.Name)
	if pred != "" {
		q += " WHERE " + pred
	}
	if _, err := s.db.ExecContext(ctx, q, args...); err != nil {
		return fmt.Errorf("mysql: delete range %s: %w", t, err)
	}
	return nil
}
