package mysql

import (
	"context"
	"database/sql"
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
	charset, err := s.loadCharset(ctx, t)
	if err != nil {
		return 0, err
	}
	if mode == engine.LoadMerge {
		return s.mergeLoad(ctx, t, cols, r, charset)
	}
	return runLoad(ctx, s.db, qualify(t.Schema, t.Name), cols, r, charset, s.localInfile)
}

// execQuerier is the subset of database/sql satisfied by *sql.DB, *sql.Tx, and
// *sql.Conn, so the load and verify paths run against a pool, a transaction
// (merge / cyclic FK_CHECKS), or a pinned connection alike.
type execQuerier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// runLoad streams r into the `into` table (real or TEMP) via a uniquely-named,
// deregistered-on-return reader handler and returns rows loaded. `localInfile`
// gates the transport: when the target has LOAD DATA LOCAL INFILE disabled, it
// halts LOUD before consuming r (v1 has no INSERT fallback), rather than failing
// cryptically with errno 1148 partway through a copy (MM9).
func runLoad(ctx context.Context, ex execQuerier, into string, cols []string, r io.Reader, charset string, localInfile bool) (int64, error) {
	if !localInfile {
		return 0, errLocalInfileRequired
	}
	name := fmt.Sprintf("rc_load_%d", loadCounter.Add(1))
	driver.RegisterReaderHandler(name, func() io.Reader { return r })
	defer driver.DeregisterReaderHandler(name)

	colList := make([]string, len(cols))
	for i, c := range cols {
		colList[i] = bq(c)
	}
	q := fmt.Sprintf("LOAD DATA LOCAL INFILE 'Reader::%s' INTO TABLE %s CHARACTER SET %s %s (%s)",
		name, into, charset, loadDataClause, strings.Join(colList, ", "))
	res, err := ex.ExecContext(ctx, q)
	if err != nil {
		return 0, fmt.Errorf("mysql: load into %s: %w", into, err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// loadCharset returns the CHARACTER SET clause value for a LOAD DATA into a target
// table. It is ALWAYS "binary" — the byte-faithful choice (§1.7): with
// CHARACTER SET binary, LOAD DATA performs no charset conversion when reading the
// file, so each field's RAW source bytes are assigned to its column and validated
// by that column's OWN type. A utf8mb4 column rejects invalid utf8 loudly; a
// VARBINARY/BLOB column keeps arbitrary bytes (including high bytes like 0xFF that
// a utf8mb4 load charset would mangle); and a table mixing text and binary columns
// — or several text charsets — loads correctly in one pass (no per-charset-group
// split needed). This pairs with reads under character_set_results=binary.
//
// It keeps the (Sink, TableRef) signature so callers are unchanged and a future
// per-column strategy can slot in here; it currently never errors.
func (s *Sink) loadCharset(context.Context, engine.TableRef) (string, error) {
	return "binary", nil
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
