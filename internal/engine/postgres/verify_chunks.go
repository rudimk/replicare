package postgres

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/rudimk/replicare/internal/engine"
)

// Chunked verify (engine.ChunkVerifier) for the Postgres Source and Sink. Every query
// is a read over a key range of the table's replication key, served by its index, so
// each one reads only the rows it checks. The source and the target run the same SQL
// for the same range, so their fingerprints are directly comparable.
var (
	_ engine.ChunkVerifier = (*Source)(nil)
	_ engine.ChunkVerifier = (*Sink)(nil)
)

// NextKey implements engine.ChunkVerifier.
func (s *Source) NextKey(ctx context.Context, t engine.TableRef, after engine.KeyValues, n int) (engine.KeyValues, error) {
	if err := s.requireConn(); err != nil {
		return nil, err
	}
	meta, err := s.tableMeta(ctx, t)
	if err != nil {
		return nil, err
	}
	return limited(ctx, s.conn, func(q pgQuerier) (engine.KeyValues, error) { return nextKey(ctx, q, t, captureColsFor(meta), after, n) })
}

// NextKey implements engine.ChunkVerifier.
func (s *Sink) NextKey(ctx context.Context, t engine.TableRef, after engine.KeyValues, n int) (engine.KeyValues, error) {
	if s.conn == nil {
		return nil, errNotConnected("sink")
	}
	meta, err := s.tableMeta(ctx, t)
	if err != nil {
		return nil, err
	}
	return limited(ctx, s.conn, func(q pgQuerier) (engine.KeyValues, error) { return nextKey(ctx, q, t, captureColsFor(meta), after, n) })
}

// VerifyRange implements engine.ChunkVerifier.
func (s *Source) VerifyRange(ctx context.Context, t engine.TableRef, cols []string, after, upTo engine.KeyValues) (engine.Fingerprint, error) {
	if err := s.requireConn(); err != nil {
		return engine.Fingerprint{}, err
	}
	meta, err := s.tableMeta(ctx, t)
	if err != nil {
		return engine.Fingerprint{}, err
	}
	return limited(ctx, s.conn, func(q pgQuerier) (engine.Fingerprint, error) {
		return verifyRange(ctx, q, t, captureColsFor(meta), cols, after, upTo)
	})
}

// VerifyRange implements engine.ChunkVerifier.
func (s *Sink) VerifyRange(ctx context.Context, t engine.TableRef, cols []string, after, upTo engine.KeyValues) (engine.Fingerprint, error) {
	if s.conn == nil {
		return engine.Fingerprint{}, errNotConnected("sink")
	}
	meta, err := s.tableMeta(ctx, t)
	if err != nil {
		return engine.Fingerprint{}, err
	}
	return limited(ctx, s.conn, func(q pgQuerier) (engine.Fingerprint, error) {
		return verifyRange(ctx, q, t, captureColsFor(meta), cols, after, upTo)
	})
}

// RangeRowHashes implements engine.ChunkVerifier.
func (s *Source) RangeRowHashes(ctx context.Context, t engine.TableRef, cols []string, after, upTo engine.KeyValues, limit int) ([]engine.RowHash, error) {
	if err := s.requireConn(); err != nil {
		return nil, err
	}
	meta, err := s.tableMeta(ctx, t)
	if err != nil {
		return nil, err
	}
	return limited(ctx, s.conn, func(q pgQuerier) ([]engine.RowHash, error) {
		return rangeRowHashes(ctx, q, t, captureColsFor(meta), cols, after, upTo, limit)
	})
}

// RangeRowHashes implements engine.ChunkVerifier.
func (s *Sink) RangeRowHashes(ctx context.Context, t engine.TableRef, cols []string, after, upTo engine.KeyValues, limit int) ([]engine.RowHash, error) {
	if s.conn == nil {
		return nil, errNotConnected("sink")
	}
	meta, err := s.tableMeta(ctx, t)
	if err != nil {
		return nil, err
	}
	return limited(ctx, s.conn, func(q pgQuerier) ([]engine.RowHash, error) {
		return rangeRowHashes(ctx, q, t, captureColsFor(meta), cols, after, upTo, limit)
	})
}

// RowHashes implements engine.ChunkVerifier.
func (s *Source) RowHashes(ctx context.Context, t engine.TableRef, cols []string, keys []engine.KeyValues) ([]engine.RowHash, error) {
	if err := s.requireConn(); err != nil {
		return nil, err
	}
	meta, err := s.tableMeta(ctx, t)
	if err != nil {
		return nil, err
	}
	return limited(ctx, s.conn, func(q pgQuerier) ([]engine.RowHash, error) {
		return rowHashes(ctx, q, t, captureColsFor(meta), cols, keys)
	})
}

// RowHashes implements engine.ChunkVerifier.
func (s *Sink) RowHashes(ctx context.Context, t engine.TableRef, cols []string, keys []engine.KeyValues) ([]engine.RowHash, error) {
	if s.conn == nil {
		return nil, errNotConnected("sink")
	}
	meta, err := s.tableMeta(ctx, t)
	if err != nil {
		return nil, err
	}
	return limited(ctx, s.conn, func(q pgQuerier) ([]engine.RowHash, error) {
		return rowHashes(ctx, q, t, captureColsFor(meta), cols, keys)
	})
}

// SampleKeys implements engine.ChunkVerifier.
func (s *Source) SampleKeys(ctx context.Context, t engine.TableRef, n int) ([]engine.KeyValues, error) {
	if err := s.requireConn(); err != nil {
		return nil, err
	}
	meta, err := s.tableMeta(ctx, t)
	if err != nil {
		return nil, err
	}
	return limited(ctx, s.conn, func(q pgQuerier) ([]engine.KeyValues, error) { return sampleKeys(ctx, q, t, captureColsFor(meta), n) })
}

// SampleKeys implements engine.ChunkVerifier.
func (s *Sink) SampleKeys(ctx context.Context, t engine.TableRef, n int) ([]engine.KeyValues, error) {
	if s.conn == nil {
		return nil, errNotConnected("sink")
	}
	meta, err := s.tableMeta(ctx, t)
	if err != nil {
		return nil, err
	}
	return limited(ctx, s.conn, func(q pgQuerier) ([]engine.KeyValues, error) { return sampleKeys(ctx, q, t, captureColsFor(meta), n) })
}

// sampleKeys picks n random keys with TABLESAMPLE SYSTEM (9.5+), which reads only the
// sampled share of the table's pages. The share is sized from the planner's row
// estimate to return about 50 rows per key wanted, so a huge table reads a few MB.
func sampleKeys(ctx context.Context, conn pgQuerier, t engine.TableRef, key []captureCol, n int) ([]engine.KeyValues, error) {
	if err := requireKey(t, key); err != nil {
		return nil, err
	}
	if n < 1 {
		return nil, nil
	}
	var est float64
	if err := conn.QueryRow(ctx, "SELECT greatest(reltuples, 0)::float8 FROM pg_class WHERE oid = $1::regclass",
		qualifyTable(t)).Scan(&est); err != nil {
		return nil, fmt.Errorf("postgres: verify %s: row estimate: %w", t, err)
	}
	pct := 100.0
	if est > 0 {
		pct = 100 * float64(n*50) / est
	}
	if pct > 100 {
		pct = 100
	}
	if pct < 0.0001 {
		pct = 0.0001
	}
	keys := quotedKeyList(key)
	q := fmt.Sprintf(`
		SELECT %[1]s FROM (
			SELECT %[2]s FROM (SELECT %[2]s FROM %[3]s TABLESAMPLE SYSTEM ($1)) s ORDER BY random() LIMIT $2
		) r
		ORDER BY %[4]s`, textAliases(key), keys, qualifyTable(t), keysOf("r", key))
	// SYSTEM sampling picks whole pages at random, so on a small table (or a stale row
	// estimate) it can return fewer keys than asked for, even none. Widen the sample
	// until there are enough or the whole table was read.
	for {
		out, err := sampleOnce(ctx, conn, t, q, pct, n)
		if err != nil || len(out) >= n || pct >= 100 {
			return out, err
		}
		pct *= 4
		if pct > 100 {
			pct = 100
		}
	}
}

func sampleOnce(ctx context.Context, conn pgQuerier, t engine.TableRef, q string, pct float64, n int) ([]engine.KeyValues, error) {
	rows, err := conn.Query(ctx, q, pct, n)
	if err != nil {
		return nil, fmt.Errorf("postgres: verify %s: sample keys: %w", t, err)
	}
	defer rows.Close()
	var out []engine.KeyValues
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, err
		}
		out = append(out, engine.KeyValues(vals))
	}
	return out, rows.Err()
}

// descKeys orders by every key column descending (row order reversed).
func descKeys(alias string, key []captureCol) string {
	parts := make([]string, len(key))
	for i, c := range key {
		parts[i] = alias + "." + quoteIdentifier(c.Name) + " DESC"
	}
	return strings.Join(parts, ", ")
}

// keysOf qualifies the key columns with a table alias.
func keysOf(alias string, key []captureCol) string {
	parts := make([]string, len(key))
	for i, c := range key {
		parts[i] = alias + "." + quoteIdentifier(c.Name)
	}
	return strings.Join(parts, ", ")
}

// rowHashExpr is the per-row content hash, identical to fingerprint's: md5 over the
// row of name-sorted columns, folded to a signed 64-bit integer.
func rowHashExpr(cols []string) string {
	sorted := append([]string(nil), cols...)
	sort.Strings(sorted)
	quoted := make([]string, len(sorted))
	for i, c := range sorted {
		quoted[i] = quoteIdentifier(c)
	}
	return "('x' || substr(md5(ROW(" + strings.Join(quoted, ", ") + ")::text), 1, 16))::bit(64)::bigint"
}

// rangePredicate builds (keys) > after AND (keys) <= upTo; nil bounds are omitted.
func rangePredicate(key []captureCol, after, upTo engine.KeyValues) (string, error) {
	tuple := "(" + quotedKeyList(key) + ")"
	var parts []string
	if after != nil {
		vt, err := valueTuple(key, after)
		if err != nil {
			return "", err
		}
		parts = append(parts, tuple+" > "+vt)
	}
	if upTo != nil {
		vt, err := valueTuple(key, upTo)
		if err != nil {
			return "", err
		}
		parts = append(parts, tuple+" <= "+vt)
	}
	if len(parts) == 0 {
		return "TRUE", nil
	}
	return strings.Join(parts, " AND "), nil
}

func requireKey(t engine.TableRef, key []captureCol) error {
	if len(key) == 0 {
		return fmt.Errorf("postgres: %s has no primary or unique key to check it by", t)
	}
	return nil
}

func nextKey(ctx context.Context, conn pgQuerier, t engine.TableRef, key []captureCol, after engine.KeyValues, n int) (engine.KeyValues, error) {
	if err := requireKey(t, key); err != nil {
		return nil, err
	}
	if n < 1 {
		n = 1
	}
	pred, err := rangePredicate(key, after, nil)
	if err != nil {
		return nil, err
	}
	keys := quotedKeyList(key)
	// The n-th key after `after`, else (fewer rows left) the last one: the largest of
	// the next n keys, read from the key index from `after` onwards.
	q := fmt.Sprintf(`
		SELECT %[1]s FROM (
			SELECT %[2]s FROM %[3]s WHERE %[4]s ORDER BY %[2]s LIMIT $1
		) r
		ORDER BY %[5]s LIMIT 1`, textAliases(key), keys, qualifyTable(t), pred, descKeys("r", key))
	rows, err := conn.Query(ctx, q, n)
	if err != nil {
		return nil, fmt.Errorf("postgres: verify %s: next key: %w", t, err)
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, rows.Err()
	}
	vals, err := rows.Values()
	if err != nil {
		return nil, err
	}
	return engine.KeyValues(vals), rows.Err()
}

// textAliases selects the key columns as text from the inner query's columns.
func textAliases(key []captureCol) string {
	parts := make([]string, len(key))
	for i, c := range key {
		parts[i] = "r." + quoteIdentifier(c.Name) + "::text"
	}
	return strings.Join(parts, ", ")
}

func verifyRange(ctx context.Context, conn pgQuerier, t engine.TableRef, key []captureCol, cols []string, after, upTo engine.KeyValues) (engine.Fingerprint, error) {
	if err := requireKey(t, key); err != nil {
		return engine.Fingerprint{}, err
	}
	pred, err := rangePredicate(key, after, upTo)
	if err != nil {
		return engine.Fingerprint{}, err
	}
	q := fmt.Sprintf(`SELECT count(*)::bigint, coalesce(sum(%s)::text, '0') FROM %s WHERE %s`,
		rowHashExpr(cols), qualifyTable(t), pred)
	var fp engine.Fingerprint
	if err := conn.QueryRow(ctx, q).Scan(&fp.Rows, &fp.Checksum); err != nil {
		return engine.Fingerprint{}, fmt.Errorf("postgres: verify %s: range: %w", t, err)
	}
	return fp, nil
}

func rangeRowHashes(ctx context.Context, conn pgQuerier, t engine.TableRef, key []captureCol, cols []string, after, upTo engine.KeyValues, limit int) ([]engine.RowHash, error) {
	if err := requireKey(t, key); err != nil {
		return nil, err
	}
	pred, err := rangePredicate(key, after, upTo)
	if err != nil {
		return nil, err
	}
	q := fmt.Sprintf(`SELECT %s, (%s)::text FROM %s WHERE %s ORDER BY %s LIMIT $1`,
		quotedKeyListAsText(key), rowHashExpr(cols), qualifyTable(t), pred, quotedKeyList(key))
	return scanRowHashes(ctx, conn, t, q, len(key), limit)
}

func rowHashes(ctx context.Context, conn pgQuerier, t engine.TableRef, key []captureCol, cols []string, keys []engine.KeyValues) ([]engine.RowHash, error) {
	if err := requireKey(t, key); err != nil {
		return nil, err
	}
	var out []engine.RowHash
	// In pieces (engine.MaxKeysPerStatement): the keys to look up again can be many.
	for _, piece := range engine.Pieces(keys, engine.MaxKeysPerStatement) {
		tuples := make([]string, len(piece))
		for i, k := range piece {
			vt, err := valueTuple(key, k)
			if err != nil {
				return nil, err
			}
			tuples[i] = vt
		}
		q := fmt.Sprintf(`SELECT %s, (%s)::text FROM %s WHERE (%s) IN (%s)`,
			quotedKeyListAsText(key), rowHashExpr(cols), qualifyTable(t), quotedKeyList(key), strings.Join(tuples, ", "))
		got, err := scanRowHashes(ctx, conn, t, q, len(key))
		if err != nil {
			return nil, err
		}
		out = append(out, got...)
	}
	return out, nil
}

func scanRowHashes(ctx context.Context, conn pgQuerier, t engine.TableRef, q string, nKey int, args ...any) ([]engine.RowHash, error) {
	rows, err := conn.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("postgres: verify %s: row hashes: %w", t, err)
	}
	defer rows.Close()
	var out []engine.RowHash
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, err
		}
		h, _ := vals[nKey].(string)
		out = append(out, engine.RowHash{Key: engine.KeyValues(vals[:nKey]), Hash: h})
	}
	return out, rows.Err()
}
