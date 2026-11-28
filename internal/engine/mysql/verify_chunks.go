package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"math/rand"
	"sort"
	"strings"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

// Chunked verify (engine.ChunkVerifier) for the MySQL Source and Sink: the same five
// read-only operations as Postgres, so `replicare verify` compares a MySQL table one
// key range at a time instead of hashing it in one statement. Every query is a SELECT
// over a range of the table's replication key, served by its index, and the source and
// the target run the same SQL for the same range, so their results compare directly.
// Everything here runs on the 5.7 floor: row-value comparison, LIMIT and derived
// tables, no window functions.
var (
	_ engine.ChunkVerifier = (*Source)(nil)
	_ engine.ChunkVerifier = (*Sink)(nil)
)

// verifyDB is the shared state the five operations need: the connection and the
// table's replication key.
type verifyDB struct {
	db  *sql.DB
	key []captureCol
}

func (s *Source) verifyDB(ctx context.Context, t engine.TableRef) (verifyDB, error) {
	if s.db == nil {
		return verifyDB{}, errNotConnected
	}
	tbl, err := s.tableMeta(ctx, t)
	if err != nil {
		return verifyDB{}, err
	}
	return newVerifyDB(s.db, t, tbl)
}

func (s *Sink) verifyDB(ctx context.Context, t engine.TableRef) (verifyDB, error) {
	if s.db == nil {
		return verifyDB{}, errNotConnected
	}
	tbl, err := s.tableMeta(ctx, t)
	if err != nil {
		return verifyDB{}, err
	}
	return newVerifyDB(s.db, t, tbl)
}

func newVerifyDB(db *sql.DB, t engine.TableRef, tbl engine.Table) (verifyDB, error) {
	key := captureColsFor(tbl)
	if len(key) == 0 {
		return verifyDB{}, fmt.Errorf("mysql: %s has no primary or unique key to check it by", t)
	}
	return verifyDB{db: db, key: key}, nil
}

// NextKey implements engine.ChunkVerifier.
func (s *Source) NextKey(ctx context.Context, t engine.TableRef, after engine.KeyValues, n int) (engine.KeyValues, error) {
	v, err := s.verifyDB(ctx, t)
	if err != nil {
		return nil, err
	}
	return v.nextKey(ctx, t, after, n)
}

// NextKey implements engine.ChunkVerifier.
func (s *Sink) NextKey(ctx context.Context, t engine.TableRef, after engine.KeyValues, n int) (engine.KeyValues, error) {
	v, err := s.verifyDB(ctx, t)
	if err != nil {
		return nil, err
	}
	return v.nextKey(ctx, t, after, n)
}

// VerifyRange implements engine.ChunkVerifier.
func (s *Source) VerifyRange(ctx context.Context, t engine.TableRef, cols []string, after, upTo engine.KeyValues) (engine.Fingerprint, error) {
	v, err := s.verifyDB(ctx, t)
	if err != nil {
		return engine.Fingerprint{}, err
	}
	return v.verifyRange(ctx, t, cols, after, upTo)
}

// VerifyRange implements engine.ChunkVerifier.
func (s *Sink) VerifyRange(ctx context.Context, t engine.TableRef, cols []string, after, upTo engine.KeyValues) (engine.Fingerprint, error) {
	v, err := s.verifyDB(ctx, t)
	if err != nil {
		return engine.Fingerprint{}, err
	}
	return v.verifyRange(ctx, t, cols, after, upTo)
}

// RangeRowHashes implements engine.ChunkVerifier.
func (s *Source) RangeRowHashes(ctx context.Context, t engine.TableRef, cols []string, after, upTo engine.KeyValues, limit int) ([]engine.RowHash, error) {
	v, err := s.verifyDB(ctx, t)
	if err != nil {
		return nil, err
	}
	return v.rangeRowHashes(ctx, t, cols, after, upTo, limit)
}

// RangeRowHashes implements engine.ChunkVerifier.
func (s *Sink) RangeRowHashes(ctx context.Context, t engine.TableRef, cols []string, after, upTo engine.KeyValues, limit int) ([]engine.RowHash, error) {
	v, err := s.verifyDB(ctx, t)
	if err != nil {
		return nil, err
	}
	return v.rangeRowHashes(ctx, t, cols, after, upTo, limit)
}

// RowHashes implements engine.ChunkVerifier.
func (s *Source) RowHashes(ctx context.Context, t engine.TableRef, cols []string, keys []engine.KeyValues) ([]engine.RowHash, error) {
	v, err := s.verifyDB(ctx, t)
	if err != nil {
		return nil, err
	}
	return v.rowHashes(ctx, t, cols, keys)
}

// RowHashes implements engine.ChunkVerifier.
func (s *Sink) RowHashes(ctx context.Context, t engine.TableRef, cols []string, keys []engine.KeyValues) ([]engine.RowHash, error) {
	v, err := s.verifyDB(ctx, t)
	if err != nil {
		return nil, err
	}
	return v.rowHashes(ctx, t, cols, keys)
}

// SampleKeys implements engine.ChunkVerifier.
func (s *Source) SampleKeys(ctx context.Context, t engine.TableRef, n int) ([]engine.KeyValues, error) {
	v, err := s.verifyDB(ctx, t)
	if err != nil {
		return nil, err
	}
	return v.sampleKeys(ctx, t, n)
}

// SampleKeys implements engine.ChunkVerifier.
func (s *Sink) SampleKeys(ctx context.Context, t engine.TableRef, n int) ([]engine.KeyValues, error) {
	v, err := s.verifyDB(ctx, t)
	if err != nil {
		return nil, err
	}
	return v.sampleKeys(ctx, t, n)
}

// timeHint is a MAX_EXECUTION_TIME optimizer hint (5.7.8+; a plain comment before)
// set to the time the context has left, so the server itself stops a slow read
// rather than only the client giving up on it.
func timeHint(ctx context.Context) string {
	dl, ok := ctx.Deadline()
	if !ok {
		return ""
	}
	ms := time.Until(dl).Milliseconds()
	if ms < 1 {
		ms = 1
	}
	return fmt.Sprintf("/*+ MAX_EXECUTION_TIME(%d) */ ", ms)
}

func (v verifyDB) keyList(alias string) string {
	parts := make([]string, len(v.key))
	for i, c := range v.key {
		if alias != "" {
			parts[i] = alias + "." + bq(c.Name)
		} else {
			parts[i] = bq(c.Name)
		}
	}
	return strings.Join(parts, ", ")
}

func (v verifyDB) keyListDesc(alias string) string {
	parts := make([]string, len(v.key))
	for i, c := range v.key {
		parts[i] = alias + "." + bq(c.Name) + " DESC"
	}
	return strings.Join(parts, ", ")
}

// rangePredicate is (key) > after AND (key) <= upTo; nil bounds are left out.
func (v verifyDB) rangePredicate(after, upTo engine.KeyValues) (string, []any) {
	tuple := "(" + v.keyList("") + ")"
	var conds []string
	var args []any
	if after != nil {
		conds = append(conds, tuple+" > ("+placeholders(len(v.key))+")")
		args = append(args, keyArgs(after)...)
	}
	if upTo != nil {
		conds = append(conds, tuple+" <= ("+placeholders(len(v.key))+")")
		args = append(args, keyArgs(upTo)...)
	}
	if len(conds) == 0 {
		return "1=1", nil
	}
	return strings.Join(conds, " AND "), args
}

// rowHashExpr is the per-row content hash, identical to fingerprint's: MD5 over the
// NUL-guarded, name-sorted columns, folded to an unsigned 64-bit integer.
func rowHashExpr(cols []string) string {
	sorted := append([]string(nil), cols...)
	sort.Strings(sorted)
	parts := make([]string, len(sorted))
	for i, c := range sorted {
		parts[i] = fmt.Sprintf("IFNULL(CAST(%s AS CHAR), CHAR(0))", bq(c))
	}
	return "CAST(CONV(SUBSTRING(MD5(CONCAT_WS(CHAR(1), " + strings.Join(parts, ", ") + ")), 1, 16), 16, 10) AS UNSIGNED)"
}

// nextKey is the n-th key after `after`, else (fewer rows left) the last one: the
// largest of the next n keys, read from the key index from `after` onwards. nil when
// no row lies after `after`.
func (v verifyDB) nextKey(ctx context.Context, t engine.TableRef, after engine.KeyValues, n int) (engine.KeyValues, error) {
	if n < 1 {
		n = 1
	}
	pred, args := v.rangePredicate(after, nil)
	q := fmt.Sprintf("SELECT %s%s FROM (SELECT %s FROM %s WHERE %s ORDER BY %s LIMIT %d) r ORDER BY %s LIMIT 1",
		timeHint(ctx), v.keyList("r"), v.keyList(""), qualify(t.Schema, t.Name), pred, v.keyList(""), n, v.keyListDesc("r"))
	key, err := scanKey(v.db.QueryRowContext(ctx, q, args...), len(v.key))
	if err == errNoBoundary {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("mysql: verify %s: next key: %w", t, err)
	}
	return key, nil
}

func (v verifyDB) verifyRange(ctx context.Context, t engine.TableRef, cols []string, after, upTo engine.KeyValues) (engine.Fingerprint, error) {
	pred, args := v.rangePredicate(after, upTo)
	q := fmt.Sprintf("SELECT %sCOUNT(*), COALESCE(SUM(%s), 0) FROM %s WHERE %s",
		timeHint(ctx), rowHashExpr(cols), qualify(t.Schema, t.Name), pred)
	var fp engine.Fingerprint
	if err := v.db.QueryRowContext(ctx, q, args...).Scan(&fp.Rows, &fp.Checksum); err != nil {
		return engine.Fingerprint{}, fmt.Errorf("mysql: verify %s: range: %w", t, err)
	}
	return fp, nil
}

func (v verifyDB) rangeRowHashes(ctx context.Context, t engine.TableRef, cols []string, after, upTo engine.KeyValues, limit int) ([]engine.RowHash, error) {
	pred, args := v.rangePredicate(after, upTo)
	q := fmt.Sprintf("SELECT %s%s, %s FROM %s WHERE %s ORDER BY %s LIMIT %d",
		timeHint(ctx), v.keyList(""), rowHashExpr(cols), qualify(t.Schema, t.Name), pred, v.keyList(""), limit)
	return v.scanRowHashes(ctx, t, q, args)
}

func (v verifyDB) rowHashes(ctx context.Context, t engine.TableRef, cols []string, keys []engine.KeyValues) ([]engine.RowHash, error) {
	tuple := "(" + placeholders(len(v.key)) + ")"
	var out []engine.RowHash
	// In pieces (engine.MaxKeysPerStatement): the keys to look up again can be many.
	for _, piece := range engine.Pieces(keys, engine.MaxKeysPerStatement) {
		tuples := make([]string, len(piece))
		args := make([]any, 0, len(piece)*len(v.key))
		for i, k := range piece {
			if len(k) != len(v.key) {
				return nil, fmt.Errorf("mysql: verify %s: key %v has %d values, want %d", t, k, len(k), len(v.key))
			}
			tuples[i] = tuple
			args = append(args, keyArgs(k)...)
		}
		q := fmt.Sprintf("SELECT %s%s, %s FROM %s WHERE (%s) IN (%s)",
			timeHint(ctx), v.keyList(""), rowHashExpr(cols), qualify(t.Schema, t.Name), v.keyList(""), strings.Join(tuples, ", "))
		got, err := v.scanRowHashes(ctx, t, q, args)
		if err != nil {
			return nil, err
		}
		out = append(out, got...)
	}
	return out, nil
}

func (v verifyDB) scanRowHashes(ctx context.Context, t engine.TableRef, q string, args []any) ([]engine.RowHash, error) {
	rows, err := v.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("mysql: verify %s: row hashes: %w", t, err)
	}
	defer func() { _ = rows.Close() }()
	n := len(v.key)
	var out []engine.RowHash
	for rows.Next() {
		raw := make([]sql.RawBytes, n+1)
		dest := make([]any, n+1)
		for i := range raw {
			dest[i] = &raw[i]
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}
		key := make(engine.KeyValues, n)
		for i := 0; i < n; i++ {
			if raw[i] != nil {
				key[i] = string(raw[i])
			}
		}
		out = append(out, engine.RowHash{Key: key, Hash: string(raw[n])})
	}
	return out, rows.Err()
}

// sampleKeys picks up to n random starting keys, each with one index lookup: a random
// point between the smallest and largest value of the key's first column, then the
// first key at or after it. Integer columns are sampled as numbers; text and binary
// columns as byte strings between the two ends (for random-looking keys such as UUIDs,
// CUIDs and hashes this spreads the samples well; for skewed text the spread follows
// the byte values). Other types (dates, decimals) fall back to random positions in key
// order, which reads through the key index up to each position and is slower on a
// large table.
func (v verifyDB) sampleKeys(ctx context.Context, t engine.TableRef, n int) ([]engine.KeyValues, error) {
	if n < 1 {
		return nil, nil
	}
	first := v.key[0]
	tbl := qualify(t.Schema, t.Name)
	var lo, hi sql.NullString
	q := fmt.Sprintf("SELECT %sMIN(%s), MAX(%s) FROM %s", timeHint(ctx), bq(first.Name), bq(first.Name), tbl)
	if err := v.db.QueryRowContext(ctx, q).Scan(&lo, &hi); err != nil {
		return nil, fmt.Errorf("mysql: verify %s: key range: %w", t, err)
	}
	if !lo.Valid || !hi.Valid {
		return nil, nil // empty table
	}
	points, ok := randomPoints(first.Type, lo.String, hi.String, n)
	seen := map[string]bool{}
	var out []engine.KeyValues
	add := func(k engine.KeyValues) {
		if k == nil {
			return
		}
		if s := engine.KeyString(k); !seen[s] {
			seen[s] = true
			out = append(out, k)
		}
	}
	if ok {
		at := fmt.Sprintf("SELECT %s%s FROM %s WHERE %s >= ? ORDER BY %s LIMIT 1",
			timeHint(ctx), v.keyList(""), tbl, bq(first.Name), v.keyList(""))
		for _, p := range points {
			k, err := scanKey(v.db.QueryRowContext(ctx, at, p), len(v.key))
			if err != nil && err != errNoBoundary {
				return nil, fmt.Errorf("mysql: verify %s: sample key: %w", t, err)
			}
			add(k)
		}
	} else {
		est, err := estimatedRows(ctx, v.db, t)
		if err != nil {
			return nil, err
		}
		for i := 0; i < n; i++ {
			off := int64(0)
			if est > 1 {
				off = rand.Int63n(est)
			}
			at := fmt.Sprintf("SELECT %s%s FROM %s ORDER BY %s LIMIT 1 OFFSET %d",
				timeHint(ctx), v.keyList(""), tbl, v.keyList(""), off)
			k, err := scanKey(v.db.QueryRowContext(ctx, at), len(v.key))
			if err != nil && err != errNoBoundary {
				return nil, fmt.Errorf("mysql: verify %s: sample key: %w", t, err)
			}
			add(k)
		}
	}
	sort.Slice(out, func(i, j int) bool { return engine.KeyString(out[i]) < engine.KeyString(out[j]) })
	return out, nil
}

// estimatedRows is InnoDB's own row estimate for the table (no scan).
func estimatedRows(ctx context.Context, db *sql.DB, t engine.TableRef) (int64, error) {
	var n sql.NullInt64
	err := db.QueryRowContext(ctx,
		"SELECT TABLE_ROWS FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
		t.Schema, t.Name).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("mysql: verify %s: row estimate: %w", t, err)
	}
	return n.Int64, nil
}

// randomPoints returns n random values between lo and hi for a column of the given
// MySQL column type, as query arguments, or ok=false when the type is not one we can
// interpolate (the caller then samples by position).
func randomPoints(colType, lo, hi string, n int) ([]any, bool) {
	typ := strings.ToLower(colType)
	switch {
	case isIntegerType(typ):
		a, okA := new(big.Int).SetString(lo, 10)
		b, okB := new(big.Int).SetString(hi, 10)
		if !okA || !okB || a.Cmp(b) > 0 {
			return nil, false
		}
		return bigPoints(a, b, n, func(x *big.Int) any { return x.String() }), true
	case isStringType(typ):
		width := len(lo)
		if len(hi) > width {
			width = len(hi)
		}
		if width == 0 || width > 64 {
			return nil, false
		}
		a := new(big.Int).SetBytes(padRight([]byte(lo), width))
		b := new(big.Int).SetBytes(padRight([]byte(hi), width))
		if a.Cmp(b) > 0 {
			return nil, false
		}
		binary := strings.Contains(typ, "binary")
		return bigPoints(a, b, n, func(x *big.Int) any {
			buf := []byte(strings.TrimRight(string(x.FillBytes(make([]byte, width))), "\x00"))
			if !binary {
				// A text column compares in its character set; keep the point plain
				// printable ASCII so it is always a valid string to compare with.
				for i, c := range buf {
					if c < 0x20 || c > 0x7e {
						buf[i] = 0x20 + c%95
					}
				}
			}
			return string(buf)
		}), true
	}
	return nil, false
}

func bigPoints(a, b *big.Int, n int, conv func(*big.Int) any) []any {
	span := new(big.Int).Sub(b, a)
	span.Add(span, big.NewInt(1))
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	out := make([]any, n)
	for i := range out {
		r := new(big.Int).Rand(rng, span)
		out[i] = conv(r.Add(r, a))
	}
	return out
}

func padRight(b []byte, width int) []byte {
	out := make([]byte, width)
	copy(out, b)
	return out
}

func isIntegerType(typ string) bool {
	for _, p := range []string{"tinyint", "smallint", "mediumint", "int", "bigint"} {
		if typ == p || strings.HasPrefix(typ, p+"(") || strings.HasPrefix(typ, p+" ") {
			return true
		}
	}
	return false
}

func isStringType(typ string) bool {
	for _, p := range []string{"char", "varchar", "binary", "varbinary"} {
		if typ == p || strings.HasPrefix(typ, p+"(") {
			return true
		}
	}
	return false
}
