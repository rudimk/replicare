package mysql

import (
	"context"
	"fmt"
	"strings"

	"github.com/rudimk/replicare/internal/engine"
)

const defaultChunkRows = 100000

// PlanChunks computes balanced keyset PK-range chunks for a table (§4.1). MySQL
// has no ctid analog and needs none — InnoDB is PK-clustered, so keyset already
// scans in physical order (mysql-plan §0.3); no-key tables are skipped upstream.
//
// Boundaries come from a deterministic keyset-LIMIT walk (universal across
// 5.7/8.x, defined-order), not window functions or user variables (§0.3/Momus
// M8): repeatedly take the key N rows past the previous boundary. Ordering uses
// the key's OWN collation (a plain ORDER BY, no forced COLLATE): because the key
// is UNIQUE under that collation, its actual values are a strict total order, so
// keyset paging cannot skip/duplicate boundary rows (the §0.3/Momus M7 hazard
// only arises when the ordering collation differs from the key's uniqueness
// collation — here they are the same by construction). Using the key's own
// collation also keeps the index usable and avoids COLLATE-vs-charset errors.
func (s *Source) PlanChunks(ctx context.Context, t engine.TableRef, opts engine.ChunkOptions) ([]engine.Chunk, error) {
	if s.db == nil {
		return nil, errNotConnected
	}
	tbl, err := s.tableMeta(ctx, t)
	if err != nil {
		return nil, err
	}
	cols := captureColsFor(tbl)
	if len(cols) == 0 {
		return nil, fmt.Errorf("mysql: plan chunks: table %s has no usable key", t)
	}
	n := opts.TargetRows
	if n <= 0 {
		n = defaultChunkRows
	}

	boundaries, err := s.keysetBoundaries(ctx, t, cols, n, opts.Lo)
	if err != nil {
		return nil, err
	}
	return buildKeysetRanges(t, opts.Lo, boundaries), nil
}

// keysetBoundaries returns the ordered chunk lower-bounds: every Nth key value
// starting from lo (or the table start). Each is one bounded index scan.
func (s *Source) keysetBoundaries(ctx context.Context, t engine.TableRef, cols []captureCol, n int, lo engine.KeyValues) ([]engine.KeyValues, error) {
	keyCols := make([]string, len(cols))
	for i, c := range cols {
		keyCols[i] = bq(c.Name)
	}
	keyList := strings.Join(keyCols, ", ")
	orderBy := keyList

	var boundaries []engine.KeyValues
	cur := lo
	for {
		var (
			where string
			args  []any
		)
		if cur != nil {
			where = fmt.Sprintf("WHERE (%s) >= (%s)", keyList, placeholders(len(cols)))
			args = keyArgs(cur)
		}
		q := fmt.Sprintf("SELECT %s FROM %s %s ORDER BY %s LIMIT 1 OFFSET %d",
			keyList, qualify(t.Schema, t.Name), where, orderBy, n)

		row := s.db.QueryRowContext(ctx, q, args...)
		next, err := scanKey(row, len(cols))
		if err == errNoBoundary {
			break // fewer than N rows past cur — last chunk is unbounded
		}
		if err != nil {
			return nil, fmt.Errorf("mysql: discover keyset boundaries for %s: %w", t, err)
		}
		boundaries = append(boundaries, next)
		cur = next
		if len(boundaries) > maxChunks {
			return nil, fmt.Errorf("mysql: table %s produced more than %d chunks; raise chunk size", t, maxChunks)
		}
	}
	return boundaries, nil
}

const maxChunks = 1_000_000

// buildKeysetRanges turns ordered lower-bounds into contiguous half-open chunks.
// The first chunk starts at lo (nil = unbounded); the last is unbounded above.
func buildKeysetRanges(t engine.TableRef, lo engine.KeyValues, boundaries []engine.KeyValues) []engine.Chunk {
	bounds := append([]engine.KeyValues{lo}, boundaries...)
	chunks := make([]engine.Chunk, 0, len(bounds))
	for i := range bounds {
		var hi engine.KeyValues
		if i+1 < len(bounds) {
			hi = bounds[i+1]
		}
		chunks = append(chunks, engine.Chunk{Table: t, Method: engine.ChunkKeyset, Lo: bounds[i], Hi: hi})
	}
	return chunks
}

// keysetPredicate builds the WHERE clause + args for a chunk's [lo, hi) range
// using row-value tuple comparison. A nil bound is unbounded on that side.
func keysetPredicate(cols []captureCol, lo, hi engine.KeyValues) (string, []any) {
	keyCols := make([]string, len(cols))
	for i, c := range cols {
		keyCols[i] = bq(c.Name)
	}
	keyList := strings.Join(keyCols, ", ")
	var conds []string
	var args []any
	if lo != nil {
		conds = append(conds, fmt.Sprintf("(%s) >= (%s)", keyList, placeholders(len(cols))))
		args = append(args, keyArgs(lo)...)
	}
	if hi != nil {
		conds = append(conds, fmt.Sprintf("(%s) < (%s)", keyList, placeholders(len(cols))))
		args = append(args, keyArgs(hi)...)
	}
	if len(conds) == 0 {
		return "", nil
	}
	return strings.Join(conds, " AND "), args
}

func placeholders(n int) string {
	ps := make([]string, n)
	for i := range ps {
		ps[i] = "?"
	}
	return strings.Join(ps, ", ")
}

// keyArgs converts KeyValues to driver args. Values are raw bytes / strings from
// boundary discovery; passed as parameters they compare faithfully.
func keyArgs(k engine.KeyValues) []any {
	args := make([]any, len(k))
	copy(args, k)
	return args
}
