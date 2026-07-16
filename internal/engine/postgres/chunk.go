package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/rudimk/replicare/internal/engine"
)

// Chunked copy planning (CLAUDE.md §4.1). Keyset PK-range is the default:
// balanced [lo, hi) ranges discovered by an index-only boundary scan that
// collects every Nth key, so ranges hold ~N rows regardless of key
// distribution. Ranges compose from row-value comparisons, so composite, UUID,
// and text keys all work. The cursor IS a key, which makes copy resumable and
// robust to concurrent row movement (PK changes are reconciled by the delta
// layer, §3.1).

// defaultChunkRows is the target rows-per-chunk when ChunkOptions.TargetRows is
// unset. Conservative by default; tuned via config.
const defaultChunkRows = 100_000

// PlanChunks computes balanced copy chunks for a table. The keyset method (the
// default) requires a usable key; callers force ctid via ChunkOptions.Method for
// keyless or pathological tables (M4 slice: ctid fallback).
func (s *Source) PlanChunks(ctx context.Context, t engine.TableRef, opts engine.ChunkOptions) ([]engine.Chunk, error) {
	if err := s.requireConn(); err != nil {
		return nil, err
	}
	method := opts.Method
	if method == "" {
		method = engine.ChunkKeyset
	}
	targetRows := opts.TargetRows
	if targetRows <= 0 {
		targetRows = defaultChunkRows
	}

	switch method {
	case engine.ChunkKeyset:
		return s.planKeysetChunks(ctx, t, targetRows)
	default:
		return nil, fmt.Errorf("postgres: chunk method %q not implemented yet", method)
	}
}

// planKeysetChunks discovers balanced key-range boundaries and builds [lo, hi)
// chunks from them.
func (s *Source) planKeysetChunks(ctx context.Context, t engine.TableRef, targetRows int) ([]engine.Chunk, error) {
	tbl, err := s.introspectTables(ctx, []engine.TableRef{t})
	if err != nil {
		return nil, err
	}
	table, ok := tbl[t]
	if !ok {
		return nil, fmt.Errorf("postgres: plan chunks: table %s not found", t)
	}
	cols := captureColsFor(table)
	if len(cols) == 0 {
		return nil, fmt.Errorf("postgres: plan chunks: table %s has no usable key for keyset chunking (use ctid)", t)
	}

	boundaries, err := s.keysetBoundaries(ctx, t, cols, targetRows)
	if err != nil {
		return nil, err
	}
	return buildKeysetRanges(t, boundaries), nil
}

// keysetBoundaries returns the lower-bound key of each chunk after the first: an
// index-only scan numbers rows in key order and returns every Nth key. Empty
// result (table smaller than one chunk) means a single unbounded chunk.
func (s *Source) keysetBoundaries(ctx context.Context, t engine.TableRef, cols []captureCol, n int) ([]engine.KeyValues, error) {
	keyList := quotedKeyList(cols)
	q := fmt.Sprintf(`
		SELECT %s FROM (
			SELECT %s, row_number() OVER (ORDER BY %s) AS rn
			FROM %s
		) s
		WHERE (rn - 1) %% $1 = 0 AND rn > 1
		ORDER BY rn`,
		keyList, keyList, keyList, qualifyTable(t))

	rows, err := s.conn.Query(ctx, q, n)
	if err != nil {
		return nil, fmt.Errorf("postgres: discover keyset boundaries for %s: %w", t, err)
	}
	defer rows.Close()

	var out []engine.KeyValues
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, fmt.Errorf("postgres: scan boundary: %w", err)
		}
		out = append(out, engine.KeyValues(vals))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: discover keyset boundaries for %s: %w", t, err)
	}
	return out, nil
}

// buildKeysetRanges turns ordered chunk lower-bounds into contiguous, gap-free
// [lo, hi) chunks: the first chunk is unbounded below, the last unbounded above,
// so together they cover the whole key space (and any concurrently inserted
// rows). With no boundaries, a single fully-unbounded chunk.
func buildKeysetRanges(t engine.TableRef, boundaries []engine.KeyValues) []engine.Chunk {
	out := make([]engine.Chunk, 0, len(boundaries)+1)
	var prev engine.KeyValues // nil = unbounded below
	for _, b := range boundaries {
		out = append(out, engine.Chunk{Table: t, Method: engine.ChunkKeyset, Lo: prev, Hi: b})
		prev = b
	}
	out = append(out, engine.Chunk{Table: t, Method: engine.ChunkKeyset, Lo: prev, Hi: nil})
	return out
}

// quotedKeyList renders the ordered key columns as a quoted, comma-separated
// list for SELECT / ORDER BY.
func quotedKeyList(cols []captureCol) string {
	parts := make([]string, len(cols))
	for i, c := range cols {
		parts[i] = quoteIdentifier(c.Name)
	}
	return strings.Join(parts, ", ")
}
