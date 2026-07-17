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

// PlanChunks computes balanced copy chunks for a table. Keyset is the default,
// automatically falling back to ctid/block-range for a keyless table; callers
// can force ctid via ChunkOptions.Method for pathological key distributions.
//
// Native declarative partitions: a partitioned parent copies correctly via
// keyset/ctid over the parent (COPY reads all child partitions), so no special
// path is needed for correctness. Per-partition *parallel* chunking (the "free
// win" of §4.1) is a deferred optimization — and unexercisable on our matrix
// anyway, since the 9.6 source floor predates declarative partitioning (PG10+).
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
	case engine.ChunkCTID:
		return s.planCtidChunks(ctx, t, targetRows)
	default:
		return nil, fmt.Errorf("postgres: chunk method %q not implemented", method)
	}
}

// planKeysetChunks discovers balanced key-range boundaries and builds [lo, hi)
// chunks. A table with no usable key automatically falls back to ctid/block-range
// chunking (CLAUDE.md §4.1).
func (s *Source) planKeysetChunks(ctx context.Context, t engine.TableRef, targetRows int) ([]engine.Chunk, error) {
	table, err := s.tableMeta(ctx, t)
	if err != nil {
		return nil, err
	}
	cols := captureColsFor(table)
	if len(cols) == 0 {
		// No usable key — fall back to ctid/block-range chunking.
		return s.planCtidChunks(ctx, t, targetRows)
	}

	boundaries, err := s.keysetBoundaries(ctx, t, cols, targetRows)
	if err != nil {
		return nil, err
	}
	return buildKeysetRanges(t, boundaries), nil
}

// planCtidChunks chunks a table by physical block range (CLAUDE.md §4.1): the
// fallback for keyless or pathological tables. Block ranges are sized from
// pg_class.relpages/reltuples so each chunk holds ~targetRows. ctid is unstable
// under concurrent writes — acceptable, since moved or missed rows are reconciled
// by the delta layer (§3.1).
func (s *Source) planCtidChunks(ctx context.Context, t engine.TableRef, targetRows int) ([]engine.Chunk, error) {
	var relpages int
	var reltuples float64
	err := s.conn.QueryRow(ctx,
		`SELECT relpages, reltuples FROM pg_class WHERE oid = $1::regclass`, qualifyTable(t)).
		Scan(&relpages, &reltuples)
	if err != nil {
		return nil, fmt.Errorf("postgres: read block stats for %s: %w", t, err)
	}

	// No stats yet (never analyzed) or a tiny table: one unbounded chunk.
	if relpages <= 0 {
		return []engine.Chunk{{Table: t, Method: engine.ChunkCTID}}, nil
	}

	blocksPerChunk := 1
	if reltuples > 0 {
		rowsPerPage := reltuples / float64(relpages)
		if rowsPerPage > 0 {
			blocksPerChunk = int(float64(targetRows) / rowsPerPage)
		}
	}
	if blocksPerChunk < 1 {
		blocksPerChunk = 1
	}

	var boundaries []int
	for b := blocksPerChunk; b < relpages; b += blocksPerChunk {
		boundaries = append(boundaries, b)
	}
	return buildCtidRanges(t, boundaries), nil
}

// buildCtidRanges turns block boundaries into contiguous ctid chunks: first
// unbounded below, last unbounded above (so it catches pages appended after
// planning). Bounds are block numbers rendered as '(block,0)'::tid predicates.
func buildCtidRanges(t engine.TableRef, boundaries []int) []engine.Chunk {
	out := make([]engine.Chunk, 0, len(boundaries)+1)
	var prev engine.KeyValues
	for _, b := range boundaries {
		out = append(out, engine.Chunk{Table: t, Method: engine.ChunkCTID, Lo: prev, Hi: engine.KeyValues{b}})
		prev = engine.KeyValues{b}
	}
	out = append(out, engine.Chunk{Table: t, Method: engine.ChunkCTID, Lo: prev, Hi: nil})
	return out
}

// keysetBoundaries returns the lower-bound key of each chunk after the first: an
// index-only scan numbers rows in key order and returns every Nth key. Empty
// result (table smaller than one chunk) means a single unbounded chunk.
//
// Boundaries are captured in their Postgres TEXT form (via ::text). Range
// predicates then cast them back with 'text'::coltype, so Postgres's own text
// I/O round-trips every key type exactly — faithful and precision-safe (§4.2),
// and inlinable as literals (COPY's simple protocol has no bind parameters).
func (s *Source) keysetBoundaries(ctx context.Context, t engine.TableRef, cols []captureCol, n int) ([]engine.KeyValues, error) {
	keyList := quotedKeyList(cols)
	textList := quotedKeyListAsText(cols)
	q := fmt.Sprintf(`
		SELECT %s FROM (
			SELECT %s, row_number() OVER (ORDER BY %s) AS rn
			FROM %s
		) s
		WHERE (rn - 1) %% $1 = 0 AND rn > 1
		ORDER BY rn`,
		textList, keyList, keyList, qualifyTable(t))

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

// quotedKeyListAsText renders the ordered key columns cast to text, so boundary
// values are captured in their faithful Postgres text representation.
func quotedKeyListAsText(cols []captureCol) string {
	parts := make([]string, len(cols))
	for i, c := range cols {
		parts[i] = quoteIdentifier(c.Name) + "::text"
	}
	return strings.Join(parts, ", ")
}
