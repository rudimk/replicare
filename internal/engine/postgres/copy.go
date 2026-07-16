package postgres

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/rudimk/replicare/internal/engine"
)

// Chunked bulk copy (CLAUDE.md §4.1, §4.2). CopyChunk streams a chunk's rows as
// text COPY out of the source; the Sink's BulkLoad streams them into the target.
// Piped together via io.Pipe (in the orchestration), a chunk never fully
// buffers. Transport is verbatim text — the source renders each value and the
// target parses it, so we are a pipe, never an interpreter (§4.2).

// CopyChunk streams one chunk as a text COPY into w. The column list is explicit
// and name-matched; GENERATED STORED columns are excluded (the target
// regenerates them). Range bounds are inlined as faithful text literals cast
// back to the key type (COPY's simple protocol has no bind parameters).
func (s *Source) CopyChunk(ctx context.Context, c engine.Chunk, w io.Writer) error {
	if err := s.requireConn(); err != nil {
		return err
	}
	table, err := s.tableMeta(ctx, c.Table)
	if err != nil {
		return err
	}
	cols := transportColumns(table)
	if len(cols) == 0 {
		return fmt.Errorf("postgres: copy chunk: table %s has no transportable columns", c.Table)
	}

	where, err := s.chunkPredicate(table, c)
	if err != nil {
		return err
	}

	sql := fmt.Sprintf("COPY (SELECT %s FROM %s WHERE %s) TO STDOUT",
		quotedColumnList(cols), qualifyTable(c.Table), where)
	if _, err := s.conn.PgConn().CopyTo(ctx, w, sql); err != nil {
		return fmt.Errorf("postgres: copy chunk of %s: %w", c.Table, err)
	}
	return nil
}

// chunkPredicate builds the WHERE clause selecting a chunk's rows.
func (s *Source) chunkPredicate(table engine.Table, c engine.Chunk) (string, error) {
	method := c.Method
	if method == "" {
		method = engine.ChunkKeyset
	}
	switch method {
	case engine.ChunkKeyset:
		keyCols := captureColsFor(table)
		if len(keyCols) == 0 {
			return "", fmt.Errorf("postgres: copy chunk: table %s has no usable key", c.Table)
		}
		return keysetPredicate(keyCols, c.Lo, c.Hi)
	default:
		return "", fmt.Errorf("postgres: copy chunk method %q not implemented yet", method)
	}
}

// keysetPredicate builds a half-open row-value range predicate
// (cols) >= (lo) AND (cols) < (hi). Bounds are text values (the faithful key
// form) cast back to their column types. A nil bound is unbounded on that side.
func keysetPredicate(cols []captureCol, lo, hi engine.KeyValues) (string, error) {
	colTuple := "(" + quotedKeyList(cols) + ")"
	var parts []string
	if lo != nil {
		vt, err := valueTuple(cols, lo)
		if err != nil {
			return "", err
		}
		parts = append(parts, colTuple+" >= "+vt)
	}
	if hi != nil {
		vt, err := valueTuple(cols, hi)
		if err != nil {
			return "", err
		}
		parts = append(parts, colTuple+" < "+vt)
	}
	if len(parts) == 0 {
		return "TRUE", nil
	}
	return strings.Join(parts, " AND "), nil
}

// valueTuple renders a key tuple as ('t1'::type1, 't2'::type2, ...), casting each
// faithful text value back to its column type.
func valueTuple(cols []captureCol, vals engine.KeyValues) (string, error) {
	if len(vals) != len(cols) {
		return "", fmt.Errorf("postgres: key tuple has %d values, want %d", len(vals), len(cols))
	}
	parts := make([]string, len(cols))
	for i, c := range cols {
		sv, ok := vals[i].(string)
		if !ok {
			return "", fmt.Errorf("postgres: key bound %d is %T, want a text value", i, vals[i])
		}
		parts[i] = sqlTextLiteral(sv) + "::" + c.Type
	}
	return "(" + strings.Join(parts, ", ") + ")", nil
}

// transportColumns returns the name-matched columns replicare moves for a table:
// every column except GENERATED STORED ones (the target regenerates those).
// Identity columns ARE transported so source key values carry over (§4.2).
func transportColumns(t engine.Table) []string {
	out := make([]string, 0, len(t.Columns))
	for _, c := range t.Columns {
		if c.Generated {
			continue
		}
		out = append(out, c.Name)
	}
	return out
}

// quotedColumnList renders column names as a quoted, comma-separated list.
func quotedColumnList(cols []string) string {
	parts := make([]string, len(cols))
	for i, c := range cols {
		parts[i] = quoteIdentifier(c)
	}
	return strings.Join(parts, ", ")
}

// sqlTextLiteral single-quotes a string literal, doubling embedded quotes.
// standard_conforming_strings is on by default (>=9.1), so backslashes are
// literal and quote-doubling is sufficient.
func sqlTextLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// tableMeta returns cached introspected metadata for a table, introspecting on
// first use.
func (s *Source) tableMeta(ctx context.Context, ref engine.TableRef) (engine.Table, error) {
	if t, ok := s.meta[ref]; ok {
		return t, nil
	}
	m, err := s.introspectTables(ctx, []engine.TableRef{ref})
	if err != nil {
		return engine.Table{}, err
	}
	t, ok := m[ref]
	if !ok {
		return engine.Table{}, fmt.Errorf("postgres: table %s not found on source", ref)
	}
	if s.meta == nil {
		s.meta = make(map[engine.TableRef]engine.Table)
	}
	s.meta[ref] = t
	return t, nil
}
