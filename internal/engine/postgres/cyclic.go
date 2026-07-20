package postgres

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/rudimk/replicare/internal/engine"
)

// Cyclic-FK initial-copy strategies (CLAUDE.md §4.1). A cyclic or self-
// referential FK component cannot be loaded parents-first (there is no valid
// topo order), so it is loaded by one of two strategies, never by touching the
// target's constraints:
//
//   - DEFERRABLE FKs -> load the whole component inside one transaction with
//     SET CONSTRAINTS ALL DEFERRED, so FKs are only checked at commit.
//   - Nullable (non-deferrable) self-ref FK -> NULL-then-fill two-pass: insert
//     rows with the FK column(s) NULL (MATCH SIMPLE leaves the FK unchecked),
//     then a second pass fills the real values once all rows exist.
//
// The NOT NULL + non-deferrable cyclic case is unloadable without disabling
// constraints and is rejected at M1 pre-flight — it never reaches here.
//
// Cyclic components are small, so these paths copy whole tables (unchunked) and
// run serially on one Source/Sink.

// LoadCyclicDeferred copies the given component tables into the target inside one
// transaction with SET CONSTRAINTS ALL DEFERRED. It requires the target FKs to be
// DEFERRABLE (the M1 classification guarantees this for the deferred strategy).
func LoadCyclicDeferred(ctx context.Context, src *Source, sink *Sink, tables []engine.TableRef) error {
	if err := src.requireConn(); err != nil {
		return err
	}
	if sink.conn == nil {
		return errNotConnected("sink")
	}
	if _, err := sink.conn.Exec(ctx, "BEGIN"); err != nil {
		return fmt.Errorf("postgres: cyclic deferred: begin: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = sink.conn.Exec(context.Background(), "ROLLBACK")
		}
	}()
	if _, err := sink.conn.Exec(ctx, "SET CONSTRAINTS ALL DEFERRED"); err != nil {
		return fmt.Errorf("postgres: cyclic deferred: defer constraints: %w", err)
	}
	for _, ref := range tables {
		table, err := src.tableMeta(ctx, ref)
		if err != nil {
			return err
		}
		cols := transportColumns(table)
		sql := fmt.Sprintf("COPY %s (%s) FROM STDIN", qualifyTable(ref), quotedColumnList(cols))
		if err := pipeCopy(ctx, src, ref, cols, sink, sql); err != nil {
			return fmt.Errorf("postgres: cyclic deferred: copy %s: %w", ref, err)
		}
	}
	if _, err := sink.conn.Exec(ctx, "COMMIT"); err != nil {
		return fmt.Errorf("postgres: cyclic deferred: commit: %w", err)
	}
	committed = true
	return nil
}

// LoadCyclicNullFill copies a self-referential table whose cyclic FK columns are
// nullable, via NULL-then-fill: pass 1 loads every row with the FK column(s)
// omitted (NULL on the target), pass 2 fills them from the source once all rows
// exist.
func LoadCyclicNullFill(ctx context.Context, src *Source, sink *Sink, ref engine.TableRef) error {
	if err := src.requireConn(); err != nil {
		return err
	}
	if sink.conn == nil {
		return errNotConnected("sink")
	}
	table, err := src.tableMeta(ctx, ref)
	if err != nil {
		return err
	}
	fkCols := selfRefFKCols(table)
	if len(fkCols) == 0 {
		return fmt.Errorf("postgres: null-fill: %s has no self-referential FK columns", ref)
	}
	pk := captureColsFor(table)
	if len(pk) == 0 {
		return fmt.Errorf("postgres: null-fill: %s has no usable key", ref)
	}

	all := transportColumns(table)
	pass1 := subtractCols(all, fkCols)

	// Pass 1: load every row with the FK column(s) omitted (NULL on target).
	sql1 := fmt.Sprintf("COPY %s (%s) FROM STDIN", qualifyTable(ref), quotedColumnList(pass1))
	if err := pipeCopy(ctx, src, ref, pass1, sink, sql1); err != nil {
		return fmt.Errorf("postgres: null-fill pass 1 (%s): %w", ref, err)
	}

	// Pass 2: fill the FK column(s) from the source via a TEMP staging table.
	return src.fillFKColumns(ctx, sink, ref, table, pk, fkCols)
}

// fillFKColumns copies (pk + fk) tuples into a TEMP table on the target and
// UPDATEs the target rows' FK columns from it, all in one transaction.
func (s *Source) fillFKColumns(ctx context.Context, sink *Sink, ref engine.TableRef, table engine.Table, pk []captureCol, fkCols []string) error {
	tgt, err := sink.tableMeta(ctx, ref)
	if err != nil {
		return err
	}
	typeByName := make(map[string]string, len(tgt.Columns))
	for _, c := range tgt.Columns {
		typeByName[c.Name] = c.DataType
	}

	pkNames := make([]string, len(pk))
	for i, c := range pk {
		pkNames[i] = c.Name
	}
	fillCols := append(append([]string{}, pkNames...), fkCols...)

	stgCols := make([]string, len(fillCols))
	for i, c := range fillCols {
		stgCols[i] = quoteIdentifier(c) + " " + typeByName[c]
	}

	if _, err := sink.conn.Exec(ctx, "BEGIN"); err != nil {
		return fmt.Errorf("postgres: null-fill pass 2: begin: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = sink.conn.Exec(context.Background(), "ROLLBACK")
		}
	}()

	const stg = "replicare_fill"
	if _, err := sink.conn.Exec(ctx, fmt.Sprintf("CREATE TEMP TABLE %s (%s) ON COMMIT DROP",
		quoteIdentifier(stg), strings.Join(stgCols, ", "))); err != nil {
		return fmt.Errorf("postgres: null-fill pass 2: staging: %w", err)
	}
	copySQL := fmt.Sprintf("COPY %s (%s) FROM STDIN", quoteIdentifier(stg), quotedColumnList(fillCols))
	if err := pipeCopy(ctx, s, ref, fillCols, sink, copySQL); err != nil {
		return fmt.Errorf("postgres: null-fill pass 2: copy: %w", err)
	}

	setParts := make([]string, len(fkCols))
	for i, c := range fkCols {
		setParts[i] = fmt.Sprintf("%s = s.%s", quoteIdentifier(c), quoteIdentifier(c))
	}
	joinParts := make([]string, len(pkNames))
	for i, c := range pkNames {
		joinParts[i] = fmt.Sprintf("t.%s = s.%s", quoteIdentifier(c), quoteIdentifier(c))
	}
	upd := fmt.Sprintf("UPDATE %s t SET %s FROM %s s WHERE %s",
		qualifyTable(ref), strings.Join(setParts, ", "), quoteIdentifier(stg), strings.Join(joinParts, " AND "))
	if _, err := sink.conn.Exec(ctx, upd); err != nil {
		return fmt.Errorf("postgres: null-fill pass 2: update: %w", err)
	}
	if _, err := sink.conn.Exec(ctx, "COMMIT"); err != nil {
		return fmt.Errorf("postgres: null-fill pass 2: commit: %w", err)
	}
	committed = true
	return nil
}

// pipeCopy streams an explicit column subset of a whole source table into the
// target via the given COPY FROM STDIN statement, through an io.Pipe.
func pipeCopy(ctx context.Context, src *Source, ref engine.TableRef, cols []string, sink *Sink, sinkCopySQL string) error {
	pr, pw := io.Pipe()
	errc := make(chan error, 1)
	go func() {
		err := src.copyAllCols(ctx, ref, cols, pw)
		_ = pw.CloseWithError(err)
		errc <- err
	}()
	_, loadErr := sink.conn.PgConn().CopyFrom(ctx, pr, sinkCopySQL)
	_ = pr.CloseWithError(loadErr)
	copyErr := <-errc
	if copyErr != nil {
		return fmt.Errorf("read side: %w", copyErr)
	}
	if loadErr != nil {
		return fmt.Errorf("write side: %w", loadErr)
	}
	return nil
}

// selfRefFKCols returns the deduplicated child columns of every self-referential
// FK on a table (FKs whose parent is the table itself), in first-seen order.
func selfRefFKCols(table engine.Table) []string {
	var out []string
	seen := map[string]bool{}
	for _, fk := range table.ForeignKeys {
		if fk.Parent != table.Ref {
			continue
		}
		for _, c := range fk.ChildCols {
			if !seen[c] {
				seen[c] = true
				out = append(out, c)
			}
		}
	}
	return out
}

// subtractCols returns cols with any name in remove filtered out, order preserved.
func subtractCols(cols, remove []string) []string {
	rm := make(map[string]bool, len(remove))
	for _, c := range remove {
		rm[c] = true
	}
	out := make([]string, 0, len(cols))
	for _, c := range cols {
		if !rm[c] {
			out = append(out, c)
		}
	}
	return out
}
