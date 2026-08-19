package postgres

import (
	"context"
	"fmt"
	"io"
	"sort"
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

// CopyCyclicComponent is the engine.CyclicComponentCopier entry point: it copies
// an FK component that contains a cycle or self-reference (single- or multi-table)
// from src into this sink, choosing a cycle-safe strategy (CLAUDE.md §4.1). It is
// invoked by the copy pipeline in place of the plain parents-first chunked copy,
// which has no valid order for a cyclic component.
//
// Strategy: if every cyclic FK is DEFERRABLE, load the whole component in one
// SET CONSTRAINTS ALL DEFERRED transaction. Otherwise (a nullable, non-deferrable
// cycle) use NULL-then-fill across the ENTIRE component — copy every table with its
// cyclic FK columns omitted (NULL on the target), in an order that respects only
// the NON-cyclic edges, then fill those columns from the source. This generalizes
// the old single-table self-ref null-fill to multi-table cycles (e.g. orders.user_id
// <-> users.primary_order_id).
//
// Tables are copied whole (unchunked) here — a documented tradeoff: a cyclic
// component gives up chunked/parallel copy for a correct load. The blocked case
// (NOT NULL + non-deferrable) never reaches here (pre-flight fails loud).
func (s *Sink) CopyCyclicComponent(ctx context.Context, src engine.Source, tables []engine.TableRef) error {
	pgSrc, ok := src.(*Source)
	if !ok {
		return fmt.Errorf("postgres: cyclic copy: source is %T, want *postgres.Source", src)
	}
	if s.conn == nil {
		return errNotConnected("sink")
	}

	// Introspect the component tables to get columns, FKs, and nullability.
	inc := make([]string, len(tables))
	for i, t := range tables {
		inc[i] = t.String()
	}
	schema, err := pgSrc.Introspect(ctx, engine.Selection{Include: inc})
	if err != nil {
		return fmt.Errorf("postgres: cyclic copy: introspect: %w", err)
	}
	inComp := make(map[engine.TableRef]bool, len(tables))
	for _, t := range tables {
		inComp[t] = true
	}
	var members []engine.Table
	for _, t := range schema.Tables {
		if inComp[t.Ref] {
			members = append(members, t)
		}
	}

	cyc := classifyCyclicFKs(members)
	if anyBlockedCyclicFK(cyc) {
		// Should be unreachable — pre-flight blocks this — but never load blindly.
		return fmt.Errorf("postgres: cyclic copy: component has a NOT NULL non-deferrable cyclic FK; make it DEFERRABLE or break the cycle")
	}

	// All cyclic FKs deferrable -> one deferred transaction over the whole component.
	if allDeferrable(cyc) {
		return LoadCyclicDeferred(ctx, pgSrc, s, orderMembers(members))
	}

	// NULL-then-fill: null every cyclic FK column, copy in non-cyclic order, then fill.
	return s.nullFillComponent(ctx, pgSrc, members, cyc)
}

// nullFillComponent runs the multi-table NULL-then-fill: copy each table with its
// cyclic FK columns omitted (in an order that respects only the non-cyclic FK
// edges, so a table always loads after its non-cyclic parents), then fill the
// cyclic FK columns from the source.
func (s *Sink) nullFillComponent(ctx context.Context, src *Source, members []engine.Table, cyc []CyclicFK) error {
	byRef := make(map[engine.TableRef]engine.Table, len(members))
	for _, t := range members {
		byRef[t.Ref] = t
	}

	// Cyclic FK child columns per table (the columns to NULL then fill).
	cyclicCols := cyclicColsByTable(cyc)
	cyclicEdge := make(map[string]bool, len(cyc))
	for _, c := range cyc {
		cyclicEdge[fkKey(c.FK)] = true
	}

	order := nonCyclicTopoOrder(members, cyclicEdge)

	// Pass 1: copy every table, omitting its cyclic FK columns (NULL on the target).
	for _, ref := range order {
		table := byRef[ref]
		cols := subtractCols(transportColumns(table), cyclicCols[ref])
		sql := fmt.Sprintf("COPY %s (%s) FROM STDIN", qualifyTable(ref), quotedColumnList(cols))
		if err := pipeCopy(ctx, src, ref, cols, s, sql); err != nil {
			return fmt.Errorf("postgres: cyclic null-fill pass 1 (%s): %w", ref, err)
		}
	}

	// Pass 2: fill the cyclic FK columns of each table that has them, in the same
	// order (every referenced row now exists, so the FK holds when filled).
	for _, ref := range order {
		fkCols := cyclicCols[ref]
		if len(fkCols) == 0 {
			continue
		}
		table := byRef[ref]
		pk := captureColsFor(table)
		if len(pk) == 0 {
			return fmt.Errorf("postgres: cyclic null-fill: %s has no usable key to fill by", ref)
		}
		if err := src.fillFKColumns(ctx, s, ref, table, pk, fkCols); err != nil {
			return fmt.Errorf("postgres: cyclic null-fill pass 2 (%s): %w", ref, err)
		}
	}
	return nil
}

// nonCyclicTopoOrder topologically sorts the component members using only the
// non-cyclic in-component FK edges (cyclic edges, identified by cyclicEdge, are
// removed first), so the result is a valid parents-before-children order for the
// acyclic remainder. Ties break by qualified name for determinism.
func nonCyclicTopoOrder(members []engine.Table, cyclicEdge map[string]bool) []engine.TableRef {
	inComp := make(map[engine.TableRef]bool, len(members))
	for _, t := range members {
		inComp[t.Ref] = true
	}
	children := map[engine.TableRef][]engine.TableRef{}
	indeg := map[engine.TableRef]int{}
	for _, t := range members {
		indeg[t.Ref] = 0
	}
	for _, t := range members {
		for _, fk := range t.ForeignKeys {
			if !inComp[fk.Parent] || cyclicEdge[fkKey(fk)] || fk.Child == fk.Parent {
				continue
			}
			children[fk.Parent] = append(children[fk.Parent], fk.Child)
			indeg[fk.Child]++
		}
	}
	var queue []engine.TableRef
	for ref, d := range indeg {
		if d == 0 {
			queue = append(queue, ref)
		}
	}
	sortRefs(queue)
	var order []engine.TableRef
	for len(queue) > 0 {
		n := queue[0]
		queue = queue[1:]
		order = append(order, n)
		var ready []engine.TableRef
		for _, c := range children[n] {
			indeg[c]--
			if indeg[c] == 0 {
				ready = append(ready, c)
			}
		}
		sortRefs(ready)
		queue = append(queue, ready...)
	}
	// Any leftover (shouldn't happen once cyclic edges are removed) appended stably.
	if len(order) < len(members) {
		emitted := make(map[engine.TableRef]bool, len(order))
		for _, r := range order {
			emitted[r] = true
		}
		var rest []engine.TableRef
		for _, t := range members {
			if !emitted[t.Ref] {
				rest = append(rest, t.Ref)
			}
		}
		sortRefs(rest)
		order = append(order, rest...)
	}
	return order
}

// orderMembers returns the member table refs sorted by qualified name (order is
// irrelevant for the deferred strategy, which checks FKs only at commit).
func orderMembers(members []engine.Table) []engine.TableRef {
	refs := make([]engine.TableRef, len(members))
	for i, t := range members {
		refs[i] = t.Ref
	}
	sortRefs(refs)
	return refs
}

// sortRefs orders table refs by qualified name in place (determinism).
func sortRefs(refs []engine.TableRef) {
	sort.Slice(refs, func(i, j int) bool { return refs[i].String() < refs[j].String() })
}

// allDeferrable reports whether every classified cyclic FK uses the deferred
// strategy (so the whole component can load in one SET CONSTRAINTS DEFERRED txn).
func allDeferrable(cyc []CyclicFK) bool {
	if len(cyc) == 0 {
		return false
	}
	for _, c := range cyc {
		if c.Strategy != CyclicDeferred {
			return false
		}
	}
	return true
}

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
