package postgres

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

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
	// classifyFKViolation marks a live-source transient FK skew (§3.3/§4) transient so
	// the syncer's coarse retry recovers instead of the daemon crash-looping.
	if allDeferrable(cyc) {
		return classifyFKViolation(LoadCyclicDeferred(ctx, pgSrc, s, orderMembers(members)))
	}

	// NULL-then-fill: null every cyclic FK column, copy in non-cyclic order, then fill.
	return classifyFKViolation(s.nullFillComponent(ctx, pgSrc, members, cyc))
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
	// Non-cyclic in-component parents per child: the tables whose rows a child's
	// KEPT (non-nulled) FK columns reference. On a LIVE source these parents keep
	// gaining rows while the copy runs, so a child copied from a later snapshot can
	// reference a parent row not yet in the parent's earlier snapshot — a transient
	// FK violation (§3.3, §4). We recover by re-copying just those parents.
	parents := nonCyclicParents(members, cyclicEdge)

	// Pass 1: copy every table, omitting its cyclic FK columns (NULL on the target).
	// Empty target → a plain streaming COPY (light: no staging, no whole-table upsert);
	// non-empty target → an idempotent staged upsert so a re-copy over existing rows
	// converges instead of colliding on the PK. See copyOrUpsert.
	for _, ref := range order {
		table := byRef[ref]
		cols := subtractCols(transportColumns(table), cyclicCols[ref])
		if err := copyChildWithParentRetry(ctx, src, s, ref, cols, parents[ref], byRef, cyclicCols); err != nil {
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
	// Probe each target's emptiness BEFORE opening the deferred transaction, so an
	// empty table takes the light direct COPY and only a populated one pays for the
	// staged upsert (see copyOrUpsert rationale).
	empty := make(map[engine.TableRef]bool, len(tables))
	for _, ref := range tables {
		e, err := targetEmpty(ctx, sink, ref)
		if err != nil {
			return fmt.Errorf("postgres: cyclic deferred: %w", err)
		}
		empty[ref] = e
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
	for i, ref := range tables {
		table, err := src.tableMeta(ctx, ref)
		if err != nil {
			return err
		}
		cols := transportColumns(table)
		if empty[ref] {
			// Empty target: light direct COPY into the deferred txn (FK checks fire at
			// COMMIT). No whole-table staging/upsert — that can't complete on a very
			// large table.
			sql := fmt.Sprintf("COPY %s (%s) FROM STDIN", qualifyTable(ref), quotedColumnList(cols))
			if err := pipeCopy(ctx, src, ref, cols, sink, sql); err != nil {
				return fmt.Errorf("postgres: cyclic deferred: copy %s: %w", ref, err)
			}
			continue
		}
		// Non-empty target: idempotent staged upsert within the deferred transaction.
		// Distinct staging name because ON COMMIT DROP fires only at the outer COMMIT.
		stg := fmt.Sprintf("replicare_stg_%d", i)
		if err := stagedUpsertCopy(ctx, src, sink, ref, cols, stg, false); err != nil {
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

	// Pass 1: load every row with the FK column(s) omitted (NULL on target). Empty
	// target → light direct COPY; non-empty → idempotent chunked staged upsert.
	if err := copyOrUpsert(ctx, src, sink, ref, pass1); err != nil {
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

// stagedUpsertCopy streams an explicit column subset of the source table into a TEMP
// staging table on the target, then INSERT ... ON CONFLICT DO UPDATE into the target —
// the idempotent, populated-target-safe equivalent of a direct COPY (it is the cyclic-
// copy analogue of Sink.mergeLoad, which the chunked copy path uses). A re-copy into a
// target that already holds these rows therefore converges instead of erroring on the
// primary key, matching the empty-vs-non-empty handling of the acyclic path.
//
// ownTxn=true wraps the load in its own BEGIN/COMMIT (used by the NULL-fill paths,
// where each table loads independently under live FK checks). ownTxn=false runs inside
// the caller's already-open transaction (the deferred strategy's single SET CONSTRAINTS
// DEFERRED txn); there the caller must pass a distinct stg name per table, since the
// staging tables' ON COMMIT DROP only fires at the one outer COMMIT.
func stagedUpsertCopy(ctx context.Context, src *Source, sink *Sink, ref engine.TableRef, cols []string, stg string, ownTxn bool) error {
	table, err := sink.tableMeta(ctx, ref)
	if err != nil {
		return err
	}
	pk := captureColsFor(table)
	if len(pk) == 0 {
		return fmt.Errorf("postgres: staged upsert: target %s has no usable key for ON CONFLICT", ref)
	}
	typeByName := make(map[string]string, len(table.Columns))
	colSet := make(map[string]bool, len(cols))
	for _, c := range cols {
		colSet[c] = true
	}
	identity := false
	for _, c := range table.Columns {
		typeByName[c.Name] = c.DataType
		if c.Identity && colSet[c.Name] {
			identity = true
		}
	}
	stgCols := make([]string, len(cols))
	for i, c := range cols {
		stgCols[i] = quoteIdentifier(c) + " " + typeByName[c]
	}

	if ownTxn {
		if _, err := sink.conn.Exec(ctx, "BEGIN"); err != nil {
			return fmt.Errorf("postgres: staged upsert: begin: %w", err)
		}
	}
	committed := !ownTxn
	defer func() {
		if ownTxn && !committed {
			_, _ = sink.conn.Exec(context.Background(), "ROLLBACK")
		}
	}()

	if _, err := sink.conn.Exec(ctx, fmt.Sprintf("CREATE TEMP TABLE %s (%s) ON COMMIT DROP",
		quoteIdentifier(stg), strings.Join(stgCols, ", "))); err != nil {
		return fmt.Errorf("postgres: staged upsert: create staging: %w", err)
	}
	copySQL := fmt.Sprintf("COPY %s (%s) FROM STDIN", quoteIdentifier(stg), quotedColumnList(cols))
	if err := pipeCopy(ctx, src, ref, cols, sink, copySQL); err != nil {
		return fmt.Errorf("postgres: staged upsert: copy %s: %w", ref, err)
	}
	// Upsert from staging in bounded keyset chunks so no single INSERT statement ever
	// processes the whole table — a whole-table INSERT ... ON CONFLICT cannot complete
	// on a very large table (it surfaced as "unexpected EOF" on the statement). Chunk
	// boundaries come from the source's PK distribution (identical keys to staging).
	if err := upsertFromStaging(ctx, src, sink, ref, stg, cols, pk, identity); err != nil {
		return err
	}
	if ownTxn {
		if _, err := sink.conn.Exec(ctx, "COMMIT"); err != nil {
			return fmt.Errorf("postgres: staged upsert: commit: %w", err)
		}
		committed = true
	}
	return nil
}

// targetEmpty reports whether the target table currently holds no rows. It is a cheap
// existence probe (stops at the first row), used to pick the cyclic-copy load strategy.
func targetEmpty(ctx context.Context, sink *Sink, ref engine.TableRef) (bool, error) {
	if sink.conn == nil {
		return false, errNotConnected("sink")
	}
	var nonEmpty bool
	if err := sink.conn.QueryRow(ctx,
		fmt.Sprintf("SELECT EXISTS(SELECT 1 FROM %s)", qualifyTable(ref))).Scan(&nonEmpty); err != nil {
		return false, fmt.Errorf("postgres: probe %s emptiness: %w", ref, err)
	}
	return !nonEmpty, nil
}

// copyOrUpsert loads a column subset of the source table into the target, picking the
// strategy by the target's current state: an EMPTY target gets a plain streaming COPY
// (light and atomic — no staging, no whole-table upsert, so it scales to very large
// tables), a NON-EMPTY target gets the idempotent chunked staged upsert (so a re-copy
// over existing rows converges instead of colliding on the PK). Used by the per-table
// NULL-fill cyclic paths (its own autocommit COPY / own-txn upsert).
func copyOrUpsert(ctx context.Context, src *Source, sink *Sink, ref engine.TableRef, cols []string) error {
	empty, err := targetEmpty(ctx, sink, ref)
	if err != nil {
		return err
	}
	if empty {
		sql := fmt.Sprintf("COPY %s (%s) FROM STDIN", qualifyTable(ref), quotedColumnList(cols))
		return pipeCopy(ctx, src, ref, cols, sink, sql)
	}
	return stagedUpsertCopy(ctx, src, sink, ref, cols, "replicare_stg", true)
}

// cyclicCopyRetries bounds the per-child retry that recovers from a live-source
// transient FK violation during cyclic pass 1. Each attempt re-copies only the
// child's non-cyclic parents (small hub tables like "user"), never the whole
// component, so convergence is cheap. A write burst that keeps inserting fresh
// parents past this bound falls through to the syncer's coarse retry.
const cyclicCopyRetries = 6

// copyChildWithParentRetry loads one component table and, on a transient FK
// violation (a parent row inserted on the live source after the parent's snapshot
// but referenced by this child's later snapshot — §3.3/§4), re-copies the child's
// non-cyclic in-component parents to pick up those rows and retries. Non-FK errors
// halt immediately. On exhaustion it returns the FK error classified transient, so
// the syncer's coarse retry (and, failing that, a clean restart) can recover
// instead of the daemon crash-looping.
func copyChildWithParentRetry(ctx context.Context, src *Source, sink *Sink, ref engine.TableRef, cols []string, parentRefs []engine.TableRef, byRef map[engine.TableRef]engine.Table, cyclicCols map[engine.TableRef][]string) error {
	var lastErr error
	for attempt := 0; attempt < cyclicCopyRetries; attempt++ {
		err := copyOrUpsert(ctx, src, sink, ref, cols)
		if err == nil {
			return nil
		}
		if !engine.IsTransientConstraint(classifyFKViolation(err)) {
			return err // not an FK skew — halt loud
		}
		lastErr = err
		if len(parentRefs) == 0 {
			break // nothing to re-copy — cannot make progress here
		}
		// Re-copy the direct non-cyclic parents so their newly-inserted rows land,
		// then retry this child on the next loop iteration.
		for _, p := range parentRefs {
			pcols := subtractCols(transportColumns(byRef[p]), cyclicCols[p])
			if e := copyOrUpsert(ctx, src, sink, p, pcols); e != nil {
				return e
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(cyclicRetryBackoff(attempt)):
		}
	}
	return classifyFKViolation(lastErr)
}

// cyclicRetryBackoff is a short, bounded backoff between parent re-copy attempts.
func cyclicRetryBackoff(attempt int) time.Duration {
	d := time.Duration(500*(attempt+1)) * time.Millisecond
	if d > 3*time.Second {
		d = 3 * time.Second
	}
	return d
}

// nonCyclicParents maps each component member to the distinct in-component parent
// tables reached by its NON-cyclic FK edges (cyclic edges are NULLed in pass 1, so
// they impose no load dependency). These are exactly the parents whose fresh rows a
// child may need re-copied to satisfy a transient FK during a live-source copy.
func nonCyclicParents(members []engine.Table, cyclicEdge map[string]bool) map[engine.TableRef][]engine.TableRef {
	inComp := make(map[engine.TableRef]bool, len(members))
	for _, t := range members {
		inComp[t.Ref] = true
	}
	out := make(map[engine.TableRef][]engine.TableRef, len(members))
	for _, t := range members {
		seen := map[engine.TableRef]bool{}
		for _, fk := range t.ForeignKeys {
			if !inComp[fk.Parent] || cyclicEdge[fkKey(fk)] || fk.Child == fk.Parent {
				continue
			}
			if !seen[fk.Parent] {
				seen[fk.Parent] = true
				out[t.Ref] = append(out[t.Ref], fk.Parent)
			}
		}
		sortRefs(out[t.Ref])
	}
	return out
}

// upsertFromStaging upserts every staged row into the target in bounded keyset chunks,
// so no single INSERT statement processes the whole table. Chunk boundaries come from
// the source's PK distribution (the staging table holds the same keys). If chunk
// planning is unavailable or falls back to non-keyset (ctid) ranges — which carry no
// key bounds to slice the staging table by — it does a single whole-staging upsert.
func upsertFromStaging(ctx context.Context, src *Source, sink *Sink, ref engine.TableRef, stg string, cols []string, pk []captureCol, identity bool) error {
	pkSet := colSetOf(pk)
	chunks, err := src.PlanChunks(ctx, ref, engine.ChunkOptions{Method: engine.ChunkKeyset, TargetRows: upsertChunkRows})
	if err != nil || !allKeyset(chunks) {
		if _, e := sink.conn.Exec(ctx, mergeInsertSQL(ref, stg, cols, pk, pkSet, identity)); e != nil {
			return fmt.Errorf("postgres: staged upsert: upsert %s: %w", ref, e)
		}
		return nil
	}
	for _, ch := range chunks {
		pred, err := keysetPredicate(pk, ch.Lo, ch.Hi)
		if err != nil {
			return err
		}
		if _, err := sink.conn.Exec(ctx, mergeInsertRangeSQL(ref, stg, cols, pk, pkSet, identity, pred)); err != nil {
			return fmt.Errorf("postgres: staged upsert: upsert %s chunk: %w", ref, err)
		}
	}
	return nil
}

// upsertChunkRows is the approximate rows-per-chunk for the staged upsert. Small enough
// that a single INSERT statement stays well within any statement/connection limit.
const upsertChunkRows = 50000

// allKeyset reports whether chunk planning produced at least one chunk and every chunk
// is a keyset range (so it carries key bounds usable to slice the staging table).
func allKeyset(chunks []engine.Chunk) bool {
	if len(chunks) == 0 {
		return false
	}
	for _, c := range chunks {
		if c.Method != engine.ChunkKeyset {
			return false
		}
	}
	return true
}

// mergeInsertRangeSQL is mergeInsertSQL restricted to a staging key range (WHERE pred),
// so the upsert can be driven one bounded chunk at a time.
func mergeInsertRangeSQL(t engine.TableRef, stg string, cols []string, pk []captureCol, pkSet map[string]bool, identity bool, pred string) string {
	overriding := ""
	if identity {
		overriding = " OVERRIDING SYSTEM VALUE"
	}
	pkNames := make([]string, len(pk))
	for i, c := range pk {
		pkNames[i] = quoteIdentifier(c.Name)
	}
	var setParts []string
	for _, c := range cols {
		if !pkSet[c] {
			setParts = append(setParts, fmt.Sprintf("%s = EXCLUDED.%s", quoteIdentifier(c), quoteIdentifier(c)))
		}
	}
	action := "DO NOTHING"
	if len(setParts) > 0 {
		action = "DO UPDATE SET " + strings.Join(setParts, ", ")
	}
	return fmt.Sprintf("INSERT INTO %s (%s)%s SELECT %s FROM %s WHERE %s ON CONFLICT (%s) %s",
		qualifyTable(t), quotedColumnList(cols), overriding, quotedColumnList(cols),
		quoteIdentifier(stg), pred, strings.Join(pkNames, ", "), action)
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
