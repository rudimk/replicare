package postgres

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
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
// The NULL-then-fill path copies each table in key-range chunks, shared out over the
// copy pool's connections; the deferred and self-reference paths load a table whole.

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
// Tables load one at a time in that order. On the NULL-then-fill path each table is
// copied in key-range chunks (see CopyCyclicComponentWith for spreading them over more
// connections); the deferred path and a self-reference that must keep its real values
// load a table whole. The blocked case (NOT NULL + non-deferrable) never reaches here
// (pre-flight fails loud).
func (s *Sink) CopyCyclicComponent(ctx context.Context, src engine.Source, tables []engine.TableRef, progress func(engine.TableRef, int64)) error {
	return s.CopyCyclicComponentWith(ctx, src, nil, tables, progress)
}

// CopyCyclicComponentWith is engine.ParallelCyclicCopier: CopyCyclicComponent with each
// table's chunks (and its fill pass) spread over the helper connection pairs as well.
// A helper that is not a connected Postgres pair is skipped.
func (s *Sink) CopyCyclicComponentWith(ctx context.Context, src engine.Source, helpers []engine.ConnPair, tables []engine.TableRef, progress func(engine.TableRef, int64)) error {
	ctx = withCopyProgress(ctx, progress)
	pgSrc, ok := src.(*Source)
	if !ok {
		return fmt.Errorf("postgres: cyclic copy: source is %T, want *postgres.Source", src)
	}
	if s.conn == nil {
		return errNotConnected("sink")
	}
	conns := []copyConn{{src: pgSrc, sink: s}}
	for _, h := range helpers {
		hs, okS := h.Src.(*Source)
		hk, okK := h.Sink.(*Sink)
		if okS && okK && hs != pgSrc && hk != s && hs.conn != nil && hk.conn != nil {
			conns = append(conns, copyConn{src: hs, sink: hk})
		}
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
	// A duplicate-key collision from emptying a cyclic column happens in the TARGET's
	// unique indexes (user-created, possibly different from the source's), so fold the
	// target's null-colliding columns into the source-derived classification.
	tgtSchema, err := s.Introspect(ctx, engine.Selection{Include: inc})
	if err != nil {
		return fmt.Errorf("postgres: cyclic copy: introspect target: %w", err)
	}
	members = withTargetNullColliding(members, tgtSchema)

	cyc := classifyCyclicFKs(members)
	if anyBlockedCyclicFK(cyc) {
		// Should be unreachable — pre-flight blocks this — but never load blindly.
		return fmt.Errorf("postgres: cyclic copy: component has a NOT NULL non-deferrable cyclic FK; make it DEFERRABLE or break the cycle")
	}

	// The component is copied from scratch on every (re)start; if an earlier, unfinished
	// attempt left rows on the target, clear them first so the copy loads into empty
	// tables instead of merging over rows the source may since have moved.
	if _, err := s.clearComponentForReload(ctx, tables); err != nil {
		return fmt.Errorf("postgres: cyclic copy: %w", err)
	}

	// All cyclic FKs deferrable -> one deferred transaction over the whole component.
	// classifyFKViolation marks a live-source transient FK skew (§3.3/§4) transient so
	// the syncer's coarse retry recovers instead of the daemon crash-looping.
	if allDeferrable(cyc) {
		return classifyFKViolation(LoadCyclicDeferred(ctx, pgSrc, s, orderMembers(members)))
	}

	// NULL-then-fill: null every cyclic FK column, copy in non-cyclic order, then fill.
	return classifyFKViolation(s.nullFillComponent(ctx, conns, members, cyc))
}

// nullFillComponent runs the multi-table NULL-then-fill: copy each table with its
// cyclic FK columns omitted (in an order that respects only the non-cyclic FK
// edges, so a table always loads after its non-cyclic parents), then fill the
// cyclic FK columns from the source. conns[0] is (src, s); any further pairs share
// the chunks of each table.
func (s *Sink) nullFillComponent(ctx context.Context, conns []copyConn, members []engine.Table, cyc []CyclicFK) error {
	src := conns[0].src
	byRef := make(map[engine.TableRef]engine.Table, len(members))
	for _, t := range members {
		byRef[t.Ref] = t
	}

	// A cross-table cyclic edge whose column sits in a unique index where emptied values
	// collide can be neither emptied nor kept real (its FK points at a table loaded in a
	// separate statement); only a DEFERRABLE FK loads it. Pre-flight blocks this, so
	// reaching here is a loud internal error — never a silent NULL-fill that collides.
	if bad := unloadableNullFill(cyc); len(bad) > 0 {
		return fmt.Errorf("postgres: cyclic copy: FK %s: %s (pre-flight should have blocked this)", bad[0].FK.Name, bad[0].Reason)
	}

	// Per table: keepReal are self-reference columns that would collide if emptied, so
	// they load at their REAL value and the table loads in a single statement (a
	// non-deferrable FK is checked at statement end, so a child may precede its parent
	// in the one stream). nullCols are every other cyclic column — emptied in pass 1,
	// filled in pass 2 as before (a whole-loaded table can still have some: a link to
	// another table in a larger loop).
	keepReal := keepRealColsByTable(cyc)
	nullCols := cyclicColsByTable(cyc)
	for ref, keep := range keepReal {
		nullCols[ref] = subtractCols(nullCols[ref], keep)
	}
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

	// Re-copying a parent (live-source skew recovery) must use the same load shape the
	// parent was first loaded with: a whole-loaded table in one statement with its real
	// self-link, anything else as the chunked upsert with its nullCols left empty. It runs
	// on the connection pair of the chunk that needed it.
	recopyOneOn := func(c copyConn) parentRecopier {
		return func(ctx context.Context, p engine.TableRef) error {
			if len(keepReal[p]) > 0 {
				return c.sink.loadSelfRefWhole(ctx, c.src, p, byRef[p], nullCols[p])
			}
			return copyOrUpsert(ctx, c.src, c.sink, p, subtractCols(transportColumns(byRef[p]), nullCols[p]))
		}
	}
	// The re-copy of a parent can itself hit the same skew one level up (the parent now
	// carries a row pointing at a grandparent row newer than the grandparent's copy), so
	// it walks the whole chain of non-cyclic parents rather than giving up after one hop.
	// Re-copies from chunks running side by side take turns: two upserts of the same
	// parent rows at once would only wait on each other (or deadlock).
	var recopyMu sync.Mutex
	recopyOn := func(c copyConn) parentRecopier {
		one := recopyOneOn(c)
		return transitiveRecopier(func(ctx context.Context, p engine.TableRef) error {
			recopyMu.Lock()
			defer recopyMu.Unlock()
			return one(ctx, p)
		}, parents)
	}
	recopy := recopyOn(conns[0])

	// A chunk that fails on missing parent rows fetches just the rows it points at, not
	// the whole parent table (see parentFetch).
	fetch := newParentFetch(members, cyclicEdge, parents, keepReal, nullCols, recopyOn, &recopyMu)
	fetchFor := func(child engine.TableRef) func(copyConn) referencedFetcher {
		return func(c copyConn) referencedFetcher {
			return func(ctx context.Context, childWhere string) error {
				return fetch.fetch(ctx, c, child, childWhere)
			}
		}
	}

	// Pass 1: copy every table, omitting its nullCols (empty on the target). Empty target
	// → a plain streaming COPY (light: no staging, no whole-table upsert); non-empty target
	// → an idempotent staged upsert so a re-copy over existing rows converges instead of
	// colliding on the PK. See copyOrUpsert. A table with keepReal columns loads WHOLE in
	// one statement instead (loadSelfRefWhole), so its self-link is never emptied; it still
	// loads after its non-cyclic parents (the `order` respects those).
	for _, ref := range order {
		table := byRef[ref]
		if len(keepReal[ref]) > 0 {
			load := func() error {
				return countedLoad(ctx, ref, func(ctx context.Context) error {
					return s.loadSelfRefWhole(ctx, src, ref, table, nullCols[ref])
				})
			}
			if err := withParentRetry(ctx, load, parents[ref], recopy); err != nil {
				return fmt.Errorf("postgres: cyclic whole-load (%s): %w", ref, err)
			}
			continue
		}
		cols := subtractCols(transportColumns(table), nullCols[ref])
		if err := copyTableChunkedWithParentRetry(ctx, conns, ref, cols, parents[ref], recopyOn, fetchFor(ref)); err != nil {
			return fmt.Errorf("postgres: cyclic null-fill pass 1 (%s): %w", ref, err)
		}
	}

	// Pass 2: fill each table's nullCols, in the same order (every referenced row now
	// exists, so the FK holds when filled). keepReal columns already carry real values.
	for _, ref := range order {
		fkCols := nullCols[ref]
		if len(fkCols) == 0 {
			continue
		}
		table := byRef[ref]
		pk := captureColsFor(table)
		if len(pk) == 0 {
			return fmt.Errorf("postgres: cyclic null-fill: %s has no usable key to fill by", ref)
		}
		if err := fillFKColumnsOn(ctx, conns, ref, table, pk, fkCols); err != nil {
			return fmt.Errorf("postgres: cyclic null-fill pass 2 (%s): %w", ref, err)
		}
	}
	return nil
}

// loadSelfRefWhole loads a self-referential table in a SINGLE statement with its real
// self-link values — the schema-change-free alternative to NULL-then-fill for when the
// self-link column sits in a unique index where emptied values collide (emptying it
// across every row would collapse otherwise-distinct rows). A non-deferrable FK is
// checked at the END of the statement, not row by row, so one COPY (or one
// INSERT … SELECT) that contains both a child and the parent it points at passes even
// though the child appears first in the stream. omit lists any OTHER cyclic columns
// (links to a different table in a larger loop) to leave empty for the pass-2 fill.
//
// The table is copied WHOLE (unchunked) — a documented tradeoff: a very large
// self-referential table under this kind of unique index gives up chunked copy (and the
// target queues one foreign-key check per row until the statement ends) in exchange for a
// correct load. A future depth-ordered load could restore chunking by loading
// parents-before-children across statements.
func (s *Sink) loadSelfRefWhole(ctx context.Context, src *Source, ref engine.TableRef, srcTable engine.Table, omit []string) error {
	cols := subtractCols(transportColumns(srcTable), omit) // self-link column(s) included
	empty, err := targetEmpty(ctx, s, ref)
	if err != nil {
		return err
	}
	if empty {
		// Empty target: one streamed COPY. The self-FK is checked at statement end, so a
		// child before its parent in the stream is fine; io.Pipe streams, so the whole
		// table is never buffered in memory.
		sql := fmt.Sprintf("COPY %s (%s) FROM STDIN", qualifyTable(ref), quotedColumnList(cols))
		return pipeCopy(ctx, src, ref, cols, s, sql)
	}
	// Non-empty target (resume / re-run): stage the whole table, then ONE
	// INSERT … ON CONFLICT DO UPDATE — NOT chunked, since chunking would split parent and
	// child across statements and reintroduce the FK problem. Idempotent on the PK.
	return s.stagedUpsertWhole(ctx, src, ref, cols)
}

// stagedUpsertWhole stages the whole source table into a TEMP table and upserts it with a
// SINGLE INSERT … ON CONFLICT statement (no range chunking), so a self-referential FK —
// checked at statement end — holds. It is the populated-target counterpart of
// loadSelfRefWhole's empty-target direct COPY; the normal cyclic path chunks the upsert
// (upsertFromStaging) because it does not need the single-statement guarantee.
//
// Rows whose key already exists in the target are applied FIRST (see
// mergeInsertExistingFirstSQL): this path typically runs over the leftovers of a failed
// NULL-then-fill attempt — rows that landed with their self-link empty — and a unique
// index is checked row by row, so a new row arriving before such a leftover is corrected
// would collide with it.
func (s *Sink) stagedUpsertWhole(ctx context.Context, src *Source, ref engine.TableRef, cols []string) error {
	tgt, err := s.tableMeta(ctx, ref)
	if err != nil {
		return err
	}
	meta := buildRangeUpsertMeta(tgt, cols)
	if len(meta.pk) == 0 {
		return fmt.Errorf("target %s has no usable key for ON CONFLICT", ref)
	}
	if _, err := s.conn.Exec(ctx, "BEGIN"); err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = s.conn.Exec(context.Background(), "ROLLBACK")
		}
	}()
	const stg = "replicare_stg"
	if _, err := s.conn.Exec(ctx, fmt.Sprintf("CREATE TEMP TABLE %s (%s) ON COMMIT DROP",
		quoteIdentifier(stg), meta.stgDDL)); err != nil {
		return fmt.Errorf("staging: %w", err)
	}
	copySQL := fmt.Sprintf("COPY %s (%s) FROM STDIN", quoteIdentifier(stg), quotedColumnList(cols))
	if err := pipeCopy(ctx, src, ref, cols, s, copySQL); err != nil {
		return fmt.Errorf("copy: %w", err)
	}
	if _, err := s.conn.Exec(ctx, mergeInsertExistingFirstSQL(ref, stg, cols, meta.pk, meta.pkSet, meta.identity)); err != nil {
		return fmt.Errorf("upsert: %w", err)
	}
	if _, err := s.conn.Exec(ctx, "COMMIT"); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	committed = true
	return nil
}

// mergeInsertExistingFirstSQL is mergeInsertSQL with the staged rows ordered so those
// whose key already exists in the target are processed FIRST, then the new ones. The
// foreign key is checked at the end of the statement (order is irrelevant to it), but a
// unique index is checked row by row: over leftovers of a failed earlier attempt — say a
// child that landed as (email, NULL) — inserting the new root (email, NULL) before that
// child is corrected to (email, its-real-parent) would collide. Correcting existing rows
// first frees those values. (Rows that genuinely swap unique values with each other can
// still collide in any order; that fails loud, never silently.)
func mergeInsertExistingFirstSQL(t engine.TableRef, stg string, cols []string, pk []captureCol, pkSet map[string]bool, identity bool) string {
	overriding := ""
	if identity {
		overriding = " OVERRIDING SYSTEM VALUE"
	}
	pkNames := make([]string, len(pk))
	match := make([]string, len(pk))
	for i, c := range pk {
		q := quoteIdentifier(c.Name)
		pkNames[i] = q
		match[i] = fmt.Sprintf("x.%s = s.%s", q, q)
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
	return fmt.Sprintf("INSERT INTO %s (%s)%s SELECT %s FROM %s s ORDER BY EXISTS (SELECT 1 FROM %s x WHERE %s) DESC ON CONFLICT (%s) %s",
		qualifyTable(t), quotedColumnList(cols), overriding, quotedColumnList(cols), quoteIdentifier(stg),
		qualifyTable(t), strings.Join(match, " AND "), strings.Join(pkNames, ", "), action)
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
	// Rows are counted per table but reported only after the one COMMIT: until then
	// nothing is durable on the target.
	copied := make(map[engine.TableRef]int64, len(tables))
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
			n, err := countAttempt(ctx, func(ctx context.Context) error { return pipeCopy(ctx, src, ref, cols, sink, sql) })
			if err != nil {
				return fmt.Errorf("postgres: cyclic deferred: copy %s: %w", ref, err)
			}
			copied[ref] += n
			continue
		}
		// Non-empty target: idempotent staged upsert within the deferred transaction.
		// Distinct staging name because ON COMMIT DROP fires only at the outer COMMIT.
		stg := fmt.Sprintf("replicare_stg_%d", i)
		n, err := countAttempt(ctx, func(ctx context.Context) error {
			return stagedUpsertCopy(ctx, src, sink, ref, cols, stg, false)
		})
		if err != nil {
			return fmt.Errorf("postgres: cyclic deferred: copy %s: %w", ref, err)
		}
		copied[ref] += n
	}
	if _, err := sink.conn.Exec(ctx, "COMMIT"); err != nil {
		return fmt.Errorf("postgres: cyclic deferred: commit: %w", err)
	}
	committed = true
	for _, ref := range tables {
		reportCopied(ctx, ref, copied[ref])
	}
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

// fillFKColumns fills the target rows' cyclic FK columns from the source, in bounded
// keyset chunks so a very large table never rides one giant COPY+UPDATE (which would
// EOF/time out just like the pass-1 copy). Each chunk stages its (pk + fk) tuples into a
// TEMP table and UPDATEs that range, in its own committed transaction. A keyless/tiny
// table (no keyset chunking) fills whole in a single transaction.
func (s *Source) fillFKColumns(ctx context.Context, sink *Sink, ref engine.TableRef, table engine.Table, pk []captureCol, fkCols []string) error {
	return fillFKColumnsOn(ctx, []copyConn{{src: s, sink: sink}}, ref, table, pk, fkCols)
}

// fillFKColumnsOn is fillFKColumns with the key-range chunks shared out over conns.
// Each chunk updates its own rows, so chunks running side by side never touch the
// same row.
func fillFKColumnsOn(ctx context.Context, conns []copyConn, ref engine.TableRef, table engine.Table, pk []captureCol, fkCols []string) error {
	first := conns[0]
	tgt, err := first.sink.tableMeta(ctx, ref)
	if err != nil {
		return err
	}
	var chunks []engine.Chunk
	if len(pk) > 0 {
		chunks, err = first.src.PlanChunks(ctx, ref, engine.ChunkOptions{Method: engine.ChunkKeyset, TargetRows: copyChunkRows})
		if err != nil {
			return err
		}
	}
	if !allKeyset(chunks) {
		return first.src.fillFKRange(ctx, first.sink, ref, tgt, pk, fkCols, "TRUE")
	}
	return runChunks(ctx, conns, chunks, func(ctx context.Context, c copyConn, ch engine.Chunk) error {
		pred, err := keysetPredicate(pk, ch.Lo, ch.Hi)
		if err != nil {
			return err
		}
		return c.src.fillFKRange(ctx, c.sink, ref, tgt, pk, fkCols, pred)
	})
}

// fillFKRange fills the cyclic FK columns for one key range (WHERE where): it stages the
// range's (pk + fk) tuples into a TEMP table and UPDATEs them, in one committed
// transaction. The UPDATE joins staging, which holds only the range's rows, so exactly
// that range is filled.
func (s *Source) fillFKRange(ctx context.Context, sink *Sink, ref engine.TableRef, tgt engine.Table, pk []captureCol, fkCols []string, where string) error {
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
	if err := pipeCopyWhere(ctx, s, ref, fillCols, where, sink, copySQL); err != nil {
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

// copyChunkRows is the approximate rows-per-chunk for the cyclic copy. Small enough
// that no single COPY statement or transaction ever spans a huge table — a whole-table
// COPY of a multi-million-row table dies with "unexpected EOF"/timeout — yet large
// enough to keep per-chunk overhead low. It is a var (not a const) only so tests can
// lower it to force multi-chunk copies on small fixtures.
var copyChunkRows = 50000

// rangeUpsertMeta is the per-table metadata a chunked staged upsert needs, computed
// ONCE per table (not per chunk) so the chunk loop does no repeated introspection.
type rangeUpsertMeta struct {
	pk       []captureCol
	pkSet    map[string]bool
	stgDDL   string // "col type, col type, ..." for the TEMP staging table
	identity bool   // any transported column is an identity column (needs OVERRIDING SYSTEM VALUE)
}

// buildRangeUpsertMeta derives the staging DDL, PK set, and identity flag for a chunked
// staged upsert of `cols` into target table `tgt`.
func buildRangeUpsertMeta(tgt engine.Table, cols []string) rangeUpsertMeta {
	typeByName := make(map[string]string, len(tgt.Columns))
	colSet := make(map[string]bool, len(cols))
	for _, c := range cols {
		colSet[c] = true
	}
	identity := false
	for _, c := range tgt.Columns {
		typeByName[c.Name] = c.DataType
		if c.Identity && colSet[c.Name] {
			identity = true
		}
	}
	stgCols := make([]string, len(cols))
	for i, c := range cols {
		stgCols[i] = quoteIdentifier(c) + " " + typeByName[c]
	}
	pk := captureColsFor(tgt)
	return rangeUpsertMeta{pk: pk, pkSet: colSetOf(pk), stgDDL: strings.Join(stgCols, ", "), identity: identity}
}

// copyOrUpsert loads a column subset of the source table into the target in bounded
// keyset key-range CHUNKS, each a committed per-chunk staged UPSERT (COPY the range into a
// TEMP table, then INSERT ... ON CONFLICT DO UPDATE from it). Chunking keeps every COPY
// and every statement bounded, so a huge table never rides one giant COPY that EOFs/times
// out. The upsert (never a DELETE) is idempotent, so a re-copy over an empty OR a populated
// target (a restart/re-run) converges — and, crucially, it NEVER removes a parent row that
// a child already references (a range DELETE on a hub table like clinic would). A keyless
// or tiny table (no usable keyset chunking) falls back to a single whole-table copy.
func copyOrUpsert(ctx context.Context, src *Source, sink *Sink, ref engine.TableRef, cols []string) error {
	tgt, err := sink.tableMeta(ctx, ref)
	if err != nil {
		return err
	}
	pk := captureColsFor(tgt)
	var chunks []engine.Chunk
	if len(pk) > 0 {
		chunks, err = src.PlanChunks(ctx, ref, engine.ChunkOptions{Method: engine.ChunkKeyset, TargetRows: copyChunkRows})
		if err != nil {
			return err
		}
	}
	if !allKeyset(chunks) {
		// No usable keyset chunking (keyless/tiny): a single whole-table copy is safe
		// because such tables are small.
		empty, e := targetEmpty(ctx, sink, ref)
		if e != nil {
			return e
		}
		if empty {
			sql := fmt.Sprintf("COPY %s (%s) FROM STDIN", qualifyTable(ref), quotedColumnList(cols))
			return pipeCopy(ctx, src, ref, cols, sink, sql)
		}
		return stagedUpsertCopy(ctx, src, sink, ref, cols, "replicare_stg", true)
	}
	meta := buildRangeUpsertMeta(tgt, cols)
	for _, ch := range chunks {
		if err := copyRange(ctx, src, sink, ref, cols, meta, ch.Lo, ch.Hi); err != nil {
			return err
		}
	}
	return nil
}

// copyRange copies one keyset key-range as a per-chunk staged upsert (see copyRangeOnce),
// retrying on a DROPPED CONNECTION: a live source/target can drop a socket mid-COPY
// ("unexpected EOF"), and — mirroring the streaming loop and the acyclic copier — the copy
// reconnects and retries instead of crashing the daemon. A chunk error with both endpoints
// still healthy (a real data error, incl. an FK-skew 23503 that copyRangeWithParentRetry
// handles) is returned immediately, never retried here.
func copyRange(ctx context.Context, src *Source, sink *Sink, ref engine.TableRef, cols []string, meta rangeUpsertMeta, lo, hi engine.KeyValues) error {
	pred, err := keysetPredicate(meta.pk, lo, hi)
	if err != nil {
		return err
	}
	return copyWhere(ctx, src, sink, ref, cols, meta, pred, loadAuto)
}

// loadMode is how copyWhereOnce writes the selected rows into the target table.
type loadMode int

const (
	// loadAuto: a plain COPY when the target holds none of the selected key range, else
	// a staged upsert. Only for a keyset range, which means the same rows on both ends.
	loadAuto loadMode = iota
	// loadMissingOnly: staged, inserting only rows whose key the target lacks. For the
	// parent rows a failed chunk points at: the ones already there are left alone (any
	// later change to them reaches the target through the delta queue).
	loadMissingOnly
)

// copyWhere copies the source rows of ref matching the source-side predicate where into
// the target (see copyWhereOnce), retrying on a dropped connection or a deadlock.
func copyWhere(ctx context.Context, src *Source, sink *Sink, ref engine.TableRef, cols []string, meta rangeUpsertMeta, where string, mode loadMode) error {
	var lastErr error
	for attempt := 0; attempt < cyclicCopyConnAttempts; attempt++ {
		_, err := countAttempt(ctx, func(ctx context.Context) error {
			return copyWhereOnce(ctx, src, sink, ref, cols, meta, where, mode)
		})
		if err == nil {
			return nil
		}
		lastErr = err
		if ctx.Err() != nil {
			return err
		}
		if engine.IsTransientConstraint(classifyFKViolation(err)) {
			// A missing parent row: running the same copy again cannot help, the caller
			// fetches the parent first. A target-side failure in the middle of the COPY makes
			// the driver close the source connection, so put both back before returning.
			pgReconnectConn(ctx, src.HealthCheck, src.Close, src.Connect)
			pgReconnectConn(ctx, sink.HealthCheck, sink.Close, sink.Connect)
			return err
		}
		if isDeadlock(err) {
			// Chunks running side by side, or a parent re-copy next to them, can lock the
			// same parent rows in opposite orders; Postgres cancels one of them. Its
			// transaction rolled back, so running it again is safe.
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(cyclicConnBackoff):
			}
			continue
		}
		if !pgEndpointDown(ctx, src.HealthCheck) && !pgEndpointDown(ctx, sink.HealthCheck) {
			return err // healthy connections → real data error (or FK skew), halt/handle upstream
		}
		pgReconnectConn(ctx, src.HealthCheck, src.Close, src.Connect)
		pgReconnectConn(ctx, sink.HealthCheck, sink.Close, sink.Connect)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(cyclicConnBackoff):
		}
	}
	return lastErr
}

// cyclicCopyConnAttempts / cyclicConnBackoff / cyclicConnHealthTimeout bound the
// dropped-connection retry for a cyclic copy chunk.
const (
	cyclicCopyConnAttempts  = 6
	cyclicConnBackoff       = 2 * time.Second
	cyclicConnHealthTimeout = 10 * time.Second
)

// pgEndpointDown reports whether a health-check fails within the bounded timeout.
func pgEndpointDown(ctx context.Context, healthCheck func(context.Context) error) bool {
	hctx, cancel := context.WithTimeout(ctx, cyclicConnHealthTimeout)
	defer cancel()
	return healthCheck(hctx) != nil
}

// pgReconnectConn re-establishes one endpoint if its health-check fails (Close +
// Connect; pgx has no auto-redial). Best-effort — a still-failing Connect surfaces as the
// next attempt's error.
func pgReconnectConn(ctx context.Context, healthCheck, closeConn, connect func(context.Context) error) {
	hctx, cancel := context.WithTimeout(ctx, cyclicConnHealthTimeout)
	healthErr := healthCheck(hctx)
	cancel()
	if healthErr == nil {
		return
	}
	_ = closeConn(context.Background())
	cctx, ccancel := context.WithTimeout(ctx, cyclicConnHealthTimeout)
	defer ccancel()
	_ = connect(cctx)
}

// copyWhereOnce copies the source rows of ref matching pred (a column subset) into the
// target in a single committed transaction. With loadAuto, pred is a keyset key-range: a
// plain COPY when the target holds none of the range yet, otherwise a staged UPSERT (COPY
// the range into a TEMP staging table, then INSERT ... ON CONFLICT DO UPDATE from it;
// staging holds only this range's rows, so the upsert is chunk-bounded). With
// loadMissingOnly it always stages and inserts only the rows the target lacks. It performs
// NO DELETE — so it never violates or stalls on a child FK by removing a parent row — and
// is idempotent, so a re-run converges.
func copyWhereOnce(ctx context.Context, src *Source, sink *Sink, ref engine.TableRef, cols []string, meta rangeUpsertMeta, pred string, mode loadMode) error {
	if _, err := sink.conn.Exec(ctx, "BEGIN"); err != nil {
		return fmt.Errorf("postgres: cyclic copy range: begin: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = sink.conn.Exec(context.Background(), "ROLLBACK")
		}
	}()
	// A range the target holds no rows of yet (the normal case: the component's tables are
	// emptied before its copy) is loaded with one plain COPY straight into the table, with
	// no staging table and no per-row conflict check. Only the copier writes a table during
	// its initial copy, so the range stays empty until this transaction commits. A range
	// that already has rows (a parent re-copied after live-source skew, or a mesh member's
	// own data) takes the staged upsert, which converges over what is there.
	occupied := true
	if mode == loadAuto {
		if err := sink.conn.QueryRow(ctx, fmt.Sprintf("SELECT EXISTS (SELECT 1 FROM %s WHERE %s)", qualifyTable(ref), pred)).Scan(&occupied); err != nil {
			return fmt.Errorf("postgres: cyclic copy range: probe %s: %w", ref, err)
		}
	}
	if !occupied {
		direct := fmt.Sprintf("COPY %s (%s) FROM STDIN", qualifyTable(ref), quotedColumnList(cols))
		if err := pipeCopyWhere(ctx, src, ref, cols, pred, sink, direct); err != nil {
			return fmt.Errorf("postgres: cyclic copy range %s: %w", ref, err)
		}
		if _, err := sink.conn.Exec(ctx, "COMMIT"); err != nil {
			return fmt.Errorf("postgres: cyclic copy range: commit: %w", err)
		}
		committed = true
		return nil
	}
	const stg = "replicare_stg"
	if _, err := sink.conn.Exec(ctx, fmt.Sprintf("CREATE TEMP TABLE %s (%s) ON COMMIT DROP",
		quoteIdentifier(stg), meta.stgDDL)); err != nil {
		return fmt.Errorf("postgres: cyclic copy range: staging %s: %w", ref, err)
	}
	sinkCopySQL := fmt.Sprintf("COPY %s (%s) FROM STDIN", quoteIdentifier(stg), quotedColumnList(cols))
	if err := pipeCopyWhere(ctx, src, ref, cols, pred, sink, sinkCopySQL); err != nil {
		return fmt.Errorf("postgres: cyclic copy range %s: %w", ref, err)
	}
	merge := mergeInsertSQL(ref, stg, cols, meta.pk, meta.pkSet, meta.identity)
	if mode == loadMissingOnly {
		merge = insertMissingSQL(ref, stg, cols, meta.pk, meta.identity)
	}
	if _, err := sink.conn.Exec(ctx, merge); err != nil {
		return fmt.Errorf("postgres: cyclic copy range: upsert %s: %w", ref, err)
	}
	if _, err := sink.conn.Exec(ctx, "COMMIT"); err != nil {
		return fmt.Errorf("postgres: cyclic copy range: commit: %w", err)
	}
	committed = true
	return nil
}

// cyclicCopyRetries bounds the per-child retry that recovers from a live-source
// transient FK violation during cyclic pass 1. Each attempt re-copies only the
// child's non-cyclic parents (small hub tables like "user"), never the whole
// component, so convergence is cheap. A write burst that keeps inserting fresh
// parents past this bound falls through to the syncer's coarse retry.
const cyclicCopyRetries = 6

// parentRecopier re-copies one non-cyclic parent table so rows a live source inserted
// after that parent's snapshot land on the target (the §3.3/§4 parent-skew recovery).
type parentRecopier func(ctx context.Context, ref engine.TableRef) error

// recopyOmitting is the default parentRecopier: an idempotent chunked upsert of the
// parent with omit[parent] (its emptied cyclic columns) left out — the same column shape
// the parent was loaded with.
func recopyOmitting(src *Source, sink *Sink, byRef map[engine.TableRef]engine.Table, omit map[engine.TableRef][]string) parentRecopier {
	return func(ctx context.Context, p engine.TableRef) error {
		return copyOrUpsert(ctx, src, sink, p, subtractCols(transportColumns(byRef[p]), omit[p]))
	}
}

// transitiveRecopier wraps a single-table re-copy so that, when re-copying a parent itself
// hits a transient FK violation (on a live source the fresh parent snapshot can reference a
// grandparent row newer than the grandparent's own copy — e.g. a new Project for a new
// User), it re-copies that parent's own non-cyclic parents and retries, recursively up the
// chain. Without this the first such hop failed the whole component and fell through to
// the syncer's coarse retry, which re-copies EVERY table of the component — on a large
// component that is minutes per attempt and, on a busy source, may never converge.
//
// The non-cyclic parent graph is acyclic (cyclic and self edges are excluded from
// `parents`), so the recursion terminates; an in-progress set guards against a loop anyway
// by falling back to a plain single re-copy for a table already being re-copied up-stack.
func transitiveRecopier(one parentRecopier, parents map[engine.TableRef][]engine.TableRef) parentRecopier {
	var rec func(ctx context.Context, p engine.TableRef, inProgress map[engine.TableRef]bool) error
	rec = func(ctx context.Context, p engine.TableRef, inProgress map[engine.TableRef]bool) error {
		if inProgress[p] {
			return one(ctx, p)
		}
		inProgress[p] = true
		defer delete(inProgress, p)
		up := func(ctx context.Context, gp engine.TableRef) error { return rec(ctx, gp, inProgress) }
		return withParentRetry(ctx, func() error { return one(ctx, p) }, parents[p], up)
	}
	return func(ctx context.Context, p engine.TableRef) error {
		return rec(ctx, p, map[engine.TableRef]bool{})
	}
}

// withParentRetry runs load and, on a transient FK violation (a parent row inserted on
// the live source after the parent's snapshot but referenced by a later child snapshot —
// §3.3/§4), re-copies the given non-cyclic parents via recopy and retries. Non-FK errors
// halt immediately. On exhaustion it returns the FK error classified transient, so the
// syncer's coarse retry (and, failing that, a clean restart) can recover instead of the
// daemon crash-looping.
func withParentRetry(ctx context.Context, load func() error, parentRefs []engine.TableRef, recopy parentRecopier) error {
	var fix func(context.Context) error
	if len(parentRefs) > 0 {
		fix = func(ctx context.Context) error {
			for _, p := range parentRefs {
				if e := recopy(ctx, p); e != nil {
					return e
				}
			}
			return nil
		}
	}
	return retryOnFK(ctx, load, fix)
}

// retryOnFK runs load and, on a transient FK violation, runs fix (which brings the missing
// parent rows across) and tries again, up to cyclicCopyRetries times. Any other error
// halts at once. With fix nil there is nothing to try, so the FK error is returned
// (classified transient) straight away.
func retryOnFK(ctx context.Context, load func() error, fix func(context.Context) error) error {
	var lastErr error
	for attempt := 0; attempt < cyclicCopyRetries; attempt++ {
		err := load()
		if err == nil {
			return nil
		}
		if !engine.IsTransientConstraint(classifyFKViolation(err)) {
			return err // not an FK skew — halt loud
		}
		lastErr = err
		if fix == nil {
			break // nothing to re-copy — cannot make progress here
		}
		if e := fix(ctx); e != nil {
			return e
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(cyclicRetryBackoff(attempt)):
		}
	}
	return classifyFKViolation(lastErr)
}

// copyChildWithParentRetry loads one whole component table (copyOrUpsert) with the
// live-source parent-skew retry (withParentRetry).
func copyChildWithParentRetry(ctx context.Context, src *Source, sink *Sink, ref engine.TableRef, cols []string, parentRefs []engine.TableRef, recopy parentRecopier) error {
	load := func() error {
		return countedLoad(ctx, ref, func(ctx context.Context) error { return copyOrUpsert(ctx, src, sink, ref, cols) })
	}
	return withParentRetry(ctx, load, parentRefs, recopy)
}

// copyTableChunkedWithParentRetry loads one component table in bounded keyset chunks
// (so a huge table never rides one giant COPY), with the live-source parent-skew retry
// applied PER CHUNK: only the failing chunk is retried and only the child's parents are
// re-copied, never the whole table. The chunks are shared out over conns, each running
// one chunk at a time. fetchOn, when set, gives for a chunk's own connection pair the
// fetch of just the parent rows that chunk points at; without it, recopyOn's whole-table
// parent re-copy is used. A keyless/tiny table (no keyset chunking) falls back to the
// whole-table copyChildWithParentRetry on conns[0].
func copyTableChunkedWithParentRetry(ctx context.Context, conns []copyConn, ref engine.TableRef, cols []string, parentRefs []engine.TableRef, recopyOn func(copyConn) parentRecopier, fetchOn func(copyConn) referencedFetcher) error {
	first := conns[0]
	tgt, err := first.sink.tableMeta(ctx, ref)
	if err != nil {
		return err
	}
	pk := captureColsFor(tgt)
	var chunks []engine.Chunk
	if len(pk) > 0 {
		chunks, err = first.src.PlanChunks(ctx, ref, engine.ChunkOptions{Method: engine.ChunkKeyset, TargetRows: copyChunkRows})
		if err != nil {
			return err
		}
	}
	if !allKeyset(chunks) {
		return copyChildWithParentRetry(ctx, first.src, first.sink, ref, cols, parentRefs, recopyOn(first))
	}
	meta := buildRangeUpsertMeta(tgt, cols)
	fixes := make(map[*Sink]referencedFetcher, len(conns))
	for _, c := range conns {
		switch {
		case len(parentRefs) == 0:
			fixes[c.sink] = nil
		case fetchOn != nil:
			fixes[c.sink] = fetchOn(c)
		default:
			whole := recopyOn(c)
			fixes[c.sink] = func(ctx context.Context, _ string) error {
				for _, p := range parentRefs {
					if err := whole(ctx, p); err != nil {
						return err
					}
				}
				return nil
			}
		}
	}
	return runChunks(ctx, conns, chunks, func(ctx context.Context, c copyConn, ch engine.Chunk) error {
		return copyRangeWithParentRetry(ctx, c.src, c.sink, ref, cols, meta, ch.Lo, ch.Hi, fixes[c.sink])
	})
}

// parentFetch brings across, for a chunk that failed on missing parent rows, just the
// parent rows that chunk points at instead of the whole parent table: on a busy source a
// whole-table re-copy of a big parent took minutes per failed chunk, and the next chunk
// could need another. It inserts only rows the target lacks, and walks up to grandparents
// when those rows in turn point at rows the target lacks. A parent loaded in one statement
// (keepReal) or without a usable key is still re-copied whole, through recopyOn.
type parentFetch struct {
	byRef    map[engine.TableRef]engine.Table
	parents  map[engine.TableRef][]engine.TableRef
	fksTo    map[engine.TableRef]map[engine.TableRef][]engine.ForeignKey
	keepReal map[engine.TableRef][]string
	nullCols map[engine.TableRef][]string
	recopyOn func(copyConn) parentRecopier
	mu       *sync.Mutex // shared with recopyOn: parent writes from parallel chunks take turns
}

func newParentFetch(members []engine.Table, cyclicEdge map[string]bool, parents map[engine.TableRef][]engine.TableRef,
	keepReal, nullCols map[engine.TableRef][]string, recopyOn func(copyConn) parentRecopier, mu *sync.Mutex) *parentFetch {
	pf := &parentFetch{
		byRef: map[engine.TableRef]engine.Table{}, parents: parents,
		fksTo:    map[engine.TableRef]map[engine.TableRef][]engine.ForeignKey{},
		keepReal: keepReal, nullCols: nullCols, recopyOn: recopyOn, mu: mu,
	}
	for _, t := range members {
		pf.byRef[t.Ref] = t
	}
	for _, t := range members {
		for _, fk := range t.ForeignKeys {
			if _, ok := pf.byRef[fk.Parent]; !ok || cyclicEdge[fkKey(fk)] || fk.Child == fk.Parent {
				continue
			}
			if pf.fksTo[t.Ref] == nil {
				pf.fksTo[t.Ref] = map[engine.TableRef][]engine.ForeignKey{}
			}
			pf.fksTo[t.Ref][fk.Parent] = append(pf.fksTo[t.Ref][fk.Parent], fk)
		}
	}
	return pf
}

func (pf *parentFetch) fetch(ctx context.Context, c copyConn, child engine.TableRef, childWhere string) error {
	for _, p := range pf.parents[child] {
		cols := subtractCols(transportColumns(pf.byRef[p]), pf.nullCols[p])
		tgt, err := c.sink.tableMeta(ctx, p)
		if err != nil {
			return err
		}
		meta := buildRangeUpsertMeta(tgt, cols)
		if len(pf.keepReal[p]) > 0 || len(meta.pk) == 0 {
			if err := pf.recopyOn(c)(ctx, p); err != nil {
				return err
			}
			continue
		}
		where := referencedWhere(child, pf.fksTo[child][p], childWhere)
		load := func() error {
			pf.mu.Lock()
			defer pf.mu.Unlock()
			return copyWhere(ctx, c.src, c.sink, p, cols, meta, where, loadMissingOnly)
		}
		var up func(context.Context) error
		if len(pf.parents[p]) > 0 {
			up = func(ctx context.Context) error { return pf.fetch(ctx, c, p, where) }
		}
		if err := retryOnFK(ctx, load, up); err != nil {
			return err
		}
	}
	return nil
}

// referencedFetcher brings across the parent rows that the child rows matching
// childWhere (a predicate on the child table, run on the source) point at.
type referencedFetcher func(ctx context.Context, childWhere string) error

// copyConn is one source/target connection pair the cyclic copy can run a chunk on.
type copyConn struct {
	src  *Source
	sink *Sink
}

// runChunks runs do for every chunk, sharing the chunks out over conns with one chunk at
// a time per pair, in chunk order. With one pair it is a plain loop. On the first error
// no further chunks start, the ones already running finish (a pgx query is never
// cancelled midway: that would break its connection for the retries that follow), and
// that first error is returned.
func runChunks(ctx context.Context, conns []copyConn, chunks []engine.Chunk, do func(context.Context, copyConn, engine.Chunk) error) error {
	if len(conns) == 1 || len(chunks) < 2 {
		for _, ch := range chunks {
			if err := do(ctx, conns[0], ch); err != nil {
				return err
			}
		}
		return nil
	}
	var (
		mu       sync.Mutex
		next     int
		firstErr error
		wg       sync.WaitGroup
	)
	take := func() (engine.Chunk, bool) {
		mu.Lock()
		defer mu.Unlock()
		if firstErr != nil || next >= len(chunks) || ctx.Err() != nil {
			return engine.Chunk{}, false
		}
		ch := chunks[next]
		next++
		return ch, true
	}
	fail := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		if firstErr == nil {
			firstErr = err
		}
	}
	n := len(conns)
	if n > len(chunks) {
		n = len(chunks)
	}
	for _, c := range conns[:n] {
		wg.Add(1)
		go func(c copyConn) {
			defer wg.Done()
			for {
				ch, ok := take()
				if !ok {
					return
				}
				if err := do(ctx, c, ch); err != nil {
					fail(err)
					return
				}
			}
		}(c)
	}
	wg.Wait()
	if firstErr != nil {
		return firstErr
	}
	return ctx.Err()
}

// copyRangeWithParentRetry copies one key-range chunk with the live-source parent-skew
// retry: only this chunk is retried, and fix brings across the parent rows it is missing
// (nil: nothing to fetch, so an FK error is returned as is).
func copyRangeWithParentRetry(ctx context.Context, src *Source, sink *Sink, ref engine.TableRef, cols []string, meta rangeUpsertMeta, lo, hi engine.KeyValues, fix referencedFetcher) error {
	pred, err := keysetPredicate(meta.pk, lo, hi)
	if err != nil {
		return err
	}
	load := func() error {
		return countedLoad(ctx, ref, func(ctx context.Context) error {
			return copyWhere(ctx, src, sink, ref, cols, meta, pred, loadAuto)
		})
	}
	err = load()
	for attempt := 0; err != nil && attempt < cyclicCopyRetries; attempt++ {
		if !engine.IsTransientConstraint(classifyFKViolation(err)) {
			return err // not an FK skew — halt loud
		}
		if fix == nil {
			break
		}
		// Fetch the missing parents and re-read the chunk from ONE source snapshot. The
		// source enforces the FK, so every row the chunk holds in that snapshot points at a
		// parent row the same snapshot holds, and the fetch brings exactly those across: the
		// retry cannot trip over a row the app created in between. (A dropped source
		// connection loses the snapshot; the retry is then as before, still converging.)
		err = inSourceSnapshot(ctx, src, func() error {
			if e := fix(ctx, pred); e != nil {
				return e
			}
			return load()
		})
		if err != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(cyclicRetryBackoff(attempt)):
			}
		}
	}
	if err != nil {
		return classifyFKViolation(err)
	}
	return nil
}

// inSourceSnapshot runs fn with every read on src inside one REPEATABLE READ, read-only
// transaction, so they all see the source as of the same instant. If the transaction
// cannot be opened, fn runs without it.
func inSourceSnapshot(ctx context.Context, src *Source, fn func() error) error {
	if src.conn == nil {
		return fn()
	}
	if _, err := src.conn.Exec(ctx, "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY"); err != nil {
		return fn()
	}
	err := fn()
	if src.conn != nil && !src.conn.IsClosed() {
		_, _ = src.conn.Exec(context.Background(), "ROLLBACK") // read-only: nothing to keep
	}
	return err
}

// referencedWhere is a predicate on a parent table selecting the rows that child rows
// matching childWhere point at through fks (all from child to that parent), e.g.
// ("id") IN (SELECT "conversationId" FROM "public"."Message" WHERE <childWhere>). It runs
// on the source, and nests: a grandparent's predicate wraps the parent's.
func referencedWhere(child engine.TableRef, fks []engine.ForeignKey, childWhere string) string {
	parts := make([]string, len(fks))
	for i, fk := range fks {
		parts[i] = fmt.Sprintf("(%s) IN (SELECT %s FROM %s WHERE %s)",
			quotedColumnList(fk.ParentCols), quotedColumnList(fk.ChildCols), qualifyTable(child), childWhere)
	}
	if len(parts) == 1 {
		return parts[0]
	}
	return "(" + strings.Join(parts, " OR ") + ")"
}

// insertMissingSQL inserts the staged rows whose key the target does not have yet and
// leaves the rest alone.
func insertMissingSQL(t engine.TableRef, stg string, cols []string, pk []captureCol, identity bool) string {
	overriding := ""
	if identity {
		overriding = " OVERRIDING SYSTEM VALUE"
	}
	pkNames := make([]string, len(pk))
	for i, c := range pk {
		pkNames[i] = quoteIdentifier(c.Name)
	}
	return fmt.Sprintf("INSERT INTO %s (%s)%s SELECT %s FROM %s ON CONFLICT (%s) DO NOTHING",
		qualifyTable(t), quotedColumnList(cols), overriding, quotedColumnList(cols),
		quoteIdentifier(stg), strings.Join(pkNames, ", "))
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
	tag, loadErr := sink.conn.PgConn().CopyFrom(ctx, pr, sinkCopySQL)
	_ = pr.CloseWithError(loadErr)
	copyErr := <-errc
	if copyErr != nil {
		return fmt.Errorf("read side: %w", copyErr)
	}
	if loadErr != nil {
		return fmt.Errorf("write side: %w", loadErr)
	}
	countCopiedRows(ctx, tag.RowsAffected())
	return nil
}

// pipeCopyWhere is pipeCopy restricted to a source key range (WHERE where): it streams
// only the rows the predicate selects into the given COPY FROM STDIN statement. Used by
// the chunked cyclic copy so each chunk moves a bounded slice of a large table.
func pipeCopyWhere(ctx context.Context, src *Source, ref engine.TableRef, cols []string, where string, sink *Sink, sinkCopySQL string) error {
	pr, pw := io.Pipe()
	errc := make(chan error, 1)
	go func() {
		err := src.copyColsWhere(ctx, ref, cols, where, pw)
		_ = pw.CloseWithError(err)
		errc <- err
	}()
	tag, loadErr := sink.conn.PgConn().CopyFrom(ctx, pr, sinkCopySQL)
	_ = pr.CloseWithError(loadErr)
	copyErr := <-errc
	if copyErr != nil {
		return fmt.Errorf("read side: %w", copyErr)
	}
	if loadErr != nil {
		return fmt.Errorf("write side: %w", loadErr)
	}
	countCopiedRows(ctx, tag.RowsAffected())
	return nil
}

// copyColsWhere streams an explicit column subset of a source table restricted to a
// WHERE predicate (a keyset range) as text COPY into w. The ranged analogue of
// copyAllCols, used by the chunked cyclic copy.
func (s *Source) copyColsWhere(ctx context.Context, ref engine.TableRef, cols []string, where string, w io.Writer) error {
	if err := s.requireConn(); err != nil {
		return err
	}
	sql := fmt.Sprintf("COPY (SELECT %s FROM %s WHERE %s) TO STDOUT", quotedColumnList(cols), qualifyTable(ref), where)
	if _, err := s.conn.PgConn().CopyTo(ctx, w, sql); err != nil {
		return fmt.Errorf("postgres: copy %s range: %w", ref, err)
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
