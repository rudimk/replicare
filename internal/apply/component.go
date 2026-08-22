package apply

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/rudimk/replicare/internal/engine"
)

// Conn is a source+sink pair used for per-table apply. A pool of distinct Conns
// lets a component's tables apply CONCURRENTLY within a drain pass (CLAUDE.md §8's
// "parallel delta apply"). Each Conn is used by at most one goroutine at a time
// (the pool is a free-list), matching the "one connection, not concurrent-safe"
// contract every engine's Source/Sink relies on. pool[0] is the primary pair and
// is also used for the serial-only work (gather, the atomic cyclic path).
type Conn struct {
	Src  engine.Source
	Sink engine.Sink
}

// FK-ordered component apply (CLAUDE.md §8.1). A component's dirty changes for
// one drain pass are applied in a single target transaction so the target is
// referentially consistent within the group: upserts apply parent->child (topo
// order), deletes apply child->parent (reverse). FK checks are deferred to
// commit, so deferrable cyclic FKs succeed; other cross-dependencies are handled
// by the retry fallback.

// tableWork is one table's coalesced dirty state for a pass.
type tableWork struct {
	ref      engine.TableRef
	distinct []engine.KeyValues
	ids      []engine.DeltaID
	cols     []string
}

// gatherWork reads and coalesces each table's dirty keys (bounded by batch),
// preserving topo order and dropping tables with an empty queue. total is the
// number of deltas observed across the pass.
func gatherWork(ctx context.Context, src engine.Source, tablesTopoOrder []engine.TableRef,
	target engine.TargetID, batch int) (work []tableWork, total int, err error) {
	for _, ref := range tablesTopoOrder {
		dirty, err := src.ReadDirtyKeys(ctx, ref, target, batch)
		if err != nil {
			return nil, 0, fmt.Errorf("apply component: read dirty %s: %w", ref, err)
		}
		if len(dirty) == 0 {
			continue
		}
		cols, err := transportColumns(ctx, src, ref)
		if err != nil {
			return nil, 0, err
		}
		distinct, ids := coalesce(dirty)
		work = append(work, tableWork{ref: ref, distinct: distinct, ids: ids, cols: cols})
		total += len(dirty)
	}
	return work, total, nil
}

// DrainComponent applies one drain pass for an FK component whose tables are
// given in topological order (parents first). It returns the number of deltas
// consumed (0 when every table's queue is empty). The whole pass is atomic on
// the target; ConfirmConsumed runs only after the commit (crash-safe). `cyclic`
// reports whether the component contains an FK cycle/self-reference; it is passed
// to BeginApply so a cycle-safe engine (MySQL) can disable FK checks and run a
// pre-commit orphan verification over the full component (CLAUDE.md §3.3, §8.1).
func DrainComponent(ctx context.Context, src engine.Source, sink engine.Sink,
	tablesTopoOrder []engine.TableRef, target engine.TargetID, batch int, cyclic bool) (int, error) {
	return DrainComponentPool(ctx, []Conn{{Src: src, Sink: sink}}, 1, tablesTopoOrder, target, batch, cyclic)
}

// DrainComponentPool is DrainComponent with a connection pool: the per-table
// upsert/delete/fill work of an (acyclic or nullable-cyclic) component runs across
// up to `concurrency` of the pool's Conns, so several tables apply at once — the
// fix for many tables backlogging behind a single-threaded drain. concurrency is
// clamped to len(pool); concurrency<=1 (or a single-Conn pool) is the original
// strictly-sequential, in-topo-order path, byte-for-byte. The atomic single-
// transaction cyclic path (all-DEFERRABLE Postgres, MySQL cyclic) is inherently
// one connection and always runs on pool[0].
func DrainComponentPool(ctx context.Context, pool []Conn, concurrency int,
	tablesTopoOrder []engine.TableRef, target engine.TargetID, batch int, cyclic bool) (int, error) {
	if len(pool) == 0 {
		return 0, fmt.Errorf("apply component: empty connection pool")
	}
	if concurrency > len(pool) {
		concurrency = len(pool)
	}
	if concurrency < 1 {
		concurrency = 1
	}
	src, sink := pool[0].Src, pool[0].Sink

	// Acyclic components drain table-by-table so parents make progress
	// independently of a transiently-blocked child (drainAcyclic).
	if !cyclic {
		return drainAcyclic(ctx, pool, concurrency, tablesTopoOrder, target, batch)
	}

	// A cyclic component whose cycle-closing FK columns are nullable drains
	// per-table with NULL-then-fill (Postgres): parents land independently of a
	// cross-batch child (the same livelock drainAcyclic avoids) and the cycle is
	// closed by a final fill phase. An engine that does not offer this
	// (NullFillCyclicSink), or a component with no nullable cyclic column (an
	// all-DEFERRABLE cycle → empty result), falls through to the single atomic
	// transaction, which relies on the engine's cycle-safe strategy (Postgres
	// SET CONSTRAINTS ALL DEFERRED, MySQL FK-checks-off + whole-component
	// pre-commit verify).
	if nf, ok := sink.(engine.NullFillCyclicSink); ok {
		cyclicCols, err := nf.CyclicCols(ctx, tablesTopoOrder)
		if err != nil {
			return 0, fmt.Errorf("apply component: resolve cyclic FK columns: %w", err)
		}
		if len(cyclicCols) > 0 {
			return drainCyclicNullFill(ctx, pool, concurrency, tablesTopoOrder, target, batch, cyclicCols)
		}
	}

	work, total, err := gatherWork(ctx, src, tablesTopoOrder, target, batch)
	if err != nil {
		return 0, err
	}
	if len(work) == 0 {
		return 0, nil
	}

	tx, err := sink.BeginApply(ctx, cyclic, tablesTopoOrder)
	if err != nil {
		return 0, fmt.Errorf("apply component: begin: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(context.Background())
		}
	}()

	// Upserts: parent -> child.
	for _, w := range work {
		if err := pipeStageUpsert(ctx, src, tx, w.ref, w.cols, w.distinct); err != nil {
			return 0, fmt.Errorf("apply component: stage %s: %w", w.ref, err)
		}
	}
	// Deletes: child -> parent (reverse).
	for i := len(work) - 1; i >= 0; i-- {
		if err := tx.DeleteAbsent(ctx, work[i].ref, work[i].distinct); err != nil {
			return 0, fmt.Errorf("apply component: delete %s: %w", work[i].ref, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	committed = true

	// Crash-safe: the pass committed to the target; now record consumption.
	for _, w := range work {
		if err := src.ConfirmConsumed(ctx, w.ref, target, w.ids); err != nil {
			return total, fmt.Errorf("apply component: confirm %s: %w", w.ref, err)
		}
	}
	return total, nil
}

// drainAcyclic drains an acyclic component with PER-TABLE committed transactions
// instead of one component-wide transaction, so a parent advances even when a
// child in the same pass transiently cannot apply. This deliberately relaxes the
// per-component single-transaction atomicity (CLAUDE.md §8.1 permits it for churn
// a single bounded pass can't close); the target still ends each pass referentially
// consistent because the two phases are FK-ordered.
//
// The livelock this avoids: per-table dirty-key batching is by each table's own
// delta_id sequence, so a child batch can reference a parent whose delta falls in a
// different batch (a PK-change cascade is the classic case: a renamed product's new
// (tenant_id, sku) is referenced by product_categories/order_items rows whose
// cascade deltas sit at unrelated positions). The target FK is checked immediately
// (a standard, non-DEFERRABLE FK ignores SET CONSTRAINTS ALL DEFERRED), so the
// child upsert fails; applying the whole component in one transaction rolls the
// parent back too, so it never advances and the split repeats forever. Committing
// per table lets the parent land; the blocked child stays dirty (unconfirmed) and
// resolves on a later pass — bounded per pass, no unbounded re-read.
//
// Two phases, matching the atomic path's intra-transaction ordering:
//   - UPSERT phase, parents→children: stage the re-read present rows and upsert.
//   - DELETE phase, children→parents: delete the dirty keys absent at the source.
//
// Deletes MUST follow upserts and run child-first (a parent row can only be deleted
// once its children are gone from the target), so a single per-table transaction
// that mixed a table's upserts and deletes would deadlock — the phases are split.
// A table is confirmed only after BOTH its phases commit; a transient failure in
// either leaves it fully dirty for the next pass. Any transient error is returned
// (after attempting every table) so the caller retries; a non-transient error
// halts loud immediately.
func drainAcyclic(ctx context.Context, pool []Conn, concurrency int,
	tablesTopoOrder []engine.TableRef, target engine.TargetID, batch int) (int, error) {
	work, _, err := gatherWork(ctx, pool[0].Src, tablesTopoOrder, target, batch)
	if err != nil {
		return 0, err
	}
	if len(work) == 0 {
		return 0, nil
	}
	if concurrency <= 1 || len(pool) <= 1 {
		return drainAcyclicSeq(ctx, pool[0], work, target)
	}
	return drainAcyclicParallel(ctx, pool, work, target)
}

// drainAcyclicSeq is the original strictly-sequential, in-topo-order acyclic drain
// (concurrency 1). Kept verbatim so the default path is unchanged.
func drainAcyclicSeq(ctx context.Context, c Conn, work []tableWork, target engine.TargetID) (int, error) {
	src, sink := c.Src, c.Sink
	var transient error
	upserted := make([]bool, len(work))

	// Phase 1 — upserts, parents→children.
	for i, w := range work {
		if err := perTableUpsert(ctx, src, sink, w); err != nil {
			if engine.IsTransientConstraint(err) {
				transient = err
				continue
			}
			return 0, fmt.Errorf("apply component: upsert %s: %w", w.ref, err)
		}
		upserted[i] = true
	}

	// Phase 2 — deletes, children→parents. Only tables whose upsert committed are
	// eligible; a table needs both phases before it is confirmed.
	total := 0
	for i := len(work) - 1; i >= 0; i-- {
		if !upserted[i] {
			continue
		}
		w := work[i]
		if err := perTableDelete(ctx, src, sink, w); err != nil {
			if engine.IsTransientConstraint(err) {
				transient = err
				continue
			}
			return total, fmt.Errorf("apply component: delete %s: %w", w.ref, err)
		}
		if err := src.ConfirmConsumed(ctx, w.ref, target, w.ids); err != nil {
			return total, fmt.Errorf("apply component: confirm %s: %w", w.ref, err)
		}
		total += len(w.ids)
	}
	return total, transient
}

// drainAcyclicParallel is the concurrent acyclic drain: the same two FK-ordered
// phases, but the tables WITHIN each phase apply across the pool (bounded to
// len(pool) concurrent tasks, each on its own Conn). FK order is not guaranteed
// within a phase, so a child that lands before its parent (or a parent deleted
// before its child) hits a transient FK and stays dirty for the caller's retry —
// exactly the sequential path's transient handling. The phase barrier preserves
// pass-end referential consistency (all upserts attempted before any delete).
func drainAcyclicParallel(ctx context.Context, pool []Conn, work []tableWork, target engine.TargetID) (int, error) {
	n := len(work)
	var transient error

	// Phase 1 — upserts (any order; transient FK on a not-yet-present parent).
	e1 := applyPhase(ctx, pool, n, forwardIdx(n), func(ctx context.Context, c Conn, i int) error {
		return perTableUpsert(ctx, c.Src, c.Sink, work[i])
	})
	upserted := make([]bool, n)
	for i, e := range e1 {
		switch {
		case e == nil:
			upserted[i] = true
		case engine.IsTransientConstraint(e):
			transient = e
		default:
			return 0, fmt.Errorf("apply component: upsert %s: %w", work[i].ref, e)
		}
	}

	// Phase 2 — deletes + confirm, only tables whose upsert committed.
	var mu sync.Mutex
	total := 0
	e2 := applyPhase(ctx, pool, n, reverseIdx(n, upserted), func(ctx context.Context, c Conn, i int) error {
		if err := perTableDelete(ctx, c.Src, c.Sink, work[i]); err != nil {
			return err
		}
		if err := c.Src.ConfirmConsumed(ctx, work[i].ref, target, work[i].ids); err != nil {
			return err
		}
		mu.Lock()
		total += len(work[i].ids)
		mu.Unlock()
		return nil
	})
	for i, e := range e2 {
		switch {
		case e == nil:
		case engine.IsTransientConstraint(e):
			transient = e
		default:
			return total, fmt.Errorf("apply component: delete/confirm %s: %w", work[i].ref, e)
		}
	}
	return total, transient
}

// drainCyclicNullFill drains a cyclic FK component with per-table committed
// transactions and NULL-then-fill, so it breaks BOTH livelocks a single-component
// transaction suffers under real (non-DEFERRABLE) target FKs:
//
//   - Non-cyclic cross-batch edges (a child batch references a parent whose delta
//     is in a different batch): per-table commits let the parent advance through
//     its own queue while the blocked child stays dirty and retries — exactly the
//     drainAcyclic fix, here applied inside a cyclic component (e.g. events→users,
//     order_items→orders).
//   - The cycle itself (users↔orders): the cyclic FK columns are loaded NULL in the
//     upsert phase (breaking the cycle so a non-DEFERRABLE FK check passes now) and
//     filled in a final phase once every referenced row is present.
//
// cyclicCols maps each table to its nullable cyclic FK columns (empty for a
// non-cyclic member). Three per-table phases, matching the initial-copy NULL-fill
// (CLAUDE.md §4.1) and the acyclic drain's ordering:
//
//	Phase 1 — upsert, parents→children, cyclic FK columns NULL. Non-cyclic
//	          cross-batch dependencies resolve by per-table progress + retry.
//	Phase 2 — delete absent, children→parents (only tables whose upsert landed).
//	Phase 3 — fill cyclic FK columns, parents→children (only cyclic-column tables
//	          whose upsert+delete landed). Runs LAST so nothing re-NULLs a filled
//	          column; a still-missing cross-batch reference stays transient.
//
// A table is confirmed only after every applicable phase commits; a transient
// failure in any phase leaves it dirty for the next pass (self-healing). Any
// transient error is returned (after attempting every table) so the caller retries;
// a non-transient error halts loud immediately.
func drainCyclicNullFill(ctx context.Context, pool []Conn, concurrency int,
	tablesTopoOrder []engine.TableRef, target engine.TargetID, batch int,
	cyclicCols map[engine.TableRef][]string) (int, error) {

	work, _, err := gatherWork(ctx, pool[0].Src, tablesTopoOrder, target, batch)
	if err != nil {
		return 0, err
	}
	if len(work) == 0 {
		return 0, nil
	}
	if concurrency <= 1 || len(pool) <= 1 {
		return drainCyclicNullFillSeq(ctx, pool[0], work, tablesTopoOrder, target, cyclicCols)
	}
	return drainCyclicNullFillParallel(ctx, pool, work, tablesTopoOrder, target, cyclicCols)
}

// drainCyclicNullFillSeq is the original strictly-sequential NULL-fill cyclic drain
// (concurrency 1). Kept verbatim so the default path is unchanged.
func drainCyclicNullFillSeq(ctx context.Context, c Conn, work []tableWork,
	tablesTopoOrder []engine.TableRef, target engine.TargetID, cyclicCols map[engine.TableRef][]string) (int, error) {
	src, sink := c.Src, c.Sink
	var transient error
	upserted := make([]bool, len(work))
	deleted := make([]bool, len(work))
	filled := make([]bool, len(work))

	// Phase 1 — upsert (cyclic FK columns NULL), parents→children.
	for i, w := range work {
		if err := perTableApplyCyclic(ctx, src, sink, w, tablesTopoOrder, false, false); err != nil {
			if engine.IsTransientConstraint(err) {
				transient = err
				continue
			}
			return 0, fmt.Errorf("apply component: upsert %s: %w", w.ref, err)
		}
		upserted[i] = true
	}

	// Phase 2 — deletes, children→parents (only tables whose upsert landed).
	for i := len(work) - 1; i >= 0; i-- {
		if !upserted[i] {
			continue
		}
		if err := perTableApplyCyclic(ctx, src, sink, work[i], tablesTopoOrder, true, false); err != nil {
			if engine.IsTransientConstraint(err) {
				transient = err
				continue
			}
			return 0, fmt.Errorf("apply component: delete %s: %w", work[i].ref, err)
		}
		deleted[i] = true
	}

	// Phase 3 — fill cyclic FK columns, parents→children. A non-cyclic member has
	// nothing to fill and is done after phases 1–2.
	for i, w := range work {
		if len(cyclicCols[w.ref]) == 0 {
			filled[i] = true
			continue
		}
		if !upserted[i] || !deleted[i] {
			continue
		}
		if err := perTableApplyCyclic(ctx, src, sink, w, tablesTopoOrder, false, true); err != nil {
			if engine.IsTransientConstraint(err) {
				transient = err
				continue
			}
			return 0, fmt.Errorf("apply component: fill %s: %w", w.ref, err)
		}
		filled[i] = true
	}

	// Confirm only tables that completed every applicable phase.
	total := 0
	for i, w := range work {
		if upserted[i] && deleted[i] && filled[i] {
			if err := src.ConfirmConsumed(ctx, w.ref, target, w.ids); err != nil {
				return total, fmt.Errorf("apply component: confirm %s: %w", w.ref, err)
			}
			total += len(w.ids)
		}
	}
	return total, transient
}

// drainCyclicNullFillParallel is the concurrent NULL-fill cyclic drain: the same
// three FK-ordered phases (upsert-NULL, delete, fill), but tables WITHIN each phase
// apply across the pool. Barriers between phases preserve the invariants; a table
// blocked in any phase stays dirty for the caller's retry (transient FK), and a
// table is confirmed only after all its applicable phases commit.
func drainCyclicNullFillParallel(ctx context.Context, pool []Conn, work []tableWork,
	tablesTopoOrder []engine.TableRef, target engine.TargetID, cyclicCols map[engine.TableRef][]string) (int, error) {
	n := len(work)
	var transient error

	// Phase 1 — upsert (cyclic FK columns NULL).
	e1 := applyPhase(ctx, pool, n, forwardIdx(n), func(ctx context.Context, c Conn, i int) error {
		return perTableApplyCyclic(ctx, c.Src, c.Sink, work[i], tablesTopoOrder, false, false)
	})
	upserted := make([]bool, n)
	for i, e := range e1 {
		switch {
		case e == nil:
			upserted[i] = true
		case engine.IsTransientConstraint(e):
			transient = e
		default:
			return 0, fmt.Errorf("apply component: upsert %s: %w", work[i].ref, e)
		}
	}

	// Phase 2 — deletes, only tables whose upsert landed.
	e2 := applyPhase(ctx, pool, n, reverseIdx(n, upserted), func(ctx context.Context, c Conn, i int) error {
		return perTableApplyCyclic(ctx, c.Src, c.Sink, work[i], tablesTopoOrder, true, false)
	})
	deleted := make([]bool, n)
	for i, e := range e2 {
		switch {
		case !upserted[i]:
		case e == nil:
			deleted[i] = true
		case engine.IsTransientConstraint(e):
			transient = e
		default:
			return 0, fmt.Errorf("apply component: delete %s: %w", work[i].ref, e)
		}
	}

	// Phase 3 — fill cyclic FK columns (only cyclic-column tables that landed both
	// prior phases). A non-cyclic member is done after phases 1–2.
	fillIdx := make([]int, 0, n)
	filled := make([]bool, n)
	for i := 0; i < n; i++ {
		if len(cyclicCols[work[i].ref]) == 0 {
			filled[i] = true
			continue
		}
		if upserted[i] && deleted[i] {
			fillIdx = append(fillIdx, i)
		}
	}
	e3 := applyPhase(ctx, pool, n, fillIdx, func(ctx context.Context, c Conn, i int) error {
		return perTableApplyCyclic(ctx, c.Src, c.Sink, work[i], tablesTopoOrder, false, true)
	})
	for _, i := range fillIdx {
		switch e := e3[i]; {
		case e == nil:
			filled[i] = true
		case engine.IsTransientConstraint(e):
			transient = e
		default:
			return 0, fmt.Errorf("apply component: fill %s: %w", work[i].ref, e)
		}
	}

	// Confirm only tables that completed every applicable phase.
	total := 0
	for i, w := range work {
		if upserted[i] && deleted[i] && filled[i] {
			if err := pool[0].Src.ConfirmConsumed(ctx, w.ref, target, w.ids); err != nil {
				return total, fmt.Errorf("apply component: confirm %s: %w", w.ref, err)
			}
			total += len(w.ids)
		}
	}
	return total, transient
}

// applyPhase runs task for each work index in `order` across the pool, bounding
// in-flight tasks to len(pool) by handing each goroutine an exclusive Conn from a
// free-list (so no connection is used concurrently). It returns per-work-index
// errors (len n; indices not in `order` stay nil). Used only for concurrency>1;
// the sequential drains iterate directly to preserve strict ordering and the
// early-halt-on-non-transient semantics.
func applyPhase(ctx context.Context, pool []Conn, n int, order []int,
	task func(ctx context.Context, c Conn, i int) error) []error {
	errs := make([]error, n)
	connCh := make(chan Conn, len(pool))
	for _, c := range pool {
		connCh <- c
	}
	var wg sync.WaitGroup
	for _, idx := range order {
		if ctx.Err() != nil {
			errs[idx] = ctx.Err()
			continue
		}
		c := <-connCh // blocks until a Conn frees, bounding concurrency to len(pool)
		wg.Add(1)
		go func(i int, c Conn) {
			defer wg.Done()
			defer func() { connCh <- c }()
			errs[i] = task(ctx, c, i)
		}(idx, c)
	}
	wg.Wait()
	return errs
}

// forwardIdx returns [0, 1, …, n-1] — parents-before-children dispatch order.
func forwardIdx(n int) []int {
	idx := make([]int, n)
	for i := range idx {
		idx[i] = i
	}
	return idx
}

// reverseIdx returns [n-1, …, 0] filtered to indices where mask is set —
// children-before-parents delete order, restricted to tables whose upsert landed.
func reverseIdx(n int, mask []bool) []int {
	idx := make([]int, 0, n)
	for i := n - 1; i >= 0; i-- {
		if mask[i] {
			idx = append(idx, i)
		}
	}
	return idx
}

// perTableApplyCyclic runs one phase of the per-table cyclic NULL-fill drain in its
// own committed transaction, under BeginApply(cyclic=true) so StageUpsert loads the
// table's cyclic FK columns NULL. It always stages the re-read present rows (so the
// cyclic fill and the delete's present/absent test have the current values); then,
// per phase: delete the dirty keys now absent at the source (del), and/or fill the
// cyclic FK columns from the staging (fill). Phase 1 passes both false.
func perTableApplyCyclic(ctx context.Context, src engine.Source, sink engine.Sink,
	w tableWork, componentTables []engine.TableRef, del, fill bool) error {
	tx, err := sink.BeginApply(ctx, true, componentTables)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(context.Background())
		}
	}()
	if err := pipeStageUpsert(ctx, src, tx, w.ref, w.cols, w.distinct); err != nil {
		return err
	}
	if fill {
		filler, ok := tx.(engine.CyclicFiller)
		if !ok {
			return fmt.Errorf("apply component: sink tx %T does not implement CyclicFiller", tx)
		}
		if err := filler.FillCyclic(ctx); err != nil {
			return err
		}
	}
	if del {
		if err := tx.DeleteAbsent(ctx, w.ref, w.distinct); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	committed = true
	return nil
}

// perTableUpsert applies one table's re-read present rows in its own committed
// transaction (no deletes). An FK violation here is transient (a child whose
// parent has not landed yet) and is classified as such by StageUpsert.
func perTableUpsert(ctx context.Context, src engine.Source, sink engine.Sink, w tableWork) error {
	tx, err := sink.BeginApply(ctx, false, nil)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(context.Background())
		}
	}()
	if err := pipeStageUpsert(ctx, src, tx, w.ref, w.cols, w.distinct); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	committed = true
	return nil
}

// perTableDelete deletes one table's dirty keys that are absent at the source, in
// its own committed transaction. It re-stages the present rows (an idempotent
// re-read + upsert) so DeleteAbsent can tell present from absent; the re-upsert is
// bounded (batch) and harmless.
func perTableDelete(ctx context.Context, src engine.Source, sink engine.Sink, w tableWork) error {
	tx, err := sink.BeginApply(ctx, false, nil)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(context.Background())
		}
	}()
	if err := pipeStageUpsert(ctx, src, tx, w.ref, w.cols, w.distinct); err != nil {
		return err
	}
	if err := tx.DeleteAbsent(ctx, w.ref, w.distinct); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	committed = true
	return nil
}

// pipeStageUpsert streams a table's re-read (source) into its staging (target tx).
func pipeStageUpsert(ctx context.Context, src engine.Source, tx engine.ApplyTx,
	ref engine.TableRef, cols []string, keys []engine.KeyValues) error {
	pr, pw := io.Pipe()
	errc := make(chan error, 1)
	go func() {
		err := src.RereadCurrent(ctx, ref, keys, pw)
		_ = pw.CloseWithError(err)
		errc <- err
	}()
	stageErr := tx.StageUpsert(ctx, ref, cols, pr)
	_ = pr.CloseWithError(stageErr)
	if rereadErr := <-errc; rereadErr != nil {
		return fmt.Errorf("re-read: %w", rereadErr)
	}
	return stageErr
}

// coalesce reduces dirty keys to one entry per distinct PK while collecting every
// observed delta_id (for exact delete-by-id consumption).
func coalesce(dirty []engine.DirtyKey) (distinct []engine.KeyValues, ids []engine.DeltaID) {
	ids = make([]engine.DeltaID, 0, len(dirty))
	seen := map[string]bool{}
	for _, d := range dirty {
		ids = append(ids, d.DeltaID)
		sig := keySignature(d.Key)
		if !seen[sig] {
			seen[sig] = true
			distinct = append(distinct, d.Key)
		}
	}
	return distinct, ids
}
