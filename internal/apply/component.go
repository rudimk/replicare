package apply

import (
	"context"
	"fmt"
	"io"

	"github.com/rudimk/replicare/internal/engine"
)

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

	// Acyclic components drain table-by-table so parents make progress
	// independently of a transiently-blocked child (drainAcyclic); only a cyclic
	// component needs the single atomic transaction (which relies on the engine's
	// cycle-safe strategy — Postgres SET CONSTRAINTS ALL DEFERRED, MySQL
	// FK-checks-off + pre-commit verify).
	if !cyclic {
		return drainAcyclic(ctx, src, sink, tablesTopoOrder, target, batch)
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
func drainAcyclic(ctx context.Context, src engine.Source, sink engine.Sink,
	tablesTopoOrder []engine.TableRef, target engine.TargetID, batch int) (int, error) {
	work, _, err := gatherWork(ctx, src, tablesTopoOrder, target, batch)
	if err != nil {
		return 0, err
	}
	if len(work) == 0 {
		return 0, nil
	}

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
