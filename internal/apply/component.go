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

// DrainComponent applies one drain pass for an FK component whose tables are
// given in topological order (parents first). It returns the number of deltas
// consumed (0 when every table's queue is empty). The whole pass is atomic on
// the target; ConfirmConsumed runs only after the commit (crash-safe). `cyclic`
// reports whether the component contains an FK cycle/self-reference; it is passed
// to BeginApply so a cycle-safe engine (MySQL) can disable FK checks and run a
// pre-commit orphan verification over the full component (CLAUDE.md §3.3, §8.1).
func DrainComponent(ctx context.Context, src engine.Source, sink engine.Sink,
	tablesTopoOrder []engine.TableRef, target engine.TargetID, batch int, cyclic bool) (int, error) {

	var work []tableWork
	total := 0
	for _, ref := range tablesTopoOrder {
		dirty, err := src.ReadDirtyKeys(ctx, ref, target, batch)
		if err != nil {
			return 0, fmt.Errorf("apply component: read dirty %s: %w", ref, err)
		}
		if len(dirty) == 0 {
			continue
		}
		cols, err := transportColumns(ctx, src, ref)
		if err != nil {
			return 0, err
		}
		distinct, ids := coalesce(dirty)
		work = append(work, tableWork{ref: ref, distinct: distinct, ids: ids, cols: cols})
		total += len(dirty)
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
