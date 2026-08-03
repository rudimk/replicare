package pipeline

import (
	"context"
	"fmt"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

// Delete reconciliation (redis-plan §0.4) is the durable, capture-less delete path
// for engines whose deletes are NOT recorded by a source-side change log — Redis.
// Such an engine implements the optional engine.KeyLister (Sink) + engine.KeyExister
// (Source) pair; capture-driven engines (Postgres/MySQL) do not, so the step below
// type-asserts them and is a complete no-op for those engines (their deletes flow
// through the normal delta path). This is the honest, bounded price of Redis's
// capture-less deletes — one additive optional seam, reusing the existing
// DeleteAbsent apply path.

// DeleteSweepStep runs ONE bounded step of the target-vs-source delete diff for a
// unit: scan a batch of target keys, ask the source which are missing, and DELETE
// exactly those on the target via an ApplyTx with empty staging (nothing staged ⇒
// every passed key is "absent" ⇒ all deleted). It returns the number deleted and
// the continuation token (next==0 ⇒ the rolling target scan completed a full pass).
// Because every sweep is a full target-vs-source diff, it subsumes the copy→stream
// cutover orphan window and the restart case with no special-casing (Momus M3).
func DeleteSweepStep(ctx context.Context, lister engine.KeyLister, exister engine.KeyExister,
	sink engine.Sink, ref engine.TableRef, cursor uint64, batch int) (deleted int, next uint64, err error) {

	keys, next, err := lister.ScanTargetKeys(ctx, ref, cursor, batch)
	if err != nil {
		return 0, cursor, fmt.Errorf("delete sweep: scan target %s: %w", ref, err)
	}
	if len(keys) == 0 {
		return 0, next, nil
	}
	missing, err := exister.MissingAtSource(ctx, ref, keys)
	if err != nil {
		return 0, cursor, fmt.Errorf("delete sweep: missing-at-source %s: %w", ref, err)
	}
	if len(missing) == 0 {
		return 0, next, nil
	}

	tx, err := sink.BeginApply(ctx, false, []engine.TableRef{ref})
	if err != nil {
		return 0, cursor, fmt.Errorf("delete sweep: begin %s: %w", ref, err)
	}
	// Empty staging: no StageUpsert, so DeleteAbsent deletes every passed key.
	if err := tx.DeleteAbsent(ctx, ref, missing); err != nil {
		_ = tx.Rollback(ctx)
		return 0, cursor, fmt.Errorf("delete sweep: delete %s: %w", ref, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, cursor, fmt.Errorf("delete sweep: commit %s: %w", ref, err)
	}
	return len(missing), next, nil
}

// deleteReconcile runs one bounded delete-sweep step per unit on a healthy pass, for
// engines that implement the KeyLister/KeyExister capability. It is a no-op for
// capture-driven engines (the type-asserts fail), so Postgres/MySQL are unaffected.
// The per-unit target cursor persists across passes (s.sweepCursors), so successive
// bounded steps walk the whole target keyspace and complete a full diff over several
// passes without ever buffering it.
func (s *Syncer) deleteReconcile(ctx context.Context) error {
	lister, ok := s.Sink.(engine.KeyLister)
	if !ok {
		return nil
	}
	exister, ok := s.Source.(engine.KeyExister)
	if !ok {
		return nil
	}
	if s.sweepCursors == nil {
		s.sweepCursors = map[engine.TableRef]uint64{}
	}
	if s.sweepStarted == nil {
		s.sweepStarted = map[engine.TableRef]time.Time{}
	}
	for _, ref := range s.Replicable {
		// Stamp the start of a fresh rolling pass (cursor at 0 and not yet timed).
		if s.sweepCursors[ref] == 0 && s.sweepStarted[ref].IsZero() {
			s.sweepStarted[ref] = time.Now()
		}
		deleted, next, err := DeleteSweepStep(ctx, lister, exister, s.Sink, ref, s.sweepCursors[ref], s.DrainBatch)
		if err != nil {
			return err
		}
		if deleted > 0 {
			s.Tel.AddDeletes(s.Name, s.Target, ref, int64(deleted))
		}
		s.sweepCursors[ref] = next
		// A completed rolling pass (next==0) publishes how long it took — the
		// delete-reconciliation staleness signal.
		if next == 0 {
			if st := s.sweepStarted[ref]; !st.IsZero() {
				s.Tel.SetDeleteLag(s.Name, s.Target, ref, time.Since(st).Seconds())
			}
			s.sweepStarted[ref] = time.Time{}
		}
	}
	return nil
}
