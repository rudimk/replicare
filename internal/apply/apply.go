// Package apply is the engine-neutral streaming drain loop (CLAUDE.md §3.3): it
// reads a target's unconsumed dirty keys (delta MINUS track), coalesces them,
// re-reads the current source values, applies them idempotently to the target,
// and only THEN records the deltas as consumed. That ordering — apply to target,
// then track — is what makes it crash-safe and at-least-once without 2PC: a crash
// before the track write leaves the deltas unconsumed, so the next pass
// reprocesses them and the idempotent re-read+apply converges harmlessly.
package apply

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/rudimk/replicare/internal/engine"
)

// Drain performs one streaming drain pass for a table and target: it processes up
// to maxBatch unconsumed deltas and returns how many it consumed (0 when the
// queue is empty). Call it repeatedly to drain a backlog.
func Drain(ctx context.Context, src engine.Source, sink engine.Sink,
	t engine.TableRef, target engine.TargetID, maxBatch int) (int, error) {

	dirty, err := src.ReadDirtyKeys(ctx, t, target, maxBatch)
	if err != nil {
		return 0, fmt.Errorf("apply %s: read dirty keys: %w", t, err)
	}
	if len(dirty) == 0 {
		return 0, nil
	}

	// Coalesce: one re-read per distinct PK (bounded work under churn), but track
	// EVERY observed delta_id so consumption is exact (delete-by-delta_id).
	distinct, ids := coalesce(dirty)

	cols, err := transportColumns(ctx, src, t)
	if err != nil {
		return 0, err
	}

	// Re-read current source rows for the dirty keys and apply them (present ->
	// upsert, absent -> delete) in one target transaction.
	if err := pipeApply(ctx, src, sink, t, cols, distinct); err != nil {
		return 0, fmt.Errorf("apply %s: %w", t, err)
	}

	// Crash-safe ordering: the apply has committed to the target; only now record
	// the deltas as consumed for this target.
	if err := src.ConfirmConsumed(ctx, t, target, ids); err != nil {
		return 0, fmt.Errorf("apply %s: confirm consumed: %w", t, err)
	}
	return len(dirty), nil
}

// DrainAll drains a table's backlog to empty (or until ctx is done), returning
// the total consumed.
func DrainAll(ctx context.Context, src engine.Source, sink engine.Sink,
	t engine.TableRef, target engine.TargetID, batch int) (int, error) {
	total := 0
	for {
		n, err := Drain(ctx, src, sink, t, target, batch)
		if err != nil {
			return total, err
		}
		total += n
		if n == 0 {
			return total, nil
		}
	}
}

// pipeApply streams the re-read (source) into the apply (target) via an io.Pipe.
func pipeApply(ctx context.Context, src engine.Source, sink engine.Sink,
	t engine.TableRef, cols []string, keys []engine.KeyValues) error {
	pr, pw := io.Pipe()
	errc := make(chan error, 1)
	go func() {
		err := src.RereadCurrent(ctx, t, keys, pw)
		_ = pw.CloseWithError(err)
		errc <- err
	}()
	applyErr := sink.ApplyPass(ctx, t, cols, keys, pr)
	_ = pr.CloseWithError(applyErr)
	if rereadErr := <-errc; rereadErr != nil {
		return fmt.Errorf("re-read: %w", rereadErr)
	}
	if applyErr != nil {
		return fmt.Errorf("apply: %w", applyErr)
	}
	return nil
}

// keySignature builds a collision-free string key from a text key tuple.
func keySignature(kv engine.KeyValues) string {
	parts := make([]string, len(kv))
	for i, v := range kv {
		if s, ok := v.(string); ok {
			parts[i] = s
		} else {
			parts[i] = fmt.Sprint(v)
		}
	}
	return strings.Join(parts, "\x00")
}

// transportColumns returns the source table's name-matched column list (SOURCE
// order, GENERATED STORED excluded) — the order RereadCurrent emits.
func transportColumns(ctx context.Context, src engine.Source, ref engine.TableRef) ([]string, error) {
	schema, err := src.Introspect(ctx, engine.Selection{Include: []string{ref.String()}})
	if err != nil {
		return nil, fmt.Errorf("apply %s: introspect columns: %w", ref, err)
	}
	for _, tbl := range schema.Tables {
		if tbl.Ref != ref {
			continue
		}
		cols := make([]string, 0, len(tbl.Columns))
		for _, c := range tbl.Columns {
			if c.Generated {
				continue
			}
			cols = append(cols, c.Name)
		}
		return cols, nil
	}
	return nil, fmt.Errorf("apply %s: table not found on source", ref)
}
