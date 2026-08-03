package redis

import (
	"context"
	"fmt"
	"io"

	"github.com/rudimk/replicare/internal/engine"
)

// RM5 — the thin Redis ApplyTx. Redis has no multi-key transaction and no staging:
// each RESTORE ... REPLACE is atomic and idempotent, and a unit is always a
// single-member acyclic component (redis-plan §0.1), so "stage then upsert" collapses
// to "RESTORE as the framing arrives." There is nothing to commit or roll back —
// the per-pass atomicity the relational engines provide is neither available nor
// needed here (each key converges independently). Deletes are RM6.

// redisApplyTx applies one streaming drain pass for the Redis unit. It tracks which
// keys StageUpsert applied so DeleteAbsent honors the neutral contract exactly:
// delete only the passed keys that were NOT staged (upserted) this pass. That makes
// both paths correct with one implementation — the RM5 upsert pass stages the
// present keys then "deletes absent" the same set (none absent → no deletes), while
// the RM6 delete sweep stages nothing then "deletes absent" the missing keys (all
// absent → all DELeted).
type redisApplyTx struct {
	db     *conn
	staged map[string]bool
}

var _ engine.ApplyTx = (*redisApplyTx)(nil)

// BeginApply starts a drain-pass apply. cyclic is always false for Redis (a unit is
// a single-member acyclic component) and componentTables is a single ref; both are
// accepted for interface parity and ignored.
func (s *Sink) BeginApply(_ context.Context, _ bool, _ []engine.TableRef) (engine.ApplyTx, error) {
	if s.db == nil {
		return nil, errNotConnected
	}
	return &redisApplyTx{db: s.db, staged: map[string]bool{}}, nil
}

// StageUpsert reads the faithful re-read framing and applies it with RESTORE ...
// REPLACE, recording each key as staged. For Redis this IS the upsert — there is no
// separate staging table.
func (tx *redisApplyTx) StageUpsert(ctx context.Context, _ engine.TableRef, _ []string, reread io.Reader) error {
	_, err := restoreStream(ctx, tx.db, reread, tx.staged)
	return err
}

// DeleteAbsent DELs the passed keys that were NOT staged this pass (absent at the
// source). The RM6 sweep calls it with the missing keys and no prior StageUpsert, so
// every key is deleted; the RM5 upsert pass calls it with the just-staged keys, so
// none are. DEL routes each key to its owning shard.
func (tx *redisApplyTx) DeleteAbsent(ctx context.Context, _ engine.TableRef, keys []engine.KeyValues) error {
	pipe := tx.db.pipeline()
	n := 0
	for _, kv := range keys {
		k := redisKey(kv)
		if tx.staged[k] {
			continue // upserted this pass — not a delete
		}
		pipe.Del(ctx, k)
		n++
	}
	if n == 0 {
		return nil
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("redis: DEL sweep: %w", err)
	}
	return nil
}

// Commit / Rollback are no-ops: RESTORE REPLACE already applied each key atomically
// as the framing arrived (no transaction to finalize or unwind).
func (tx *redisApplyTx) Commit(context.Context) error   { return nil }
func (tx *redisApplyTx) Rollback(context.Context) error { return nil }
