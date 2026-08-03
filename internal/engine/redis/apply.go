package redis

import (
	"context"
	"io"

	"github.com/rudimk/replicare/internal/engine"
)

// RM5 — the thin Redis ApplyTx. Redis has no multi-key transaction and no staging:
// each RESTORE ... REPLACE is atomic and idempotent, and a unit is always a
// single-member acyclic component (redis-plan §0.1), so "stage then upsert" collapses
// to "RESTORE as the framing arrives." There is nothing to commit or roll back —
// the per-pass atomicity the relational engines provide is neither available nor
// needed here (each key converges independently). Deletes are RM6.

// redisApplyTx applies one streaming drain pass for the Redis unit.
type redisApplyTx struct {
	db *conn
}

var _ engine.ApplyTx = (*redisApplyTx)(nil)

// BeginApply starts a drain-pass apply. cyclic is always false for Redis (a unit is
// a single-member acyclic component) and componentTables is a single ref; both are
// accepted for interface parity and ignored.
func (s *Sink) BeginApply(_ context.Context, _ bool, _ []engine.TableRef) (engine.ApplyTx, error) {
	if s.db == nil {
		return nil, errNotConnected
	}
	return &redisApplyTx{db: s.db}, nil
}

// StageUpsert reads the faithful re-read framing and applies it with RESTORE ...
// REPLACE. For Redis this IS the upsert — there is no separate staging table.
func (tx *redisApplyTx) StageUpsert(ctx context.Context, _ engine.TableRef, _ []string, reread io.Reader) error {
	_, err := restoreStream(ctx, tx.db, reread)
	return err
}

// DeleteAbsent is a no-op in RM5: streaming deletes are the durable target-vs-source
// diff of RM6, not this present-key upsert path. The keys handed here are exactly the
// present ones just re-read, so none are absent.
func (tx *redisApplyTx) DeleteAbsent(context.Context, engine.TableRef, []engine.KeyValues) error {
	return nil
}

// Commit / Rollback are no-ops: RESTORE REPLACE already applied each key atomically
// as the framing arrived (no transaction to finalize or unwind).
func (tx *redisApplyTx) Commit(context.Context) error   { return nil }
func (tx *redisApplyTx) Rollback(context.Context) error { return nil }
