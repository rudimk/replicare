package redis

import (
	"context"
	"fmt"
	"io"

	goredis "github.com/redis/go-redis/v9"

	"github.com/rudimk/replicare/internal/engine"
)

// RM5 — streaming UPSERT convergence via rolling reconciliation SCAN. Redis has no
// change stream, so "dirty keys" are simply the keys PRESENT at the source, re-read
// and RESTORE-REPLACEd every pass (idempotent → convergent; CLAUDE.md §3.2). Deletes
// are RM6. This reuses the neutral apply layer (DrainComponent) unchanged.
//
// The reconciliation is a BOUNDED pass-boundary state machine (redis-plan §0.4): the
// live SCAN cursor is held across ReadDirtyKeys calls and each call returns at most
// `max` keys — memory is O(batch), never O(keyset). A completed pass (all shards
// SCANned to cursor 0) returns an empty batch so the drain loop idles, then a fresh
// rolling pass begins.

// reconState is the per-unit bounded SCAN state: one cursor per shard master, held
// across ticks. Nothing durable — a crash just restarts the pass from cursor 0
// (idempotent re-read; Momus m1).
type reconState struct {
	scanners []goredis.Cmdable
	cursors  []uint64
	done     []bool
	idx      int
}

func (r *reconState) allDone() bool {
	for _, d := range r.done {
		if !d {
			return false
		}
	}
	return true
}

func (r *reconState) reset() {
	for i := range r.cursors {
		r.cursors[i] = 0
		r.done[i] = false
	}
	r.idx = 0
}

// ReadDirtyKeys returns the next bounded batch of present keys for the unit as
// upserts (Op=U). It advances the held SCAN cursor; when a full rolling pass
// completes it resets and returns empty so the caller idles before the next pass.
func (s *Source) ReadDirtyKeys(ctx context.Context, _ engine.TableRef, _ engine.TargetID, max int) ([]engine.DirtyKey, error) {
	if s.db == nil {
		return nil, errNotConnected
	}
	if max <= 0 {
		max = defaultScanCount
	}
	if s.recon == nil {
		sc, err := s.db.shardScanners(ctx, s.cfg)
		if err != nil {
			return nil, err
		}
		s.recon = &reconState{scanners: sc, cursors: make([]uint64, len(sc)), done: make([]bool, len(sc))}
	}
	tun := tuningFromParams(s.cfg.Params)
	keys, _, err := scanBatch(ctx, s.recon, tun.scanCount, max, s.sel)
	if err != nil {
		return nil, err
	}
	batch := make([]engine.DirtyKey, 0, len(keys))
	for _, k := range keys {
		s.changeID++
		batch = append(batch, engine.DirtyKey{
			DeltaID: engine.DeltaID(s.changeID),
			Op:      engine.OpUpdate,
			Key:     engine.KeyValues{k},
		})
	}
	return batch, nil
}

// scanBatch collects up to ~max selected keys from the held per-shard SCAN state,
// advancing the cursors. It is the shared bounded-pass primitive behind both the
// source reconciliation (ReadDirtyKeys) and the target sweep (ScanTargetKeys).
// passComplete is true when THIS call finished the rolling pass (the state was
// reset for the next one); the empty batch it returns signals the caller to idle.
func scanBatch(ctx context.Context, r *reconState, scanCount int64, max int, sel *selection) (keys []string, passComplete bool, err error) {
	for len(keys) < max {
		if r.allDone() {
			r.reset()
			return keys, true, nil
		}
		if r.done[r.idx] {
			r.idx = (r.idx + 1) % len(r.scanners)
			continue
		}
		ks, cur, err := r.scanners[r.idx].Scan(ctx, r.cursors[r.idx], "", scanCount).Result()
		if err != nil {
			return nil, false, fmt.Errorf("redis: SCAN: %w", err)
		}
		r.cursors[r.idx] = cur
		for _, k := range ks {
			if sel == nil || sel.match(k) {
				keys = append(keys, k)
			}
		}
		if cur == 0 {
			r.done[r.idx] = true
			r.idx = (r.idx + 1) % len(r.scanners)
		}
	}
	return keys, false, nil
}

// MissingAtSource returns the keys that do NOT exist at the source — the target
// keys to delete (redis-plan §0.4). It reads the MASTER (replica read routing is
// never enabled on our clients, RM1), so a lagging replica cannot report a
// just-written key absent and cause a false delete (redis-plan §0.5, Momus M5).
func (s *Source) MissingAtSource(ctx context.Context, _ engine.TableRef, keys []engine.KeyValues) ([]engine.KeyValues, error) {
	if s.db == nil {
		return nil, errNotConnected
	}
	if len(keys) == 0 {
		return nil, nil
	}
	strs := make([]string, len(keys))
	pipe := s.db.pipeline()
	cmds := make([]*goredis.IntCmd, len(keys))
	for i, kv := range keys {
		strs[i] = redisKey(kv)
		cmds[i] = pipe.Exists(ctx, strs[i]) // routes to the owning master
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("redis: EXISTS pipeline: %w", err)
	}
	var missing []engine.KeyValues
	for i, c := range cmds {
		n, err := c.Result()
		if err != nil {
			return nil, fmt.Errorf("redis: EXISTS %q: %w", strs[i], err)
		}
		if n == 0 {
			missing = append(missing, engine.KeyValues{strs[i]})
		}
	}
	return missing, nil
}

// RereadCurrent DUMPs the current value+TTL of each dirty key into the framing —
// the faithful re-read the apply layer pipes into RESTORE REPLACE. Reuses the RM4
// framing (big-key handling included); keys are routed per shard by the client.
func (s *Source) RereadCurrent(ctx context.Context, _ engine.TableRef, keys []engine.KeyValues, w io.Writer) error {
	if s.db == nil {
		return errNotConnected
	}
	tun := tuningFromParams(s.cfg.Params)
	var nowMillis int64
	if tun.absTTL {
		now, err := s.db.uc.Time(ctx).Result()
		if err != nil {
			return fmt.Errorf("redis: TIME (absttl base): %w", err)
		}
		nowMillis = now.UnixMilli()
	}
	strs := make([]string, 0, len(keys))
	for _, kv := range keys {
		strs = append(strs, redisKey(kv))
	}
	sw := newSyncWriter(w)
	// sel=nil: the keys are already selected; do not re-filter.
	return frameKeys(ctx, s.db.uc, sw, tun, nil, strs, nowMillis)
}

// ConfirmConsumed is largely vestigial for Redis (redis-plan §0.4): there is no
// durable source-side queue to advance — "consumed" is a lag/phase hint, and
// crash-safety means "reprocessed on the next full pass," not the next tick. The
// per-unit change-id already advanced in ReadDirtyKeys.
func (s *Source) ConfirmConsumed(context.Context, engine.TableRef, engine.TargetID, []engine.DeltaID) error {
	return nil
}

// DeltaBacklog reports the reconciliation backlog. Redis holds no durable delta
// queue, so there is nothing pinned on the source: an empty backlog keeps the
// neutral retention/reseed machinery inert (there is no source footprint to bound).
// Reconciliation lag is surfaced through RM8's delete/pass telemetry instead.
func (s *Source) DeltaBacklog(context.Context, engine.TableRef, engine.TargetID) (engine.DeltaBacklog, error) {
	if s.db == nil {
		return engine.DeltaBacklog{}, errNotConnected
	}
	return engine.DeltaBacklog{}, nil
}

// Purge is a no-op for Redis: there is no source-side capture to purge (SCAN
// reconciliation writes nothing to the source). Returns empty stats.
func (s *Source) Purge(context.Context, engine.TableRef, []engine.TargetID, engine.RetentionPolicy) (engine.PurgeStats, error) {
	return engine.PurgeStats{}, nil
}

// redisKey extracts the single Redis key from an engine.KeyValues (the neutral
// overload: one key per KeyValues). Accepts string or []byte (binary keys).
func redisKey(kv engine.KeyValues) string {
	if len(kv) == 0 {
		return ""
	}
	switch v := kv[0].(type) {
	case string:
		return v
	case []byte:
		return string(v)
	default:
		return fmt.Sprint(v)
	}
}
