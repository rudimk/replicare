package redis

import (
	"context"
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"sync"

	goredis "github.com/redis/go-redis/v9"

	"github.com/rudimk/replicare/internal/engine"
)

// This file implements engine.Verifier for the Redis Source and Sink: the
// read-only live key count and key-set fingerprint behind `replicare status`
// (live mode) and `replicare verify`.
//
// Scope of the Redis fingerprint (v1): it hashes the selected KEY SET (count plus
// an order-independent XOR of each key name's MD5), not per-key VALUES. It catches
// the dominant Redis divergence — missing or extra keys — cheaply, with a SCAN and
// no value reads, and needs no ordered keyspace. It does NOT detect same-key value
// drift; that is not expected, because transport is value-faithful DUMP→RESTORE
// (CLAUDE.md §3.2), and deep per-value content verification is a documented
// follow-up. A sync is single-engine (§6), so source and target hash identically.

var (
	_ engine.Verifier = (*Source)(nil)
	_ engine.Verifier = (*Sink)(nil)
)

// CountRows returns the number of selected keys in the unit (across all shard
// masters in cluster mode).
func (s *Source) CountRows(ctx context.Context, _ engine.TableRef) (int64, error) {
	if s.db == nil {
		return 0, errNotConnected
	}
	n, _, err := scanUnit(ctx, s.db, s.cfg, s.sel, false)
	return n, err
}

// Fingerprint returns the selected key count plus the key-set membership hash. cols
// is ignored (a Redis unit has no columns).
func (s *Source) Fingerprint(ctx context.Context, _ engine.TableRef, _ []string) (engine.Fingerprint, error) {
	if s.db == nil {
		return engine.Fingerprint{}, errNotConnected
	}
	n, sum, err := scanUnit(ctx, s.db, s.cfg, s.sel, true)
	if err != nil {
		return engine.Fingerprint{}, err
	}
	return engine.Fingerprint{Rows: n, Checksum: fmt.Sprintf("%016x", sum)}, nil
}

// CountRows returns the number of selected keys on the target.
func (s *Sink) CountRows(ctx context.Context, _ engine.TableRef) (int64, error) {
	if s.db == nil {
		return 0, errNotConnected
	}
	n, _, err := scanUnit(ctx, s.db, s.cfg, s.sel, false)
	return n, err
}

// Fingerprint returns the target's selected key count plus the key-set membership hash.
func (s *Sink) Fingerprint(ctx context.Context, _ engine.TableRef, _ []string) (engine.Fingerprint, error) {
	if s.db == nil {
		return engine.Fingerprint{}, errNotConnected
	}
	n, sum, err := scanUnit(ctx, s.db, s.cfg, s.sel, true)
	if err != nil {
		return engine.Fingerprint{}, err
	}
	return engine.Fingerprint{Rows: n, Checksum: fmt.Sprintf("%016x", sum)}, nil
}

// scanUnit SCANs every shard master once (cursor 0 → 0), counting selected keys and
// (when hash is true) XOR-folding each key name's MD5 into an order-independent
// set-membership checksum. It is a full rolling pass in bounded per-batch memory
// (SCAN COUNT), never buffering the keyspace. Selection filtering matches the
// streaming path so it never counts a key the sync did not select.
func scanUnit(ctx context.Context, c *conn, cfg engine.ConnConfig, sel *selection, hash bool) (count int64, checksum uint64, err error) {
	tun := tuningFromParams(cfg.Params)
	var mu sync.Mutex
	ferr := c.forEachShard(ctx, func(ctx context.Context, rc goredis.Cmdable) error {
		var cursor uint64
		for {
			keys, cur, scanErr := rc.Scan(ctx, cursor, "", tun.scanCount).Result()
			if scanErr != nil {
				return fmt.Errorf("redis: SCAN: %w", scanErr)
			}
			for _, k := range keys {
				if sel != nil && !sel.match(k) {
					continue
				}
				mu.Lock()
				count++
				if hash {
					sum := md5.Sum([]byte(k))
					checksum ^= binary.BigEndian.Uint64(sum[:8])
				}
				mu.Unlock()
			}
			cursor = cur
			if cursor == 0 {
				return nil
			}
		}
	})
	return count, checksum, ferr
}
