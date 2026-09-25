package redis

import (
	"context"
	"crypto/md5"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	goredis "github.com/redis/go-redis/v9"

	"github.com/rudimk/replicare/internal/engine"
)

// This file implements engine.Verifier for the Redis Source and Sink: the
// read-only live key count and the content fingerprint behind `replicare status`
// (live mode) and `replicare verify`.
//
// The Redis fingerprint hashes the selected keyspace by CONTENT, not just key-set
// membership: for every selected key it folds the key name, its type, and its
// canonical LOGICAL value into an order-independent XOR checksum. So it catches both
// missing/extra keys AND same-key value drift (a SET/HSET/RPUSH that changed a value
// without changing the key set) — the latter is invisible to a membership-only hash,
// which is why value comparison is the DEFAULT here, not an opt-in.
//
// Version-gap safety: the hash is over each value's canonical LOGICAL form obtained
// with type-aware reads (GET / LRANGE / SMEMBERS / ZRANGE WITHSCORES / HGETALL /
// XRANGE), never the raw RDB DUMP bytes — DUMP is not byte-identical across Redis
// versions even for equal values, so hashing it would false-positive across a version
// gap. Transport is value-faithful (RESTORE re-encodes), so the logical values match
// and the two ends hash identically. A sync is single-engine (§6), so source and
// target are directly comparable.
//
// TTL is deliberately excluded from the hash: replicare replicates TTL as a relative
// value (skew-safe, CLAUDE.md §3.2), so the exact remaining TTL differs by design and
// would cause spurious drift. Stream consumer GROUPS/PEL are not deep-verified in v1
// (entries are, via XRANGE); that remains a documented follow-up.
//
// Cost: the fingerprint reads every selected key's value. Reads are PIPELINED per SCAN
// batch (one TYPE round-trip + one value round-trip per batch), so it stays bounded
// (O(batch) memory, ~2 round-trips per scanCount keys) rather than a round-trip per
// key. CountRows stays a cheap count-only SCAN (no value reads) for the status path.

var (
	_ engine.Verifier = (*Source)(nil)
	_ engine.Verifier = (*Sink)(nil)
)

// CountRows returns the number of selected keys in the unit (across all shard
// masters in cluster mode). Count-only: it does not read values.
func (s *Source) CountRows(ctx context.Context, _ engine.TableRef) (int64, error) {
	if s.db == nil {
		return 0, errNotConnected
	}
	return countUnit(ctx, s.db, s.cfg, s.sel)
}

// Fingerprint returns the selected key count plus the content hash (key + type +
// canonical value, XOR-folded). cols is ignored (a Redis unit has no columns).
func (s *Source) Fingerprint(ctx context.Context, _ engine.TableRef, _ []string) (engine.Fingerprint, error) {
	if s.db == nil {
		return engine.Fingerprint{}, errNotConnected
	}
	n, sum, err := fingerprintUnit(ctx, s.db, s.cfg, s.sel)
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
	return countUnit(ctx, s.db, s.cfg, s.sel)
}

// Fingerprint returns the target's selected key count plus the content hash.
func (s *Sink) Fingerprint(ctx context.Context, _ engine.TableRef, _ []string) (engine.Fingerprint, error) {
	if s.db == nil {
		return engine.Fingerprint{}, errNotConnected
	}
	n, sum, err := fingerprintUnit(ctx, s.db, s.cfg, s.sel)
	if err != nil {
		return engine.Fingerprint{}, err
	}
	return engine.Fingerprint{Rows: n, Checksum: fmt.Sprintf("%016x", sum)}, nil
}

// countUnit SCANs every shard master once (cursor 0 → 0) and counts selected keys,
// in bounded per-batch memory (SCAN COUNT), never buffering the keyspace or reading
// values. Selection filtering matches the streaming path.
func countUnit(ctx context.Context, c *conn, cfg engine.ConnConfig, sel *selection) (int64, error) {
	tun := tuningFromParams(cfg.Params)
	var mu sync.Mutex
	var count int64
	err := c.forEachShard(ctx, func(ctx context.Context, rc goredis.Cmdable) error {
		var cursor uint64
		for {
			keys, cur, scanErr := rc.Scan(ctx, cursor, "", tun.scanCount).Result()
			if scanErr != nil {
				return fmt.Errorf("redis: SCAN: %w", scanErr)
			}
			var n int64
			for _, k := range keys {
				if sel == nil || sel.match(k) {
					n++
				}
			}
			mu.Lock()
			count += n
			mu.Unlock()
			cursor = cur
			if cursor == 0 {
				return nil
			}
		}
	})
	return count, err
}

// fingerprintUnit SCANs every shard master once and folds each selected key's content
// (key + type + canonical logical value) into an order-independent XOR checksum, one
// pipelined batch at a time.
func fingerprintUnit(ctx context.Context, c *conn, cfg engine.ConnConfig, sel *selection) (count int64, checksum uint64, err error) {
	tun := tuningFromParams(cfg.Params)
	var mu sync.Mutex
	ferr := c.forEachShard(ctx, func(ctx context.Context, rc goredis.Cmdable) error {
		var cursor uint64
		for {
			keys, cur, scanErr := rc.Scan(ctx, cursor, "", tun.scanCount).Result()
			if scanErr != nil {
				return fmt.Errorf("redis: SCAN: %w", scanErr)
			}
			selected := keys[:0:0]
			for _, k := range keys {
				if sel == nil || sel.match(k) {
					selected = append(selected, k)
				}
			}
			if len(selected) > 0 {
				sum, n, herr := hashBatch(ctx, rc, selected)
				if herr != nil {
					return herr
				}
				mu.Lock()
				checksum ^= sum
				count += n
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

// hashBatch content-hashes a batch of keys with two pipelined round-trips: TYPE for
// every key, then the type-appropriate value read for every key. Keys that vanish
// mid-scan (expiry/deletion → redis.Nil) or whose type changed under a concurrent
// write (WRONGTYPE) are skipped best-effort — a live source mutates during the scan,
// and such a key is simply not counted this pass. A genuine connection error aborts.
func hashBatch(ctx context.Context, rc goredis.Cmdable, keys []string) (xor uint64, count int64, err error) {
	typePipe := rc.Pipeline()
	typeCmds := make([]*goredis.StatusCmd, len(keys))
	for i, k := range keys {
		typeCmds[i] = typePipe.Type(ctx, k)
	}
	if _, e := typePipe.Exec(ctx); e != nil && !errors.Is(e, goredis.Nil) {
		return 0, 0, fmt.Errorf("redis: TYPE pipeline: %w", e)
	}

	types := make([]string, len(keys))
	valPipe := rc.Pipeline()
	valCmds := make([]interface{}, len(keys))
	for i, k := range keys {
		t := typeCmds[i].Val()
		types[i] = t
		switch t {
		case "string":
			valCmds[i] = valPipe.Get(ctx, k)
		case "list":
			valCmds[i] = valPipe.LRange(ctx, k, 0, -1)
		case "set":
			valCmds[i] = valPipe.SMembers(ctx, k)
		case "zset":
			valCmds[i] = valPipe.ZRangeWithScores(ctx, k, 0, -1)
		case "hash":
			valCmds[i] = valPipe.HGetAll(ctx, k)
		case "stream":
			valCmds[i] = valPipe.XRange(ctx, k, "-", "+")
		default:
			// "none" (vanished) or an unknown type: skip.
		}
	}
	if _, e := valPipe.Exec(ctx); e != nil && !errors.Is(e, goredis.Nil) && !isWrongType(e) {
		return 0, 0, fmt.Errorf("redis: value pipeline: %w", e)
	}

	for i, k := range keys {
		if valCmds[i] == nil {
			continue
		}
		canon, ok, e := canonicalValue(types[i], valCmds[i])
		if e != nil {
			return 0, 0, e
		}
		if !ok {
			continue // vanished / raced type change
		}
		h := md5.New()
		writeField(h, []byte(k))
		writeField(h, []byte(types[i]))
		writeField(h, canon)
		var sum [md5.Size]byte
		h.Sum(sum[:0])
		xor ^= binary.BigEndian.Uint64(sum[:8])
		count++
	}
	return xor, count, nil
}

// canonicalValue renders a key's value into a deterministic byte form that is stable
// across Redis versions for equal logical values. ok is false when the key vanished
// or its type changed under the read (skip it).
func canonicalValue(typ string, cmd interface{}) (canon []byte, ok bool, err error) {
	switch c := cmd.(type) {
	case *goredis.StringCmd: // string
		b, e := c.Bytes()
		if errors.Is(e, goredis.Nil) {
			return nil, false, nil
		}
		if e != nil {
			return nil, false, skipOrErr(e)
		}
		return b, true, nil
	case *goredis.StringSliceCmd: // list (ordered) or set (unordered)
		vals, e := c.Result()
		if errors.Is(e, goredis.Nil) {
			return nil, false, nil
		}
		if e != nil {
			return nil, false, skipOrErr(e)
		}
		if typ == "set" {
			sort.Strings(vals) // set has no order — canonicalize
		}
		return joinFields(vals), true, nil
	case *goredis.ZSliceCmd: // zset: members with scores, in ZRANGE order (score,member)
		zs, e := c.Result()
		if errors.Is(e, goredis.Nil) {
			return nil, false, nil
		}
		if e != nil {
			return nil, false, skipOrErr(e)
		}
		parts := make([]string, 0, len(zs)*2)
		for _, z := range zs {
			member, _ := z.Member.(string)
			parts = append(parts, member, strconv.FormatFloat(z.Score, 'g', -1, 64))
		}
		return joinFields(parts), true, nil
	case *goredis.MapStringStringCmd: // hash: sort field names for order-independence
		m, e := c.Result()
		if errors.Is(e, goredis.Nil) {
			return nil, false, nil
		}
		if e != nil {
			return nil, false, skipOrErr(e)
		}
		fields := make([]string, 0, len(m))
		for f := range m {
			fields = append(fields, f)
		}
		sort.Strings(fields)
		parts := make([]string, 0, len(m)*2)
		for _, f := range fields {
			parts = append(parts, f, m[f])
		}
		return joinFields(parts), true, nil
	case *goredis.XMessageSliceCmd: // stream: entries in id order (fields sorted per entry)
		msgs, e := c.Result()
		if errors.Is(e, goredis.Nil) {
			return nil, false, nil
		}
		if e != nil {
			return nil, false, skipOrErr(e)
		}
		var parts []string
		for _, m := range msgs {
			parts = append(parts, m.ID)
			fields := make([]string, 0, len(m.Values))
			for f := range m.Values {
				fields = append(fields, f)
			}
			sort.Strings(fields)
			for _, f := range fields {
				parts = append(parts, f, fmt.Sprint(m.Values[f]))
			}
		}
		return joinFields(parts), true, nil
	default:
		return nil, false, nil
	}
}

// skipOrErr maps a per-key command error to skip (WRONGTYPE from a concurrent type
// change) or a real error to abort on.
func skipOrErr(e error) error {
	if isWrongType(e) {
		return nil
	}
	return e
}

func isWrongType(e error) bool {
	return e != nil && strings.Contains(e.Error(), "WRONGTYPE")
}

// writeField writes a length-prefixed field into the hash so concatenation is
// unambiguous (no delimiter can be forged by the data itself).
func writeField(h interface{ Write([]byte) (int, error) }, b []byte) {
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], uint64(len(b)))
	_, _ = h.Write(n[:])
	_, _ = h.Write(b)
}

// joinFields length-prefix-encodes a slice of strings into one unambiguous byte blob.
func joinFields(vals []string) []byte {
	size := 8 * len(vals)
	for _, v := range vals {
		size += len(v)
	}
	out := make([]byte, 0, size)
	var n [8]byte
	for _, v := range vals {
		binary.BigEndian.PutUint64(n[:], uint64(len(v)))
		out = append(out, n[:]...)
		out = append(out, v...)
	}
	return out
}
