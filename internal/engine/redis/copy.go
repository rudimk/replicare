package redis

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/rudimk/replicare/internal/engine"
)

// RM4 — value-faithful initial copy. Redis has no ordered key space to chunk
// against and no text COPY, so the neutral copy contract is overloaded:
//
//   - PlanChunks returns ONE opaque chunk per unit (the whole keyspace). The
//     neutral pool parallelizes ACROSS units/tables; Redis has a single unit, so
//     the intra-unit parallelism lives INSIDE CopyChunk (per-shard SCAN fan-out).
//   - CopyChunk SCANs every master, DUMP+PTTLs each selected key, and writes the
//     private binary framing (framing.go) into the copy pipe.
//   - BulkLoad reads that framing and pipelines RESTORE ... REPLACE, value-faithful
//     (never reconstructed; CLAUDE.md §1.7). TTL is relative by default.
//   - DeleteRange / keyset Watermark are no-ops (redis-plan §0.5): a Redis unit
//     checkpoints coarsely, and a restart re-SCANs from cursor 0 (RESTORE REPLACE
//     is idempotent).

// defaultScanCount is the SCAN COUNT hint when the config leaves scan_count unset.
const defaultScanCount = 512

// copyTuning is the per-copy knob set resolved from ConnConfig.Params (rc_-prefixed,
// RM0.5). Kept tiny and value-typed so CopyChunk/BulkLoad stay pure.
type copyTuning struct {
	scanCount    int64
	types        []string
	bigKeyWarn   int64 // MEMORY USAGE warn threshold (0 = off)
	bigKeyRefuse int64 // MEMORY USAGE refuse threshold (0 = off)
	absTTL       bool
}

func tuningFromParams(p map[string]string) copyTuning {
	t := copyTuning{scanCount: defaultScanCount}
	if v := p[paramScanCount]; v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			t.scanCount = n
		}
	}
	if v := p[paramTypes]; v != "" {
		t.types = splitCSV(v)
	}
	t.bigKeyWarn = parseInt64(p[paramBigKeyWarn])
	t.bigKeyRefuse = parseInt64(p[paramBigKeyRefuse])
	t.absTTL = p[paramTTLMode] == "absttl"
	return t
}

func parseInt64(s string) int64 {
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}

func splitCSV(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

// PlanChunks returns the single opaque chunk covering a Redis unit. The keyset
// Lo/Method are meaningless for Redis (no ordered key space), so the chunk is just
// the unit ref; intra-unit parallelism happens inside CopyChunk (redis-plan §0.5).
func (s *Source) PlanChunks(_ context.Context, t engine.TableRef, _ engine.ChunkOptions) ([]engine.Chunk, error) {
	return []engine.Chunk{{Table: t}}, nil
}

// CopyChunk streams the unit's entire keyspace as the binary DUMP framing. It
// fans SCAN out across shard masters (concurrent, per redis-plan §0.5) into one
// serialized writer. Keys not matching the sync selection are skipped; a key that
// expires mid-scan (nil DUMP / PTTL -2) is skipped; a key over the big-key refuse
// cap fails loud.
func (s *Source) CopyChunk(ctx context.Context, _ engine.Chunk, w io.Writer) error {
	if s.db == nil {
		return errNotConnected
	}
	tun := tuningFromParams(s.cfg.Params)
	if len(tun.types) > 0 {
		ver, err := serverVersion(ctx, s.db)
		if err != nil {
			return err
		}
		if ver < 60000 {
			return fmt.Errorf("redis: selection type filter requires Redis 6.0+ (source is %d); remove redis.types", ver)
		}
	}
	sw := newSyncWriter(w)
	return s.db.forEachShard(ctx, func(ctx context.Context, rc goredis.Cmdable) error {
		return scanShard(ctx, rc, sw, tun, s.sel)
	})
}

// scanShard SCANs one master and frames every selected key. When a TYPE filter is
// configured it runs one server-side SCAN ... TYPE pass per type (6.0+); otherwise
// one plain SCAN pass filtered client-side by the key-glob selection.
func scanShard(ctx context.Context, rc goredis.Cmdable, sw *syncWriter, tun copyTuning, sel *selection) error {
	var nowMillis int64
	if tun.absTTL {
		now, err := rc.Time(ctx).Result()
		if err != nil {
			return fmt.Errorf("redis: TIME (absttl base): %w", err)
		}
		nowMillis = now.UnixMilli()
	}
	types := tun.types
	if len(types) == 0 {
		types = []string{""} // one untyped pass
	}
	for _, typ := range types {
		var cursor uint64
		for {
			var (
				keys []string
				err  error
			)
			if typ == "" {
				keys, cursor, err = rc.Scan(ctx, cursor, "", tun.scanCount).Result()
			} else {
				keys, cursor, err = rc.ScanType(ctx, cursor, "", tun.scanCount, typ).Result()
			}
			if err != nil {
				return fmt.Errorf("redis: SCAN: %w", err)
			}
			if err := frameKeys(ctx, rc, sw, tun, sel, keys, nowMillis); err != nil {
				return err
			}
			if cursor == 0 {
				break
			}
		}
	}
	return nil
}

// frameKeys DUMP+PTTLs a batch of scanned keys in one pipeline and writes the
// selected, still-present ones into the framing.
func frameKeys(ctx context.Context, rc goredis.Cmdable, sw *syncWriter, tun copyTuning, sel *selection, keys []string, nowMillis int64) error {
	// Client-side selection: exclude-wins key-glob match (redis-plan §0.5).
	sifted := keys[:0:0]
	for _, k := range keys {
		if sel == nil || sel.match(k) {
			sifted = append(sifted, k)
		}
	}
	if len(sifted) == 0 {
		return nil
	}

	pipe := rc.Pipeline()
	dumps := make([]*goredis.StringCmd, len(sifted))
	pttls := make([]*goredis.DurationCmd, len(sifted))
	var mems []*goredis.IntCmd
	measure := tun.bigKeyWarn > 0 || tun.bigKeyRefuse > 0
	if measure {
		mems = make([]*goredis.IntCmd, len(sifted))
	}
	for i, k := range sifted {
		dumps[i] = pipe.Dump(ctx, k)
		pttls[i] = pipe.PTTL(ctx, k)
		if measure {
			mems[i] = pipe.MemoryUsage(ctx, k)
		}
	}
	// A pipeline "error" is non-nil when ANY command failed; per-command errors
	// (e.g. redis.Nil for a vanished key) are inspected individually below.
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, goredis.Nil) {
		return fmt.Errorf("redis: DUMP/PTTL pipeline: %w", err)
	}

	for i, k := range sifted {
		payload, err := dumps[i].Result()
		if errors.Is(err, goredis.Nil) {
			continue // expired/deleted between SCAN and DUMP — skip (reconciled later)
		}
		if err != nil {
			return fmt.Errorf("redis: DUMP %q: %w", k, err)
		}
		d := pttls[i].Val()
		if d == time.Duration(-2) {
			continue // key gone
		}
		if measure {
			if used, err := mems[i].Result(); err == nil {
				if tun.bigKeyRefuse > 0 && used >= tun.bigKeyRefuse {
					return fmt.Errorf("redis: key %q is %d bytes, over the big_key_refuse_bytes cap (%d): refusing to copy (redis-plan §0.5)", k, used, tun.bigKeyRefuse)
				}
				if tun.bigKeyWarn > 0 && used >= tun.bigKeyWarn {
					slog.Warn("redis big key",
						slog.String("event", "redis.big_key"),
						slog.String("key", k),
						slog.Int64("bytes", used),
						slog.Int64("warn_bytes", tun.bigKeyWarn))
				}
			}
		}
		ttl, flags := restoreTTL(d, tun.absTTL, nowMillis)
		if err := sw.write(record{key: []byte(k), ttl: ttl, flags: flags, dump: []byte(payload)}); err != nil {
			return err
		}
	}
	return nil
}

// restoreBatch bounds how many RESTOREs are pipelined per round-trip group.
const restoreBatch = 256

// restoreStream reads the DUMP framing and applies each record with RESTORE ...
// REPLACE, batched into pipelines. A single failing RESTORE (bad payload, target
// RDB too old, etc.) halts loud — never silently skipped (CLAUDE.md §1.7).
func restoreStream(ctx context.Context, db *conn, r io.Reader) (int64, error) {
	var total int64
	recs := make([]record, 0, restoreBatch)

	flush := func() error {
		if len(recs) == 0 {
			return nil
		}
		pipe := db.pipeline()
		cmds := make([]*goredis.Cmd, len(recs))
		for i, rec := range recs {
			args := []any{"RESTORE", string(rec.key), rec.ttl, string(rec.dump), "REPLACE"}
			if rec.flags&flagAbsTTL != 0 {
				args = append(args, "ABSTTL")
			}
			cmds[i] = pipe.Do(ctx, args...)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			for i, c := range cmds {
				if e := c.Err(); e != nil {
					return fmt.Errorf("redis: RESTORE %q: %w", string(recs[i].key), e)
				}
			}
			return fmt.Errorf("redis: RESTORE pipeline: %w", err)
		}
		total += int64(len(recs))
		recs = recs[:0]
		return nil
	}

	for {
		rec, err := readRecord(r)
		if err == io.EOF {
			break
		}
		if err != nil {
			return total, err
		}
		recs = append(recs, rec)
		if len(recs) >= restoreBatch {
			if err := flush(); err != nil {
				return total, err
			}
		}
	}
	if err := flush(); err != nil {
		return total, err
	}
	return total, nil
}

// restoreTTL maps a source PTTL to the framed (ttl, flags): relative ms by default
// (0 = no expiry), or an absolute unix-ms when absttl is enabled and the key has an
// expiry. -1 (no expiry) always frames as relative 0 (redis-plan §0.5).
func restoreTTL(pttl time.Duration, absTTL bool, nowMillis int64) (int64, uint8) {
	if pttl == time.Duration(-1) || pttl < 0 {
		return 0, 0 // no expiry
	}
	ms := pttl.Milliseconds()
	if ms <= 0 {
		ms = 1 // sub-ms remaining: keep it briefly alive rather than persisting forever
	}
	if absTTL {
		return nowMillis + ms, flagAbsTTL
	}
	return ms, 0
}
