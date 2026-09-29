package redis

import (
	"context"
	"crypto/md5"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/rudimk/replicare/internal/engine"
)

// MM6 — Redis mesh SOURCE side: origin identity, version-stamping re-read, dirty-key
// enumeration from BOTH the data keyspace (present keys) and the metadata keyspace
// (deletes/tombstones), and tombstone GC. All of this is gated on s.cluster; a
// one-way source never enters these paths (docs/multi-master.md §5.4).

// EnableClusterReads implements engine.ClusterReadSource: switch this source to the
// version-aware, stamping re-read for a cluster edge. nodeID is this member's origin
// identity, used to stamp detected local writes. Called for the primary source AND
// every copy-pool source (both re-read during streaming).
func (s *Source) EnableClusterReads(nodeID string) {
	s.cluster = true
	if nodeID != "" {
		s.nodeID = nodeID
	}
}

// InstallOriginCapture implements engine.OriginAwareCapturer. Redis is capture-less:
// there is no trigger to install and nothing to suppress at capture time (loop
// suppression happens at apply, by version comparison). So this records the origin
// nodeID, marks cluster mode, and reuses the one-way notification accelerator (if the
// endpoint enabled it) to shorten reconciliation latency. The version register is
// maintained lazily by the stamping re-read, not by an installed object.
func (s *Source) InstallOriginCapture(ctx context.Context, tables []engine.TableRef, nodeID string) error {
	s.cluster = true
	s.nodeID = nodeID
	return s.InstallCapture(ctx, tables)
}

// readDirtyKeysCluster enumerates the cluster (mesh) dirty set: present data keys as
// upserts, plus metadata-keyspace entries whose data key is absent as deletes (an
// existing tombstone to (re)propagate, or a fresh local delete the stamping re-read
// will tombstone). Both scans are bounded rolling passes (O(batch)). rereadCurrent-
// Cluster decides the final op per key from the live data/meta state, so the Op here
// is only a hint; we set it accurately anyway for observability.
func (s *Source) readDirtyKeysCluster(ctx context.Context, _ engine.TableRef, _ engine.TargetID, max int) ([]engine.DirtyKey, error) {
	if s.recon == nil {
		sc, err := s.db.shardScanners(ctx, s.cfg)
		if err != nil {
			return nil, err
		}
		s.recon = &reconState{scanners: sc, cursors: make([]uint64, len(sc)), done: make([]bool, len(sc))}
	}
	if s.metaRecon == nil {
		sc, err := s.db.shardScanners(ctx, s.cfg)
		if err != nil {
			return nil, err
		}
		s.metaRecon = &reconState{scanners: sc, cursors: make([]uint64, len(sc)), done: make([]bool, len(sc))}
	}
	tun := tuningFromParams(s.cfg.Params)
	batch := make([]engine.DirtyKey, 0, max)

	// Notification accelerator: flagged (changed) keys drained ahead of the scan.
	// Metadata keys are never surfaced as data. A gap resets both rolling scans.
	if s.notify != nil {
		flagged, gapped := s.notify.drain(max)
		if gapped {
			s.recon.reset()
			s.metaRecon.reset()
		}
		for _, k := range flagged {
			if isMetaKey(k) {
				continue
			}
			if s.sel == nil || s.sel.match(k) {
				s.changeID++
				batch = append(batch, engine.DirtyKey{DeltaID: engine.DeltaID(s.changeID), Op: engine.OpUpdate, Key: engine.KeyValues{k}})
			}
		}
		if len(batch) >= max {
			return batch, nil
		}
	}

	// Data keyspace: present, selected keys (metadata keys excluded) -> upserts.
	dataKeys, _, err := scanBatchFiltered(ctx, s.recon, tun.scanCount, max-len(batch), s.sel, "", isMetaKey)
	if err != nil {
		return nil, err
	}
	for _, k := range dataKeys {
		s.changeID++
		batch = append(batch, engine.DirtyKey{DeltaID: engine.DeltaID(s.changeID), Op: engine.OpUpdate, Key: engine.KeyValues{k}})
	}
	if len(batch) >= max {
		return batch, nil
	}

	// Metadata keyspace: derive each data key; those absent at the source are deletes.
	metaKeys, _, err := scanBatchFiltered(ctx, s.metaRecon, tun.scanCount, max-len(batch), nil, metaPrefix+"*", nil)
	if err != nil {
		return nil, err
	}
	dels, err := s.metaDeletes(ctx, metaKeys)
	if err != nil {
		return nil, err
	}
	for _, k := range dels {
		s.changeID++
		batch = append(batch, engine.DirtyKey{DeltaID: engine.DeltaID(s.changeID), Op: engine.OpDelete, Key: engine.KeyValues{k}})
	}
	return batch, nil
}

// metaDeletes maps scanned metadata keys to the data keys that should be propagated as
// deletes: a selected data key whose value is absent at the source (an already-recorded
// tombstone, or a live register entry whose data key was locally deleted). A metadata
// entry whose data key still exists is skipped (the data scan covers it).
func (s *Source) metaDeletes(ctx context.Context, metaKeys []string) ([]string, error) {
	dataKeys := make([]string, 0, len(metaKeys))
	for _, mk := range metaKeys {
		dk, ok := dataKeyFromMeta(mk)
		if !ok {
			continue
		}
		if s.sel != nil && !s.sel.match(dk) {
			continue
		}
		dataKeys = append(dataKeys, dk)
	}
	if len(dataKeys) == 0 {
		return nil, nil
	}
	pipe := s.db.pipeline()
	cmds := make([]*goredis.IntCmd, len(dataKeys))
	for i, dk := range dataKeys {
		cmds[i] = pipe.Exists(ctx, dk)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("redis mesh: EXISTS pipeline: %w", err)
	}
	var missing []string
	for i, c := range cmds {
		n, err := c.Result()
		if err != nil {
			return nil, fmt.Errorf("redis mesh: EXISTS %q: %w", dataKeys[i], err)
		}
		if n == 0 {
			missing = append(missing, dataKeys[i])
		}
	}
	return missing, nil
}

// rereadCurrentCluster frames each dirty key as a mesh record carrying its version
// register. A present key whose logical value changed since it was last stamped (or was
// never stamped, or is un-deleting) gets a fresh (hlc, nodeID) stamped into its
// metadata and framed alive; an unchanged key is framed with its existing version. An
// absent key with a live register entry is a local delete -> a tombstone is stamped and
// framed deleted; an existing tombstone is re-framed deleted. This is where the
// capture-less "capture" happens: the register is maintained by reading current state.
func (s *Source) rereadCurrentCluster(ctx context.Context, _ engine.TableRef, keys []engine.KeyValues, w io.Writer) error {
	tun := tuningFromParams(s.cfg.Params)
	var nowMillis int64
	if tun.absTTL {
		t, err := s.db.uc.Time(ctx).Result()
		if err != nil {
			return fmt.Errorf("redis mesh: TIME (absttl base): %w", err)
		}
		nowMillis = t.UnixMilli()
	}
	now := time.Now().UnixMilli()
	sw := newSyncWriter(w)
	rc := s.db.uc
	for _, kv := range keys {
		k := redisKey(kv)
		mk := metaKey(k)
		stored, present, err := readMeta(ctx, rc, mk)
		if err != nil {
			return err
		}
		dump, ttl, flags, exists, err := dumpOne(ctx, rc, k, tun, nowMillis)
		if err != nil {
			return err
		}
		if exists {
			vh, ok, err := logicalVHash(ctx, rc, k)
			if err != nil {
				return err
			}
			if ok {
				ver := stored
				if !present || stored.deleted || stored.vhash != vh {
					// A local write replicare could not observe at capture time: stamp a
					// fresh version off the key's own stored version (per-key monotonic,
					// so it beats what any peer last wrote here).
					phys, lg := hlcTick(now, stored.phys, stored.log)
					ver = version{phys: phys, log: lg, node: s.nodeID, deleted: false, vhash: vh}
					if err := stampMeta(ctx, rc, mk, ver); err != nil {
						return err
					}
				}
				if err := sw.write(record{key: []byte(k), ttl: ttl, flags: flags | flagMesh, dump: dump, ver: ver}); err != nil {
					return err
				}
				continue
			}
			// Vanished between DUMP and the value read — fall through to absent handling.
		}
		// Data absent.
		switch {
		case present && !stored.deleted:
			// Local delete: stamp a tombstone off the stored version and propagate it.
			phys, lg := hlcTick(now, stored.phys, stored.log)
			ver := version{phys: phys, log: lg, node: s.nodeID, deleted: true}
			if err := stampMeta(ctx, rc, mk, ver); err != nil {
				return err
			}
			if err := sw.write(record{key: []byte(k), flags: flagMesh | flagDeleted, ver: ver}); err != nil {
				return err
			}
		case present && stored.deleted:
			// Existing tombstone: re-propagate (a lagging peer may not have it yet).
			if err := sw.write(record{key: []byte(k), flags: flagMesh | flagDeleted, ver: stored}); err != nil {
				return err
			}
		default:
			// No data and no register entry: nothing to replicate (GC'd or never seen).
		}
	}
	return nil
}

// GCTombstones implements engine.TombstoneGC. Redis has no delta queue to use as the
// distributed watermark (the Postgres mechanism), so v1 reclaims tombstones by AGE: a
// tombstone older than the retention bound is DELeted from the metadata keyspace. The
// bound must exceed the worst-case cross-node propagation lag; a peer lagged beyond it
// is a reseed case (MM7). Bounded rolling scan, so it is O(batch) per call.
func (s *Source) GCTombstones(ctx context.Context, _ engine.TableRef) (int64, error) {
	if !s.cluster || s.db == nil {
		return 0, nil
	}
	cutoff := time.Now().Add(-tombstoneRetention(s.cfg.Params)).UnixMilli()
	tun := tuningFromParams(s.cfg.Params)
	var removed int64
	err := s.db.forEachShard(ctx, func(ctx context.Context, rc goredis.Cmdable) error {
		var cursor uint64
		for {
			mks, cur, err := rc.Scan(ctx, cursor, metaPrefix+"*", tun.scanCount).Result()
			if err != nil {
				return fmt.Errorf("redis mesh GC: SCAN: %w", err)
			}
			for _, mk := range mks {
				v, present, err := readMeta(ctx, rc, mk)
				if err != nil {
					return err
				}
				if present && v.deleted && v.phys < cutoff {
					if err := rc.Del(ctx, mk).Err(); err != nil {
						return fmt.Errorf("redis mesh GC: DEL %q: %w", mk, err)
					}
					removed++
				}
			}
			cursor = cur
			if cursor == 0 {
				return nil
			}
		}
	})
	return removed, err
}

// dumpOne DUMP+PTTLs a single key, applying big-key caps. exists is false when the key
// is absent/expired. Mirrors frameKeys' per-key logic for the mesh re-read.
func dumpOne(ctx context.Context, rc goredis.Cmdable, key string, tun copyTuning, nowMillis int64) (dump []byte, ttl int64, flags uint8, exists bool, err error) {
	pipe := rc.Pipeline()
	dc := pipe.Dump(ctx, key)
	pc := pipe.PTTL(ctx, key)
	var mc *goredis.IntCmd
	measure := tun.bigKeyWarn > 0 || tun.bigKeyRefuse > 0
	if measure {
		mc = pipe.MemoryUsage(ctx, key)
	}
	if _, e := pipe.Exec(ctx); e != nil && !errors.Is(e, goredis.Nil) {
		return nil, 0, 0, false, fmt.Errorf("redis mesh: DUMP/PTTL %q: %w", key, e)
	}
	payload, e := dc.Result()
	if errors.Is(e, goredis.Nil) {
		return nil, 0, 0, false, nil
	}
	if e != nil {
		return nil, 0, 0, false, fmt.Errorf("redis mesh: DUMP %q: %w", key, e)
	}
	d := pc.Val()
	if d == time.Duration(-2) {
		return nil, 0, 0, false, nil
	}
	if measure {
		if used, merr := mc.Result(); merr == nil {
			if tun.bigKeyRefuse > 0 && used >= tun.bigKeyRefuse {
				return nil, 0, 0, false, fmt.Errorf("redis: key %q is %d bytes, over big_key_refuse_bytes (%d)", key, used, tun.bigKeyRefuse)
			}
		}
	}
	ttl, flags = restoreTTL(d, tun.absTTL, nowMillis)
	return []byte(payload), ttl, flags, true, nil
}

// logicalVHash computes a version-gap-stable fingerprint of a key's LOGICAL value
// (type-aware reads, never DUMP bytes — see verify.go), so a change on any node is
// detected the same way and equal logical values hash identically across a version
// gap. ok is false when the key vanished or its type is unreadable.
func logicalVHash(ctx context.Context, rc goredis.Cmdable, key string) (uint64, bool, error) {
	t, err := rc.Type(ctx, key).Result()
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("redis mesh: TYPE %q: %w", key, err)
	}
	if t == "none" {
		return 0, false, nil
	}
	var cmd interface{}
	switch t {
	case "string":
		cmd = rc.Get(ctx, key)
	case "list":
		cmd = rc.LRange(ctx, key, 0, -1)
	case "set":
		cmd = rc.SMembers(ctx, key)
	case "zset":
		cmd = rc.ZRangeWithScores(ctx, key, 0, -1)
	case "hash":
		cmd = rc.HGetAll(ctx, key)
	case "stream":
		cmd = rc.XRange(ctx, key, "-", "+")
	default:
		return 0, false, nil
	}
	canon, ok, err := canonicalValue(t, cmd)
	if err != nil {
		return 0, false, fmt.Errorf("redis mesh: value read %q: %w", key, err)
	}
	if !ok {
		return 0, false, nil
	}
	h := md5.New()
	writeField(h, []byte(key))
	writeField(h, []byte(t))
	writeField(h, canon)
	var sum [md5.Size]byte
	h.Sum(sum[:0])
	return binary.BigEndian.Uint64(sum[:8]), true, nil
}

// readMeta reads a data key's version register entry from its metadata key. present is
// false when no entry exists.
func readMeta(ctx context.Context, rc goredis.Cmdable, mk string) (version, bool, error) {
	m, err := rc.HGetAll(ctx, mk).Result()
	if err != nil {
		return version{}, false, fmt.Errorf("redis mesh: HGETALL %q: %w", mk, err)
	}
	if len(m) == 0 {
		return version{}, false, nil
	}
	v := version{node: m["n"], deleted: m["d"] == "1"}
	if p, e := strconv.ParseInt(m["p"], 10, 64); e == nil {
		v.phys = p
	}
	if l, e := strconv.ParseInt(m["l"], 10, 32); e == nil {
		v.log = int32(l)
	}
	if vh, e := strconv.ParseUint(m["v"], 10, 64); e == nil {
		v.vhash = vh
	}
	return v, true, nil
}

// stampMeta writes a data key's version register entry (atomic single-key HSET; the
// value+register atomicity across the pair is provided by the apply Lua on the sink).
func stampMeta(ctx context.Context, rc goredis.Cmdable, mk string, v version) error {
	d := "0"
	if v.deleted {
		d = "1"
	}
	return rc.HSet(ctx, mk, "p", v.phys, "l", v.log, "n", v.node, "d", d, "v", v.vhash).Err()
}

// dataKeyFromMeta inverts metaKey: given a metadata key it returns the data key it
// describes. ok is false for a string that is not a metadata key.
func dataKeyFromMeta(mk string) (string, bool) {
	if !strings.HasPrefix(mk, metaPrefix) {
		return "", false
	}
	rest := mk[len(metaPrefix):] // "{tag}datakey"
	if len(rest) == 0 || rest[0] != '{' {
		return "", false
	}
	end := strings.IndexByte(rest, '}')
	if end < 0 {
		return "", false
	}
	return rest[end+1:], true
}

// tombstoneRetention is the age after which a delete tombstone may be GC'd. It must
// exceed the worst-case cross-node propagation lag; a peer lagged beyond it is a reseed
// case (MM7). Default 24h (matching the delta-retention default), overridable per
// endpoint via the rc_tombstone_retention param.
func tombstoneRetention(params map[string]string) time.Duration {
	const def = 24 * time.Hour
	if params == nil {
		return def
	}
	if v, ok := params[paramTombstoneRetention]; ok && v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return def
}
