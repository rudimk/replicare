package redis

import (
	"context"
	"fmt"
	"io"
	"strconv"

	goredis "github.com/redis/go-redis/v9"
)

// MM6 — Redis mesh SINK side: version-guarded apply. Every inbound record carries its
// origin's version register (phys, log, node); apply RESTOREs (or DELetes) the key and
// updates the local register ONLY when the incoming version strictly beats the local
// one. That single comparison is both loop suppression (replicare's own applied write
// carries a version the peer already has, so it loses) and HLC last-write-wins conflict
// resolution (docs/multi-master.md §5.3). The value and its register are written
// together in one Lua script — they share a cluster slot (metaKey co-location), so the
// pair can never diverge (an MM6 atomicity requirement).

// EnableOriginMarking implements engine.OriginMarkingSink: switch this sink to
// version-guarded apply for a cluster edge. nodeID is this target member's origin
// identity (accepted for symmetry; the winning version's node travels in each record).
func (s *Sink) EnableOriginMarking(nodeID string) {
	s.cluster = true
	s.nodeID = nodeID
}

// meshApplyScript compares the incoming version against the local register and, only if
// it strictly wins (a never-seen local register always loses; node_id breaks equal-HLC
// ties), applies the change and records the version. KEYS[1]=data key, KEYS[2]=metadata
// key (same slot). ARGV: 1=op(0 alive/1 delete) 2=phys 3=log 4=node 5=vhash 6=ttl
// 7=absttl(0/1) 8=dump. Returns 1 if applied, 0 if the incoming version lost.
var meshApplyScript = goredis.NewScript(`
local m = redis.call('HMGET', KEYS[2], 'p', 'l', 'n')
local ep = tonumber(m[1])
local el = tonumber(m[2])
local en = m[3]
local ip = tonumber(ARGV[2])
local il = tonumber(ARGV[3])
local innode = ARGV[4]
local win
if ep == nil then
  win = true
elseif ip > ep then
  win = true
elseif ip == ep and il > el then
  win = true
elseif ip == ep and il == el and innode > en then
  win = true
else
  win = false
end
if not win then return 0 end
if ARGV[1] == '1' then
  redis.call('DEL', KEYS[1])
  redis.call('HSET', KEYS[2], 'p', ARGV[2], 'l', ARGV[3], 'n', ARGV[4], 'd', '1', 'v', '0')
else
  if ARGV[7] == '1' then
    redis.call('RESTORE', KEYS[1], ARGV[6], ARGV[8], 'REPLACE', 'ABSTTL')
  else
    redis.call('RESTORE', KEYS[1], ARGV[6], ARGV[8], 'REPLACE')
  end
  redis.call('HSET', KEYS[2], 'p', ARGV[2], 'l', ARGV[3], 'n', ARGV[4], 'd', '0', 'v', ARGV[5])
end
return 1
`)

// stageUpsertCluster reads the mesh framing and applies each record with the
// version-guarded Lua. It records every key it processed as staged so DeleteAbsent is a
// no-op in cluster mode (deletes flow only as version-guarded tombstone records).
func stageUpsertCluster(ctx context.Context, db *conn, r io.Reader, staged map[string]bool) error {
	for {
		rec, err := readRecord(r)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if rec.flags&flagMesh == 0 {
			// A cluster edge must receive mesh records; a plain record here is a bug.
			return fmt.Errorf("redis mesh: apply received a non-mesh record for key %q", string(rec.key))
		}
		k := string(rec.key)
		staged[k] = true
		op := "0"
		absttl := "0"
		if rec.flags&flagDeleted != 0 {
			op = "1"
		}
		if rec.flags&flagAbsTTL != 0 {
			absttl = "1"
		}
		args := []any{
			op,
			strconv.FormatInt(rec.ver.phys, 10),
			strconv.FormatInt(int64(rec.ver.log), 10),
			rec.ver.node,
			strconv.FormatUint(rec.ver.vhash, 10),
			strconv.FormatInt(rec.ttl, 10),
			absttl,
			string(rec.dump),
		}
		if err := meshApplyScript.Run(ctx, db.uc, []string{k, metaKey(k)}, args...).Err(); err != nil {
			return fmt.Errorf("redis mesh: apply %q: %w", k, err)
		}
	}
}
