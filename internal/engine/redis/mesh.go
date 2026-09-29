package redis

import (
	"strconv"
	"strings"
	"sync"
)

// MM6 — Redis active-active mesh foundations (docs/multi-master.md §5.4).
//
// Redis is capture-less: a local application write to a key is invisible to
// replicare (there is no trigger, no change stream). So the mesh maintains a
// PARALLEL METADATA KEYSPACE — one sibling entry per data key holding the version
// register {hlc, node_id, deleted?} plus a version-gap-stable value fingerprint —
// and resolves conflicts with HLC last-write-wins over that register. The data
// value itself is never wrapped (faithful transport, CLAUDE.md §1.7): DUMP/RESTORE
// still moves the value verbatim; the version travels beside it.
//
// This file holds the PURE, engine-independent pieces (naming, encoding, ordering,
// clock arithmetic) so they are unit-testable without a live Redis. The wiring that
// consults Redis (versioned reread, Lua apply, tombstone GC) lives in the mesh_*.go
// files and is exercised by the local integration gate.

// metaPrefix is the reserved namespace for the metadata keyspace. The leading NUL
// byte keeps it out of any ordinary user keyspace (a control char is not a normal
// key), so metadata keys never collide with user data and are excluded from data
// selection in cluster mode (never replicated as data, never delete-swept).
const metaPrefix = "\x00rc:m:"

// slotTag returns the exact substring Redis uses to compute a key's cluster hash
// slot: the content between the first '{' and the first '}' AFTER it, when that
// content is non-empty; otherwise the whole key. This mirrors Redis's own key-hashing
// rule (CLUSTER KEYSLOT) without needing CRC16 — we only need to reproduce the tag so
// a metadata key co-locates in the same slot as its data key.
func slotTag(key string) string {
	open := strings.IndexByte(key, '{')
	if open < 0 {
		return key
	}
	end := strings.IndexByte(key[open+1:], '}')
	if end <= 0 { // no '}' after '{', or empty "{}" → whole key hashes
		return key
	}
	return key[open+1 : open+1+end]
}

// metaKey is the metadata sibling key for a data key, built so it hashes to the SAME
// cluster slot as the data key — value and metadata co-locate, so a single Lua script
// may touch both atomically (MM6 acceptance). Construction: metaPrefix + "{" + <a
// canonical brace-free tag that hashes to key's slot> + "}" + key. Injecting a
// canonical per-slot tag (rather than key's own tag substring) is robust for every key
// shape — including keys that contain '}' but no valid tag (e.g. "a}b"), where naively
// wrapping the key would truncate at the embedded '}'. Appending the full key keeps
// metadata keys distinct even when two data keys share a slot.
func metaKey(key string) string {
	return metaPrefix + "{" + slotToTag(slotOf(key)) + "}" + key
}

// hashSlots is the fixed Redis Cluster slot count.
const hashSlots = 16384

// slotOf returns the Redis Cluster hash slot for a key: CRC16 of its hash tag, mod
// 16384 — exactly CLUSTER KEYSLOT. Standalone/sentinel ignore slots, but computing it
// uniformly is harmless and keeps metaKey co-location correct in cluster mode.
func slotOf(key string) uint16 {
	return crc16(slotTag(key)) % hashSlots
}

// slotTagTable maps each of the 16384 slots to a canonical brace-free tag string that
// hashes to it, built once on first use. It is populated lazily (not in package init)
// so a one-way daemon that never forms a mesh pays nothing.
var (
	slotTagOnce  sync.Once
	slotTagTable [hashSlots]string
)

// slotToTag returns a canonical brace-free string whose CRC16 lands in the given slot.
func slotToTag(slot uint16) string {
	slotTagOnce.Do(func() {
		filled := 0
		for i := 0; filled < hashSlots; i++ {
			s := strconv.Itoa(i)
			sl := crc16(s) % hashSlots
			if slotTagTable[sl] == "" {
				slotTagTable[sl] = s
				filled++
			}
		}
	})
	return slotTagTable[slot]
}

// crc16Tab is the CRC16-CCITT (XMODEM) table Redis uses for cluster key hashing.
var crc16Tab = buildCRC16Table()

func buildCRC16Table() [256]uint16 {
	const poly = 0x1021
	var tab [256]uint16
	for i := 0; i < 256; i++ {
		crc := uint16(i) << 8
		for j := 0; j < 8; j++ {
			if crc&0x8000 != 0 {
				crc = (crc << 1) ^ poly
			} else {
				crc <<= 1
			}
		}
		tab[i] = crc
	}
	return tab
}

// crc16 computes the CRC16-CCITT (XMODEM) checksum Redis uses for CLUSTER KEYSLOT.
func crc16(s string) uint16 {
	var crc uint16
	for i := 0; i < len(s); i++ {
		crc = (crc << 8) ^ crc16Tab[byte(crc>>8)^s[i]]
	}
	return crc
}

// isMetaKey reports whether a scanned key belongs to the reserved metadata keyspace
// (so the data-keyspace scan can skip it and never treat metadata as user data).
func isMetaKey(key string) bool {
	return strings.HasPrefix(key, "\x00rc:")
}

// version is the per-key mesh version register entry: a hybrid logical clock value
// (phys, log), the origin node, a tombstone flag, and vhash — a version-gap-stable
// fingerprint of the key's logical value (verify.go canonicalValue), used to detect
// a local write that replicare could not observe at capture time.
type version struct {
	phys    int64  // hybrid logical clock physical component (unix ms)
	log     int32  // hybrid logical clock logical counter
	node    string // origin node_id
	deleted bool   // tombstone
	vhash   uint64 // logical-value fingerprint (0 for a tombstone)
}

// beats reports whether v strictly wins last-write-wins over other, under the total
// order (phys, log, node). A zero/never-seen other always loses. node_id breaks
// equal-HLC ties, so the order is total with no ties (docs/multi-master.md §5.3).
// present distinguishes "other is a real stored version" from "no version at all"
// (the latter always loses).
func (v version) beats(other version, present bool) bool {
	if !present {
		return true
	}
	if v.phys != other.phys {
		return v.phys > other.phys
	}
	if v.log != other.log {
		return v.log > other.log
	}
	return v.node > other.node
}

// hlcTick advances a hybrid logical clock for a new LOCAL write given the current
// wall-clock (nowMillis) and the clock's stored (phys, log). If wall-clock has moved
// past the stored physical time, physical jumps forward and logical resets; otherwise
// physical holds and logical increments — so the returned value is strictly greater
// than the input under the (phys, log) order and tracks wall-clock when it advances.
func hlcTick(nowMillis, phys int64, log int32) (int64, int32) {
	if nowMillis > phys {
		return nowMillis, 0
	}
	return phys, log + 1
}

// hlcObserve advances a hybrid logical clock past an incoming (inPhys, inLog) seen on
// apply, so a subsequent local tick strictly exceeds anything observed (skew
// tolerance). New physical = max(wall-clock, stored, incoming); logical is bumped so
// the pair is >= every input. This is the receive-side counterpart to hlcTick.
func hlcObserve(nowMillis, phys int64, log int32, inPhys int64, inLog int32) (int64, int32) {
	newPhys := phys
	if nowMillis > newPhys {
		newPhys = nowMillis
	}
	if inPhys > newPhys {
		newPhys = inPhys
	}
	switch {
	case newPhys == phys && newPhys == inPhys:
		// both stored and incoming sit at the winning physical → max logical, +1.
		l := log
		if inLog > l {
			l = inLog
		}
		return newPhys, l + 1
	case newPhys == phys:
		return newPhys, log + 1
	case newPhys == inPhys:
		return newPhys, inLog + 1
	default:
		// wall clock advanced past both stored and incoming → logical resets.
		return newPhys, 0
	}
}
