package redis

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"
)

// Redis has no text COPY wire format, so the copy/apply layers move values over a
// PRIVATE length-prefixed binary framing of DUMP payloads (redis-plan §0.2). Each
// record is one key's serialized value plus its TTL:
//
//	keyLen(uint32) | key(bytes) | ttlMillis(int64) | flags(uint8) | dumpLen(uint32) | dump(bytes)
//
// All integers are big-endian. Keys and DUMP payloads are ARBITRARY bytes (a key
// name can hold any byte; a DUMP payload is RDB-serialized binary), so the framing
// is length-prefixed, never delimiter-based. `flags` bit0 marks an ABSTTL value
// (ttlMillis is absolute unix-ms); otherwise ttlMillis is relative (0 = no expiry).
// This is the exact byte stream CopyChunk writes into the copy pipe and BulkLoad
// reads back out — the neutral io.Pipe plumbing (internal/copy) is unchanged.
//
// MM6 mesh extension: when `flags` bit1 (flagMesh) is set, the mesh version register
// travels with the record, inserted BETWEEN flags and dumpLen:
//
//	… | flags(uint8) | phys(int64) | log(int32) | vhash(uint64) | nodeLen(uint16) | node(bytes) | dumpLen(uint32) | dump
//
// A tombstone sets flagDeleted (bit2) and carries dumpLen=0. One-way records never
// set flagMesh, so their wire bytes are byte-for-byte what they always were — the
// backward-compatibility invariant (a config with no clusters is exactly today's
// one-way daemon). The reader keys entirely off the flag byte, so a one-way reader
// and a mesh reader agree on every one-way record.

const (
	flagAbsTTL  uint8 = 1 << 0 // ttlMillis is absolute unix-ms (else relative)
	flagMesh    uint8 = 1 << 1 // record carries the mesh version register (MM6)
	flagDeleted uint8 = 1 << 2 // mesh tombstone: DEL the key (dump is empty)
)

// record is one framed key/value/TTL triple. In mesh mode it additionally carries
// the version register (phys, log, node, vhash); ver is only meaningful when
// flags&flagMesh != 0.
type record struct {
	key   []byte
	ttl   int64 // relative ms (0 = no expiry) unless absTTL, then absolute unix-ms
	flags uint8
	dump  []byte
	ver   version // mesh version register; valid iff flags&flagMesh != 0
}

// syncWriter serializes concurrent record writes onto one io.Writer. CopyChunk
// fans SCAN out across shards (goroutines) into a single pipe; each record is
// written under the lock as one contiguous Write, so whole records never
// interleave (the framing itself is self-delimiting via length prefixes).
type syncWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func newSyncWriter(w io.Writer) *syncWriter { return &syncWriter{w: w} }

// write emits one record atomically.
func (sw *syncWriter) write(rec record) error {
	buf := make([]byte, 0, 4+len(rec.key)+8+1+4+len(rec.dump))
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(rec.key)))
	buf = append(buf, rec.key...)
	buf = binary.BigEndian.AppendUint64(buf, uint64(rec.ttl))
	buf = append(buf, rec.flags)
	if rec.flags&flagMesh != 0 {
		buf = binary.BigEndian.AppendUint64(buf, uint64(rec.ver.phys))
		buf = binary.BigEndian.AppendUint32(buf, uint32(rec.ver.log))
		buf = binary.BigEndian.AppendUint64(buf, rec.ver.vhash)
		buf = binary.BigEndian.AppendUint16(buf, uint16(len(rec.ver.node)))
		buf = append(buf, rec.ver.node...)
	}
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(rec.dump)))
	buf = append(buf, rec.dump...)

	sw.mu.Lock()
	defer sw.mu.Unlock()
	_, err := sw.w.Write(buf)
	return err
}

// readRecord reads one framed record from r. It returns io.EOF exactly when the
// stream ends cleanly on a record boundary; a truncated record is io.ErrUnexpectedEOF.
func readRecord(r io.Reader) (record, error) {
	var hdr [4]byte
	// The only place a clean EOF is allowed is before the first byte of a record.
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		if err == io.ErrUnexpectedEOF {
			return record{}, io.ErrUnexpectedEOF
		}
		return record{}, err // io.EOF at a clean boundary, or a real error
	}
	keyLen := binary.BigEndian.Uint32(hdr[:])
	key := make([]byte, keyLen)
	if _, err := io.ReadFull(r, key); err != nil {
		return record{}, framingEOF(err)
	}
	var mid [9]byte // ttl(8) + flags(1)
	if _, err := io.ReadFull(r, mid[:]); err != nil {
		return record{}, framingEOF(err)
	}
	ttl := int64(binary.BigEndian.Uint64(mid[:8]))
	flags := mid[8]
	var ver version
	if flags&flagMesh != 0 {
		var mv [22]byte // phys(8) + log(4) + vhash(8) + nodeLen(2)
		if _, err := io.ReadFull(r, mv[:]); err != nil {
			return record{}, framingEOF(err)
		}
		ver.phys = int64(binary.BigEndian.Uint64(mv[:8]))
		ver.log = int32(binary.BigEndian.Uint32(mv[8:12]))
		ver.vhash = binary.BigEndian.Uint64(mv[12:20])
		nodeLen := binary.BigEndian.Uint16(mv[20:22])
		node := make([]byte, nodeLen)
		if _, err := io.ReadFull(r, node); err != nil {
			return record{}, framingEOF(err)
		}
		ver.node = string(node)
		ver.deleted = flags&flagDeleted != 0
	}
	var dl [4]byte
	if _, err := io.ReadFull(r, dl[:]); err != nil {
		return record{}, framingEOF(err)
	}
	dump := make([]byte, binary.BigEndian.Uint32(dl[:]))
	if _, err := io.ReadFull(r, dump); err != nil {
		return record{}, framingEOF(err)
	}
	return record{key: key, ttl: ttl, flags: flags, dump: dump, ver: ver}, nil
}

// framingEOF turns a mid-record EOF into an unexpected-EOF error: once a record
// has started, ending is truncation, never a clean stream end.
func framingEOF(err error) error {
	if err == io.EOF {
		return fmt.Errorf("redis framing: truncated record: %w", io.ErrUnexpectedEOF)
	}
	return err
}
