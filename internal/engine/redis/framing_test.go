package redis

import (
	"bytes"
	"io"
	"testing"
	"time"
)

// TestFramingRoundTrip: arbitrary-byte keys and DUMP payloads survive the binary
// framing exactly, including embedded NULs, length-prefix boundary values, and the
// TTL/flags fields. This underpins the value-faithful promise (§1.7): the copy pipe
// never mangles a byte.
func TestFramingRoundTrip(t *testing.T) {
	recs := []record{
		{key: []byte("simple"), ttl: 0, flags: 0, dump: []byte("payload")},
		{key: []byte{0x00, 0xff, 0x01, 0x00}, ttl: 12345, flags: 0, dump: []byte{0x00, 0x00, 0xde, 0xad}},
		{key: []byte(""), ttl: -1, flags: 0, dump: []byte{}}, // empty key + empty dump
		{key: []byte("abs"), ttl: 1893456000000, flags: flagAbsTTL, dump: bytes.Repeat([]byte{0xAB}, 1000)},
		{key: bytes.Repeat([]byte{0x7f}, 300), ttl: 1, flags: 0, dump: []byte{0xC3}}, // key > 255
	}

	var buf bytes.Buffer
	sw := newSyncWriter(&buf)
	for _, r := range recs {
		if err := sw.write(r); err != nil {
			t.Fatalf("write: %v", err)
		}
	}

	for i, want := range recs {
		got, err := readRecord(&buf)
		if err != nil {
			t.Fatalf("readRecord[%d]: %v", i, err)
		}
		if !bytes.Equal(got.key, want.key) || got.ttl != want.ttl || got.flags != want.flags || !bytes.Equal(got.dump, want.dump) {
			t.Errorf("record[%d] mismatch:\n got  %+v\n want %+v", i, got, want)
		}
	}
	// Clean EOF exactly at the boundary.
	if _, err := readRecord(&buf); err != io.EOF {
		t.Errorf("trailing read = %v, want io.EOF", err)
	}
}

// TestFramingMeshRoundTrip: mesh records carry the version register (phys, log,
// node, vhash) and tombstone flag through the framing intact, and a mesh reader still
// reads plain one-way records correctly (mixed stream). The one-way record's bytes are
// unchanged by the mesh extension (flagMesh unset → no extra fields).
func TestFramingMeshRoundTrip(t *testing.T) {
	recs := []record{
		// plain one-way record in the same stream — must still round-trip.
		{key: []byte("plain"), ttl: 10, flags: 0, dump: []byte("v")},
		// alive mesh record with a version + value.
		{key: []byte("k1"), ttl: 5000, flags: flagMesh, dump: []byte{0x01, 0x02},
			ver: version{phys: 1893456000000, log: 7, node: "eu", vhash: 0xDEADBEEFCAFEBABE}},
		// mesh tombstone: deleted, empty dump, node with odd bytes.
		{key: []byte{0x00, 0xff}, ttl: 0, flags: flagMesh | flagDeleted, dump: []byte{},
			ver: version{phys: 42, log: 0, node: "us-west-1", deleted: true}},
		// mesh + absttl together.
		{key: []byte("k2"), ttl: 1893456000000, flags: flagMesh | flagAbsTTL, dump: []byte{0xAB},
			ver: version{phys: 99, log: 3, node: "", vhash: 1}},
	}

	var buf bytes.Buffer
	sw := newSyncWriter(&buf)
	for _, r := range recs {
		if err := sw.write(r); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	for i, want := range recs {
		got, err := readRecord(&buf)
		if err != nil {
			t.Fatalf("readRecord[%d]: %v", i, err)
		}
		if !bytes.Equal(got.key, want.key) || got.ttl != want.ttl || got.flags != want.flags || !bytes.Equal(got.dump, want.dump) {
			t.Errorf("record[%d] base mismatch:\n got  %+v\n want %+v", i, got, want)
		}
		if want.flags&flagMesh != 0 && got.ver != want.ver {
			t.Errorf("record[%d] version mismatch:\n got  %+v\n want %+v", i, got.ver, want.ver)
		}
	}
	if _, err := readRecord(&buf); err != io.EOF {
		t.Errorf("trailing read = %v, want io.EOF", err)
	}
}

// TestFramingMeshBytesUnchangedForOneWay: a record with flagMesh unset serializes to
// the exact same bytes as before the mesh extension existed — the wire-level backward
// compatibility invariant.
func TestFramingMeshBytesUnchangedForOneWay(t *testing.T) {
	var buf bytes.Buffer
	sw := newSyncWriter(&buf)
	if err := sw.write(record{key: []byte("k"), ttl: 5, flags: flagAbsTTL, dump: []byte{0x01, 0x02}}); err != nil {
		t.Fatalf("write: %v", err)
	}
	// keyLen(4)=1 + key(1) + ttl(8) + flags(1) + dumpLen(4)=2 + dump(2) = 20 bytes; NO mesh fields.
	if got := buf.Len(); got != 4+1+8+1+4+2 {
		t.Errorf("one-way record length = %d, want %d (mesh extension must not touch it)", got, 4+1+8+1+4+2)
	}
}

// TestFramingTruncated: a stream cut mid-record is a loud ErrUnexpectedEOF, never a
// silent short record.
func TestFramingTruncated(t *testing.T) {
	var buf bytes.Buffer
	sw := newSyncWriter(&buf)
	if err := sw.write(record{key: []byte("k"), ttl: 5, dump: bytes.Repeat([]byte{0x01}, 50)}); err != nil {
		t.Fatalf("write: %v", err)
	}
	full := buf.Bytes()
	// Truncate partway into the dump.
	if _, err := readRecord(bytes.NewReader(full[:len(full)-10])); err != io.ErrUnexpectedEOF {
		t.Errorf("truncated read = %v, want io.ErrUnexpectedEOF", err)
	}
}

// TestRestoreTTL covers the PTTL -> (ttl, flags) mapping.
func TestRestoreTTL(t *testing.T) {
	const ms = int64(1e6)
	// -1 (no expiry) -> relative 0.
	if ttl, fl := restoreTTL(-1, false, 0); ttl != 0 || fl != 0 {
		t.Errorf("no-expiry = (%d,%d), want (0,0)", ttl, fl)
	}
	// relative: 5000ms remaining -> ttl 5000, no flag.
	if ttl, fl := restoreTTL(5000*time.Millisecond, false, 0); ttl != 5000 || fl != 0 {
		t.Errorf("relative = (%d,%d), want (5000,0)", ttl, fl)
	}
	// absttl: 5000ms remaining + now -> absolute, flag set.
	if ttl, fl := restoreTTL(5000*time.Millisecond, true, ms); ttl != ms+5000 || fl != flagAbsTTL {
		t.Errorf("absttl = (%d,%d), want (%d,%d)", ttl, fl, ms+5000, flagAbsTTL)
	}
	// no-expiry under absttl is still relative 0.
	if ttl, fl := restoreTTL(-1, true, ms); ttl != 0 || fl != 0 {
		t.Errorf("absttl no-expiry = (%d,%d), want (0,0)", ttl, fl)
	}
}
