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
