package engine

import (
	"context"
	"fmt"
	"strings"
)

// ChunkVerifier is an optional Verifier extension: compare a table one key range at
// a time instead of in one whole-table read. A whole-table fingerprint of a large
// table is one long full scan on each side, heavy on a busy source and liable to
// outrun any per-query time limit; chunks are small index range reads with a pause
// between them, and a chunk that differs can be narrowed to the exact rows.
//
// Keys are the table's replication key (its primary key, else first usable unique
// key) as faithful text values, the same form the initial copy uses. Ranges are
// half-open below and closed above: after < key <= upTo, nil meaning unbounded.
type ChunkVerifier interface {
	// NextKey returns the key of the n-th row after `after` in key order, or the last
	// key after it when fewer than n rows remain; nil when no rows remain.
	NextKey(ctx context.Context, t TableRef, after KeyValues, n int) (KeyValues, error)
	// VerifyRange fingerprints the rows with after < key <= upTo over the given
	// name-matched columns, with the same hash as Verifier.Fingerprint.
	VerifyRange(ctx context.Context, t TableRef, cols []string, after, upTo KeyValues) (Fingerprint, error)
	// RangeRowHashes returns each row's key and content hash in after < key <= upTo,
	// in key order, at most limit rows.
	RangeRowHashes(ctx context.Context, t TableRef, cols []string, after, upTo KeyValues, limit int) ([]RowHash, error)
	// RowHashes returns the key and content hash of each of the given keys that exists.
	RowHashes(ctx context.Context, t TableRef, cols []string, keys []KeyValues) ([]RowHash, error)
	// SampleKeys returns up to n keys picked at random across the table, in key order,
	// reading only a small sample of its pages (for a quick spot check).
	SampleKeys(ctx context.Context, t TableRef, n int) ([]KeyValues, error)
}

// RowHash is one row's key and content hash.
type RowHash struct {
	Key  KeyValues
	Hash string
}

// KeyString renders a key for display and as a map key: its text values joined by
// commas (a single-column key is just its value).
func KeyString(k KeyValues) string {
	parts := make([]string, len(k))
	for i, v := range k {
		parts[i] = fmt.Sprint(v)
	}
	return strings.Join(parts, ",")
}
