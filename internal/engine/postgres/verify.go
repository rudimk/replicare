package postgres

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/rudimk/replicare/internal/engine"
)

// This file implements engine.Verifier for the Postgres Source and Sink: the
// read-only live row count and content fingerprint powering `replicare status`
// (live mode) and `replicare verify`. Both perform only SELECTs and install
// nothing, so they are safe against a source we may not own and against a live
// target. Because a sync is single-engine (CLAUDE.md §6), a source and its target
// run the identical fingerprint query, so their checksums are directly comparable.

var (
	_ engine.Verifier = (*Source)(nil)
	_ engine.Verifier = (*Sink)(nil)
)

// CountRows returns the live row count of a table.
func (s *Source) CountRows(ctx context.Context, t engine.TableRef) (int64, error) {
	if s.conn == nil {
		return 0, errNotConnected("source")
	}
	return countRows(ctx, s.conn, t)
}

// Fingerprint returns the table's count plus an order-independent content hash.
func (s *Source) Fingerprint(ctx context.Context, t engine.TableRef, cols []string) (engine.Fingerprint, error) {
	if s.conn == nil {
		return engine.Fingerprint{}, errNotConnected("source")
	}
	return fingerprint(ctx, s.conn, t, cols)
}

// CountRows returns the live row count of a target table.
func (s *Sink) CountRows(ctx context.Context, t engine.TableRef) (int64, error) {
	if s.conn == nil {
		return 0, errNotConnected("sink")
	}
	return countRows(ctx, s.conn, t)
}

// Fingerprint returns the target table's count plus an order-independent content hash.
func (s *Sink) Fingerprint(ctx context.Context, t engine.TableRef, cols []string) (engine.Fingerprint, error) {
	if s.conn == nil {
		return engine.Fingerprint{}, errNotConnected("sink")
	}
	return fingerprint(ctx, s.conn, t, cols)
}

func countRows(ctx context.Context, conn *pgx.Conn, t engine.TableRef) (int64, error) {
	var n int64
	if err := conn.QueryRow(ctx, "SELECT count(*)::bigint FROM "+qualifyTable(t)).Scan(&n); err != nil {
		return 0, fmt.Errorf("postgres: count %s: %w", t, err)
	}
	return n, nil
}

// fingerprint computes count(*) plus an order-independent content hash: the sum
// (as text, an arbitrary-precision numeric) over every row of the row's md5 folded
// to a signed 64-bit integer. It is order-independent (SUM, not string_agg), so it
// needs no ORDER BY and no key, and streams in constant server memory rather than
// buffering a giant aggregate — important against a source we may not own (§1). The
// row is rendered as ROW(<sorted cols>)::text over an EXPLICIT, name-sorted column
// projection, so it matches between a source and a target whose physical column
// order differs (faithful apply is name-matched, §4.2). The md5→bigint idiom
// ('x'||hex)::bit(64)::bigint is old-Postgres-safe (CLAUDE.md §1.6).
func fingerprint(ctx context.Context, conn *pgx.Conn, t engine.TableRef, cols []string) (engine.Fingerprint, error) {
	if len(cols) == 0 {
		// No projectable columns: fall back to a count-only fingerprint.
		n, err := countRows(ctx, conn, t)
		return engine.Fingerprint{Rows: n}, err
	}
	sorted := append([]string(nil), cols...)
	sort.Strings(sorted)
	quoted := make([]string, len(sorted))
	for i, c := range sorted {
		quoted[i] = quoteIdentifier(c)
	}
	rowExpr := "ROW(" + strings.Join(quoted, ", ") + ")::text"
	sql := fmt.Sprintf(
		`SELECT count(*)::bigint,
		        coalesce(sum(('x' || substr(md5(%s), 1, 16))::bit(64)::bigint)::text, '0')
		 FROM %s`, rowExpr, qualifyTable(t))
	var n int64
	var sum string
	if err := conn.QueryRow(ctx, sql).Scan(&n, &sum); err != nil {
		return engine.Fingerprint{}, fmt.Errorf("postgres: fingerprint %s: %w", t, err)
	}
	return engine.Fingerprint{Rows: n, Checksum: sum}, nil
}
