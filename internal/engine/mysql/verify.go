package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/rudimk/replicare/internal/engine"
)

// This file implements engine.Verifier for the MySQL Source and Sink: the
// read-only live row count and content fingerprint behind `replicare status`
// (live mode) and `replicare verify`. Both are pure SELECTs and install nothing.
// A sync is single-engine (CLAUDE.md §6), so a source and its target run the
// identical fingerprint query and their checksums are directly comparable.

var (
	_ engine.Verifier = (*Source)(nil)
	_ engine.Verifier = (*Sink)(nil)
)

// CountRows returns the live row count of a table.
func (s *Source) CountRows(ctx context.Context, t engine.TableRef) (int64, error) {
	if s.db == nil {
		return 0, errNotConnected
	}
	return tableRowCount(ctx, s.db, t)
}

// Fingerprint returns the table's count plus an order-independent content hash.
func (s *Source) Fingerprint(ctx context.Context, t engine.TableRef, cols []string) (engine.Fingerprint, error) {
	if s.db == nil {
		return engine.Fingerprint{}, errNotConnected
	}
	return fingerprint(ctx, s.db, t, cols)
}

// CountRows returns the live row count of a target table.
func (s *Sink) CountRows(ctx context.Context, t engine.TableRef) (int64, error) {
	if s.db == nil {
		return 0, errNotConnected
	}
	return tableRowCount(ctx, s.db, t)
}

// Fingerprint returns the target table's count plus an order-independent content hash.
func (s *Sink) Fingerprint(ctx context.Context, t engine.TableRef, cols []string) (engine.Fingerprint, error) {
	if s.db == nil {
		return engine.Fingerprint{}, errNotConnected
	}
	return fingerprint(ctx, s.db, t, cols)
}

func tableRowCount(ctx context.Context, db *sql.DB, t engine.TableRef) (int64, error) {
	var n int64
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+qualify(t.Schema, t.Name)).Scan(&n); err != nil {
		return 0, fmt.Errorf("mysql: count %s: %w", t, err)
	}
	return n, nil
}

// fingerprint computes COUNT(*) plus an order-independent content hash: the exact
// SUM over every row of the row's MD5 folded to a 64-bit unsigned integer, kept in
// exact DECIMAL arithmetic (CAST … AS UNSIGNED, whose SUM is exact DECIMAL — never
// the lossy floating-point SUM of a bare string). It is order-independent (SUM, not
// GROUP_CONCAT), so it needs no ORDER BY, no key, and dodges group_concat_max_len
// truncation, streaming in constant server memory. The row is rendered over an
// EXPLICIT, name-sorted column projection so it matches between a source and target
// whose physical column order differs (faithful apply is name-matched, §4.2); each
// column is NUL-guarded so a NULL is distinguished from the empty string.
func fingerprint(ctx context.Context, db *sql.DB, t engine.TableRef, cols []string) (engine.Fingerprint, error) {
	if len(cols) == 0 {
		n, err := tableRowCount(ctx, db, t)
		return engine.Fingerprint{Rows: n}, err
	}
	sorted := append([]string(nil), cols...)
	sort.Strings(sorted)
	parts := make([]string, len(sorted))
	for i, c := range sorted {
		// CHAR(0) sentinel marks NULL, distinguishing it from an empty string.
		parts[i] = fmt.Sprintf("IFNULL(CAST(%s AS CHAR), CHAR(0))", bq(c))
	}
	rowExpr := "CONCAT_WS(CHAR(1), " + strings.Join(parts, ", ") + ")"
	q := fmt.Sprintf(
		`SELECT COUNT(*),
		        COALESCE(SUM(CAST(CONV(SUBSTRING(MD5(%s), 1, 16), 16, 10) AS UNSIGNED)), 0)
		 FROM %s`, rowExpr, qualify(t.Schema, t.Name))
	var n int64
	var sum string // exact DECIMAL scanned as text to preserve full precision
	if err := db.QueryRowContext(ctx, q).Scan(&n, &sum); err != nil {
		return engine.Fingerprint{}, fmt.Errorf("mysql: fingerprint %s: %w", t, err)
	}
	return engine.Fingerprint{Rows: n, Checksum: sum}, nil
}
