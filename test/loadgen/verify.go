package main

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// tableDiff is one table's source-vs-target comparison.
type tableDiff struct {
	name               string
	srcCount, dstCount int64
	srcSum, dstSum     string
	ok                 bool
}

// verifyGUCs pin the same formatting GUCs replicare pins on its transport
// connections, so row::text renders identically on both ends and we don't raise
// false drift from DateStyle / float-digit / bytea-format differences.
var verifyGUCs = []string{
	"SET DateStyle = 'ISO, YMD'",
	"SET TimeZone = 'UTC'",
	"SET extra_float_digits = 3",
	"SET IntervalStyle = 'postgres'",
	"SET bytea_output = 'hex'",
	"SET client_encoding = 'UTF8'",
}

func pinGUCs(ctx context.Context, conn *pgx.Conn) error {
	for _, s := range verifyGUCs {
		if _, err := conn.Exec(ctx, s); err != nil {
			return fmt.Errorf("pin GUC %q: %w", s, err)
		}
	}
	return nil
}

// tableChecksum returns the row count and a content hash: md5 over the
// key-ordered per-row md5(row::text). Deterministic given identical GUCs.
func tableChecksum(ctx context.Context, conn *pgx.Conn, t table) (int64, string, error) {
	q := fmt.Sprintf(
		`SELECT count(*)::bigint,
		        coalesce(md5(string_agg(md5(x::text), '' ORDER BY %s)), '')
		 FROM %s x`, t.order, t.name)
	var n int64
	var sum string
	if err := conn.QueryRow(ctx, q).Scan(&n, &sum); err != nil {
		return 0, "", fmt.Errorf("checksum %s: %w", t.name, err)
	}
	return n, sum, nil
}

// verifyOnce compares every replicated table across the two connections in a
// single pass. It does NOT retry; the caller loops for replication lag.
func verifyOnce(ctx context.Context, src, dst *pgx.Conn) ([]tableDiff, error) {
	diffs := make([]tableDiff, 0, len(replicatedTables))
	for _, t := range replicatedTables {
		sc, ss, err := tableChecksum(ctx, src, t)
		if err != nil {
			return nil, fmt.Errorf("source: %w", err)
		}
		dc, ds, err := tableChecksum(ctx, dst, t)
		if err != nil {
			return nil, fmt.Errorf("target: %w", err)
		}
		diffs = append(diffs, tableDiff{
			name:     t.name,
			srcCount: sc, dstCount: dc,
			srcSum: ss, dstSum: ds,
			ok: sc == dc && ss == ds,
		})
	}
	return diffs, nil
}

func allConverged(diffs []tableDiff) bool {
	for _, d := range diffs {
		if !d.ok {
			return false
		}
	}
	return true
}

// reportDiffs prints a per-table convergence table. Returns the count of drifted
// tables.
func reportDiffs(diffs []tableDiff, log logf) int {
	drift := 0
	log("%-32s %12s %12s  %s", "table", "source", "target", "status")
	for _, d := range diffs {
		status := "OK"
		if !d.ok {
			drift++
			if d.srcCount != d.dstCount {
				status = fmt.Sprintf("DRIFT (count %+d)", d.dstCount-d.srcCount)
			} else {
				status = "DRIFT (checksum)"
			}
		}
		log("%-32s %12d %12d  %s", d.name, d.srcCount, d.dstCount, status)
	}
	return drift
}
