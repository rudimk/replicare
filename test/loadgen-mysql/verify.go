package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// verify compares the source and target keyspaces VALUE-faithfully but rendering the
// content itself server-side, so it works across a version gap (the harness runs an old
// 5.7 source against a modern 8.4 target). For each replicated table it compares the row
// count and an order-independent content checksum: per row, MD5 over a NULL-safe,
// type-aware rendering of every column; the per-row hashes are folded with BIT_XOR (so
// no ORDER BY is needed and there is no GROUP_CONCAT length limit). Every row is unique
// by primary key (which is part of the rendered content), so XOR cannot cancel two real
// rows.
//
// Cross-version rendering notes:
//   - binary/blob columns render via HEX (raw bytes, version-independent).
//   - FLOAT/DOUBLE render via CAST AS DECIMAL(65,15): replicare moves the value bit-
//     faithfully, so both servers hold the identical double, and normalizing to a fixed
//     decimal removes any server-version difference in raw double-to-string formatting.
//   - everything else (INT/DECIMAL/DATE/DATETIME/JSON/text) renders via CAST AS CHAR
//     under a pinned time_zone; MySQL's JSON and decimal text output is stable across
//     5.7->8.x for identical stored values.

// colInfo is one column's name and MySQL data_type, used to pick a stable render.
type colInfo struct {
	name     string
	dataType string
}

// tableDiff is one table's source-vs-target comparison.
type tableDiff struct {
	name               string
	srcCount, dstCount int64
	srcSum, dstSum     string
	ok                 bool
}

// loadColumns reads the loadgen schema's columns (ordered) from the SOURCE's
// information_schema, so both ends render each row identically. The target carries the
// same schema via ddl.
func loadColumns(ctx context.Context, db *sql.DB) (map[string][]colInfo, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT table_name, column_name, data_type
		FROM information_schema.columns
		WHERE table_schema = 'loadgen'
		ORDER BY table_name, ordinal_position`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := map[string][]colInfo{}
	for rows.Next() {
		var t, c, dt string
		if err := rows.Scan(&t, &c, &dt); err != nil {
			return nil, err
		}
		out[t] = append(out[t], colInfo{name: c, dataType: strings.ToLower(dt)})
	}
	return out, rows.Err()
}

// renderCol is the deterministic, version-stable text rendering of one column.
func renderCol(c colInfo) string {
	q := "`" + c.name + "`"
	switch c.dataType {
	case "binary", "varbinary", "blob", "tinyblob", "mediumblob", "longblob", "bit":
		return "HEX(" + q + ")"
	case "float", "double":
		return "CAST(" + q + " AS DECIMAL(65,15))"
	default:
		return "CAST(" + q + " AS CHAR)"
	}
}

// rowHashExpr builds MD5(CONCAT_WS('|', COALESCE(render,'~NULL~'), ...)) over every
// column, so two rows differing only by NULL-vs-empty still hash differently.
func rowHashExpr(cols []colInfo) string {
	parts := make([]string, 0, len(cols))
	for _, c := range cols {
		parts = append(parts, "COALESCE("+renderCol(c)+", '~NULL~')")
	}
	return "MD5(CONCAT_WS('|', " + strings.Join(parts, ", ") + "))"
}

// tableChecksum returns the row count and an order-independent 128-bit content hash
// (two 64-bit BIT_XOR folds of each row's MD5, concatenated as hex). Empty table -> all
// zeros on both ends.
func tableChecksum(ctx context.Context, db *sql.DB, tbl string, cols []colInfo) (int64, string, error) {
	q := fmt.Sprintf(`
		SELECT COUNT(*),
		       CONCAT(
		         LPAD(HEX(COALESCE(BIT_XOR(CAST(CONV(SUBSTRING(h,1,16),16,10) AS UNSIGNED)),0)),16,'0'),
		         LPAD(HEX(COALESCE(BIT_XOR(CAST(CONV(SUBSTRING(h,17,16),16,10) AS UNSIGNED)),0)),16,'0')
		       )
		FROM (SELECT %s AS h FROM loadgen.%s) t`, rowHashExpr(cols), tbl)
	var n int64
	var sum string
	if err := db.QueryRowContext(ctx, q).Scan(&n, &sum); err != nil {
		return 0, "", fmt.Errorf("checksum %s: %w", tbl, err)
	}
	return n, sum, nil
}

// verifyOnce compares every replicated table across the two connections in a single
// pass. It does NOT retry; the caller loops for replication lag.
func verifyOnce(ctx context.Context, src, dst *sql.DB, cols map[string][]colInfo) ([]tableDiff, error) {
	diffs := make([]tableDiff, 0, len(replicatedTables))
	for _, t := range replicatedTables {
		tc := cols[t]
		if len(tc) == 0 {
			return nil, fmt.Errorf("no columns found for loadgen.%s (schema not applied?)", t)
		}
		sc, ss, err := tableChecksum(ctx, src, t, tc)
		if err != nil {
			return nil, fmt.Errorf("source: %w", err)
		}
		dc, ds, err := tableChecksum(ctx, dst, t, tc)
		if err != nil {
			return nil, fmt.Errorf("target: %w", err)
		}
		diffs = append(diffs, tableDiff{
			name:     t,
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

// reportDiffs prints a per-table convergence table. Returns the count of drifted tables.
func reportDiffs(diffs []tableDiff, log logf) int {
	drift := 0
	log("%-24s %12s %12s  %s", "table", "source", "target", "status")
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
		log("%-24s %12d %12d  %s", d.name, d.srcCount, d.dstCount, status)
	}
	return drift
}
