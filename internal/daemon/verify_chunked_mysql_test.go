package daemon

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/config"
	"github.com/rudimk/replicare/internal/observability/live"
)

// insertRows inserts rows built by row(i) for i in [1, n], in batches.
func insertRows(t *testing.T, ctx context.Context, db *sql.DB, table string, n int, row func(i int) string) {
	t.Helper()
	for start := 1; start <= n; start += 500 {
		vals := make([]string, 0, 500)
		for i := start; i < start+500 && i <= n; i++ {
			vals = append(vals, row(i))
		}
		myExec(t, ctx, db, "INSERT INTO "+table+" VALUES "+strings.Join(vals, ","))
	}
}

// TestVerifyChunkedMySQLIntegration is the MySQL counterpart of the Postgres chunked
// verify test, with no daemon running: a matching table reads ok over several ranges;
// missing, extra and changed rows are found and named by key; a composite text key
// works; --table narrows the check; a row fixed before the re-check is not reported;
// and sampling works on an integer key, a text (UUID) key, and a date key (the
// by-position fallback).
func TestVerifyChunkedMySQLIntegration(t *testing.T) {
	if !mysqlIntegration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	const db = "rc_vcm"
	src, tgt := resetMySQLPair(t, ctx, db,
		"CREATE TABLE rc_vcm.items (id INT PRIMARY KEY, note VARCHAR(40), amt DECIMAL(12,2)) ENGINE=InnoDB",
		"CREATE TABLE rc_vcm.pairs (region VARCHAR(10), code VARCHAR(10), val INT, PRIMARY KEY (region, code)) ENGINE=InnoDB",
		"CREATE TABLE rc_vcm.uuids (id CHAR(36) PRIMARY KEY, n INT) ENGINE=InnoDB",
		"CREATE TABLE rc_vcm.days (d DATE PRIMARY KEY, n INT) ENGINE=InnoDB",
	)
	for _, c := range []*sql.DB{src, tgt} {
		insertRows(t, ctx, c, "rc_vcm.items", 2500, func(i int) string { return fmt.Sprintf("(%d,'n%d',%d.50)", i, i, i) })
		insertRows(t, ctx, c, "rc_vcm.pairs", 700, func(i int) string { return fmt.Sprintf("('r%d','c%d',%d)", i%7, i, i) })
		insertRows(t, ctx, c, "rc_vcm.uuids", 1200, func(i int) string {
			return fmt.Sprintf("(LOWER(CONCAT(LPAD(HEX(%d*2654435761 %% 4294967296), 8, '0'), '-0000-4000-8000-', LPAD(HEX(%d), 12, '0'))),%d)", i, i, i)
		})
		insertRows(t, ctx, c, "rc_vcm.days", 900, func(i int) string { return fmt.Sprintf("(DATE_ADD('2020-01-01', INTERVAL %d DAY),%d)", i, i) })
		myExec(t, ctx, c, "ANALYZE TABLE rc_vcm.items, rc_vcm.pairs, rc_vcm.uuids, rc_vcm.days")
	}

	// A text key that sorts differently on the two sides: case-insensitive on the
	// source, case-sensitive (binary) on the target. Ranges are cut in the source's
	// order, so rows land in different ranges on the target; the key-by-key re-check
	// must sort that out without reporting drift.
	myExec(t, ctx, src, "CREATE TABLE rc_vcm.names (name VARCHAR(20) COLLATE utf8mb4_general_ci PRIMARY KEY, n INT) ENGINE=InnoDB")
	myExec(t, ctx, tgt, "CREATE TABLE rc_vcm.names (name VARCHAR(20) COLLATE utf8mb4_bin PRIMARY KEY, n INT) ENGINE=InnoDB")
	for _, c := range []*sql.DB{src, tgt} {
		insertRows(t, ctx, c, "rc_vcm.names", 900, func(i int) string {
			if i%2 == 0 {
				return fmt.Sprintf("('Name%04d',%d)", i, i)
			}
			return fmt.Sprintf("('name%04d',%d)", i, i)
		})
	}

	cfg, err := config.Load(writeConfig(t, mysqlDaemonConfigYAML(`
syncs:
  - name: m1
    source: mysrc
    targets: [mydst]
    include: ["rc_vcm.*"]
`)))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	e := live.New(cfg, 20*time.Second)
	fast := live.VerifyOptions{ChunkRows: 300, Pause: -1, RecheckAfter: -1}

	// --- matching data: ok, chunked, every row covered, several ranges.
	vr := e.VerifyWith(ctx, "m1", fast)
	if vr.Error != "" || !vr.Converged {
		t.Fatalf("matching data: converged=%v err=%q %+v", vr.Converged, vr.Error, vr)
	}
	it := findVerifyTable(vr, "mydst", "rc_vcm.items")
	if it == nil || it.Status != "ok" || it.Mode != "chunked" || it.SourceRows != 2500 || it.TargetRows != 2500 || it.Chunks < 9 {
		t.Fatalf("items = %+v, want ok/chunked 2500/2500 over at least 9 ranges", it)
	}
	for _, name := range []string{"rc_vcm.pairs", "rc_vcm.uuids", "rc_vcm.days", "rc_vcm.names"} {
		if p := findVerifyTable(vr, "mydst", name); p == nil || p.Status != "ok" || p.Mode != "chunked" {
			t.Fatalf("%s = %+v, want ok/chunked", name, p)
		}
	}

	// --- drift: missing, extra (past the source's last key), changed; composite key.
	myExec(t, ctx, tgt,
		"DELETE FROM rc_vcm.items WHERE id = 1234",
		"INSERT INTO rc_vcm.items VALUES (9999, 'stray', 0)",
		"UPDATE rc_vcm.items SET note = 'changed' WHERE id = 42",
		"UPDATE rc_vcm.pairs SET val = -1 WHERE region = 'r3' AND code = 'c10'")
	vr = e.VerifyWith(ctx, "m1", fast)
	if vr.Converged {
		t.Fatalf("drift not caught: %+v", vr)
	}
	it = findVerifyTable(vr, "mydst", "rc_vcm.items")
	if it == nil || it.Status != "drift-count" || it.Missing != 1 || it.Extra != 1 || it.Different != 1 {
		t.Fatalf("items drift = %+v, want 1 missing, 1 extra, 1 different", it)
	}
	want := map[string]string{"1234": "missing", "9999": "extra", "42": "different"}
	for _, k := range it.Keys {
		if want[k.Key] != k.Problem {
			t.Fatalf("unexpected key %+v in %+v", k, it.Keys)
		}
		delete(want, k.Key)
	}
	if len(want) != 0 {
		t.Fatalf("keys not reported: %v (got %+v)", want, it.Keys)
	}
	p := findVerifyTable(vr, "mydst", "rc_vcm.pairs")
	if p == nil || p.Status != "drift-checksum" || p.Different != 1 || len(p.Keys) != 1 || p.Keys[0].Key != "r3,c10" {
		t.Fatalf("pairs drift = %+v, want one different row r3,c10", p)
	}

	// --- --table narrows the check.
	only := fast
	only.Tables = []string{"rc_vcm.pairs"}
	vr = e.VerifyWith(ctx, "m1", only)
	if len(vr.Targets) != 1 || len(vr.Targets[0].Tables) != 1 || vr.Targets[0].Tables[0].Table != "rc_vcm.pairs" {
		t.Fatalf("--table rc_vcm.pairs checked %+v", vr.Targets)
	}

	// --- a row fixed between the scan and the re-check is not reported.
	myExec(t, ctx, tgt,
		"INSERT INTO rc_vcm.items VALUES (1234, 'n1234', 1234.50)",
		"DELETE FROM rc_vcm.items WHERE id = 9999",
		"UPDATE rc_vcm.pairs SET val = 10 WHERE region = 'r3' AND code = 'c10'")
	healing := fast
	healing.RecheckAfter = 2 * time.Second
	healing.Tables = []string{"rc_vcm.items"}
	go func() {
		time.Sleep(500 * time.Millisecond)
		_, _ = tgt.Exec("UPDATE rc_vcm.items SET note = 'n42' WHERE id = 42")
	}()
	vr = e.VerifyWith(ctx, "m1", healing)
	if it = findVerifyTable(vr, "mydst", "rc_vcm.items"); it == nil || it.Status != "ok" {
		t.Fatalf("row fixed before the re-check still reported: %+v", it)
	}

	// --- sampled: integer, text and date keys each check part of the table.
	sampled := fast
	sampled.Sample = 3
	vr = e.VerifyWith(ctx, "m1", sampled)
	for _, name := range []string{"rc_vcm.items", "rc_vcm.uuids", "rc_vcm.days"} {
		s := findVerifyTable(vr, "mydst", name)
		if s == nil || s.Mode != "sampled" || s.Status != "ok" || s.Chunks < 1 || s.Chunks > 3 || s.SourceRows > 3*300 {
			t.Fatalf("sampled %s = %+v, want ok over 1-3 ranges of at most 300 rows each", name, s)
		}
	}
	// ...and a sample still catches a difference inside the ranges it checks.
	myExec(t, ctx, tgt, "UPDATE rc_vcm.items SET note = 'x'")
	vr = e.VerifyWith(ctx, "m1", sampled)
	if s := findVerifyTable(vr, "mydst", "rc_vcm.items"); s == nil || s.Status != "drift-checksum" {
		t.Fatalf("sampled after changing every row = %+v, want drift-checksum", s)
	}
}
