package mysql

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/apply"
	"github.com/rudimk/replicare/internal/engine"
)

// MM9 hardening gate: fault tolerance, byte-fidelity breadth, and the
// local_infile requirement — the checks that make the MySQL engine shippable.

// TestLocalInfileRequiredHaltsLoud proves the MM9 requirement: when the target
// has LOAD DATA LOCAL INFILE disabled, a load halts LOUD with an actionable error
// BEFORE consuming any data (no partial copy), rather than failing cryptically
// mid-stream. v1 has no INSERT fallback (deferred).
func TestLocalInfileRequiredHaltsLoud(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	sink := connectSink(t, ctx)

	// Disable local_infile on the target for the duration of the test, then restore
	// it (the harness server is shared; -p 1 serializes, but the GLOBAL persists).
	mustExec(t, ctx, sink.db, "SET GLOBAL local_infile = 0")
	t.Cleanup(func() { _, _ = sink.db.Exec("SET GLOBAL local_infile = 1") })

	// A freshly-connected sink probes the (now-disabled) capability.
	off := &Sink{cfg: tgtCfg()}
	if err := off.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = off.Close(context.Background()) })
	if off.localInfile {
		t.Fatal("probe reported local_infile ON after SET GLOBAL local_infile=0")
	}

	mustExec(t, ctx, off.db, "DROP DATABASE IF EXISTS rc_it", "CREATE DATABASE rc_it",
		"CREATE TABLE rc_it.orders (id INT PRIMARY KEY, note VARCHAR(50)) ENGINE=InnoDB")
	t.Cleanup(func() { _, _ = off.db.Exec("DROP DATABASE IF EXISTS rc_it") })

	// A load must halt loud without consuming the reader.
	consumed := false
	r := readerFunc(func(p []byte) (int, error) { consumed = true; return 0, nil })
	_, err := off.BulkLoad(ctx, engine.TableRef{Schema: "rc_it", Name: "orders"}, []string{"id", "note"}, r, engine.LoadDirect)
	if !errors.Is(err, errLocalInfileRequired) {
		t.Fatalf("want errLocalInfileRequired, got %v", err)
	}
	if consumed {
		t.Error("reader was consumed before the loud halt — a partial copy could occur")
	}
	if !strings.Contains(err.Error(), "local_infile") {
		t.Errorf("error should name local_infile actionably: %v", err)
	}
}

type readerFunc func([]byte) (int, error)

func (f readerFunc) Read(p []byte) (int, error) { return f(p) }

// TestExpandedFidelityCorpus widens the byte-fidelity proof (MM9): a diverse
// corpus — high-precision DECIMAL, fractional DATETIME/TIME, a §0.4 ZERO DATE,
// boundary BIGINT, ENUM/SET, multi-byte and trailing-space text, empty-string vs
// NULL, and binary with NUL bytes — copies byte-identically source→target and the
// empty-string/NULL distinction survives.
func TestExpandedFidelityCorpus(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	src := connectSource(t, ctx)
	sink := connectSink(t, ctx)
	t.Cleanup(func() { _, _ = src.db.Exec("DROP DATABASE IF EXISTS replicare") })

	ddl := `CREATE TABLE rc_it.fid (
		id   INT PRIMARY KEY,
		num  DECIMAL(30,10),
		dt   DATETIME(6),
		d    DATE,
		tm   TIME(6),
		big  BIGINT,
		txt  VARCHAR(100) CHARACTER SET utf8mb4,
		bin  VARBINARY(64),
		en   ENUM('a','b','c') CHARACTER SET utf8mb4,
		st   SET('x','y','z') CHARACTER SET utf8mb4
	) ENGINE=InnoDB`
	mustExec(t, ctx, src.db, ddl)
	mustExec(t, ctx, sink.db, "DROP DATABASE IF EXISTS rc_it", "CREATE DATABASE rc_it", ddl)
	t.Cleanup(func() { _, _ = sink.db.Exec("DROP DATABASE IF EXISTS rc_it") })

	ref := engine.TableRef{Schema: "rc_it", Name: "fid"}
	if err := src.InstallCapture(ctx, []engine.TableRef{ref}); err != nil {
		t.Fatalf("install capture: %v", err)
	}

	// Rows chosen to stress fidelity edges.
	mustExec(t, ctx, src.db,
		// high-precision decimal, fractional datetime/time, boundary bigint, emoji.
		"INSERT INTO rc_it.fid VALUES (1, -12345678901234567890.0123456789, '2021-03-04 05:06:07.891011', '2021-03-04', '12:34:56.789012', 9223372036854775807, 'héllo 😀 x', UNHEX('00FF00'), 'b', 'x,z')",
		// §0.4 zero-date (permitted verbatim under the strict-safe sql_mode), empty
		// string (NOT null), min bigint, NUL-containing binary.
		"INSERT INTO rc_it.fid VALUES (2, 0.0, '2020-01-01 00:00:00.000000', '0000-00-00', '00:00:00.000000', -9223372036854775808, '', UNHEX('0000'), 'a', '')",
		// trailing space kept, NULLs across nullable columns.
		"INSERT INTO rc_it.fid VALUES (3, NULL, NULL, NULL, NULL, NULL, 'trailing   ', NULL, NULL, NULL)",
	)

	if _, err := apply.DrainAll(ctx, src, sink, ref, "dst", 100); err != nil {
		t.Fatalf("drain: %v", err)
	}

	// Byte-identical, row-by-row (dumpRows scans raw bytes; NULL renders <null>).
	q := "SELECT id, num, dt, d, tm, big, txt, HEX(bin), en, st FROM rc_it.fid ORDER BY id"
	want := dumpRows(t, ctx, src.db, q)
	got := dumpRows(t, ctx, sink.db, q)
	if len(want) != len(got) {
		t.Fatalf("row count: source=%d target=%d", len(want), len(got))
	}
	for i := range want {
		if want[i] != got[i] {
			t.Fatalf("row %d differs:\n source=%q\n target=%q", i, want[i], got[i])
		}
	}

	// The empty-string vs NULL distinction must survive (row 2 txt='' , row 3 txt set).
	var emptyIsNull, emptyLen int
	_ = sink.db.QueryRowContext(ctx, "SELECT txt IS NULL, CHAR_LENGTH(txt) FROM rc_it.fid WHERE id=2").Scan(&emptyIsNull, &emptyLen)
	if emptyIsNull != 0 || emptyLen != 0 {
		t.Errorf("empty string collapsed: id=2 txt IS NULL=%d len=%d, want 0/0", emptyIsNull, emptyLen)
	}
	var nullIsNull int
	_ = sink.db.QueryRowContext(ctx, "SELECT num IS NULL FROM rc_it.fid WHERE id=3").Scan(&nullIsNull)
	if nullIsNull != 1 {
		t.Errorf("NULL not preserved: id=3 num IS NULL=%d, want 1", nullIsNull)
	}
}

// TestHistoryListStallLogicalPurge proves replicare's LOGICAL source-footprint
// bounding is robust to the InnoDB history-list stall (the MySQL xmin-horizon
// analog, §3.4): a long-running source transaction blocks InnoDB from physically
// reclaiming purged rows, but replicare's consumption-gated DELETE still drains
// the logical backlog to zero — the physical bloat is surfaced (DeltaBacklog.Bytes),
// never a correctness failure.
func TestHistoryListStallLogicalPurge(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	src := connectSource(t, ctx)
	sink := connectSink(t, ctx)
	t.Cleanup(func() { _, _ = src.db.Exec("DROP DATABASE IF EXISTS replicare") })

	ddl := "CREATE TABLE rc_it.orders (id INT PRIMARY KEY, note VARCHAR(50)) ENGINE=InnoDB"
	mustExec(t, ctx, src.db, ddl)
	mustExec(t, ctx, sink.db, "DROP DATABASE IF EXISTS rc_it", "CREATE DATABASE rc_it", ddl)
	t.Cleanup(func() { _, _ = sink.db.Exec("DROP DATABASE IF EXISTS rc_it") })

	ref := engine.TableRef{Schema: "rc_it", Name: "orders"}
	if err := src.InstallCapture(ctx, []engine.TableRef{ref}); err != nil {
		t.Fatalf("install capture: %v", err)
	}

	// Open a long-running READ transaction on a SEPARATE connection: it holds a
	// read view, so InnoDB cannot purge undo — the history-list stall.
	stall := rawConn(t, ctx)
	stallTx, err := stall.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin stall tx: %v", err)
	}
	if _, err := stallTx.ExecContext(ctx, "SELECT 1"); err != nil { // materialize the read view
		t.Fatalf("stall read: %v", err)
	}
	defer func() { _ = stallTx.Rollback() }()

	// Churn deltas while the stall is held, then drain to the target.
	mustExec(t, ctx, src.db, "INSERT INTO rc_it.orders SELECT n, CONCAT('v', n) FROM "+
		"(SELECT 1 n UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) s",
		"UPDATE rc_it.orders SET note = CONCAT(note, '-u') WHERE id <= 3",
		"DELETE FROM rc_it.orders WHERE id = 5")
	if _, err := apply.DrainAll(ctx, src, sink, ref, "dst", 100); err != nil {
		t.Fatalf("drain under stall: %v", err)
	}

	// The consumed deltas purge LOGICALLY even under the stall.
	ps, err := src.Purge(ctx, ref, []engine.TargetID{"dst"}, engine.RetentionPolicy{})
	if err != nil {
		t.Fatalf("purge under stall: %v", err)
	}
	if ps.DeltasPurged == 0 {
		t.Error("no deltas purged under the history-list stall — logical bounding failed")
	}
	bl, err := src.DeltaBacklog(ctx, ref, "dst")
	if err != nil {
		t.Fatalf("backlog: %v", err)
	}
	if bl.Rows != 0 || bl.HasBacklog {
		t.Errorf("logical backlog not drained under stall: %+v", bl)
	}
	// Bytes is the surfaced physical footprint (may be non-zero while the stall
	// pins undo); its value is informational, not asserted.
	t.Logf("post-purge backlog under stall: rows=%d bytes=%d (bytes surfaces physical bloat)", bl.Rows, bl.Bytes)
}

// TestKillMidInstallRerunConverges proves crash-safe, idempotent capture install
// despite MySQL auto-committing DDL (Momus M1): a partially-installed capture (one
// trigger missing, as a crash mid-install would leave) is repaired by simply
// re-running InstallCapture, and capture then works.
func TestKillMidInstallRerunConverges(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	src := connectSource(t, ctx)
	t.Cleanup(func() { _, _ = src.db.Exec("DROP DATABASE IF EXISTS replicare") })

	mustExec(t, ctx, src.db, "CREATE TABLE rc_it.orders (id INT PRIMARY KEY, note VARCHAR(50)) ENGINE=InnoDB")
	ref := engine.TableRef{Schema: "rc_it", Name: "orders"}
	if err := src.InstallCapture(ctx, []engine.TableRef{ref}); err != nil {
		t.Fatalf("install capture: %v", err)
	}

	// Simulate a crash mid-install: drop one of the three capture triggers so the
	// on-disk state is partial.
	var name string
	if err := src.db.QueryRowContext(ctx,
		"SELECT TRIGGER_NAME FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA='rc_it' AND EVENT_OBJECT_TABLE='orders' AND EVENT_MANIPULATION='UPDATE' LIMIT 1").
		Scan(&name); err != nil {
		t.Fatalf("find trigger: %v", err)
	}
	mustExec(t, ctx, src.db, "DROP TRIGGER rc_it."+bq(name))
	if n := triggerCount(t, ctx, src); n != 2 {
		t.Fatalf("after dropping one trigger, count=%d want 2", n)
	}

	// Re-run install: idempotent DROP-then-CREATE restores the missing trigger
	// (and does not block on its own surviving triggers).
	if err := src.InstallCapture(ctx, []engine.TableRef{ref}); err != nil {
		t.Fatalf("re-install after partial: %v", err)
	}
	if n := triggerCount(t, ctx, src); n != 3 {
		t.Fatalf("after re-install, trigger count=%d want 3", n)
	}

	// Capture works: an UPDATE (the repaired trigger's event) enqueues a delta.
	mustExec(t, ctx, src.db, "INSERT INTO rc_it.orders VALUES (1,'a')", "UPDATE rc_it.orders SET note='b' WHERE id=1")
	dirty, err := src.ReadDirtyKeys(ctx, ref, "dst", 100)
	if err != nil {
		t.Fatalf("read dirty: %v", err)
	}
	if len(dirty) == 0 {
		t.Error("no deltas after re-install — the repaired trigger is not capturing")
	}
}

// TestApplyThroughputSmoke records a coarse apply-throughput number for the MySQL
// streaming hot path (re-read -> LOAD DATA staging -> upsert) and asserts only a
// generous sanity floor. Perf is correctness-first here (mysql-plan §MM9): the
// number is recorded, not a tight CI gate — MySQL integration runs locally, and a
// hard threshold under docker/emulation would be flaky. The local_infile-OFF
// "fallback bound" is N/A: v1 requires local_infile (no fallback; see MM9).
func TestApplyThroughputSmoke(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	src := connectSource(t, ctx)
	sink := connectSink(t, ctx)
	t.Cleanup(func() { _, _ = src.db.Exec("DROP DATABASE IF EXISTS replicare") })

	ddl := "CREATE TABLE rc_it.orders (id INT PRIMARY KEY, note VARCHAR(64)) ENGINE=InnoDB"
	mustExec(t, ctx, src.db, ddl)
	mustExec(t, ctx, sink.db, "DROP DATABASE IF EXISTS rc_it", "CREATE DATABASE rc_it", ddl)
	t.Cleanup(func() { _, _ = sink.db.Exec("DROP DATABASE IF EXISTS rc_it") })

	ref := engine.TableRef{Schema: "rc_it", Name: "orders"}
	if err := src.InstallCapture(ctx, []engine.TableRef{ref}); err != nil {
		t.Fatalf("install capture: %v", err)
	}
	// Seed a few thousand post-capture rows (each becomes a dirty delta).
	const rows = 5000
	mustExec(t, ctx, src.db, "INSERT INTO rc_it.orders SELECT id, CONCAT('v', id) FROM "+numbers(rows))

	start := time.Now()
	n, err := apply.DrainAll(ctx, src, sink, ref, "dst", 1000)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if n != rows {
		t.Fatalf("consumed %d, want %d", n, rows)
	}
	if got := countRows(t, ctx, sink, "rc_it.orders"); got != rows {
		t.Fatalf("target has %d rows, want %d", got, rows)
	}
	rps := float64(rows) / elapsed.Seconds()
	t.Logf("apply throughput: %d rows in %s = %.0f rows/s", rows, elapsed.Round(time.Millisecond), rps)
	if rps < 50 { // generous floor: only catches a gross regression / stall
		t.Errorf("apply throughput %.0f rows/s is implausibly low (floor 50)", rps)
	}
}

// numbers builds "(SELECT id FROM (<digit cross-join>) nums WHERE id <= n) s" —
// ids 1..n from four crossed 0..9 digit tables (0..9999), a 5.7-portable
// generate_series stand-in for bulk seeding (n <= 10000).
func numbers(n int) string {
	d := "(SELECT 0 v UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 " +
		"UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9)"
	return fmt.Sprintf("(SELECT id FROM (SELECT a.v + b.v*10 + c.v*100 + d.v*1000 + 1 AS id FROM "+
		"%[1]s a CROSS JOIN %[1]s b CROSS JOIN %[1]s c CROSS JOIN %[1]s d) nums WHERE id <= %d) s", d, n)
}

func triggerCount(t *testing.T, ctx context.Context, src *Source) int {
	t.Helper()
	var n int
	if err := src.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA='rc_it' AND EVENT_OBJECT_TABLE='orders'").Scan(&n); err != nil {
		t.Fatalf("count triggers: %v", err)
	}
	return n
}
