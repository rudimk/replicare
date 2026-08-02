package mysql

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/apply"
	"github.com/rudimk/replicare/internal/engine"
)

// rawConn opens a second raw connection to the source (for uncommitted-txn tests).
func rawConn(t *testing.T, ctx context.Context) *sql.DB {
	t.Helper()
	d, err := dsn(srcCfg())
	if err != nil {
		t.Fatalf("dsn: %v", err)
	}
	db, err := sql.Open("mysql", d)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func dumpOrders(t *testing.T, ctx context.Context, db *sql.DB) []string {
	t.Helper()
	rows, err := db.QueryContext(ctx, "SELECT id, IFNULL(note,'<null>') FROM rc_it.orders ORDER BY id")
	if err != nil {
		t.Fatalf("dump: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id int
		var note string
		_ = rows.Scan(&id, &note)
		out = append(out, note)
	}
	return out
}

// TestStreamConverges is the MM5a keystone end-to-end: capture-first, then
// insert/update/delete/PK-change on the source converge on the target through the
// neutral drain (ReadDirtyKeys -> RereadCurrent -> ApplyPass -> ConfirmConsumed).
// Coalescing is covered — multiple updates to one PK re-read once to the final
// value.
func TestStreamConverges(t *testing.T) {
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

	// All changes after capture is installed are captured; target starts empty.
	mustExec(t, ctx, src.db,
		"INSERT INTO rc_it.orders VALUES (1,'a'),(2,'b'),(3,'c')",
		"UPDATE rc_it.orders SET note='b1' WHERE id=2",
		"UPDATE rc_it.orders SET note='b2' WHERE id=2", // coalesces to final value
		"DELETE FROM rc_it.orders WHERE id=3",
		"UPDATE rc_it.orders SET id=4 WHERE id=1", // PK change: delete(1) + upsert(4)
	)

	if _, err := apply.DrainAll(ctx, src, sink, ref, "dst", 100); err != nil {
		t.Fatalf("drain: %v", err)
	}

	got := dumpOrders(t, ctx, sink.db)
	want := dumpOrders(t, ctx, src.db)
	if len(got) != len(want) {
		t.Fatalf("row count: target %d, source %d (target=%v source=%v)", len(got), len(want), got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("row %d differs: target %q source %q (target=%v source=%v)", i, got[i], want[i], got, want)
		}
	}
	// Concretely: id2 coalesced to b2, id3 deleted, id1->id4.
	if want[0] != "b2" && want[1] != "b2" { // b2 present somewhere
		t.Errorf("coalesced value b2 missing: %v", want)
	}
}

// TestCommitOrderHazard proves the dirty-key-set model defeats the commit-order
// hazard (§3.3): a delta row from an UNCOMMITTED transaction is not consumed;
// once committed, a later pass applies it.
func TestCommitOrderHazard(t *testing.T) {
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

	// Open an uncommitted transaction that inserts a row (its delta row exists but
	// is invisible until commit).
	raw := rawConn(t, ctx)
	tx, err := raw.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin raw tx: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO rc_it.orders VALUES (1,'pending')"); err != nil {
		t.Fatalf("insert in tx: %v", err)
	}

	// Drain now: the uncommitted row must NOT be consumed/applied.
	if _, err := apply.DrainAll(ctx, src, sink, ref, "dst", 100); err != nil {
		t.Fatalf("drain 1: %v", err)
	}
	if n := len(dumpOrders(t, ctx, sink.db)); n != 0 {
		t.Fatalf("uncommitted row leaked to target: %d rows", n)
	}

	// Commit, then drain: now it applies.
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if _, err := apply.DrainAll(ctx, src, sink, ref, "dst", 100); err != nil {
		t.Fatalf("drain 2: %v", err)
	}
	if n := len(dumpOrders(t, ctx, sink.db)); n != 1 {
		t.Errorf("after commit+drain, target has %d rows, want 1", n)
	}
}

// TestApplyPreservesOnUpdateTimestamp proves an ON UPDATE CURRENT_TIMESTAMP
// column lands the verbatim SOURCE value, not the target's apply-time now()
// (§0.4/Momus M2).
func TestApplyPreservesOnUpdateTimestamp(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	src := connectSource(t, ctx)
	sink := connectSink(t, ctx)
	t.Cleanup(func() { _, _ = src.db.Exec("DROP DATABASE IF EXISTS replicare") })

	ddl := "CREATE TABLE rc_it.ev (id INT PRIMARY KEY, ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, n INT) ENGINE=InnoDB"
	mustExec(t, ctx, src.db, ddl)
	mustExec(t, ctx, sink.db, "DROP DATABASE IF EXISTS rc_it", "CREATE DATABASE rc_it", ddl)
	t.Cleanup(func() { _, _ = sink.db.Exec("DROP DATABASE IF EXISTS rc_it") })

	ref := engine.TableRef{Schema: "rc_it", Name: "ev"}
	if err := src.InstallCapture(ctx, []engine.TableRef{ref}); err != nil {
		t.Fatalf("install capture: %v", err)
	}
	// Insert with a fixed old timestamp so a target now() would obviously differ.
	mustExec(t, ctx, src.db, "INSERT INTO rc_it.ev (id, ts, n) VALUES (1, '2020-01-01 00:00:00', 5)")
	time.Sleep(1100 * time.Millisecond) // ensure "now" != the source value

	if _, err := apply.DrainAll(ctx, src, sink, ref, "dst", 100); err != nil {
		t.Fatalf("drain: %v", err)
	}
	var srcTs, tgtTs string
	_ = src.db.QueryRowContext(ctx, "SELECT ts FROM rc_it.ev WHERE id=1").Scan(&srcTs)
	_ = sink.db.QueryRowContext(ctx, "SELECT ts FROM rc_it.ev WHERE id=1").Scan(&tgtTs)
	if tgtTs != srcTs {
		t.Errorf("ON UPDATE timestamp not verbatim: source %q, target %q (auto-mutated to now?)", srcTs, tgtTs)
	}
}
