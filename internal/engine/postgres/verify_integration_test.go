package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

// TestVerifyFingerprintIntegration exercises engine.Verifier against the real
// multi-version harness (old source, modern target): CountRows and the
// order-independent content Fingerprint that `replicare verify`/`status` rely on.
// It proves (a) identical data on both ends yields identical checksums even across
// a major-version gap and a different physical column order, (b) a value change
// flips the checksum while the count holds, and (c) a delete moves the count. The
// md5→bit(64) idiom must work on the OLD source too (CLAUDE.md §1.6).
func TestVerifyFingerprintIntegration(t *testing.T) {
	cc := harnessConn(t, "source") // skips unless REPLICARE_INTEGRATION=1
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	src := &Source{cfg: cc}
	if err := src.Connect(ctx); err != nil {
		t.Fatalf("connect source: %v", err)
	}
	tgt := &Sink{cfg: harnessConn(t, "target")}
	if err := tgt.Connect(ctx); err != nil {
		t.Fatalf("connect target: %v", err)
	}

	mustExecSrc := func(sql string) {
		t.Helper()
		if _, err := src.conn.Exec(ctx, sql); err != nil {
			t.Fatalf("source exec %q: %v", sql, err)
		}
	}
	mustExecTgt := func(sql string) {
		t.Helper()
		if _, err := tgt.conn.Exec(ctx, sql); err != nil {
			t.Fatalf("target exec %q: %v", sql, err)
		}
	}

	// Cleanups run LIFO: register Close first so the schema-drop (registered next)
	// runs while the connection is still open, then the connection closes.
	t.Cleanup(func() { _ = src.Close(context.Background()) })
	t.Cleanup(func() { _, _ = src.conn.Exec(context.Background(), "DROP SCHEMA IF EXISTS rc_it CASCADE") })
	t.Cleanup(func() { _ = tgt.Close(context.Background()) })
	t.Cleanup(func() { _, _ = tgt.conn.Exec(context.Background(), "DROP SCHEMA IF EXISTS rc_it CASCADE") })

	mustExecSrc("DROP SCHEMA IF EXISTS rc_it CASCADE")
	mustExecSrc("CREATE SCHEMA rc_it")
	mustExecTgt("DROP SCHEMA IF EXISTS rc_it CASCADE")
	mustExecTgt("CREATE SCHEMA rc_it")

	// Source: canonical column order. Target: DIFFERENT physical column order, same
	// data — the fingerprint must still match (name-sorted projection, §4.2).
	mustExecSrc(`CREATE TABLE rc_it.t (id int PRIMARY KEY, name text, amt numeric, ts timestamptz, flag bool)`)
	mustExecTgt(`CREATE TABLE rc_it.t (flag bool, ts timestamptz, amt numeric, name text, id int PRIMARY KEY)`)

	rows := `
		(1, 'alice',  10.50, '2021-03-04 05:06:07+00', true),
		(2, 'bob',    -3.14, '1999-12-31 23:59:59+00', false),
		(3, NULL,     0,     NULL,                      NULL)`
	mustExecSrc(`INSERT INTO rc_it.t (id, name, amt, ts, flag) VALUES ` + rows)
	mustExecTgt(`INSERT INTO rc_it.t (id, name, amt, ts, flag) VALUES ` + rows)

	ref := engine.TableRef{Schema: "rc_it", Name: "t"}
	cols := []string{"id", "name", "amt", "ts", "flag"}

	sf, err := src.Fingerprint(ctx, ref, cols)
	if err != nil {
		t.Fatalf("source fingerprint: %v", err)
	}
	tf, err := tgt.Fingerprint(ctx, ref, cols)
	if err != nil {
		t.Fatalf("target fingerprint: %v", err)
	}
	if sf.Rows != 3 || tf.Rows != 3 {
		t.Fatalf("rows: src=%d tgt=%d, want 3/3", sf.Rows, tf.Rows)
	}
	if sf.Checksum == "" {
		t.Fatal("source checksum empty (expected a content hash)")
	}
	if sf.Checksum != tf.Checksum {
		t.Fatalf("converged data has differing checksums: src=%s tgt=%s", sf.Checksum, tf.Checksum)
	}

	// Value drift: change one row on the target -> checksum flips, count holds.
	mustExecTgt(`UPDATE rc_it.t SET name = 'BOB' WHERE id = 2`)
	tf2, err := tgt.Fingerprint(ctx, ref, cols)
	if err != nil {
		t.Fatalf("target fingerprint after update: %v", err)
	}
	if tf2.Rows != 3 {
		t.Errorf("row count changed on value drift: %d, want 3", tf2.Rows)
	}
	if tf2.Checksum == sf.Checksum {
		t.Error("checksum unchanged after a value drift")
	}

	// Count drift: delete a row.
	mustExecTgt(`DELETE FROM rc_it.t WHERE id = 3`)
	n, err := tgt.CountRows(ctx, ref)
	if err != nil {
		t.Fatalf("target count: %v", err)
	}
	if n != 2 {
		t.Errorf("target count after delete = %d, want 2", n)
	}
	if sn, err := src.CountRows(ctx, ref); err != nil || sn != 3 {
		t.Errorf("source count = %d err=%v, want 3", sn, err)
	}
}
