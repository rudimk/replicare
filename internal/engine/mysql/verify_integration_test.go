package mysql

import (
	"context"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

// TestVerifyFingerprintIntegration exercises engine.Verifier against the real
// MySQL harness (5.7 source, 8.4 target): CountRows and the order-independent
// content Fingerprint behind `replicare verify`/`status`. It proves identical data
// on both ends yields identical checksums across the version gap and a different
// physical column order, that a value change flips the checksum while the count
// holds, and that a delete moves the count. The exact-SUM (CAST … AS UNSIGNED)
// must not lose precision.
func TestVerifyFingerprintIntegration(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	src := connectSource(t, ctx) // creates+drops rc_it on the source
	sink := connectSink(t, ctx)
	mustExec(t, ctx, sink.db, "DROP DATABASE IF EXISTS rc_it", "CREATE DATABASE rc_it")
	t.Cleanup(func() { _, _ = sink.db.Exec("DROP DATABASE IF EXISTS rc_it") })

	// Source: canonical column order. Target: DIFFERENT physical order, same data.
	mustExec(t, ctx, src.db,
		"CREATE TABLE rc_it.t (id INT PRIMARY KEY, name VARCHAR(50), amt DECIMAL(12,2), ts DATETIME, flag TINYINT) ENGINE=InnoDB")
	mustExec(t, ctx, sink.db,
		"CREATE TABLE rc_it.t (flag TINYINT, ts DATETIME, amt DECIMAL(12,2), name VARCHAR(50), id INT PRIMARY KEY) ENGINE=InnoDB")

	insert := `INSERT INTO rc_it.t (id, name, amt, ts, flag) VALUES
		(1, 'alice', 10.50, '2021-03-04 05:06:07', 1),
		(2, 'bob',  -3.14, '1999-12-31 23:59:59', 0),
		(3, NULL,    0.00, NULL,                  NULL)`
	mustExec(t, ctx, src.db, insert)
	mustExec(t, ctx, sink.db, insert)

	ref := engine.TableRef{Schema: "rc_it", Name: "t"}
	cols := []string{"id", "name", "amt", "ts", "flag"}

	sf, err := src.Fingerprint(ctx, ref, cols)
	if err != nil {
		t.Fatalf("source fingerprint: %v", err)
	}
	tf, err := sink.Fingerprint(ctx, ref, cols)
	if err != nil {
		t.Fatalf("target fingerprint: %v", err)
	}
	if sf.Rows != 3 || tf.Rows != 3 {
		t.Fatalf("rows: src=%d tgt=%d, want 3/3", sf.Rows, tf.Rows)
	}
	if sf.Checksum == "" {
		t.Fatal("source checksum empty")
	}
	if sf.Checksum != tf.Checksum {
		t.Fatalf("converged data has differing checksums: src=%s tgt=%s", sf.Checksum, tf.Checksum)
	}

	// Value drift.
	mustExec(t, ctx, sink.db, "UPDATE rc_it.t SET name='BOB' WHERE id=2")
	tf2, err := sink.Fingerprint(ctx, ref, cols)
	if err != nil {
		t.Fatalf("target fingerprint after update: %v", err)
	}
	if tf2.Rows != 3 {
		t.Errorf("row count changed on value drift: %d, want 3", tf2.Rows)
	}
	if tf2.Checksum == sf.Checksum {
		t.Error("checksum unchanged after a value drift")
	}

	// Count drift.
	mustExec(t, ctx, sink.db, "DELETE FROM rc_it.t WHERE id=3")
	n, err := sink.CountRows(ctx, ref)
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
