package redis

import (
	"context"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

// TestVerifyKeysetIntegration exercises engine.Verifier against the real Redis
// harness (6.2 -> 7.4 standalone): CountRows and the key-set membership Fingerprint
// behind `replicare verify`/`status`. It proves an identical key set on both ends
// yields identical count+checksum across the version gap, that a delete moves both,
// and that a same-count but different key SET is caught by the checksum (missing vs
// extra keys — the dominant Redis divergence, redis-plan §0.4).
func TestVerifyKeysetIntegration(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	src, sink := mustConnectPair(t, ctx, srcCfg(), tgtCfg())
	sc, tc := src.db.uc, sink.db.uc
	if err := sc.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flush source: %v", err)
	}
	if err := tc.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flush target: %v", err)
	}

	keys := []string{"a", "b", "c", "d", "e"}
	for _, k := range keys {
		must(t, sc.Set(ctx, k, "v-"+k, 0).Err())
		must(t, tc.Set(ctx, k, "v-"+k, 0).Err())
	}

	ref := unitRef(src.cfg)

	sf, err := src.Fingerprint(ctx, ref, nil)
	if err != nil {
		t.Fatalf("source fingerprint: %v", err)
	}
	tf, err := sink.Fingerprint(ctx, ref, nil)
	if err != nil {
		t.Fatalf("target fingerprint: %v", err)
	}
	if sf.Rows != 5 || tf.Rows != 5 {
		t.Fatalf("counts: src=%d tgt=%d, want 5/5", sf.Rows, tf.Rows)
	}
	if sf.Checksum == "" || sf.Checksum != tf.Checksum {
		t.Fatalf("identical key sets differ: src=%s tgt=%s", sf.Checksum, tf.Checksum)
	}

	// Delete a key on the target -> count and checksum both drift.
	must(t, tc.Del(ctx, "c").Err())
	tf2, err := sink.Fingerprint(ctx, ref, nil)
	if err != nil {
		t.Fatalf("target fingerprint after del: %v", err)
	}
	if tf2.Rows != 4 {
		t.Errorf("target count after del = %d, want 4", tf2.Rows)
	}
	if tf2.Checksum == sf.Checksum {
		t.Error("checksum unchanged after a missing key")
	}

	// Restore count with a DIFFERENT key (extra on target, missing 'c'): same count,
	// different key SET -> the membership checksum must still catch it.
	must(t, tc.Set(ctx, "zzz", "extra", 0).Err())
	tf3, err := sink.Fingerprint(ctx, ref, nil)
	if err != nil {
		t.Fatalf("target fingerprint after swap: %v", err)
	}
	if tf3.Rows != sf.Rows {
		t.Fatalf("counts realigned to %d, want %d for the set-membership check", tf3.Rows, sf.Rows)
	}
	if tf3.Checksum == sf.Checksum {
		t.Error("equal counts but different key set not detected by checksum")
	}

	// Selection: only 'keep:*' keys should be counted after introspecting a glob.
	must(t, sc.Set(ctx, "keep:1", "x", 0).Err())
	must(t, sc.Set(ctx, "keep:2", "x", 0).Err())
	must(t, sc.Set(ctx, "skip:1", "x", 0).Err())
	selSrc := &Source{cfg: srcCfg()}
	if err := selSrc.Connect(ctx); err != nil {
		t.Fatalf("connect sel source: %v", err)
	}
	t.Cleanup(func() { _ = selSrc.Close(context.Background()) })
	if _, err := selSrc.Introspect(ctx, engine.Selection{Include: []string{"keep:*"}}); err != nil {
		t.Fatalf("introspect sel: %v", err)
	}
	n, err := selSrc.CountRows(ctx, ref)
	if err != nil {
		t.Fatalf("sel count: %v", err)
	}
	if n != 2 {
		t.Errorf("selected count = %d, want 2 (keep:* only)", n)
	}
}
