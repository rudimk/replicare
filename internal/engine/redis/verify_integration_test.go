package redis

import (
	"context"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

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

// TestVerifyValueDriftIntegration proves the content fingerprint catches same-key
// VALUE drift — the case a key-set-only hash misses (a SET/HSET/etc. that changes a
// value without changing the key set). For each native type it seeds identical
// key+value on both ends (checksum matches across the 6.2->7.4 version gap), mutates
// only the target's value, and asserts the count is unchanged but the checksum drifts.
func TestVerifyValueDriftIntegration(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	src, sink := mustConnectPair(t, ctx, srcCfg(), tgtCfg())
	sc, tc := src.db.uc, sink.db.uc
	must(t, sc.FlushDB(ctx).Err())
	must(t, tc.FlushDB(ctx).Err())
	ref := unitRef(src.cfg)

	// Seed one key of each native type, identical on both ends.
	seed := func(c goredis.Cmdable) {
		must(t, c.Set(ctx, "s", "v1", 0).Err())
		must(t, c.HSet(ctx, "h", "f", "1", "g", "2").Err())
		must(t, c.RPush(ctx, "l", "a", "b", "c").Err())
		must(t, c.SAdd(ctx, "st", "x", "y", "z").Err())
		must(t, c.ZAdd(ctx, "z", goredis.Z{Score: 1, Member: "m1"}, goredis.Z{Score: 2, Member: "m2"}).Err())
	}
	seed(sc)
	seed(tc)

	sf, err := src.Fingerprint(ctx, ref, nil)
	must(t, err)
	tf, err := sink.Fingerprint(ctx, ref, nil)
	must(t, err)
	if sf.Rows != 5 || tf.Rows != 5 {
		t.Fatalf("counts: src=%d tgt=%d, want 5/5", sf.Rows, tf.Rows)
	}
	if sf.Checksum != tf.Checksum {
		t.Fatalf("identical content differs across version gap: src=%s tgt=%s", sf.Checksum, tf.Checksum)
	}

	// Mutate only the TARGET's value for each type (key set unchanged) and assert drift.
	cases := []struct {
		name   string
		mutate func()
	}{
		{"string", func() { must(t, tc.Set(ctx, "s", "v2", 0).Err()) }},
		{"hash", func() { must(t, tc.HSet(ctx, "h", "f", "999").Err()) }},
		{"list", func() { must(t, tc.RPush(ctx, "l", "d").Err()) }},
		{"set", func() { must(t, tc.SAdd(ctx, "st", "w").Err()) }},
		{"zset", func() { must(t, tc.ZAdd(ctx, "z", goredis.Z{Score: 5, Member: "m1"}).Err()) }},
	}
	for _, tcase := range cases {
		// Re-seed the target to the identical baseline before each case.
		must(t, tc.FlushDB(ctx).Err())
		seed(tc)
		base, err := sink.Fingerprint(ctx, ref, nil)
		must(t, err)
		if base.Checksum != sf.Checksum {
			t.Fatalf("%s: baseline re-seed did not match source: %s vs %s", tcase.name, base.Checksum, sf.Checksum)
		}
		tcase.mutate()
		got, err := sink.Fingerprint(ctx, ref, nil)
		must(t, err)
		if got.Rows != sf.Rows {
			t.Errorf("%s: count changed to %d (want %d) — value drift should not change the key count", tcase.name, got.Rows, sf.Rows)
		}
		if got.Checksum == sf.Checksum {
			t.Errorf("%s: value drift NOT detected — checksum unchanged after mutating the target's value", tcase.name)
		}
	}
}
