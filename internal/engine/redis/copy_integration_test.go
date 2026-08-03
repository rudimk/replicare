package redis

import (
	"context"
	"io"
	"reflect"
	"sort"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/rudimk/replicare/internal/engine"
)

// copyUnit drives one initial-copy of the source unit into the sink exactly as the
// neutral copy layer does (PlanChunks -> CopyChunk piped to BulkLoad), but without
// a StateStore so the Redis integration job needs no Postgres. Returns keys loaded.
func copyUnit(t *testing.T, ctx context.Context, src *Source, sink *Sink) int64 {
	t.Helper()
	ref := unitRef(src.cfg)
	chunks, err := src.PlanChunks(ctx, ref, engine.ChunkOptions{})
	if err != nil {
		t.Fatalf("PlanChunks: %v", err)
	}
	if len(chunks) != 1 {
		t.Fatalf("PlanChunks returned %d chunks, want 1 (one atomic chunk per unit)", len(chunks))
	}
	pr, pw := io.Pipe()
	errc := make(chan error, 1)
	go func() {
		err := src.CopyChunk(ctx, chunks[0], pw)
		_ = pw.CloseWithError(err)
		errc <- err
	}()
	n, loadErr := sink.BulkLoad(ctx, ref, []string{"key"}, pr, engine.LoadDirect)
	_ = pr.CloseWithError(loadErr)
	if cErr := <-errc; cErr != nil {
		t.Fatalf("CopyChunk: %v", cErr)
	}
	if loadErr != nil {
		t.Fatalf("BulkLoad: %v", loadErr)
	}
	return n
}

func mustConnectPair(t *testing.T, ctx context.Context, sc, tc engine.ConnConfig) (*Source, *Sink) {
	t.Helper()
	src := &Source{cfg: sc}
	if err := src.Connect(ctx); err != nil {
		t.Fatalf("connect source: %v", err)
	}
	t.Cleanup(func() { _ = src.Close(context.Background()) })
	sink := &Sink{cfg: tc}
	if err := sink.Connect(ctx); err != nil {
		t.Fatalf("connect sink: %v", err)
	}
	t.Cleanup(func() { _ = sink.Close(context.Background()) })
	return src, sink
}

// TestInitialCopyCorpus is the RM4 keystone (6.2 -> 7.4 standalone): all six native
// types, a TTL, high-precision ZSET scores, binary values, and a STREAM WITH GROUPS
// AND PEL copy value-faithfully, verified by type-appropriate LOGICAL comparison
// (not DUMP-byte equality — RESTORE re-encodes across the version gap, Momus M7).
func TestInitialCopyCorpus(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	src, sink := mustConnectPair(t, ctx, srcCfg(), tgtCfg())
	sc, tc := src.db.uc, sink.db.uc
	if err := sc.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flush source: %v", err)
	}
	if err := tc.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flush target: %v", err)
	}

	// --- seed all six types + edge cases on the source ---
	binVal := string([]byte{0x00, 0xff, 0x10, 0x00, 0xfe, 0x01})
	must(t, sc.Set(ctx, "str:bin", binVal, 0).Err())
	must(t, sc.Set(ctx, "str:ttl", "expires", 0).Err())
	must(t, sc.PExpire(ctx, "str:ttl", 50*time.Second).Err())
	must(t, sc.RPush(ctx, "list:a", "x", "y", binVal, "z").Err())
	must(t, sc.SAdd(ctx, "set:a", "m1", "m2", binVal).Err())
	must(t, sc.HSet(ctx, "hash:a", "f1", "v1", "f2", binVal).Err())
	// High-precision ZSET scores (round-trippable double precision).
	must(t, sc.ZAdd(ctx, "zset:a",
		goredis.Z{Score: 3.141592653589793, Member: "pi"},
		goredis.Z{Score: 1e300, Member: "big"},
		goredis.Z{Score: -2.5, Member: "neg"}).Err())
	// STREAM with a consumer group and a pending (unacked) entry -> non-empty PEL.
	must(t, sc.XAdd(ctx, &goredis.XAddArgs{Stream: "stream:a", Values: map[string]any{"k": "v1"}}).Err())
	must(t, sc.XAdd(ctx, &goredis.XAddArgs{Stream: "stream:a", Values: map[string]any{"k": binVal}}).Err())
	must(t, sc.XGroupCreate(ctx, "stream:a", "g1", "0").Err())
	if _, err := sc.XReadGroup(ctx, &goredis.XReadGroupArgs{
		Group: "g1", Consumer: "c1", Count: 1, Streams: []string{"stream:a", ">"},
	}).Result(); err != nil {
		t.Fatalf("seed PEL: %v", err)
	}

	copyUnit(t, ctx, src, sink)

	// --- logical comparisons on the target ---
	if got, _ := tc.Get(ctx, "str:bin").Result(); got != binVal {
		t.Errorf("str:bin = %q, want %q", got, binVal)
	}
	if pttl := tc.PTTL(ctx, "str:ttl").Val(); pttl <= 0 || pttl > 50*time.Second {
		t.Errorf("str:ttl PTTL = %v, want (0, 50s]", pttl)
	}
	assertListEqual(t, ctx, sc, tc, "list:a")
	assertSetEqual(t, ctx, sc, tc, "set:a")
	assertHashEqual(t, ctx, sc, tc, "hash:a")
	assertZSetEqual(t, ctx, sc, tc, "zset:a")
	assertStreamEqual(t, ctx, sc, tc, "stream:a")

	// Stream group + PEL survived: g1 exists with one pending entry.
	groups, err := tc.XInfoGroups(ctx, "stream:a").Result()
	if err != nil || len(groups) != 1 {
		t.Fatalf("target XINFO GROUPS = %v (err %v), want 1 group", groups, err)
	}
	if groups[0].Name != "g1" || groups[0].Pending != 1 {
		t.Errorf("group = %+v, want name g1 pending 1", groups[0])
	}
}

// TestInitialCopySameVersionByteIdentity: within one server (same RDB version), a
// copied key's DUMP bytes are IDENTICAL to the source's — the strict binary-safety
// round-trip (RQ-6). Uses the 7.4 target as both endpoints across two logical DBs.
func TestInitialCopySameVersionByteIdentity(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srcC := tgtCfg()
	srcC.Database = "0"
	dstC := tgtCfg()
	dstC.Database = "1"
	src, sink := mustConnectPair(t, ctx, srcC, dstC)
	sc, tc := src.db.uc, sink.db.uc
	must(t, sc.FlushDB(ctx).Err())
	must(t, tc.FlushDB(ctx).Err())

	must(t, sc.Set(ctx, "k:str", string([]byte{0x00, 0xff, 0x00}), 0).Err())
	must(t, sc.ZAdd(ctx, "k:zset", goredis.Z{Score: 1e300, Member: "m"}).Err())

	copyUnit(t, ctx, src, sink)

	for _, key := range []string{"k:str", "k:zset"} {
		srcDump, err := sc.Dump(ctx, key).Result()
		if err != nil {
			t.Fatalf("source DUMP %s: %v", key, err)
		}
		dstDump, err := tc.Dump(ctx, key).Result()
		if err != nil {
			t.Fatalf("target DUMP %s: %v", key, err)
		}
		if srcDump != dstDump {
			t.Errorf("same-version DUMP bytes differ for %s (%d vs %d bytes)", key, len(srcDump), len(dstDump))
		}
	}
}

// TestInitialCopyRestartConverges: re-running the copy is idempotent (RESTORE
// REPLACE), and a key added between passes is picked up — the crash-restart model
// (a restart just re-SCANs from cursor 0).
func TestInitialCopyRestartConverges(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	src, sink := mustConnectPair(t, ctx, srcCfg(), tgtCfg())
	sc, tc := src.db.uc, sink.db.uc
	must(t, sc.FlushDB(ctx).Err())
	must(t, tc.FlushDB(ctx).Err())

	must(t, sc.Set(ctx, "k1", "v1", 0).Err())
	copyUnit(t, ctx, src, sink)

	// A key added after the first pass appears on the second (restart re-scans).
	must(t, sc.Set(ctx, "k2", "v2", 0).Err())
	// An overwrite converges to the latest value (idempotent REPLACE).
	must(t, sc.Set(ctx, "k1", "v1b", 0).Err())
	copyUnit(t, ctx, src, sink)

	if got, _ := tc.Get(ctx, "k1").Result(); got != "v1b" {
		t.Errorf("k1 = %q, want v1b (converged)", got)
	}
	if got, _ := tc.Get(ctx, "k2").Result(); got != "v2" {
		t.Errorf("k2 = %q, want v2 (picked up on restart)", got)
	}
}

// TestInitialCopyExpiredKeySkipped: a key that has already expired at the source is
// simply absent — never copied, no error.
func TestInitialCopyExpiredKeySkipped(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	src, sink := mustConnectPair(t, ctx, srcCfg(), tgtCfg())
	sc, tc := src.db.uc, sink.db.uc
	must(t, sc.FlushDB(ctx).Err())
	must(t, tc.FlushDB(ctx).Err())

	must(t, sc.Set(ctx, "live", "here", 0).Err())
	must(t, sc.Set(ctx, "gone", "bye", 0).Err())
	must(t, sc.PExpire(ctx, "gone", 20*time.Millisecond).Err())
	time.Sleep(100 * time.Millisecond) // let "gone" expire out of the keyspace

	copyUnit(t, ctx, src, sink)

	if got, _ := tc.Get(ctx, "live").Result(); got != "here" {
		t.Errorf("live = %q, want here", got)
	}
	if n, _ := tc.Exists(ctx, "gone").Result(); n != 0 {
		t.Errorf("expired key was copied (Exists=%d), want absent", n)
	}
}

// --- logical comparison helpers ---

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
}

func assertListEqual(t *testing.T, ctx context.Context, sc, tc goredis.Cmdable, key string) {
	t.Helper()
	s, _ := sc.LRange(ctx, key, 0, -1).Result()
	d, _ := tc.LRange(ctx, key, 0, -1).Result()
	if !reflect.DeepEqual(s, d) {
		t.Errorf("list %s mismatch:\n src %q\n dst %q", key, s, d)
	}
}

func assertSetEqual(t *testing.T, ctx context.Context, sc, tc goredis.Cmdable, key string) {
	t.Helper()
	s, _ := sc.SMembers(ctx, key).Result()
	d, _ := tc.SMembers(ctx, key).Result()
	sort.Strings(s)
	sort.Strings(d)
	if !reflect.DeepEqual(s, d) {
		t.Errorf("set %s mismatch:\n src %q\n dst %q", key, s, d)
	}
}

func assertHashEqual(t *testing.T, ctx context.Context, sc, tc goredis.Cmdable, key string) {
	t.Helper()
	s, _ := sc.HGetAll(ctx, key).Result()
	d, _ := tc.HGetAll(ctx, key).Result()
	if !reflect.DeepEqual(s, d) {
		t.Errorf("hash %s mismatch:\n src %v\n dst %v", key, s, d)
	}
}

func assertZSetEqual(t *testing.T, ctx context.Context, sc, tc goredis.Cmdable, key string) {
	t.Helper()
	s, _ := sc.ZRangeWithScores(ctx, key, 0, -1).Result()
	d, _ := tc.ZRangeWithScores(ctx, key, 0, -1).Result()
	if !reflect.DeepEqual(s, d) {
		t.Errorf("zset %s mismatch (scores must be exact):\n src %v\n dst %v", key, s, d)
	}
}

func assertStreamEqual(t *testing.T, ctx context.Context, sc, tc goredis.Cmdable, key string) {
	t.Helper()
	s, _ := sc.XRange(ctx, key, "-", "+").Result()
	d, _ := tc.XRange(ctx, key, "-", "+").Result()
	if !reflect.DeepEqual(s, d) {
		t.Errorf("stream %s entries mismatch:\n src %v\n dst %v", key, s, d)
	}
}
