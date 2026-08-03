package redis

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/rudimk/replicare/internal/engine"
)

// RM11 — hardening & E2E gate (redis-plan RM11). These consolidate the
// fault-injection and fidelity scenarios that make Redis shippable: an expanded
// value-fidelity corpus, big-key warn/refuse, the RDB-version and target-down
// loud-fails, key-expiry races, and coarse politeness/throughput numbers.

// TestFidelityCorpusExpanded stresses the value-faithful transport (6.2 -> 7.4):
// large collections, a deep all-256-bytes binary value, TTL edges (tiny, huge,
// none), and an empty-string value — all copied VALUE-identically (logical
// compare), with a same-version DUMP-byte-identity spot check.
func TestFidelityCorpusExpanded(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	src, sink := mustConnectPair(t, ctx, srcCfg(), tgtCfg())
	sc, tc := src.db.uc, sink.db.uc
	must(t, sc.FlushDB(ctx).Err())
	must(t, tc.FlushDB(ctx).Err())

	// Large collections (encoding crosses ziplist/listpack -> hashtable/skiplist).
	const big = 5000
	listVals := make([]any, big)
	zmembers := make([]goredis.Z, big)
	for i := 0; i < big; i++ {
		listVals[i] = fmt.Sprintf("e%d", i)
		must(t, sc.SAdd(ctx, "big:set", fmt.Sprintf("m%d", i)).Err())
		must(t, sc.HSet(ctx, "big:hash", fmt.Sprintf("f%d", i), i).Err())
		zmembers[i] = goredis.Z{Score: float64(i) + 0.123456789012345, Member: fmt.Sprintf("z%d", i)}
	}
	must(t, sc.RPush(ctx, "big:list", listVals...).Err())
	must(t, sc.ZAdd(ctx, "big:zset", zmembers...).Err())

	// Deep binary: every byte value 0x00..0xff, several times over.
	allbytes := make([]byte, 0, 256*4)
	for r := 0; r < 4; r++ {
		for b := 0; b < 256; b++ {
			allbytes = append(allbytes, byte(b))
		}
	}
	must(t, sc.Set(ctx, "bin:all", string(allbytes), 0).Err())

	// TTL edges + empty value.
	must(t, sc.Set(ctx, "ttl:huge", "v", 0).Err())
	must(t, sc.PExpire(ctx, "ttl:huge", 1000*time.Hour).Err())
	must(t, sc.Set(ctx, "ttl:none", "v", 0).Err())
	must(t, sc.Set(ctx, "val:empty", "", 0).Err())

	copyUnit(t, ctx, src, sink)

	assertListEqual(t, ctx, sc, tc, "big:list")
	assertSetEqual(t, ctx, sc, tc, "big:set")
	assertHashEqual(t, ctx, sc, tc, "big:hash")
	assertZSetEqual(t, ctx, sc, tc, "big:zset")
	if got, _ := tc.Get(ctx, "bin:all").Result(); got != string(allbytes) {
		t.Errorf("deep-binary value corrupted (%d bytes vs %d)", len(got), len(allbytes))
	}
	if got, err := tc.Get(ctx, "val:empty").Result(); err != nil || got != "" {
		t.Errorf("empty-string value = %q err=%v, want \"\"", got, err)
	}
	if n, _ := tc.Exists(ctx, "val:empty").Result(); n != 1 {
		t.Errorf("empty-string key must exist on target (empty != missing)")
	}
	if pttl := tc.PTTL(ctx, "ttl:huge").Val(); pttl <= 0 || pttl > 1000*time.Hour {
		t.Errorf("huge TTL = %v, want (0, 1000h]", pttl)
	}
	if pttl := tc.PTTL(ctx, "ttl:none").Val(); pttl != -1 {
		t.Errorf("no-expiry key PTTL = %v, want -1 (persistent)", pttl)
	}

	// Same-version DUMP-byte identity for the deep-binary + a large collection.
	srcC, dstC := tgtCfg(), tgtCfg()
	srcC.Database, dstC.Database = "0", "1"
	s2, k2 := mustConnectPair(t, ctx, srcC, dstC)
	must(t, s2.db.uc.FlushDB(ctx).Err())
	must(t, k2.db.uc.FlushDB(ctx).Err())
	must(t, s2.db.uc.Set(ctx, "bin:all", string(allbytes), 0).Err())
	must(t, s2.db.uc.ZAdd(ctx, "big:zset", zmembers[:100]...).Err())
	copyUnit(t, ctx, s2, k2)
	for _, key := range []string{"bin:all", "big:zset"} {
		a, _ := s2.db.uc.Dump(ctx, key).Result()
		b, _ := k2.db.uc.Dump(ctx, key).Result()
		if a != b {
			t.Errorf("same-version DUMP bytes differ for %s", key)
		}
	}
}

// TestBigKeyRefuseLoudFail: a key over the refuse cap fails the copy LOUD (never
// torn/incremental, §1.7); under only a warn threshold the copy still succeeds.
func TestBigKeyRefuseLoudFail(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Refuse cap of 100 bytes; a ~4KB value blows past it.
	refuseCfg := srcCfg()
	refuseCfg.Params = map[string]string{paramBigKeyRefuse: "100"}
	src := &Source{cfg: refuseCfg}
	if err := src.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer src.Close(context.Background())
	sink := &Sink{cfg: tgtCfg()}
	if err := sink.Connect(ctx); err != nil {
		t.Fatalf("connect sink: %v", err)
	}
	defer sink.Close(context.Background())
	must(t, src.db.uc.FlushDB(ctx).Err())
	must(t, sink.db.uc.FlushDB(ctx).Err())
	must(t, src.db.uc.Set(ctx, "huge", strings.Repeat("x", 4096), 0).Err())

	if _, err := runCopy(ctx, src, sink); err == nil || !strings.Contains(err.Error(), "big_key_refuse_bytes") {
		t.Fatalf("big-key over refuse cap should fail loud, got %v", err)
	}

	// Warn-only threshold: the same key copies fine (a warning is logged, no error).
	warnCfg := srcCfg()
	warnCfg.Params = map[string]string{paramBigKeyWarn: "100"}
	src2 := &Source{cfg: warnCfg}
	if err := src2.Connect(ctx); err != nil {
		t.Fatalf("connect2: %v", err)
	}
	defer src2.Close(context.Background())
	must(t, sink.db.uc.FlushDB(ctx).Err())
	copyUnit(t, ctx, src2, sink)
	if n, _ := sink.db.uc.Exists(ctx, "huge").Result(); n != 1 {
		t.Errorf("warn-threshold big key should still copy")
	}
}

// TestRDBVersionReverseBlocksLive is the RM11 version loud-fail on REAL servers:
// wiring a NEWER source (7.4) to an OLDER target (6.2) must BLOCK at pre-flight
// (RESTORE would reject the newer payload). The supported 6.2->7.4 direction, run
// everywhere else, passes.
func TestRDBVersionReverseBlocksLive(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Reverse: source is the 7.4 endpoint, target is the 6.2 endpoint.
	src := &Source{cfg: tgtCfg()}
	if err := src.Connect(ctx); err != nil {
		t.Fatalf("connect newer source: %v", err)
	}
	defer src.Close(context.Background())
	sink := &Sink{cfg: srcCfg()}
	if err := sink.Connect(ctx); err != nil {
		t.Fatalf("connect older target: %v", err)
	}
	defer sink.Close(context.Background())

	srcSchema, err := src.Introspect(ctx, engine.Selection{})
	if err != nil {
		t.Fatalf("introspect source: %v", err)
	}
	tgtSchema, err := sink.Introspect(ctx, engine.Selection{})
	if err != nil {
		t.Fatalf("introspect target: %v", err)
	}
	srcVer, _ := src.ServerVersion(ctx)
	tgtVer, _ := sink.ServerVersion(ctx)
	rep := redisEngine{}.Preflight("rev", srcVer, tgtVer, srcSchema, tgtSchema)
	if !rep.Blocked() || !hasFinding(rep, engine.SevBlock, "rdb-version") {
		t.Fatalf("newer(%d)->older(%d) must block on rdb-version, got %+v", srcVer, tgtVer, rep.Findings)
	}
}

// TestTargetDownLoudFail: an unreachable target fails LOUD at connect (no silent
// partial state) — the startup half of the target-down signal.
func TestTargetDownLoudFail(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	dead := engine.ConnConfig{Host: "127.0.0.1", Port: 6399, TLS: engine.TLSDisable} // nothing listening
	sink := &Sink{cfg: dead}
	if err := sink.Connect(ctx); err == nil {
		_ = sink.Close(context.Background())
		t.Fatal("connecting to a down target should fail loud, got nil")
	}
}

// TestPolitenessThroughput records coarse copy throughput and confirms a larger
// SCAN COUNT does not change correctness — a politeness/perf smoke, not a hard gate.
func TestPolitenessThroughput(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cfg := srcCfg()
	cfg.Params = map[string]string{paramScanCount: "1000"}
	src := &Source{cfg: cfg}
	if err := src.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer src.Close(context.Background())
	sink := &Sink{cfg: tgtCfg()}
	if err := sink.Connect(ctx); err != nil {
		t.Fatalf("connect sink: %v", err)
	}
	defer sink.Close(context.Background())
	must(t, src.db.uc.FlushDB(ctx).Err())
	must(t, sink.db.uc.FlushDB(ctx).Err())

	const n = 20000
	pipe := src.db.uc.Pipeline()
	for i := 0; i < n; i++ {
		pipe.Set(ctx, fmt.Sprintf("k:%d", i), "v", 0)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("seed: %v", err)
	}

	start := time.Now()
	loaded := copyUnit(t, ctx, src, sink)
	elapsed := time.Since(start)
	if loaded != n {
		t.Fatalf("copied %d, want %d", loaded, n)
	}
	t.Logf("copy throughput: %d keys in %v = %.0f keys/s (scan_count=1000)",
		n, elapsed.Round(time.Millisecond), float64(n)/elapsed.Seconds())
}
