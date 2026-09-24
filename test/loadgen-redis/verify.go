package main

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	goredis "github.com/redis/go-redis/v9"
)

// verify compares the source and target keyspaces VALUE-faithfully but VERSION-
// independently. It cannot compare raw DUMP bytes: the harness runs an old source
// (6.2) against a modern target (7.4), whose RDB serializations differ even for
// identical data. Instead it hashes a canonical, type-aware rendering of each
// key's content (plus TTL PRESENCE, since the exact remaining TTL legitimately
// differs by the replication delay), and compares the resulting key->hash maps.

// fpBatch is how many keys are hashed per pair of pipelines. Larger = far fewer
// round-trips, which is what dominates over a high-latency link (e.g. a bastion
// tunnel to a managed Redis) — the whole verify is O(keys/fpBatch) serial
// round-trips. fpProgressEvery gates the "fingerprinted N/total" heartbeat so a
// large keyspace visibly ticks instead of looking hung.
const (
	fpBatch         = 1000
	fpProgressEvery = 50000
)

type verifyResult struct {
	srcKeys  int
	missing  []string // on source, absent on target
	extra    []string // on target, absent on source
	mismatch []string // present on both, content/TTL-presence differs
	skipLeak int      // lgskip:* keys that leaked to the target (must be 0)
}

func (v verifyResult) converged() bool {
	return len(v.missing) == 0 && len(v.extra) == 0 && len(v.mismatch) == 0 && v.skipLeak == 0
}

func verifyOnce(ctx context.Context, srcR, dstR *rdb, log logf) (verifyResult, error) {
	// Fingerprint both ends CONCURRENTLY — they're independent read-only scans on
	// separate connections, so over a slow link the pass takes max(src,dst) instead
	// of src+dst.
	var (
		src, dst       map[string]string
		srcErr, dstErr error
		wg             sync.WaitGroup
	)
	wg.Add(2)
	go func() { defer wg.Done(); src, srcErr = fingerprintAll(ctx, srcR, "source", log) }()
	go func() { defer wg.Done(); dst, dstErr = fingerprintAll(ctx, dstR, "target", log) }()
	wg.Wait()
	if srcErr != nil {
		return verifyResult{}, fmt.Errorf("source: %w", srcErr)
	}
	if dstErr != nil {
		return verifyResult{}, fmt.Errorf("target: %w", dstErr)
	}

	res := verifyResult{srcKeys: len(src)}
	for k, sh := range src {
		dh, ok := dst[k]
		if !ok {
			res.missing = append(res.missing, k)
		} else if sh != dh {
			res.mismatch = append(res.mismatch, k)
		}
	}
	for k := range dst {
		if _, ok := src[k]; !ok {
			res.extra = append(res.extra, k)
		}
	}

	// The excluded cohort must never reach the target.
	skip, err := scanKeys(ctx, dstR, skipPrefix+"*")
	if err != nil {
		return verifyResult{}, fmt.Errorf("target skip scan: %w", err)
	}
	res.skipLeak = len(skip)
	return res, nil
}

// fingerprintAll scans every lg:* key and returns key -> canonical content hash.
// label ("source"/"target") + log make the sweep emit a start line and a periodic
// progress heartbeat, so a large keyspace over a slow link doesn't look hung. log
// may be nil (no output).
func fingerprintAll(ctx context.Context, r *rdb, label string, log logf) (map[string]string, error) {
	if log != nil {
		log("%s: scanning keyspace...", label)
	}
	keys, err := scanKeys(ctx, r, keyPrefix+"*")
	if err != nil {
		return nil, err
	}
	if log != nil {
		log("%s: %d keys, fingerprinting...", label, len(keys))
	}
	out := make(map[string]string, len(keys))
	for lo := 0; lo < len(keys); lo += fpBatch {
		hi := lo + fpBatch
		if hi > len(keys) {
			hi = len(keys)
		}
		if err := fingerprintBatch(ctx, r, keys[lo:hi], out); err != nil {
			return nil, err
		}
		if log != nil && lo/fpProgressEvery != hi/fpProgressEvery {
			log("%s: fingerprinted %d/%d keys", label, hi, len(keys))
		}
	}
	return out, nil
}

// fingerprintBatch computes hashes for a batch of keys with two pipelines: one for
// TYPE + PTTL, then one for the type-specific value read. A key that vanished
// between the SCAN and now (TYPE "none") is skipped -- it is reconciled by a later
// pass, not a drift.
func fingerprintBatch(ctx context.Context, r *rdb, keys []string, out map[string]string) error {
	p1 := r.uc.Pipeline()
	types := make([]*goredis.StatusCmd, len(keys))
	pttls := make([]*goredis.DurationCmd, len(keys))
	for i, k := range keys {
		types[i] = p1.Type(ctx, k)
		pttls[i] = p1.PTTL(ctx, k)
	}
	if _, err := p1.Exec(ctx); err != nil && err != goredis.Nil {
		return fmt.Errorf("type/pttl: %w", err)
	}

	p2 := r.uc.Pipeline()
	readers := make([]func() string, len(keys))
	for i, k := range keys {
		typ := types[i].Val()
		ttl := "0"
		if pttls[i].Val() > 0 {
			ttl = "1"
		}
		readers[i] = valueReader(ctx, p2, k, typ, ttl)
	}
	if _, err := p2.Exec(ctx); err != nil && err != goredis.Nil {
		return fmt.Errorf("value read: %w", err)
	}
	for i, k := range keys {
		if readers[i] == nil {
			continue // vanished
		}
		out[k] = readers[i]()
	}
	return nil
}

// valueReader stages the type-specific read command(s) for key k into pipe p and
// returns a closure that, after p.Exec, renders the canonical content hash. It
// returns nil for a vanished key (type "none"). Streams also read their consumer
// groups' last-delivered-id (full PEL diffing is overkill for a load test).
func valueReader(ctx context.Context, p goredis.Pipeliner, k, typ, ttl string) func() string {
	hash := func(content string) string {
		sum := md5.Sum([]byte(typ + "|" + ttl + "|" + content))
		return hex.EncodeToString(sum[:])
	}
	switch typ {
	case "string":
		c := p.Get(ctx, k)
		return func() string { return hash(c.Val()) }
	case "list":
		c := p.LRange(ctx, k, 0, -1)
		return func() string { return hash(strings.Join(c.Val(), "\x00")) }
	case "set":
		c := p.SMembers(ctx, k)
		return func() string {
			m := c.Val()
			sort.Strings(m)
			return hash(strings.Join(m, "\x00"))
		}
	case "hash":
		c := p.HGetAll(ctx, k)
		return func() string { return hash(canonMap(c.Val())) }
	case "zset":
		c := p.ZRangeWithScores(ctx, k, 0, -1)
		return func() string {
			var b strings.Builder
			for _, z := range c.Val() {
				fmt.Fprint(&b, z.Member)
				b.WriteByte('=')
				b.WriteString(strconv.FormatFloat(z.Score, 'g', -1, 64))
				b.WriteByte('\x00')
			}
			return hash(b.String())
		}
	case "stream":
		msgs := p.XRange(ctx, k, "-", "+")
		groups := p.XInfoGroups(ctx, k)
		return func() string { return hash(canonStream(msgs.Val()) + "||" + canonGroups(groups.Val())) }
	default: // "none" -- key gone between SCAN and read
		return nil
	}
}

func canonMap(m map[string]string) string {
	fields := make([]string, 0, len(m))
	for f := range m {
		fields = append(fields, f)
	}
	sort.Strings(fields)
	var b strings.Builder
	for _, f := range fields {
		b.WriteString(f)
		b.WriteByte('=')
		b.WriteString(m[f])
		b.WriteByte('\x00')
	}
	return b.String()
}

func canonStream(msgs []goredis.XMessage) string {
	var b strings.Builder
	for _, m := range msgs {
		b.WriteString(m.ID)
		b.WriteByte(':')
		b.WriteString(canonAnyMap(m.Values))
		b.WriteByte('\n')
	}
	return b.String()
}

func canonAnyMap(m map[string]any) string {
	fields := make([]string, 0, len(m))
	for f := range m {
		fields = append(fields, f)
	}
	sort.Strings(fields)
	var b strings.Builder
	for _, f := range fields {
		b.WriteString(f)
		b.WriteByte('=')
		fmt.Fprint(&b, m[f])
		b.WriteByte('\x00')
	}
	return b.String()
}

func canonGroups(gs []goredis.XInfoGroup) string {
	lines := make([]string, 0, len(gs))
	for _, g := range gs {
		lines = append(lines, g.Name+"@"+g.LastDeliveredID)
	}
	sort.Strings(lines)
	return strings.Join(lines, ",")
}

// report prints a compact convergence summary and up to a few example drifted keys.
func (v verifyResult) report(log logf) {
	log("%-14s src_keys=%d  missing=%d  extra=%d  content_drift=%d  skip_leak=%d",
		"verify:", v.srcKeys, len(v.missing), len(v.extra), len(v.mismatch), v.skipLeak)
	sample := func(label string, keys []string) {
		if len(keys) == 0 {
			return
		}
		n := len(keys)
		if n > 5 {
			n = 5
		}
		log("  %s (showing %d/%d): %s", label, n, len(keys), strings.Join(keys[:n], ", "))
	}
	sample("missing", v.missing)
	sample("extra", v.extra)
	sample("content_drift", v.mismatch)
}
