package live

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

// keyRange is one half-open key range, after < key <= upTo (nil = unbounded).
type keyRange struct {
	after, upTo engine.KeyValues
}

// chunkState is one target's running comparison of a table.
type chunkState struct {
	row      VerifyTable
	suspects map[string]engine.KeyValues // keys that differed in some range, to re-check
	failed   bool
}

func (c *chunkState) suspect(k engine.KeyValues) {
	if len(c.suspects) >= maxSuspects {
		c.row.Truncated = true
		return
	}
	c.suspects[engine.KeyString(k)] = k
}

// verifyChunked compares a table range by range. The source picks each range's end
// (the key ChunkRows rows further on), then the source and every target fingerprint
// that same range. A range that differs is fetched row by row on both sides to find
// the keys that differ; those keys are looked up again by key at the end, after
// RecheckAfter, and only the ones still different are reported. Looking them up by
// key also clears differences that only come from the two servers ordering text keys
// differently (a different collation), which would otherwise move rows between ranges.
func (e *Enricher) verifyChunked(ctx context.Context, src engine.ChunkVerifier, targets []*verifyTarget, rt replicableTable, opts VerifyOptions) map[*verifyTarget]VerifyTable {
	mode := "chunked"
	if opts.Sample > 0 {
		mode = "sampled"
	}
	states := make(map[*verifyTarget]*chunkState, len(targets))
	for _, vt := range targets {
		states[vt] = &chunkState{row: VerifyTable{Table: rt.ref.String(), Mode: mode}, suspects: map[string]engine.KeyValues{}}
	}
	fail := func(vt *verifyTarget, msg string) {
		st := states[vt]
		st.failed = true
		st.row.Status, st.row.Error = "error", msg
	}
	failAll := func(msg string) map[*verifyTarget]VerifyTable {
		out := make(map[*verifyTarget]VerifyTable, len(targets))
		for _, vt := range targets {
			fail(vt, msg)
			out[vt] = states[vt].row
		}
		return out
	}
	q := func() (context.Context, context.CancelFunc) { return context.WithTimeout(ctx, e.timeout) }

	ranges, err := e.planRanges(ctx, src, rt, opts)
	if err != nil {
		return failAll(fmt.Sprintf("source: %v", err))
	}
	var checked int64
	for i := 0; ; i++ {
		r, ok, err := ranges(i)
		if err != nil {
			return failAll(fmt.Sprintf("source: %v", err))
		}
		if !ok {
			break
		}
		qctx, cancel := q()
		sfp, err := src.VerifyRange(qctx, rt.ref, rt.cols, r.after, r.upTo)
		cancel()
		if err != nil {
			return failAll(fmt.Sprintf("source: %v", err))
		}
		checked += sfp.Rows
		for _, vt := range targets {
			st := states[vt]
			if st.failed {
				continue
			}
			qctx, cancel := q()
			tfp, err := vt.chunked.VerifyRange(qctx, rt.ref, rt.cols, r.after, r.upTo)
			cancel()
			if err != nil {
				fail(vt, fmt.Sprintf("target: %v", err))
				continue
			}
			st.row.Chunks++
			st.row.SourceRows += sfp.Rows
			st.row.TargetRows += tfp.Rows
			if sfp == tfp {
				continue
			}
			if err := e.pinpoint(ctx, src, vt.chunked, rt, r, opts, st); err != nil {
				fail(vt, err.Error())
			}
		}
		if opts.Progress != nil {
			opts.Progress(rt.ref.String(), checked)
		}
		if !sleepCtx(ctx, opts.Pause) {
			return failAll("cancelled")
		}
	}

	// Re-check every key that differed, by key, after a pause.
	anySuspects := false
	for _, st := range states {
		if !st.failed && len(st.suspects) > 0 {
			anySuspects = true
		}
	}
	if anySuspects && !sleepCtx(ctx, opts.RecheckAfter) {
		return failAll("cancelled")
	}
	out := make(map[*verifyTarget]VerifyTable, len(targets))
	for _, vt := range targets {
		st := states[vt]
		if !st.failed {
			if err := e.recheck(ctx, src, vt.chunked, rt, opts, st); err != nil {
				fail(vt, err.Error())
			}
		}
		if !st.failed {
			switch {
			case st.row.Missing > 0 || st.row.Extra > 0:
				st.row.Status = "drift-count"
			case st.row.Different > 0 || st.row.Truncated:
				st.row.Status = "drift-checksum"
			default:
				st.row.Status, st.row.Converged = "ok", true
			}
		}
		out[vt] = st.row
	}
	return out
}

// planRanges returns a function yielding the i-th key range to compare. A full check
// walks the table from the start, each range ending ChunkRows rows further on and the
// last one open-ended, so rows beyond the source's last key (extra on a target) are
// covered too. A sampled check takes Sample random keys and compares the ChunkRows
// rows after each.
func (e *Enricher) planRanges(ctx context.Context, src engine.ChunkVerifier, rt replicableTable, opts VerifyOptions) (func(int) (keyRange, bool, error), error) {
	nextKey := func(after engine.KeyValues) (engine.KeyValues, error) {
		qctx, cancel := context.WithTimeout(ctx, e.timeout)
		defer cancel()
		return src.NextKey(qctx, rt.ref, after, opts.ChunkRows)
	}
	if opts.Sample > 0 {
		qctx, cancel := context.WithTimeout(ctx, e.timeout)
		starts, err := src.SampleKeys(qctx, rt.ref, opts.Sample)
		cancel()
		if err != nil {
			return nil, err
		}
		if len(starts) == 0 {
			starts = []engine.KeyValues{nil} // a small or empty table: check from the start
		}
		return func(i int) (keyRange, bool, error) {
			if i >= len(starts) {
				return keyRange{}, false, nil
			}
			upTo, err := nextKey(starts[i])
			if err != nil {
				return keyRange{}, false, err
			}
			if upTo == nil {
				// Nothing after this key on the source: compare up to the end, which
				// also catches extra rows on a target.
				return keyRange{after: starts[i]}, true, nil
			}
			return keyRange{after: starts[i], upTo: upTo}, true, nil
		}, nil
	}
	var after engine.KeyValues
	done := false
	return func(int) (keyRange, bool, error) {
		if done {
			return keyRange{}, false, nil
		}
		upTo, err := nextKey(after)
		if err != nil {
			return keyRange{}, false, err
		}
		r := keyRange{after: after, upTo: upTo}
		if upTo == nil {
			done = true // the final, open-ended range
		}
		after = upTo
		return r, true, nil
	}, nil
}

// pinpoint fetches a differing range row by row on both sides and records the keys
// that differ as suspects for the re-check.
func (e *Enricher) pinpoint(ctx context.Context, src, tgt engine.ChunkVerifier, rt replicableTable, r keyRange, opts VerifyOptions, st *chunkState) error {
	limit := opts.ChunkRows*2 + 1
	qctx, cancel := context.WithTimeout(ctx, e.timeout)
	sh, err := src.RangeRowHashes(qctx, rt.ref, rt.cols, r.after, r.upTo, limit)
	cancel()
	if err != nil {
		return fmt.Errorf("source: %v", err)
	}
	qctx, cancel = context.WithTimeout(ctx, e.timeout)
	th, err := tgt.RangeRowHashes(qctx, rt.ref, rt.cols, r.after, r.upTo, limit)
	cancel()
	if err != nil {
		return fmt.Errorf("target: %v", err)
	}
	if len(sh) >= limit || len(th) >= limit {
		// Far more rows than a chunk: the range is too different to list in full.
		st.row.Truncated = true
	}
	tmap := make(map[string]engine.RowHash, len(th))
	for _, h := range th {
		tmap[engine.KeyString(h.Key)] = h
	}
	for _, h := range sh {
		k := engine.KeyString(h.Key)
		if t, ok := tmap[k]; !ok || t.Hash != h.Hash {
			st.suspect(h.Key)
		}
		delete(tmap, k)
	}
	for _, h := range tmap {
		st.suspect(h.Key)
	}
	return nil
}

// recheckBatch bounds how many keys one re-check lookup sends.
const recheckBatch = 500

// recheck looks every suspect key up by key on both sides and counts the ones that
// still differ: missing on the target, extra on the target, or different values.
func (e *Enricher) recheck(ctx context.Context, src, tgt engine.ChunkVerifier, rt replicableTable, opts VerifyOptions, st *chunkState) error {
	if len(st.suspects) == 0 {
		return nil
	}
	names := make([]string, 0, len(st.suspects))
	for k := range st.suspects {
		names = append(names, k)
	}
	sort.Strings(names)
	for i := 0; i < len(names); i += recheckBatch {
		end := i + recheckBatch
		if end > len(names) {
			end = len(names)
		}
		keys := make([]engine.KeyValues, 0, end-i)
		for _, n := range names[i:end] {
			keys = append(keys, st.suspects[n])
		}
		qctx, cancel := context.WithTimeout(ctx, e.timeout)
		sh, err := src.RowHashes(qctx, rt.ref, rt.cols, keys)
		cancel()
		if err != nil {
			return fmt.Errorf("source: %v", err)
		}
		qctx, cancel = context.WithTimeout(ctx, e.timeout)
		th, err := tgt.RowHashes(qctx, rt.ref, rt.cols, keys)
		cancel()
		if err != nil {
			return fmt.Errorf("target: %v", err)
		}
		smap := hashMap(sh)
		tmap := hashMap(th)
		for _, n := range names[i:end] {
			s, inS := smap[n]
			t, inT := tmap[n]
			var problem string
			switch {
			case inS && !inT:
				problem = "missing"
				st.row.Missing++
			case !inS && inT:
				problem = "extra"
				st.row.Extra++
			case inS && inT && s != t:
				problem = "different"
				st.row.Different++
			default:
				continue // matches now (it was on its way), or gone from both
			}
			if len(st.row.Keys) < opts.MaxKeys {
				st.row.Keys = append(st.row.Keys, VerifyKey{Key: n, Problem: problem})
			}
			if opts.CollectKeys {
				st.row.Differing = append(st.row.Differing, st.suspects[n])
			}
		}
	}
	return nil
}

func hashMap(hs []engine.RowHash) map[string]string {
	m := make(map[string]string, len(hs))
	for _, h := range hs {
		m[engine.KeyString(h.Key)] = h.Hash
	}
	return m
}

// sleepCtx waits d, or returns false if ctx ends first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
