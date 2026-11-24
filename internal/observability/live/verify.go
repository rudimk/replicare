package live

import (
	"context"
	"fmt"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

// VerifyReport is a sync's source↔target convergence spot-check. It is read-only:
// it counts and content-fingerprints each replicated unit on the source and on each
// target and compares. A sync is single-engine (§6), so the two ends fingerprint
// identically and their checksums are directly comparable.
type VerifyReport struct {
	Sync      string         `json:"sync"`
	Paused    bool           `json:"paused,omitempty"`
	Targets   []VerifyTarget `json:"targets"`
	Converged bool           `json:"converged"`
	Error     string         `json:"error,omitempty"`
}

// VerifyTarget is one target's per-table convergence within the sync.
type VerifyTarget struct {
	Target    string        `json:"target"`
	Tables    []VerifyTable `json:"tables"`
	Converged bool          `json:"converged"`
	Error     string        `json:"error,omitempty"`
}

// VerifyTable is one table's source-vs-target comparison. Status is one of:
// "ok" (counts and checksums match), "count-only-ok" (counts match; the engine does
// not content-hash, so only membership was compared), "drift-count",
// "drift-checksum", or "error".
type VerifyTable struct {
	Table          string `json:"table"`
	SourceRows     int64  `json:"source_rows"`
	TargetRows     int64  `json:"target_rows"`
	SourceChecksum string `json:"source_checksum,omitempty"`
	TargetChecksum string `json:"target_checksum,omitempty"`
	Converged      bool   `json:"converged"`
	Status         string `json:"status"`
	Error          string `json:"error,omitempty"`
	// Mode is how the table was compared: "chunked" (every row, one key range at a
	// time), "sampled" (random key ranges only, --sample), or "whole" (one fingerprint
	// of the whole table, for engines without chunked checks).
	Mode string `json:"mode"`
	// Chunks is how many key ranges were compared (chunked and sampled modes).
	Chunks int `json:"chunks,omitempty"`
	// Missing, Extra and Different count the rows that still differed after the
	// re-check: on the source but not the target, on the target but not the source,
	// and on both with different values. Keys lists up to MaxKeys of them; Truncated
	// says there were more differences than were tracked.
	Missing   int         `json:"missing_on_target,omitempty"`
	Extra     int         `json:"extra_on_target,omitempty"`
	Different int         `json:"different,omitempty"`
	Keys      []VerifyKey `json:"keys,omitempty"`
	Truncated bool        `json:"truncated,omitempty"`
}

// VerifyKey is one row that differs: its key, and how ("missing", "extra" or
// "different").
type VerifyKey struct {
	Key     string `json:"key"`
	Problem string `json:"problem"`
}

// VerifyOptions tunes a verify run. The zero value is a full chunked check with the
// defaults below.
type VerifyOptions struct {
	// Tables limits the check to these tables (schema.table); empty means all.
	Tables []string
	// Targets limits the check to these of the sync's targets; empty means all.
	Targets []string
	// ChunkRows is how many rows each key range covers (default 10,000).
	ChunkRows int
	// Pause is the wait between key ranges, to keep the load on the source low
	// (default 100ms).
	Pause time.Duration
	// Sample, when above 0, checks only this many random key ranges per table (a
	// quick spot check) instead of every row.
	Sample int
	// RecheckAfter is how long to wait before re-checking rows that differed, so rows
	// that were simply still on their way to the target are not reported (default
	// 30s; negative means re-check straight away).
	RecheckAfter time.Duration
	// MaxKeys is how many differing keys to list per table (default 20).
	MaxKeys int
	// Progress, if set, is called after each key range with the rows checked so far.
	Progress func(table string, rowsChecked int64)
}

const (
	defaultVerifyChunkRows = 10000
	defaultVerifyPause     = 100 * time.Millisecond
	defaultRecheckAfter    = 30 * time.Second
	defaultMaxKeys         = 20
	// maxSuspects bounds how many differing keys are tracked per (table, target), so a
	// badly diverged table cannot exhaust memory; beyond it the table is "truncated".
	maxSuspects = 10000
)

func (o VerifyOptions) withDefaults() VerifyOptions {
	if o.ChunkRows <= 0 {
		o.ChunkRows = defaultVerifyChunkRows
	}
	if o.Pause == 0 {
		o.Pause = defaultVerifyPause
	}
	if o.Pause < 0 {
		o.Pause = 0
	}
	if o.RecheckAfter == 0 {
		o.RecheckAfter = defaultRecheckAfter
	}
	if o.RecheckAfter < 0 {
		o.RecheckAfter = 0
	}
	if o.MaxKeys <= 0 {
		o.MaxKeys = defaultMaxKeys
	}
	return o
}

// Verify runs a full, chunked convergence check for sync `name` with the default
// options. See VerifyWith.
func (e *Enricher) Verify(ctx context.Context, name string) VerifyReport {
	return e.VerifyWith(ctx, name, VerifyOptions{})
}

// VerifyWith runs the convergence check for sync `name`. For engines that support it
// (engine.ChunkVerifier: Postgres, MySQL) each table is compared one key range at a time: the
// source picks the end of each range, both ends fingerprint it with the same query,
// and a range that differs is narrowed to the exact rows, which are re-checked after
// opts.RecheckAfter so rows still on their way are not reported. Other engines get one
// whole-table fingerprint per side. It never returns an error value; failures are
// captured in the report (report.Error for a whole-sync problem, a target's Error for
// one unreachable target, a table's Error for one failed comparison), so a --json
// consumer always gets a structured result.
func (e *Enricher) VerifyWith(ctx context.Context, name string, opts VerifyOptions) VerifyReport {
	opts = opts.withDefaults()
	rep := VerifyReport{Sync: name}
	sync := e.findSync(name)
	if sync == nil {
		rep.Error = fmt.Sprintf("sync %q not found in config", name)
		return rep
	}
	sel := engine.Selection{Include: sync.Include, Exclude: sync.Exclude}

	srcEp := e.cfg.Sources[sync.Source]
	if srcEp == nil {
		rep.Error = fmt.Sprintf("source %q not found in config", sync.Source)
		return rep
	}
	srcCtx, cancel := context.WithTimeout(ctx, e.timeout)
	src, schema, closeSrc, err := openSource(srcCtx, srcEp, sel)
	cancel()
	if err != nil {
		rep.Error = fmt.Sprintf("source %q unreachable: %v", sync.Source, err)
		return rep
	}
	defer closeSrc()

	srcVerifier, ok := src.(engine.Verifier)
	if !ok {
		rep.Error = fmt.Sprintf("engine %q does not support verify", srcEp.Engine)
		return rep
	}
	tables, err := filterTables(replicableTables(schema), opts.Tables)
	if err != nil {
		rep.Error = err.Error()
		return rep
	}

	// Open every target up front, so each key range is read from the source once and
	// compared against all of them.
	rep.Converged = true
	targets := make([]*verifyTarget, 0, len(sync.Targets))
	for _, tgtName := range sync.Targets {
		if len(opts.Targets) > 0 && !contains(opts.Targets, tgtName) {
			continue
		}
		vt := &verifyTarget{report: VerifyTarget{Target: tgtName, Converged: true}}
		targets = append(targets, vt)
		tgtEp := e.cfg.Targets[tgtName]
		if tgtEp == nil {
			vt.report.Error = fmt.Sprintf("target %q not found in config", tgtName)
			continue
		}
		tctx, tcancel := context.WithTimeout(ctx, e.timeout)
		sink, closeSink, err := openSink(tctx, tgtEp, sel)
		tcancel()
		if err != nil {
			vt.report.Error = fmt.Sprintf("unreachable: %v", err)
			continue
		}
		defer closeSink()
		v, ok := sink.(engine.Verifier)
		if !ok {
			vt.report.Error = fmt.Sprintf("engine %q does not support verify", tgtEp.Engine)
			continue
		}
		vt.verifier = v
		vt.chunked, _ = sink.(engine.ChunkVerifier)
	}

	srcChunked, _ := src.(engine.ChunkVerifier)
	for _, rt := range tables {
		live := make([]*verifyTarget, 0, len(targets))
		allChunked := srcChunked != nil
		for _, vt := range targets {
			if vt.verifier == nil {
				continue
			}
			live = append(live, vt)
			if vt.chunked == nil {
				allChunked = false
			}
		}
		if len(live) == 0 {
			continue
		}
		var rows map[*verifyTarget]VerifyTable
		if allChunked {
			rows = e.verifyChunked(ctx, srcChunked, live, rt, opts)
		} else {
			rows = e.verifyWhole(ctx, srcVerifier, live, rt)
		}
		for _, vt := range live {
			vt.report.Tables = append(vt.report.Tables, rows[vt])
		}
	}

	for _, vt := range targets {
		if vt.report.Error != "" {
			vt.report.Converged = false
		}
		for _, t := range vt.report.Tables {
			if !t.Converged {
				vt.report.Converged = false
			}
		}
		if !vt.report.Converged {
			rep.Converged = false
		}
		rep.Targets = append(rep.Targets, vt.report)
	}
	return rep
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

// verifyTarget is one target being compared: its report and its verifiers.
type verifyTarget struct {
	report   VerifyTarget
	verifier engine.Verifier
	chunked  engine.ChunkVerifier
}

// filterTables keeps the tables named in only (schema.table); every name must be a
// replicated table of the sync.
func filterTables(tables []replicableTable, only []string) ([]replicableTable, error) {
	if len(only) == 0 {
		return tables, nil
	}
	byName := make(map[string]replicableTable, len(tables))
	for _, rt := range tables {
		byName[rt.ref.String()] = rt
	}
	out := make([]replicableTable, 0, len(only))
	for _, name := range only {
		rt, ok := byName[name]
		if !ok {
			return nil, fmt.Errorf("table %q is not replicated by this sync (use schema.table)", name)
		}
		out = append(out, rt)
	}
	return out, nil
}

// verifyWhole compares one whole-table fingerprint per side (engines without chunked
// checks).
func (e *Enricher) verifyWhole(ctx context.Context, src engine.Verifier, targets []*verifyTarget, rt replicableTable) map[*verifyTarget]VerifyTable {
	out := make(map[*verifyTarget]VerifyTable, len(targets))
	qctx, qcancel := context.WithTimeout(ctx, e.timeout)
	sfp, serr := src.Fingerprint(qctx, rt.ref, rt.cols)
	qcancel()
	for _, vt := range targets {
		row := VerifyTable{Table: rt.ref.String(), Mode: "whole"}
		if serr != nil {
			row.Status, row.Error = "error", fmt.Sprintf("source: %v", serr)
			out[vt] = row
			continue
		}
		qctx, qcancel := context.WithTimeout(ctx, e.timeout)
		tf, err := vt.verifier.Fingerprint(qctx, rt.ref, rt.cols)
		qcancel()
		if err != nil {
			row.Status, row.Error = "error", fmt.Sprintf("target: %v", err)
			out[vt] = row
			continue
		}
		row.SourceRows, row.TargetRows = sfp.Rows, tf.Rows
		row.SourceChecksum, row.TargetChecksum = sfp.Checksum, tf.Checksum
		row.Status, row.Converged = classify(sfp, tf)
		out[vt] = row
	}
	return out
}

// classify compares a source and target fingerprint into a status + converged flag.
// When either checksum is empty (the engine does not content-hash), it compares row
// counts only ("count-only-ok" / "drift-count").
func classify(src, tgt engine.Fingerprint) (string, bool) {
	if src.Rows != tgt.Rows {
		return "drift-count", false
	}
	if src.Checksum == "" || tgt.Checksum == "" {
		return "count-only-ok", true
	}
	if src.Checksum != tgt.Checksum {
		return "drift-checksum", false
	}
	return "ok", true
}
