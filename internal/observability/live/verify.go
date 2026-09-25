package live

import (
	"context"
	"fmt"

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
}

// Verify runs the convergence spot-check for sync `name`. It never returns an error
// value; failures are captured in the report (report.Error for a whole-sync problem,
// a target's Error for one unreachable target, a table's Error for one failed
// comparison), so a --json consumer always gets a structured result.
func (e *Enricher) Verify(ctx context.Context, name string) VerifyReport {
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
	tables := replicableTables(schema)

	// Fingerprint the source once per table (reused across all targets).
	type srcFP struct {
		fp  engine.Fingerprint
		err error
	}
	srcFPs := make(map[engine.TableRef]srcFP, len(tables))
	for _, rt := range tables {
		qctx, qcancel := context.WithTimeout(ctx, e.timeout)
		fp, err := srcVerifier.Fingerprint(qctx, rt.ref, rt.cols)
		qcancel()
		srcFPs[rt.ref] = srcFP{fp: fp, err: err}
	}

	rep.Converged = true
	for _, tgtName := range sync.Targets {
		vt := VerifyTarget{Target: tgtName, Converged: true}
		tgtEp := e.cfg.Targets[tgtName]
		if tgtEp == nil {
			vt.Error = fmt.Sprintf("target %q not found in config", tgtName)
			vt.Converged = false
			rep.Converged = false
			rep.Targets = append(rep.Targets, vt)
			continue
		}
		tctx, tcancel := context.WithTimeout(ctx, e.timeout)
		sink, closeSink, err := openSink(tctx, tgtEp, sel)
		tcancel()
		if err != nil {
			vt.Error = fmt.Sprintf("unreachable: %v", err)
			vt.Converged = false
			rep.Converged = false
			rep.Targets = append(rep.Targets, vt)
			continue
		}
		sinkVerifier, ok := sink.(engine.Verifier)
		if !ok {
			vt.Error = fmt.Sprintf("engine %q does not support verify", tgtEp.Engine)
			vt.Converged = false
			rep.Converged = false
			closeSink()
			rep.Targets = append(rep.Targets, vt)
			continue
		}
		for _, rt := range tables {
			row := VerifyTable{Table: rt.ref.String()}
			sf := srcFPs[rt.ref]
			if sf.err != nil {
				row.Status, row.Error = "error", fmt.Sprintf("source: %v", sf.err)
				vt.Converged, rep.Converged = false, false
				vt.Tables = append(vt.Tables, row)
				continue
			}
			qctx, qcancel := context.WithTimeout(ctx, e.timeout)
			tf, err := sinkVerifier.Fingerprint(qctx, rt.ref, rt.cols)
			qcancel()
			if err != nil {
				row.Status, row.Error = "error", fmt.Sprintf("target: %v", err)
				vt.Converged, rep.Converged = false, false
				vt.Tables = append(vt.Tables, row)
				continue
			}
			row.SourceRows, row.TargetRows = sf.fp.Rows, tf.Rows
			row.SourceChecksum, row.TargetChecksum = sf.fp.Checksum, tf.Checksum
			row.Status, row.Converged = classify(sf.fp, tf)
			if !row.Converged {
				vt.Converged, rep.Converged = false, false
			}
			vt.Tables = append(vt.Tables, row)
		}
		closeSink()
		rep.Targets = append(rep.Targets, vt)
	}
	return rep
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
