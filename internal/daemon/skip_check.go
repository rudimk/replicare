package daemon

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/observability/live"
	"github.com/rudimk/replicare/internal/pipeline"
)

// skipCheckTimeout bounds each connect and introspect step of the check (the per-range
// queries have their own deadlines). It is not a limit on the whole check, which reads
// every row of every skipped table and can take hours on a large database.
const skipCheckTimeout = 30 * time.Second

// maxSkipCheckKeys is how many differing keys a drift message lists.
const maxSkipCheckKeys = 5

// skipCheckOptions tunes the check: verify's defaults (10,000-row ranges, a short pause
// between them, a 30s wait before re-checking rows that differed). Tests shorten it.
var skipCheckOptions = live.VerifyOptions{}

// checkRepairWait is how long the check waits after queueing rows that differ before
// comparing again (0 = the pipeline's default). Tests shorten it.
var checkRepairWait time.Duration

// skipCheck returns the check of a sync's skipped tables (and tables waiting for an
// automatic check) against the source for one
// target: the same row-by-row comparison as `replicare verify`, on connections of its
// own, limited to the given tables and this target.
func (d *Daemon) skipCheck(syncName, targetName string) func(context.Context, []engine.TableRef) ([]pipeline.SkipCheckResult, error) {
	return func(ctx context.Context, tables []engine.TableRef) ([]pipeline.SkipCheckResult, error) {
		names := make([]string, len(tables))
		for i, t := range tables {
			names[i] = t.String()
		}
		opts := skipCheckOptions
		opts.CollectKeys = true // a difference is repaired by sending these rows again
		opts.Tables = names
		opts.Targets = []string{targetName}
		rep := live.New(d.cfg, skipCheckTimeout).VerifyWith(ctx, syncName, opts)
		return skipCheckResults(rep, targetName, tables)
	}
}

// skipCheckResults turns a verify report into one result per table. A report that
// does not cover the target is an error, never a pass: a check that compared nothing
// must not clear anything.
func skipCheckResults(rep live.VerifyReport, targetName string, tables []engine.TableRef) ([]pipeline.SkipCheckResult, error) {
	if rep.Error != "" {
		return nil, errors.New(rep.Error)
	}
	if len(rep.Targets) != 1 || rep.Targets[0].Target != targetName {
		return nil, fmt.Errorf("verify did not report on target %q", targetName)
	}
	tr := rep.Targets[0]
	if tr.Error != "" {
		return nil, fmt.Errorf("target %q: %s", targetName, tr.Error)
	}
	byName := make(map[string]live.VerifyTable, len(tr.Tables))
	for _, vt := range tr.Tables {
		byName[vt.Table] = vt
	}
	out := make([]pipeline.SkipCheckResult, 0, len(tables))
	for _, t := range tables {
		vt, ok := byName[t.String()]
		switch {
		case !ok:
			out = append(out, pipeline.SkipCheckResult{Table: t, Failed: true, Detail: "not compared"})
		case vt.Status == "error":
			out = append(out, pipeline.SkipCheckResult{Table: t, Failed: true, Detail: vt.Error})
		case vt.Converged && vt.Mode != "sampled":
			out = append(out, pipeline.SkipCheckResult{Table: t, Match: true})
		default:
			out = append(out, pipeline.SkipCheckResult{Table: t, Detail: driftDetail(vt),
				Keys: vt.Differing, Differing: vt.Missing + vt.Extra + vt.Different, Partial: vt.Truncated})
		}
	}
	return out, nil
}

// driftDetail summarises how a table differs, in plain words.
func driftDetail(vt live.VerifyTable) string {
	var parts []string
	if vt.Missing > 0 {
		parts = append(parts, fmt.Sprintf("%s missing on the target", rowsWord(vt.Missing)))
	}
	if vt.Extra > 0 {
		parts = append(parts, fmt.Sprintf("%s on the target that the source doesn't have", rowsWord(vt.Extra)))
	}
	if vt.Different > 0 {
		parts = append(parts, fmt.Sprintf("%s with different values", rowsWord(vt.Different)))
	}
	if len(parts) == 0 {
		parts = append(parts, fmt.Sprintf("%s (source %d rows, target %d rows)", vt.Status, vt.SourceRows, vt.TargetRows))
	}
	msg := strings.Join(parts, ", ")
	if vt.Truncated {
		msg += " (at least; more rows differ than were counted)"
	}
	if len(vt.Keys) > 0 {
		keys := vt.Keys
		if len(keys) > maxSkipCheckKeys {
			keys = keys[:maxSkipCheckKeys]
		}
		ks := make([]string, len(keys))
		for i, k := range keys {
			ks[i] = k.Key
		}
		msg += "; for example keys " + strings.Join(ks, ", ")
	}
	return msg
}

func rowsWord(n int) string {
	if n == 1 {
		return "1 row"
	}
	return fmt.Sprintf("%d rows", n)
}
