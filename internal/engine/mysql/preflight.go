package mysql

import (
	"fmt"
	"strings"

	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/fkgraph"
)

// buildPreflight is the MySQL pre-flight (mysql-plan §MM1b): it classifies the
// selected source tables against the target, computes FK components, and emits
// the MySQL-specific findings incl. the block-on-incompatible / warn-on-risky
// type policy and the structural blocks (secondary-unique, non-InnoDB). Tables
// without a usable key are skipped+warned (§3.1).
//
// The pre-existing-trigger block (mysql-plan §0.5) is NOT here: it needs a live
// connection to enumerate existing triggers, which Preflight lacks. It is
// enforced at capture-install time (MM3), where the connection and CREATE TRIGGER
// live, still as a loud, actionable error.
func buildPreflight(syncName string, srcVersion, tgtVersion int, source, target *engine.Schema) *engine.PreflightReport {
	r := &engine.PreflightReport{Sync: syncName, SourceVersion: srcVersion, TargetVersion: tgtVersion}
	if source == nil {
		return r
	}
	tgtByRef := map[engine.TableRef]*engine.Table{}
	if target != nil {
		for i := range target.Tables {
			tgtByRef[target.Tables[i].Ref] = &target.Tables[i]
		}
	}

	var replicable []engine.Table
	for i := range source.Tables {
		st := source.Tables[i]

		// No usable key -> skip + warn (§3.1).
		if !st.HasUsableKey() {
			r.Skipped = append(r.Skipped, engine.SkippedTable{Table: st.Ref, Reason: "no primary key or unique key"})
			r.Findings = append(r.Findings, finding(engine.SevWarn, st.Ref, "no-key",
				"table has no primary key or unique key; skipped (CLAUDE.md §3.1)"))
			continue
		}
		replicable = append(replicable, st)
		r.Replicable = append(r.Replicable, st.Ref)

		// Non-InnoDB source (breaks §3.3 source consume/track atomicity).
		blockNonInnoDB(r, st, "source")

		tgt, ok := tgtByRef[st.Ref]
		if !ok {
			r.Findings = append(r.Findings, finding(engine.SevBlock, st.Ref, "missing-target",
				"table is selected on the source but does not exist on the target (target schema is data-only, CLAUDE.md §7)"))
			continue
		}
		blockNonInnoDB(r, *tgt, "target")
		blockSecondaryUnique(r, *tgt)
		compareColumns(r, st, tgt)
	}

	// FK components over the replicable set + giant/dangling warnings + cyclic
	// strategy notes.
	comps := fkgraph.Components(replicable)
	r.Components = comps
	if g := fkgraph.DetectGiant(comps, len(replicable)); g.Present {
		r.Findings = append(r.Findings, finding(engine.SevWarn, engine.TableRef{}, "giant-component",
			fmt.Sprintf("one FK component covers %.0f%% of the selection, collapsing parallelism (CLAUDE.md §8.1)", g.Fraction*100)))
	}
	for _, fk := range fkgraph.DanglingEdges(replicable) {
		r.Findings = append(r.Findings, finding(engine.SevWarn, fk.Child, "dangling-fk",
			fmt.Sprintf("FK %s references %s which is not in the selection; the target must already satisfy it", fk.Name, fk.Parent)))
	}
	noteCyclicStrategy(r, comps, replicable)
	return r
}

// blockNonInnoDB blocks a non-transactional storage engine (MyISAM/MEMORY/...).
// An empty engine string (engine without the concept) is not blocked.
func blockNonInnoDB(r *engine.PreflightReport, t engine.Table, role string) {
	if t.StorageEngine == "" || strings.EqualFold(t.StorageEngine, "InnoDB") {
		return
	}
	r.Findings = append(r.Findings, finding(engine.SevBlock, t.Ref, "non-innodb",
		fmt.Sprintf("%s table uses storage engine %q; only InnoDB is supported (non-transactional engines break atomic apply/capture, mysql-plan §0.5)", role, t.StorageEngine)))
}

// blockSecondaryUnique blocks a target table carrying a UNIQUE key beyond its
// replication key: ON DUPLICATE KEY UPDATE would fire on it and silently mutate
// the wrong row (mysql-plan §0.4 / Momus B1).
func blockSecondaryUnique(r *engine.PreflightReport, t engine.Table) {
	rk := replicationKey(t)
	for _, u := range t.UniqueKeys {
		if rk != nil && sameCols(u.Columns, rk.Columns) {
			continue // this unique IS the replication key
		}
		r.Findings = append(r.Findings, finding(engine.SevBlock, t.Ref, "secondary-unique",
			fmt.Sprintf("target table has a secondary UNIQUE key %q beyond the replication key; ON DUPLICATE KEY UPDATE cannot preserve faithful halt-loud semantics for it (mysql-plan §0.4)", u.Name)))
		return
	}
}

// compareColumns classifies each name-matched source->target column pair and
// emits risky(WARN)/incompatible(BLOCK) findings.
func compareColumns(r *engine.PreflightReport, src engine.Table, tgt *engine.Table) {
	tgtCols := map[string]engine.Column{}
	for _, c := range tgt.Columns {
		tgtCols[c.Name] = c
	}
	for _, sc := range src.Columns {
		if sc.Generated {
			continue // generated columns are excluded from transport (target regenerates)
		}
		tc, ok := tgtCols[sc.Name]
		if !ok {
			r.Findings = append(r.Findings, finding(engine.SevBlock, src.Ref, "missing-column",
				fmt.Sprintf("column %q exists on the source but not the target", sc.Name)))
			continue
		}
		lvl, reason := classifyColumn(sc, tc)
		switch {
		case lvl.blocks():
			r.Findings = append(r.Findings, finding(engine.SevBlock, src.Ref, "type-incompatible",
				fmt.Sprintf("column %q: %s (%s -> %s)", sc.Name, reason, sc.DataType, tc.DataType)))
		case lvl == compatRisky:
			r.Findings = append(r.Findings, finding(engine.SevWarn, src.Ref, "type-risky",
				fmt.Sprintf("column %q: %s", sc.Name, reason)))
		}
	}
}

// noteCyclicStrategy emits an INFO finding per cyclic component describing the
// MySQL handling (nullable -> NULL-then-fill; NOT NULL -> FOREIGN_KEY_CHECKS=0 +
// pre-commit orphan-verification, §0.2). No block: MySQL has no DEFERRABLE, so
// NOT NULL cycles are handled, not refused (diverges from Postgres).
func noteCyclicStrategy(r *engine.PreflightReport, comps []engine.Component, tables []engine.Table) {
	byRef := map[engine.TableRef]engine.Table{}
	for _, t := range tables {
		byRef[t.Ref] = t
	}
	for _, c := range comps {
		if !c.HasCycle() {
			continue
		}
		strategy := "NULL-then-fill (nullable cyclic FK)"
		if cyclicHasNotNullFK(c, byRef) {
			strategy = "scoped FOREIGN_KEY_CHECKS=0 + pre-commit orphan-verification (NOT NULL cyclic FK, mysql-plan §0.2)"
		}
		for _, ref := range c.Cyclic {
			r.Findings = append(r.Findings, finding(engine.SevInfo, ref, "cyclic-fk",
				"member of an FK cycle/self-reference; initial load strategy: "+strategy))
		}
	}
}

// cyclicHasNotNullFK reports whether any FK among the component's cyclic members
// has all-NOT-NULL child columns (so NULL-then-fill is impossible).
func cyclicHasNotNullFK(c engine.Component, byRef map[engine.TableRef]engine.Table) bool {
	cyclic := map[engine.TableRef]bool{}
	for _, ref := range c.Cyclic {
		cyclic[ref] = true
	}
	for _, ref := range c.Cyclic {
		t := byRef[ref]
		for _, fk := range t.ForeignKeys {
			if !cyclic[fk.Parent] && fk.Parent != ref {
				continue
			}
			if allNotNull(t, fk.ChildCols) {
				return true
			}
		}
	}
	return false
}

func allNotNull(t engine.Table, cols []string) bool {
	byName := map[string]engine.Column{}
	for _, c := range t.Columns {
		byName[c.Name] = c
	}
	for _, name := range cols {
		if byName[name].Nullable {
			return false
		}
	}
	return true
}

// replicationKey returns the table's replication identity: the PK, else the first
// unique key.
func replicationKey(t engine.Table) *engine.Key {
	if t.PrimaryKey != nil {
		return t.PrimaryKey
	}
	if len(t.UniqueKeys) > 0 {
		return &t.UniqueKeys[0]
	}
	return nil
}

func sameCols(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func finding(sev engine.Severity, ref engine.TableRef, category, msg string) engine.Finding {
	return engine.Finding{Severity: sev, Table: ref, Category: category, Message: msg}
}
