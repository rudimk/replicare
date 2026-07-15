package postgres

import (
	"fmt"
	"sort"

	"github.com/rudimk/replicare/internal/engine"
)

// Pre-flight assembly (CLAUDE.md §4.2, §8.1). Given the introspected source and
// target schemas, this produces a single classified report: which tables are
// replicable, which are skipped (no usable key), the FK components and their
// warnings, cyclic-FK load strategies, and per-column type compatibility. The
// policy is block-on-incompatible / warn-on-risky: the report Blocks() when any
// finding is fatal, so `validate` can refuse to start.
//
// This is pure assembly over already-introspected data and is fully unit-tested;
// the CLI performs the actual connect+introspect and renders the report.

// Severity classifies a pre-flight finding.
type Severity int

const (
	SevInfo Severity = iota
	SevWarn
	SevBlock
)

func (s Severity) String() string {
	switch s {
	case SevInfo:
		return "INFO"
	case SevWarn:
		return "WARN"
	case SevBlock:
		return "BLOCK"
	default:
		return "?"
	}
}

// Finding is one classified pre-flight observation. Table is the zero value for
// selection-wide findings (e.g. a giant component).
type Finding struct {
	Severity Severity
	Table    engine.TableRef
	Category string
	Message  string
}

// TablePreflight is the per-table pre-flight result.
type TablePreflight struct {
	Table    engine.TableRef
	Identity *engine.Key // chosen replication identity (PK, else first unique)
	Compat   TableCompat
}

// SkippedTable is a table excluded from replication with a reason.
type SkippedTable struct {
	Table  engine.TableRef
	Reason string
}

// Preflight is the full pre-flight report for one sync.
type Preflight struct {
	Sync          string
	SourceVersion int
	TargetVersion int
	Replicable    []TablePreflight
	Skipped       []SkippedTable
	Components    []Component
	Giant         GiantComponent
	Dangling      []engine.ForeignKey
	CyclicFKs     []CyclicFK
	Findings      []Finding
}

// Blocked reports whether any finding is fatal (block-on-incompatible policy).
func (p Preflight) Blocked() bool {
	for _, f := range p.Findings {
		if f.Severity == SevBlock {
			return true
		}
	}
	return false
}

// Counts returns the number of findings at each severity.
func (p Preflight) Counts() (info, warn, block int) {
	for _, f := range p.Findings {
		switch f.Severity {
		case SevInfo:
			info++
		case SevWarn:
			warn++
		case SevBlock:
			block++
		}
	}
	return info, warn, block
}

// buildPreflight assembles the report from introspected schemas. Tables without a
// usable key are skipped+warned and excluded from component/compat analysis
// (CLAUDE.md §3.1). Findings are returned in a deterministic order.
func buildPreflight(syncName string, srcVersion, tgtVersion int, source, target *engine.Schema) Preflight {
	p := Preflight{Sync: syncName, SourceVersion: srcVersion, TargetVersion: tgtVersion}

	// Partition selected source tables into replicable (keyed) and skipped.
	var replicable []engine.Table
	for _, t := range source.Tables {
		if !t.HasUsableKey() {
			p.Skipped = append(p.Skipped, SkippedTable{Table: t.Ref, Reason: "no primary key or usable unique key"})
			p.add(SevWarn, t.Ref, "no-key", "skipped: no primary key or usable unique key")
			continue
		}
		replicable = append(replicable, t)
	}

	// FK structural analysis over the replicable set.
	p.Components = computeComponents(replicable)
	p.Giant = detectGiantComponent(p.Components, len(replicable))
	if p.Giant.Present {
		p.add(SevWarn, engine.TableRef{}, "giant-component",
			fmt.Sprintf("one FK component spans %.0f%% of selected tables (%d of %d), limiting parallelism",
				p.Giant.Fraction*100, len(p.Giant.Component.Tables), len(replicable)))
	}
	p.Dangling = danglingFKEdges(replicable)
	for _, fk := range p.Dangling {
		p.add(SevWarn, fk.Child, "dangling-fk",
			fmt.Sprintf("FK %s references unselected table %s; the target must already satisfy it", fk.Name, fk.Parent))
	}

	// Cyclic-FK load classification.
	p.CyclicFKs = classifyCyclicFKs(replicable)
	for _, c := range p.CyclicFKs {
		if c.Blocked() {
			p.add(SevBlock, c.FK.Child, "cyclic-fk",
				fmt.Sprintf("FK %s: %s", c.FK.Name, c.Reason))
		} else {
			p.add(SevInfo, c.FK.Child, "cyclic-fk",
				fmt.Sprintf("FK %s cyclic; load strategy: %s", c.FK.Name, c.Strategy))
		}
	}

	// Per-table type compatibility against the target.
	tgtByRef := map[engine.TableRef]*engine.Table{}
	if target != nil {
		for i := range target.Tables {
			tgtByRef[target.Tables[i].Ref] = &target.Tables[i]
		}
	}
	for _, t := range replicable {
		compat := compareTable(t, tgtByRef[t.Ref])
		p.Replicable = append(p.Replicable, TablePreflight{
			Table:    t.Ref,
			Identity: replicationKey(t),
			Compat:   compat,
		})
		p.addCompatFindings(t.Ref, compat)
	}

	sort.SliceStable(p.Findings, func(i, j int) bool {
		if p.Findings[i].Table.String() != p.Findings[j].Table.String() {
			return p.Findings[i].Table.String() < p.Findings[j].Table.String()
		}
		if p.Findings[i].Severity != p.Findings[j].Severity {
			return p.Findings[i].Severity > p.Findings[j].Severity // block first
		}
		return p.Findings[i].Category < p.Findings[j].Category
	})
	return p
}

// addCompatFindings turns a table's compatibility result into findings.
func (p *Preflight) addCompatFindings(ref engine.TableRef, c TableCompat) {
	if !c.TargetExists {
		p.add(SevBlock, ref, "missing-table", "target table does not exist")
		return
	}
	for _, col := range c.MissingInTarget {
		p.add(SevBlock, ref, "missing-column", fmt.Sprintf("column %q is absent from the target", col))
	}
	for _, cc := range c.Columns {
		switch cc.Level {
		case CompatIncompatible:
			p.add(SevBlock, ref, "type",
				fmt.Sprintf("column %q: %s -> %s is incompatible (%s)", cc.Column, cc.SourceType, cc.TargetType, cc.Detail))
		case CompatRisky:
			p.add(SevWarn, ref, "type",
				fmt.Sprintf("column %q: %s -> %s is risky (%s)", cc.Column, cc.SourceType, cc.TargetType, cc.Detail))
		}
	}
	for _, col := range c.ExtraNotNull {
		p.add(SevWarn, ref, "extra-not-null",
			fmt.Sprintf("target column %q is NOT NULL but not supplied by the source; inserts will fail unless it has a default", col))
	}
}

func (p *Preflight) add(sev Severity, table engine.TableRef, category, msg string) {
	p.Findings = append(p.Findings, Finding{Severity: sev, Table: table, Category: category, Message: msg})
}

// replicationKey picks a table's replication identity: the primary key, else the
// first usable unique key (CLAUDE.md §3.1).
func replicationKey(t engine.Table) *engine.Key {
	if t.PrimaryKey != nil {
		return t.PrimaryKey
	}
	if len(t.UniqueKeys) > 0 {
		k := t.UniqueKeys[0]
		return &k
	}
	return nil
}
