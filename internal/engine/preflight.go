package engine

// Pre-flight is engine-specific analysis (type-compatibility rules differ per
// engine) with engine-neutral inputs and outputs. The engine implementation
// consumes two introspected Schemas and produces this neutral PreflightReport,
// so the daemon/CLI can render it and gate startup without knowing engine
// internals (CLAUDE.md §4.2, §5). The block-on-incompatible / warn-on-risky
// policy is encoded in the findings' severities.

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
	Table    TableRef
	Category string
	Message  string
}

// SkippedTable is a table excluded from replication with a reason (e.g. no
// usable key; CLAUDE.md §3.1).
type SkippedTable struct {
	Table  TableRef
	Reason string
}

// Component is an FK connected component of the selected tables (CLAUDE.md §8.1):
// the unit of dependency ordering, parallelism, and per-drain-pass consistency.
// Tables are the members (sorted by name); Order is the topological apply order
// (parents first, cyclic members appended last) that copy and streaming apply
// consume; Cyclic lists the members involved in a cycle or self-reference.
type Component struct {
	Tables []TableRef
	Order  []TableRef
	Cyclic []TableRef
}

// HasCycle reports whether the component contains an FK cycle or self-reference.
func (c Component) HasCycle() bool { return len(c.Cyclic) > 0 }

// PreflightReport is the engine-neutral result of pre-flight for one
// (sync, target) pair.
type PreflightReport struct {
	Sync          string
	SourceVersion int
	TargetVersion int
	Replicable    []TableRef     // tables that will be replicated, in report order
	Skipped       []SkippedTable // tables excluded (no usable key, ...)
	Components    []Component    // FK connected components with topological order
	Findings      []Finding
}

// Blocked reports whether any finding is fatal (block-on-incompatible policy).
func (r *PreflightReport) Blocked() bool {
	for _, f := range r.Findings {
		if f.Severity == SevBlock {
			return true
		}
	}
	return false
}

// Counts returns the number of findings at each severity.
func (r *PreflightReport) Counts() (info, warn, block int) {
	for _, f := range r.Findings {
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
