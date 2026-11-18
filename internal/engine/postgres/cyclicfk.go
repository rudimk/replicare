package postgres

import (
	"sort"

	"github.com/rudimk/replicare/internal/engine"
)

// Cyclic-FK classification (CLAUDE.md §4.1). Initial copy loads an FK component
// parents-before-children, which is impossible when the component contains an FK
// cycle or self-reference. We never touch target constraints, so each cyclic FK
// is classified into one of three load strategies:
//
//   - Deferrable          -> load the component in one txn with SET CONSTRAINTS
//     DEFERRED (works even for NOT NULL columns). Preferred when available.
//   - Nullable (>=1 col)  -> NULL-then-fill two-pass: insert with the nullable
//     FK column(s) NULL (MATCH SIMPLE leaves the whole FK unchecked when any
//     referencing column is NULL), then a second pass fills the real values.
//   - NOT NULL + non-deferrable -> BLOCKED. We cannot insert NULL into a NOT NULL
//     column and will not disable the constraint, so pre-flight fails loud and
//     the user must make the FK DEFERRABLE or break the cycle.
//
// Streaming apply handles cyclic dependencies separately via the §3.3 retry
// fallback; this classification is strictly about the initial-copy load path.

// CyclicFKStrategy is how a cyclic FK's component can be initially loaded.
type CyclicFKStrategy string

const (
	CyclicDeferred     CyclicFKStrategy = "set_constraints_deferred"
	CyclicNullThenFill CyclicFKStrategy = "null_then_fill"
	CyclicBlocked      CyclicFKStrategy = "blocked"
)

// CyclicFK is one FK edge that participates in a cycle or self-reference, plus
// its classified load strategy.
type CyclicFK struct {
	FK       engine.ForeignKey
	Strategy CyclicFKStrategy
	Nullable bool // at least one child column is nullable
	// NullFillUnsafe marks a NULL-then-fill edge whose child column(s) also
	// participate in a UNIQUE key. NULL-fill pass 1 sets those columns NULL on
	// every row at once; if the unique treats NULLs as not-distinct (PG15+
	// `UNIQUE … NULLS NOT DISTINCT`, or any arrangement that collapses under NULL),
	// rows that were distinct only by that column collide → a mid-copy duplicate-key
	// failure. A SELF-REFERENTIAL (child==parent) such table is loaded whole in a
	// single statement with real values instead (cyclic.go, loadSelfRefWhole); a
	// cross-table cyclic edge with this property is genuinely unloadable without
	// deferring and is blocked at pre-flight.
	NullFillUnsafe bool
	Reason         string // human-readable explanation (esp. for Blocked)
}

// SelfReference reports whether this cyclic FK is a table referencing itself.
func (c CyclicFK) SelfReference() bool { return c.FK.Child == c.FK.Parent }

// Blocked reports whether this cyclic FK prevents initial copy.
func (c CyclicFK) Blocked() bool { return c.Strategy == CyclicBlocked }

// classifyCyclicFKs finds every FK edge on a cycle/self-reference within the
// selected tables and classifies its initial-copy load strategy. Results are
// deterministic. Edges to unselected parents are ignored (they are dangling, not
// cyclic — a separate warning).
func classifyCyclicFKs(tables []engine.Table) []CyclicFK {
	inSel := make(map[engine.TableRef]bool, len(tables))
	byRef := make(map[engine.TableRef]engine.Table, len(tables))
	for _, t := range tables {
		inSel[t.Ref] = true
		byRef[t.Ref] = t
	}

	// Directed adjacency child -> parents (in-selection edges only).
	adj := map[engine.TableRef][]engine.TableRef{}
	for _, t := range tables {
		for _, fk := range t.ForeignKeys {
			if inSel[fk.Parent] {
				adj[fk.Child] = append(adj[fk.Child], fk.Parent)
			}
		}
	}

	var out []CyclicFK
	for _, t := range tables {
		for _, fk := range t.ForeignKeys {
			if !inSel[fk.Parent] {
				continue // dangling
			}
			onCycle := fk.Child == fk.Parent || reachable(adj, fk.Parent, fk.Child)
			if !onCycle {
				continue
			}
			out = append(out, classifyOne(fk, byRef))
		}
	}
	sort.Slice(out, func(i, j int) bool { return fkKey(out[i].FK) < fkKey(out[j].FK) })
	return out
}

// classifyOne classifies a single cyclic FK edge using the child columns'
// nullability from the introspected child table.
func classifyOne(fk engine.ForeignKey, byRef map[engine.TableRef]engine.Table) CyclicFK {
	nullable := anyChildColNullable(fk, byRef)
	switch {
	case fk.Deferrable:
		return CyclicFK{FK: fk, Strategy: CyclicDeferred, Nullable: nullable,
			Reason: "DEFERRABLE: load the component in one transaction with SET CONSTRAINTS DEFERRED"}
	case nullable:
		unsafe := childColInUniqueKey(fk, byRef)
		reason := "nullable FK column(s): NULL-then-fill two-pass load"
		if unsafe {
			reason = "nullable FK column(s) that also participate in a UNIQUE key: NULL-fill would collide, so load whole (self-reference) or make the FK DEFERRABLE (cross-table cycle)"
		}
		return CyclicFK{FK: fk, Strategy: CyclicNullThenFill, Nullable: true, NullFillUnsafe: unsafe, Reason: reason}
	default:
		return CyclicFK{FK: fk, Strategy: CyclicBlocked, Nullable: false,
			Reason: "NOT NULL and non-deferrable FK on a cycle: cannot load without disabling the constraint; make the FK DEFERRABLE or break the cycle"}
	}
}

// anyChildColNullable reports whether at least one of the FK's child columns is
// nullable. With Postgres' default MATCH SIMPLE semantics a single NULL
// referencing column leaves the entire FK unchecked, so one nullable column is
// enough for the NULL-then-fill strategy. A column not found in the introspected
// table is treated conservatively as NOT NULL.
func anyChildColNullable(fk engine.ForeignKey, byRef map[engine.TableRef]engine.Table) bool {
	child, ok := byRef[fk.Child]
	if !ok {
		return false
	}
	nullableByName := make(map[string]bool, len(child.Columns))
	for _, c := range child.Columns {
		nullableByName[c.Name] = c.Nullable
	}
	for _, col := range fk.ChildCols {
		if nullableByName[col] {
			return true
		}
	}
	return false
}

// childColInUniqueKey reports whether any of the FK's child columns participates
// in a UNIQUE key on the child table (the primary key is excluded: PK columns are
// NOT NULL, so a cyclic FK column in the PK is never nullable and never reaches the
// NULL-fill branch). It is the NULL-fill-unsafe signal: NULLing such a column across
// every row in pass 1 can collapse otherwise-distinct rows onto one key. We treat
// participation in ANY unique key as unsafe (conservative) rather than probing the
// index's NULLS-DISTINCT option — the whole-table load it routes to is always correct
// regardless, so a false positive only loses chunking, never correctness. A column
// absent from the introspected table is treated as not-in-a-unique.
func childColInUniqueKey(fk engine.ForeignKey, byRef map[engine.TableRef]engine.Table) bool {
	child, ok := byRef[fk.Child]
	if !ok {
		return false
	}
	inFK := make(map[string]bool, len(fk.ChildCols))
	for _, c := range fk.ChildCols {
		inFK[c] = true
	}
	for _, uk := range child.UniqueKeys {
		for _, c := range uk.Columns {
			if inFK[c] {
				return true
			}
		}
	}
	return false
}

// cyclicColsByTable groups the cyclic FKs' child columns by child table (the
// columns to load/apply NULL then fill), deduplicated and in first-seen order.
func cyclicColsByTable(cyc []CyclicFK) map[engine.TableRef][]string {
	out := map[engine.TableRef][]string{}
	seen := map[engine.TableRef]map[string]bool{}
	for _, c := range cyc {
		if seen[c.FK.Child] == nil {
			seen[c.FK.Child] = map[string]bool{}
		}
		for _, col := range c.FK.ChildCols {
			if !seen[c.FK.Child][col] {
				seen[c.FK.Child][col] = true
				out[c.FK.Child] = append(out[c.FK.Child], col)
			}
		}
	}
	return out
}

// nullFillColsByTable is cyclicColsByTable restricted to the NULL-then-fill
// strategy — the cyclic FK child columns that are actually nullable. This is the
// set the STREAMING cyclic apply may load NULL: a DEFERRABLE cyclic FK's columns
// (which may be NOT NULL) must NOT be nulled — they are handled by the
// single-transaction SET CONSTRAINTS ALL DEFERRED path — and nulling a NOT NULL
// column would fail loud. An empty result means the component has no nullable
// cyclic column, so the streaming drain keeps the single-transaction path.
func nullFillColsByTable(cyc []CyclicFK) map[engine.TableRef][]string {
	nf := make([]CyclicFK, 0, len(cyc))
	for _, c := range cyc {
		if c.Strategy == CyclicNullThenFill {
			nf = append(nf, c)
		}
	}
	return cyclicColsByTable(nf)
}

// anyBlockedCyclicFK reports whether any classified cyclic FK blocks initial copy.
func anyBlockedCyclicFK(cs []CyclicFK) bool {
	for _, c := range cs {
		if c.Blocked() {
			return true
		}
	}
	return false
}

// reachable reports whether `to` is reachable from `from` in the directed
// adjacency (child -> parent edges), via DFS. Used to decide whether an edge
// closes a cycle.
func reachable(adj map[engine.TableRef][]engine.TableRef, from, to engine.TableRef) bool {
	seen := map[engine.TableRef]bool{}
	stack := []engine.TableRef{from}
	for len(stack) > 0 {
		n := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if n == to {
			return true
		}
		if seen[n] {
			continue
		}
		seen[n] = true
		stack = append(stack, adj[n]...)
	}
	return false
}

// fkKey is a stable sort/identity key for an FK edge.
func fkKey(fk engine.ForeignKey) string {
	return fk.Child.String() + "->" + fk.Parent.String() + ":" + fk.Name
}
