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
	// NullFillUnsafe marks a NULL-then-fill edge whose child column(s) are in the
	// child table's NullCollidingCols: a unique index where emptied values can clash
	// (NULLS NOT DISTINCT, or an expression/partial unique that mentions the column).
	// NULL-then-fill empties those columns on many rows at once, so rows that were
	// distinct only by that column collide → a duplicate-key failure, in the initial
	// copy and (for new rows) in the streaming apply alike. A SELF-REFERENCE
	// (child==parent) instead keeps the column at its real value — initial copy loads
	// the table in a single statement (cyclic.go, loadSelfRefWhole) and streaming
	// upserts it unchanged — since a non-deferrable FK is checked at statement end. A
	// cross-table cyclic edge with this property cannot be loaded that way and is
	// blocked at pre-flight (make the FK DEFERRABLE).
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
		unsafe := childColNullColliding(fk, byRef)
		reason := "nullable FK column(s): NULL-then-fill two-pass load"
		switch {
		case unsafe && fk.Child == fk.Parent:
			reason = "self-reference column is in a unique index where empty values collide: kept at its real value (initial copy loads the table in one statement)"
		case unsafe:
			reason = "a unique index on this cyclic FK column would collide if the column were emptied, and the cycle spans tables so it cannot be loaded in one statement: make the FK DEFERRABLE"
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

// childColNullColliding reports whether any of the FK's child columns is in the child
// table's NullCollidingCols (a unique index where emptied values can clash — see
// engine.Table). It is the NULL-fill-unsafe signal. An ordinary NULLS DISTINCT unique
// on the column does NOT count, because emptied values never clash there, so such a
// table keeps the cheaper chunked NULL-then-fill load.
func childColNullColliding(fk engine.ForeignKey, byRef map[engine.TableRef]engine.Table) bool {
	child, ok := byRef[fk.Child]
	if !ok {
		return false
	}
	colliding := make(map[string]bool, len(child.NullCollidingCols))
	for _, c := range child.NullCollidingCols {
		colliding[c] = true
	}
	for _, c := range fk.ChildCols {
		if colliding[c] {
			return true
		}
	}
	return false
}

// withTargetNullColliding returns copies of tables whose NullCollidingCols also include
// the matching target table's. Classification runs over SOURCE catalogs (the FK graph
// comes from the source), but a duplicate-key collision happens in the TARGET's unique
// indexes, which the user creates separately and which may differ. Taking the union is
// conservative: a false positive only costs chunking (the single-statement path is
// always correct), a false negative would be the failure. A nil target leaves tables
// unchanged.
func withTargetNullColliding(tables []engine.Table, target *engine.Schema) []engine.Table {
	if target == nil {
		return tables
	}
	tgt := make(map[engine.TableRef][]string, len(target.Tables))
	for _, t := range target.Tables {
		tgt[t.Ref] = t.NullCollidingCols
	}
	out := make([]engine.Table, len(tables))
	for i, t := range tables {
		out[i] = t
		extra := tgt[t.Ref]
		if len(extra) == 0 {
			continue
		}
		seen := make(map[string]bool, len(t.NullCollidingCols)+len(extra))
		merged := make([]string, 0, len(t.NullCollidingCols)+len(extra))
		for _, c := range append(append([]string{}, t.NullCollidingCols...), extra...) {
			if !seen[c] {
				seen[c] = true
				merged = append(merged, c)
			}
		}
		out[i].NullCollidingCols = merged
	}
	return out
}

// keepRealColsByTable returns, per table, the self-reference columns that must keep
// their REAL value instead of being emptied by NULL-then-fill: the child columns of
// every NullFillUnsafe self-reference edge. Emptying them would collide in a unique
// index; keeping them real is safe because a non-deferrable FK is checked at the end
// of the statement. Cross-table unsafe edges are NOT included (they are blocked).
func keepRealColsByTable(cyc []CyclicFK) map[engine.TableRef][]string {
	keep := make([]CyclicFK, 0, len(cyc))
	for _, c := range cyc {
		if c.NullFillUnsafe && c.SelfReference() {
			keep = append(keep, c)
		}
	}
	return cyclicColsByTable(keep)
}

// unloadableNullFill returns the cross-table cyclic edges that are NullFillUnsafe: they
// can neither be emptied (a unique index would collide) nor kept real (the FK points at
// another table loaded in a separate statement). Only a DEFERRABLE FK loads them.
func unloadableNullFill(cyc []CyclicFK) []CyclicFK {
	var out []CyclicFK
	for _, c := range cyc {
		if c.NullFillUnsafe && !c.SelfReference() {
			out = append(out, c)
		}
	}
	return out
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
//
// Self-reference columns that keepRealColsByTable says must keep their real value are
// removed: streaming would otherwise insert NEW rows with that column empty, and a
// unique index where emptied values collide (e.g. NULLS NOT DISTINCT (email, parent))
// rejects a new child whose email matches an existing parentless row — forever, since
// the row stays dirty and re-applies identically. Kept real, the upsert's FK is checked
// at statement end (parent already present or in the same batch); a parent landing in a
// later batch is a transient FK violation that the drain retries (§3.3). A table left
// with no columns to empty is dropped from the map.
func nullFillColsByTable(cyc []CyclicFK) map[engine.TableRef][]string {
	nf := make([]CyclicFK, 0, len(cyc))
	for _, c := range cyc {
		if c.Strategy == CyclicNullThenFill {
			nf = append(nf, c)
		}
	}
	out := cyclicColsByTable(nf)
	for ref, keep := range keepRealColsByTable(cyc) {
		rest := subtractCols(out[ref], keep)
		if len(rest) == 0 {
			delete(out, ref)
			continue
		}
		out[ref] = rest
	}
	return out
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
