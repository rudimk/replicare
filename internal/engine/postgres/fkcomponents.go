package postgres

import (
	"sort"

	"github.com/rudimk/replicare/internal/engine"
)

// FK connected-component analysis (CLAUDE.md §8.1). Within a sync's selected
// tables the engine auto-partitions into FK connected components: no FK edge
// crosses a component boundary, so components are the units of dependency
// ordering, parallelism, and per-drain-pass consistency.
//
// This file is pure structural analysis over an already-introspected schema; it
// does no I/O and is fully unit-tested. Cyclic-FK *classification* (nullable /
// deferrable / not-null) lives in the cyclic-FK slice; here we only expose which
// tables participate in a cycle so that slice can classify them.

// giantComponentFraction and minTablesForGiantWarning gate the "one component
// dominates the selection" warning (CLAUDE.md §8.1). We only warn once the
// selection is large enough that lost parallelism actually matters, to avoid
// noise on small schemas.
const (
	giantComponentFraction   = 0.6
	minTablesForGiantWarning = 5
)

// Component is one FK connected component: a set of member tables plus a
// topological apply order (parents before children) for upserts. Deletes apply
// in reverse. Tables that participate in an FK cycle or self-reference cannot be
// topologically ordered and are reported in Cyclic (handled by the cyclic-FK
// classification and, at stream time, the retry fallback — §3.3).
type Component struct {
	Tables []engine.TableRef // members, sorted by qualified name
	Order  []engine.TableRef // topological order (parents first); cyclic members appended last
	Cyclic []engine.TableRef // members involved in a cycle/self-reference, sorted
}

// HasCycle reports whether the component contains an FK cycle or self-reference.
func (c Component) HasCycle() bool { return len(c.Cyclic) > 0 }

// computeComponents partitions selected tables into FK connected components. Only
// FK edges whose parent is also in the selection connect components; edges to
// excluded tables are dangling (see danglingFKEdges) and do not merge components.
// Components and their members are returned in a deterministic order.
func computeComponents(selected []engine.Table) []Component {
	inSel := make(map[engine.TableRef]bool, len(selected))
	for _, t := range selected {
		inSel[t.Ref] = true
	}

	// Union-find over selected tables, joined by in-selection FK edges.
	uf := newUnionFind()
	for _, t := range selected {
		uf.add(t.Ref)
	}
	for _, t := range selected {
		for _, fk := range t.ForeignKeys {
			if inSel[fk.Parent] {
				uf.union(fk.Child, fk.Parent)
			}
		}
	}

	// Group tables by component root.
	groups := map[engine.TableRef][]engine.Table{}
	for _, t := range selected {
		root := uf.find(t.Ref)
		groups[root] = append(groups[root], t)
	}

	comps := make([]Component, 0, len(groups))
	for _, members := range groups {
		comps = append(comps, buildComponent(members, inSel))
	}
	// Deterministic ordering: by first (smallest) member name.
	sort.Slice(comps, func(i, j int) bool {
		return comps[i].Tables[0].String() < comps[j].Tables[0].String()
	})
	return comps
}

// buildComponent constructs a Component from its member tables, computing the
// topological apply order and the set of cyclic members. inSel bounds edges to
// the component (edges to excluded parents are ignored here).
func buildComponent(members []engine.Table, inSel map[engine.TableRef]bool) Component {
	refs := make([]engine.TableRef, 0, len(members))
	memberSet := make(map[engine.TableRef]bool, len(members))
	for _, m := range members {
		refs = append(refs, m.Ref)
		memberSet[m.Ref] = true
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i].String() < refs[j].String() })

	order, cyclic := topoOrder(members, memberSet, inSel)
	return Component{Tables: refs, Order: order, Cyclic: cyclic}
}

// topoOrder returns a topological order of the members (parents before children)
// via Kahn's algorithm, plus the members that remain in cycles (including
// self-references). Dependency edge: child depends on parent, so a parent is
// emitted before its children. Ties break by qualified name for determinism.
// Cyclic members are appended to the order after the acyclic prefix so callers
// still get a total order to iterate.
func topoOrder(members []engine.Table, memberSet, inSel map[engine.TableRef]bool) (order, cyclic []engine.TableRef) {
	// Build dependency edges parent -> child within the component. Deduplicate
	// (a table may have multiple FK edges to the same parent). Self-references
	// (child == parent) are recorded as an immediate cycle.
	children := map[engine.TableRef]map[engine.TableRef]bool{} // parent -> set of children
	indeg := map[engine.TableRef]int{}
	selfCycle := map[engine.TableRef]bool{}
	for _, m := range members {
		if _, ok := indeg[m.Ref]; !ok {
			indeg[m.Ref] = 0
		}
	}
	for _, m := range members {
		for _, fk := range m.ForeignKeys {
			if !inSel[fk.Parent] || !memberSet[fk.Parent] {
				continue // dangling or cross-component (shouldn't happen within a component)
			}
			if fk.Child == fk.Parent {
				selfCycle[fk.Child] = true
				continue
			}
			if children[fk.Parent] == nil {
				children[fk.Parent] = map[engine.TableRef]bool{}
			}
			if !children[fk.Parent][fk.Child] {
				children[fk.Parent][fk.Child] = true
				indeg[fk.Child]++
			}
		}
	}

	// Kahn's algorithm with deterministic tie-breaking.
	var queue []engine.TableRef
	for ref, d := range indeg {
		if d == 0 && !selfCycle[ref] {
			queue = append(queue, ref)
		}
	}
	sort.Slice(queue, func(i, j int) bool { return queue[i].String() < queue[j].String() })

	emitted := map[engine.TableRef]bool{}
	for len(queue) > 0 {
		n := queue[0]
		queue = queue[1:]
		order = append(order, n)
		emitted[n] = true
		// Emit children whose in-degree drops to zero.
		kids := make([]engine.TableRef, 0, len(children[n]))
		for c := range children[n] {
			kids = append(kids, c)
		}
		sort.Slice(kids, func(i, j int) bool { return kids[i].String() < kids[j].String() })
		for _, c := range kids {
			indeg[c]--
			if indeg[c] == 0 && !selfCycle[c] {
				// Insert maintaining sorted order for determinism.
				queue = append(queue, c)
				sort.Slice(queue, func(i, j int) bool { return queue[i].String() < queue[j].String() })
			}
		}
	}

	// Anything not emitted is part of a cycle (or a self-reference).
	for _, m := range members {
		if !emitted[m.Ref] {
			cyclic = append(cyclic, m.Ref)
		}
	}
	sort.Slice(cyclic, func(i, j int) bool { return cyclic[i].String() < cyclic[j].String() })
	// Append cyclic members to the order so callers have a total iteration order.
	order = append(order, cyclic...)
	return order, cyclic
}

// GiantComponent flags the case where one component dominates the selection,
// collapsing parallelism (CLAUDE.md §8.1). Present is false when no component
// dominates or the selection is too small to matter.
type GiantComponent struct {
	Present   bool
	Component *Component
	Fraction  float64 // dominant component size / total selected tables
}

// detectGiantComponent reports the dominant component if one covers at least
// giantComponentFraction of a selection of at least minTablesForGiantWarning
// tables.
func detectGiantComponent(comps []Component, totalTables int) GiantComponent {
	if totalTables < minTablesForGiantWarning || len(comps) == 0 {
		return GiantComponent{}
	}
	biggest := &comps[0]
	for i := range comps {
		if len(comps[i].Tables) > len(biggest.Tables) {
			biggest = &comps[i]
		}
	}
	frac := float64(len(biggest.Tables)) / float64(totalTables)
	if frac < giantComponentFraction {
		return GiantComponent{}
	}
	return GiantComponent{Present: true, Component: biggest, Fraction: frac}
}

// danglingFKEdges returns FK edges from a selected table to a parent that is NOT
// in the selection (CLAUDE.md §8.1). These are warned about: the target must
// already satisfy those parents or apply will fail. Results are deterministic.
func danglingFKEdges(selected []engine.Table) []engine.ForeignKey {
	inSel := make(map[engine.TableRef]bool, len(selected))
	for _, t := range selected {
		inSel[t.Ref] = true
	}
	var out []engine.ForeignKey
	for _, t := range selected {
		for _, fk := range t.ForeignKeys {
			if !inSel[fk.Parent] {
				out = append(out, fk)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Child.String() != out[j].Child.String() {
			return out[i].Child.String() < out[j].Child.String()
		}
		return out[i].Parent.String() < out[j].Parent.String()
	})
	return out
}

// --- union-find ---

type unionFind struct {
	parent map[engine.TableRef]engine.TableRef
	rank   map[engine.TableRef]int
}

func newUnionFind() *unionFind {
	return &unionFind{
		parent: map[engine.TableRef]engine.TableRef{},
		rank:   map[engine.TableRef]int{},
	}
}

func (u *unionFind) add(x engine.TableRef) {
	if _, ok := u.parent[x]; !ok {
		u.parent[x] = x
		u.rank[x] = 0
	}
}

func (u *unionFind) find(x engine.TableRef) engine.TableRef {
	for u.parent[x] != x {
		u.parent[x] = u.parent[u.parent[x]] // path halving
		x = u.parent[x]
	}
	return x
}

func (u *unionFind) union(a, b engine.TableRef) {
	u.add(a)
	u.add(b)
	ra, rb := u.find(a), u.find(b)
	if ra == rb {
		return
	}
	if u.rank[ra] < u.rank[rb] {
		ra, rb = rb, ra
	}
	u.parent[rb] = ra
	if u.rank[ra] == u.rank[rb] {
		u.rank[ra]++
	}
}
