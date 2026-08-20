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

// topoOrder returns a valid parents-before-children order for the component and
// the members that participate in a cycle or self-reference. It topologically
// sorts over the NON-cyclic FK edges only — the cyclic edges are removed first
// (identified via classifyCyclicFKs), so the remaining graph is a DAG and EVERY
// member gets a valid position (a cyclic member is ordered by its non-cyclic
// parents). This matters for both the cyclic initial copy and the cyclic streaming
// apply: with the cyclic FK columns loaded/applied NULL then filled, the non-cyclic
// edges are the real dependencies, so ordering by them (e.g. order_items after
// orders) is what keeps the load valid. Ties break by qualified name.
func topoOrder(members []engine.Table, memberSet, inSel map[engine.TableRef]bool) (order, cyclic []engine.TableRef) {
	cyc := classifyCyclicFKs(members)
	cyclicEdge := make(map[string]bool, len(cyc))
	cyclicMember := map[engine.TableRef]bool{}
	for _, c := range cyc {
		cyclicEdge[fkKey(c.FK)] = true
		cyclicMember[c.FK.Child] = true
		cyclicMember[c.FK.Parent] = true
	}
	order = nonCyclicTopoOrder(members, cyclicEdge)
	for ref := range cyclicMember {
		cyclic = append(cyclic, ref)
	}
	sort.Slice(cyclic, func(i, j int) bool { return cyclic[i].String() < cyclic[j].String() })
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
