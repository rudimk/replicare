// Package fkgraph is engine-neutral FK connected-component analysis (CLAUDE.md
// §8.1): within a sync's selected tables it partitions into FK connected
// components (no FK edge crosses a boundary), computes each component's
// topological apply order (parents before children), and reports cyclic members,
// the giant-component case, and dangling edges to excluded tables. Pure
// structural analysis over an introspected schema — no I/O.
//
// The Postgres engine has an equivalent internal copy predating this package;
// new engines (MySQL) use this shared version, and Postgres can migrate to it.
package fkgraph

import (
	"sort"

	"github.com/rudimk/replicare/internal/engine"
)

// giantComponentFraction and minTablesForGiantWarning gate the "one component
// dominates the selection" warning (CLAUDE.md §8.1): we only warn once the
// selection is large enough that lost parallelism matters.
const (
	giantComponentFraction   = 0.6
	minTablesForGiantWarning = 5
)

// Components partitions selected tables into FK connected components, each with a
// topological apply order and its cyclic members. Only FK edges whose parent is
// also selected connect components; edges to excluded tables are dangling (see
// DanglingEdges) and never merge components. Output order is deterministic.
func Components(selected []engine.Table) []engine.Component {
	inSel := make(map[engine.TableRef]bool, len(selected))
	for _, t := range selected {
		inSel[t.Ref] = true
	}

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

	groups := map[engine.TableRef][]engine.Table{}
	for _, t := range selected {
		root := uf.find(t.Ref)
		groups[root] = append(groups[root], t)
	}

	comps := make([]engine.Component, 0, len(groups))
	for _, members := range groups {
		comps = append(comps, buildComponent(members, inSel))
	}
	sort.Slice(comps, func(i, j int) bool {
		return comps[i].Tables[0].String() < comps[j].Tables[0].String()
	})
	return comps
}

func buildComponent(members []engine.Table, inSel map[engine.TableRef]bool) engine.Component {
	refs := make([]engine.TableRef, 0, len(members))
	memberSet := make(map[engine.TableRef]bool, len(members))
	for _, m := range members {
		refs = append(refs, m.Ref)
		memberSet[m.Ref] = true
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i].String() < refs[j].String() })
	order, cyclic := topoOrder(members, memberSet, inSel)
	return engine.Component{Tables: refs, Order: order, Cyclic: cyclic}
}

// topoOrder returns a topological order (parents before children) via Kahn's
// algorithm plus the members that remain in cycles (incl. self-references).
// Cyclic members are appended after the acyclic prefix so callers get a total
// order. Ties break by qualified name for determinism.
func topoOrder(members []engine.Table, memberSet, inSel map[engine.TableRef]bool) (order, cyclic []engine.TableRef) {
	children := map[engine.TableRef]map[engine.TableRef]bool{}
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
				continue
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
		kids := make([]engine.TableRef, 0, len(children[n]))
		for c := range children[n] {
			kids = append(kids, c)
		}
		sort.Slice(kids, func(i, j int) bool { return kids[i].String() < kids[j].String() })
		for _, c := range kids {
			indeg[c]--
			if indeg[c] == 0 && !selfCycle[c] {
				queue = append(queue, c)
				sort.Slice(queue, func(i, j int) bool { return queue[i].String() < queue[j].String() })
			}
		}
	}

	for _, m := range members {
		if !emitted[m.Ref] {
			cyclic = append(cyclic, m.Ref)
		}
	}
	sort.Slice(cyclic, func(i, j int) bool { return cyclic[i].String() < cyclic[j].String() })
	order = append(order, cyclic...)
	return order, cyclic
}

// Giant flags the case where one component dominates the selection, collapsing
// parallelism. Present is false when none dominates or the selection is small.
type Giant struct {
	Present   bool
	Component *engine.Component
	Fraction  float64
}

// DetectGiant reports the dominant component if one covers at least 60% of a
// selection of at least 5 tables.
func DetectGiant(comps []engine.Component, totalTables int) Giant {
	if totalTables < minTablesForGiantWarning || len(comps) == 0 {
		return Giant{}
	}
	biggest := &comps[0]
	for i := range comps {
		if len(comps[i].Tables) > len(biggest.Tables) {
			biggest = &comps[i]
		}
	}
	frac := float64(len(biggest.Tables)) / float64(totalTables)
	if frac < giantComponentFraction {
		return Giant{}
	}
	return Giant{Present: true, Component: biggest, Fraction: frac}
}

// DanglingEdges returns FK edges from a selected table to a parent NOT in the
// selection (CLAUDE.md §8.1): the target must already satisfy those parents or
// apply fails. Deterministic order.
func DanglingEdges(selected []engine.Table) []engine.ForeignKey {
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
	return &unionFind{parent: map[engine.TableRef]engine.TableRef{}, rank: map[engine.TableRef]int{}}
}

func (u *unionFind) add(x engine.TableRef) {
	if _, ok := u.parent[x]; !ok {
		u.parent[x] = x
		u.rank[x] = 0
	}
}

func (u *unionFind) find(x engine.TableRef) engine.TableRef {
	for u.parent[x] != x {
		u.parent[x] = u.parent[u.parent[x]]
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
