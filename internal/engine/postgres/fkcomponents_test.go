package postgres

import (
	"testing"

	"github.com/rudimk/replicare/internal/engine"
)

// ref parses "schema.table" into a TableRef.
func ref(s string) engine.TableRef {
	for i := 0; i < len(s); i++ {
		if s[i] == '.' {
			return engine.TableRef{Schema: s[:i], Name: s[i+1:]}
		}
	}
	return engine.TableRef{Name: s}
}

// fk builds a single-column FK edge child -> parent.
func fk(child, parent string) engine.ForeignKey {
	return engine.ForeignKey{
		Name:       child + "_to_" + parent,
		Child:      ref(child),
		ChildCols:  []string{"parent_id"},
		Parent:     ref(parent),
		ParentCols: []string{"id"},
	}
}

// tbl builds a table with the given FK edges.
func tbl(name string, fks ...engine.ForeignKey) engine.Table {
	return engine.Table{Ref: ref(name), ForeignKeys: fks}
}

func refNames(rs []engine.TableRef) []string {
	out := make([]string, len(rs))
	for i, r := range rs {
		out[i] = r.String()
	}
	return out
}

func TestComponentsIndependent(t *testing.T) {
	sel := []engine.Table{
		tbl("public.a"),
		tbl("public.b", fk("public.b", "public.a")),
		tbl("public.x"),
		tbl("public.y", fk("public.y", "public.x")),
	}
	comps := computeComponents(sel)
	if len(comps) != 2 {
		t.Fatalf("expected 2 components, got %d: %+v", len(comps), comps)
	}
	// Sorted by first member: {a,b} then {x,y}.
	if !eq(refNames(comps[0].Tables), []string{"public.a", "public.b"}) {
		t.Errorf("comp0 = %v", refNames(comps[0].Tables))
	}
	if !eq(refNames(comps[1].Tables), []string{"public.x", "public.y"}) {
		t.Errorf("comp1 = %v", refNames(comps[1].Tables))
	}
}

func TestComponentTopoOrderParentsFirst(t *testing.T) {
	// chain: c -> b -> a  (a is root parent)
	sel := []engine.Table{
		tbl("public.a"),
		tbl("public.b", fk("public.b", "public.a")),
		tbl("public.c", fk("public.c", "public.b")),
	}
	comps := computeComponents(sel)
	if len(comps) != 1 {
		t.Fatalf("expected 1 component, got %d", len(comps))
	}
	if comps[0].HasCycle() {
		t.Fatalf("chain should not be cyclic: %+v", comps[0].Cyclic)
	}
	if !eq(refNames(comps[0].Order), []string{"public.a", "public.b", "public.c"}) {
		t.Errorf("topo order = %v, want a,b,c", refNames(comps[0].Order))
	}
}

func TestComponentDiamondTopoOrder(t *testing.T) {
	// diamond: b->a, c->a, d->b, d->c. a first, d last.
	sel := []engine.Table{
		tbl("public.a"),
		tbl("public.b", fk("public.b", "public.a")),
		tbl("public.c", fk("public.c", "public.a")),
		tbl("public.d", fk("public.d", "public.b"), fk("public.d", "public.c")),
	}
	comps := computeComponents(sel)
	if len(comps) != 1 || comps[0].HasCycle() {
		t.Fatalf("expected 1 acyclic component, got %+v", comps)
	}
	order := refNames(comps[0].Order)
	// a must come before b,c; b,c before d.
	pos := map[string]int{}
	for i, n := range order {
		pos[n] = i
	}
	before := func(x, y string) bool { return pos[x] < pos[y] }
	if !before("public.a", "public.b") || !before("public.a", "public.c") ||
		!before("public.b", "public.d") || !before("public.c", "public.d") {
		t.Errorf("invalid topo order: %v", order)
	}
}

func TestComponentSelfReferenceIsCyclic(t *testing.T) {
	sel := []engine.Table{
		tbl("public.emp", fk("public.emp", "public.emp")), // manager_id -> emp.id
	}
	comps := computeComponents(sel)
	if len(comps) != 1 {
		t.Fatalf("expected 1 component, got %d", len(comps))
	}
	if !comps[0].HasCycle() {
		t.Fatal("self-reference should be cyclic")
	}
	if !eq(refNames(comps[0].Cyclic), []string{"public.emp"}) {
		t.Errorf("cyclic = %v", refNames(comps[0].Cyclic))
	}
}

func TestComponentTwoCycleIsCyclic(t *testing.T) {
	// a -> b and b -> a : mutual cycle.
	sel := []engine.Table{
		tbl("public.a", fk("public.a", "public.b")),
		tbl("public.b", fk("public.b", "public.a")),
	}
	comps := computeComponents(sel)
	if len(comps) != 1 {
		t.Fatalf("expected 1 component, got %d", len(comps))
	}
	if !comps[0].HasCycle() {
		t.Fatal("mutual FK should be cyclic")
	}
	if !eq(refNames(comps[0].Cyclic), []string{"public.a", "public.b"}) {
		t.Errorf("cyclic = %v", refNames(comps[0].Cyclic))
	}
}

func TestDanglingFKEdges(t *testing.T) {
	// b references a, but a is NOT selected -> dangling.
	sel := []engine.Table{
		tbl("public.b", fk("public.b", "public.a")),
		tbl("public.c", fk("public.c", "public.b")),
	}
	dangling := danglingFKEdges(sel)
	if len(dangling) != 1 {
		t.Fatalf("expected 1 dangling edge, got %d: %+v", len(dangling), dangling)
	}
	if dangling[0].Child.String() != "public.b" || dangling[0].Parent.String() != "public.a" {
		t.Errorf("unexpected dangling edge: %+v", dangling[0])
	}
}

func TestDanglingEdgeDoesNotMergeComponents(t *testing.T) {
	// b->a(excluded) and c->d(excluded): b and c stay separate components.
	sel := []engine.Table{
		tbl("public.b", fk("public.b", "public.a")),
		tbl("public.c", fk("public.c", "public.d")),
	}
	comps := computeComponents(sel)
	if len(comps) != 2 {
		t.Fatalf("dangling edges must not merge components; got %d", len(comps))
	}
}

func TestDetectGiantComponent(t *testing.T) {
	// 6 tables: 5 in one FK chain, 1 standalone -> 5/6 = 0.83 >= 0.6.
	sel := []engine.Table{
		tbl("public.a"),
		tbl("public.b", fk("public.b", "public.a")),
		tbl("public.c", fk("public.c", "public.b")),
		tbl("public.d", fk("public.d", "public.c")),
		tbl("public.e", fk("public.e", "public.d")),
		tbl("public.solo"),
	}
	comps := computeComponents(sel)
	g := detectGiantComponent(comps, len(sel))
	if !g.Present {
		t.Fatalf("expected giant component, got none (comps=%d)", len(comps))
	}
	if len(g.Component.Tables) != 5 {
		t.Errorf("giant size = %d, want 5", len(g.Component.Tables))
	}
	if g.Fraction < 0.8 {
		t.Errorf("fraction = %f, want >= 0.8", g.Fraction)
	}
}

func TestNoGiantOnSmallSelection(t *testing.T) {
	// Below minTablesForGiantWarning: never warn even if fully connected.
	sel := []engine.Table{
		tbl("public.a"),
		tbl("public.b", fk("public.b", "public.a")),
		tbl("public.c", fk("public.c", "public.b")),
	}
	comps := computeComponents(sel)
	if detectGiantComponent(comps, len(sel)).Present {
		t.Error("small selection should not trigger giant-component warning")
	}
}

func TestNoGiantWhenBalanced(t *testing.T) {
	// 6 tables in 3 equal components of 2: max fraction 2/6 = 0.33 < 0.6.
	sel := []engine.Table{
		tbl("public.a1"), tbl("public.a2", fk("public.a2", "public.a1")),
		tbl("public.b1"), tbl("public.b2", fk("public.b2", "public.b1")),
		tbl("public.c1"), tbl("public.c2", fk("public.c2", "public.c1")),
	}
	comps := computeComponents(sel)
	if detectGiantComponent(comps, len(sel)).Present {
		t.Error("balanced components should not trigger giant-component warning")
	}
}
