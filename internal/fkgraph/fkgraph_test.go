package fkgraph

import (
	"testing"

	"github.com/rudimk/replicare/internal/engine"
)

func ref(name string) engine.TableRef { return engine.TableRef{Schema: "app", Name: name} }

func tbl(name string, fks ...engine.ForeignKey) engine.Table {
	return engine.Table{Ref: ref(name), ForeignKeys: fks}
}

func fk(child, parent string) engine.ForeignKey {
	return engine.ForeignKey{Name: child + "_fk", Child: ref(child), Parent: ref(parent)}
}

func names(refs []engine.TableRef) []string {
	out := make([]string, len(refs))
	for i, r := range refs {
		out[i] = r.Name
	}
	return out
}

func TestComponentsAndTopoOrder(t *testing.T) {
	// orders -> customers (child -> parent); standalone products.
	tables := []engine.Table{
		tbl("orders", fk("orders", "customers")),
		tbl("customers"),
		tbl("products"),
	}
	comps := Components(tables)
	if len(comps) != 2 {
		t.Fatalf("got %d components, want 2", len(comps))
	}
	// The customers/orders component: parent (customers) before child (orders).
	var fkComp *engine.Component
	for i := range comps {
		if len(comps[i].Tables) == 2 {
			fkComp = &comps[i]
		}
	}
	if fkComp == nil {
		t.Fatal("2-table component missing")
	}
	if got := names(fkComp.Order); got[0] != "customers" || got[1] != "orders" {
		t.Errorf("topo order = %v, want [customers orders]", got)
	}
	if fkComp.HasCycle() {
		t.Error("acyclic component reported cyclic")
	}
}

func TestSelfReferenceIsCyclic(t *testing.T) {
	comps := Components([]engine.Table{tbl("employees", fk("employees", "employees"))})
	if len(comps) != 1 || !comps[0].HasCycle() {
		t.Fatalf("self-ref should be one cyclic component, got %+v", comps)
	}
}

func TestMutualCycle(t *testing.T) {
	comps := Components([]engine.Table{
		tbl("a", fk("a", "b")),
		tbl("b", fk("b", "a")),
	})
	if len(comps) != 1 || len(comps[0].Cyclic) != 2 {
		t.Fatalf("mutual cycle should give one component with 2 cyclic members, got %+v", comps)
	}
}

func TestDanglingEdges(t *testing.T) {
	// orders references customers, but customers is NOT selected.
	sel := []engine.Table{tbl("orders", fk("orders", "customers"))}
	d := DanglingEdges(sel)
	if len(d) != 1 || d[0].Parent.Name != "customers" {
		t.Fatalf("dangling = %+v, want 1 edge to customers", d)
	}
	// When both are selected, no dangling edge.
	if got := DanglingEdges([]engine.Table{tbl("orders", fk("orders", "customers")), tbl("customers")}); len(got) != 0 {
		t.Errorf("expected no dangling edges, got %+v", got)
	}
}

func TestDetectGiant(t *testing.T) {
	// One 4-table component + 2 singletons = 6 tables; 4/6 = 0.67 >= 0.6.
	tables := []engine.Table{
		tbl("a"), tbl("b", fk("b", "a")), tbl("c", fk("c", "a")), tbl("d", fk("d", "a")),
		tbl("x"), tbl("y"),
	}
	comps := Components(tables)
	g := DetectGiant(comps, len(tables))
	if !g.Present {
		t.Fatalf("expected a giant component (4/6 tables)")
	}
	// Below the 5-table floor: no warning.
	if DetectGiant(Components([]engine.Table{tbl("a"), tbl("b", fk("b", "a"))}), 2).Present {
		t.Error("should not warn on a tiny selection")
	}
}
