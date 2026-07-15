package postgres

import (
	"testing"

	"github.com/rudimk/replicare/internal/engine"
)

func col(name string, nullable bool) engine.Column {
	return engine.Column{Name: name, DataType: "integer", Nullable: nullable}
}

// tblC builds a table with explicit columns and FK edges.
func tblC(name string, cols []engine.Column, fks ...engine.ForeignKey) engine.Table {
	return engine.Table{Ref: ref(name), Columns: cols, ForeignKeys: fks}
}

// fkN builds an FK edge with explicit child columns and deferrable flag.
func fkN(name, child, parent string, childCols []string, deferrable bool) engine.ForeignKey {
	return engine.ForeignKey{
		Name:       name,
		Child:      ref(child),
		ChildCols:  childCols,
		Parent:     ref(parent),
		ParentCols: []string{"id"},
		Deferrable: deferrable,
	}
}

func findFK(cs []CyclicFK, name string) (CyclicFK, bool) {
	for _, c := range cs {
		if c.FK.Name == name {
			return c, true
		}
	}
	return CyclicFK{}, false
}

func TestCyclicSelfRefNullable(t *testing.T) {
	// employees.manager_id -> employees.id, nullable, non-deferrable.
	tables := []engine.Table{
		tblC("public.emp",
			[]engine.Column{col("id", false), col("manager_id", true)},
			fkN("emp_mgr", "public.emp", "public.emp", []string{"manager_id"}, false),
		),
	}
	cs := classifyCyclicFKs(tables)
	if len(cs) != 1 {
		t.Fatalf("expected 1 cyclic FK, got %d", len(cs))
	}
	if cs[0].Strategy != CyclicNullThenFill {
		t.Errorf("strategy = %q, want null_then_fill", cs[0].Strategy)
	}
	if anyBlockedCyclicFK(cs) {
		t.Error("nullable self-ref must not be blocked")
	}
}

func TestCyclicSelfRefNotNullNonDeferrableBlocked(t *testing.T) {
	// manager_id NOT NULL, non-deferrable -> the unloadable case.
	tables := []engine.Table{
		tblC("public.emp",
			[]engine.Column{col("id", false), col("manager_id", false)},
			fkN("emp_mgr", "public.emp", "public.emp", []string{"manager_id"}, false),
		),
	}
	cs := classifyCyclicFKs(tables)
	if len(cs) != 1 || cs[0].Strategy != CyclicBlocked {
		t.Fatalf("expected blocked, got %+v", cs)
	}
	if !anyBlockedCyclicFK(cs) {
		t.Error("anyBlockedCyclicFK should be true")
	}
}

func TestCyclicSelfRefNotNullDeferrableOK(t *testing.T) {
	// NOT NULL but DEFERRABLE -> SET CONSTRAINTS DEFERRED, not blocked.
	tables := []engine.Table{
		tblC("public.emp",
			[]engine.Column{col("id", false), col("manager_id", false)},
			fkN("emp_mgr", "public.emp", "public.emp", []string{"manager_id"}, true),
		),
	}
	cs := classifyCyclicFKs(tables)
	if len(cs) != 1 || cs[0].Strategy != CyclicDeferred {
		t.Fatalf("expected deferred, got %+v", cs)
	}
	if anyBlockedCyclicFK(cs) {
		t.Error("deferrable FK must not be blocked")
	}
}

func TestCyclicMutualBothBlocked(t *testing.T) {
	// a.b_id -> b, b.a_id -> a, both NOT NULL non-deferrable.
	tables := []engine.Table{
		tblC("public.a",
			[]engine.Column{col("id", false), col("b_id", false)},
			fkN("a_to_b", "public.a", "public.b", []string{"b_id"}, false),
		),
		tblC("public.b",
			[]engine.Column{col("id", false), col("a_id", false)},
			fkN("b_to_a", "public.b", "public.a", []string{"a_id"}, false),
		),
	}
	cs := classifyCyclicFKs(tables)
	if len(cs) != 2 {
		t.Fatalf("expected 2 cyclic FKs, got %d: %+v", len(cs), cs)
	}
	for _, c := range cs {
		if c.Strategy != CyclicBlocked {
			t.Errorf("%s: strategy = %q, want blocked", c.FK.Name, c.Strategy)
		}
	}
}

func TestCyclicMutualMixed(t *testing.T) {
	// a.b_id -> b (deferrable), b.a_id -> a (nullable, non-deferrable).
	tables := []engine.Table{
		tblC("public.a",
			[]engine.Column{col("id", false), col("b_id", false)},
			fkN("a_to_b", "public.a", "public.b", []string{"b_id"}, true),
		),
		tblC("public.b",
			[]engine.Column{col("id", false), col("a_id", true)},
			fkN("b_to_a", "public.b", "public.a", []string{"a_id"}, false),
		),
	}
	cs := classifyCyclicFKs(tables)
	if len(cs) != 2 {
		t.Fatalf("expected 2, got %d", len(cs))
	}
	if c, ok := findFK(cs, "a_to_b"); !ok || c.Strategy != CyclicDeferred {
		t.Errorf("a_to_b: %+v", c)
	}
	if c, ok := findFK(cs, "b_to_a"); !ok || c.Strategy != CyclicNullThenFill {
		t.Errorf("b_to_a: %+v", c)
	}
	if anyBlockedCyclicFK(cs) {
		t.Error("mixed cycle with deferrable+nullable must not be blocked")
	}
}

func TestCyclicCompositeOneNullableColumn(t *testing.T) {
	// Composite self-ref FK (k1,k2); k1 NOT NULL, k2 nullable -> NULL-then-fill
	// is possible because MATCH SIMPLE unchecks the FK when any column is NULL.
	tables := []engine.Table{
		tblC("public.node",
			[]engine.Column{col("id", false), col("k1", false), col("k2", true)},
			fkN("node_self", "public.node", "public.node", []string{"k1", "k2"}, false),
		),
	}
	cs := classifyCyclicFKs(tables)
	if len(cs) != 1 || cs[0].Strategy != CyclicNullThenFill {
		t.Fatalf("expected null_then_fill, got %+v", cs)
	}
	if !cs[0].Nullable {
		t.Error("composite with a nullable column should report Nullable=true")
	}
}

func TestAcyclicFKsNotClassified(t *testing.T) {
	// Plain parent/child chain has no cycles -> no cyclic FKs.
	tables := []engine.Table{
		tblC("public.a", []engine.Column{col("id", false)}),
		tblC("public.b",
			[]engine.Column{col("id", false), col("a_id", false)},
			fkN("b_to_a", "public.b", "public.a", []string{"a_id"}, false),
		),
	}
	cs := classifyCyclicFKs(tables)
	if len(cs) != 0 {
		t.Fatalf("acyclic schema should yield no cyclic FKs, got %+v", cs)
	}
	if anyBlockedCyclicFK(cs) {
		t.Error("acyclic schema must not be blocked")
	}
}

func TestCyclicIgnoresDanglingParent(t *testing.T) {
	// self-ref would be cyclic, but here the FK points to an unselected table:
	// it's dangling, not cyclic.
	tables := []engine.Table{
		tblC("public.b",
			[]engine.Column{col("id", false), col("a_id", false)},
			fkN("b_to_a", "public.b", "public.a", []string{"a_id"}, false),
		),
	}
	cs := classifyCyclicFKs(tables)
	if len(cs) != 0 {
		t.Fatalf("dangling FK must not be classified as cyclic, got %+v", cs)
	}
}
