package postgres

import (
	"testing"

	"github.com/rudimk/replicare/internal/engine"
)

// keyed sets a primary key on a table built by tblC.
func keyed(t engine.Table, pkCols ...string) engine.Table {
	t.PrimaryKey = &engine.Key{Name: t.Ref.Name + "_pkey", Columns: pkCols, IsPrimary: true}
	return t
}

func schemaOf(ts ...engine.Table) *engine.Schema { return &engine.Schema{Tables: ts} }

func blockFindings(p Preflight) []engine.Finding {
	var out []engine.Finding
	for _, f := range p.Findings {
		if f.Severity == engine.SevBlock {
			out = append(out, f)
		}
	}
	return out
}

func hasFinding(p Preflight, sev engine.Severity, category string) bool {
	for _, f := range p.Findings {
		if f.Severity == sev && f.Category == category {
			return true
		}
	}
	return false
}

func TestPreflightHappyPath(t *testing.T) {
	src := schemaOf(
		keyed(tblC("public.customers", []engine.Column{col2("id", "integer", false), col2("name", "text", true)}), "id"),
		keyed(tblC("public.orders",
			[]engine.Column{col2("id", "integer", false), col2("customer_id", "integer", false)},
			fkN("orders_cust", "public.orders", "public.customers", []string{"customer_id"}, false),
		), "id"),
	)
	tgt := schemaOf(
		keyed(tblC("public.customers", []engine.Column{col2("id", "integer", false), col2("name", "text", true)}), "id"),
		keyed(tblC("public.orders", []engine.Column{col2("id", "integer", false), col2("customer_id", "integer", false)}), "id"),
	)
	p := buildPreflight("s1", 90600, 170000, src, tgt)

	if p.Blocked() {
		t.Fatalf("happy path should not block: %+v", blockFindings(p))
	}
	if len(p.Replicable) != 2 {
		t.Errorf("expected 2 replicable tables, got %d", len(p.Replicable))
	}
	if len(p.Components) != 1 {
		t.Errorf("expected 1 FK component, got %d", len(p.Components))
	}
}

func TestPreflightNoKeySkipped(t *testing.T) {
	src := schemaOf(
		keyed(tblC("public.a", []engine.Column{col2("id", "integer", false)}), "id"),
		tblC("public.logs", []engine.Column{col2("msg", "text", true)}), // no PK
	)
	tgt := schemaOf(
		keyed(tblC("public.a", []engine.Column{col2("id", "integer", false)}), "id"),
		tblC("public.logs", []engine.Column{col2("msg", "text", true)}),
	)
	p := buildPreflight("s", 90600, 170000, src, tgt)

	if len(p.Skipped) != 1 || p.Skipped[0].Table.String() != "public.logs" {
		t.Fatalf("expected public.logs skipped, got %+v", p.Skipped)
	}
	if !hasFinding(p, engine.SevWarn, "no-key") {
		t.Error("expected a no-key warning")
	}
	if len(p.Replicable) != 1 {
		t.Errorf("no-key table must be excluded from replicable, got %d", len(p.Replicable))
	}
	if p.Blocked() {
		t.Error("a skipped no-key table is a warning, not a block")
	}
}

func TestPreflightMissingTargetTableBlocks(t *testing.T) {
	src := schemaOf(keyed(tblC("public.a", []engine.Column{col2("id", "integer", false)}), "id"))
	tgt := schemaOf() // empty target
	p := buildPreflight("s", 90600, 170000, src, tgt)

	if !p.Blocked() {
		t.Fatal("missing target table must block")
	}
	if !hasFinding(p, engine.SevBlock, "missing-table") {
		t.Error("expected a missing-table block finding")
	}
}

func TestPreflightIncompatibleColumnBlocks(t *testing.T) {
	src := schemaOf(keyed(tblC("public.a",
		[]engine.Column{col2("id", "integer", false), col2("flag", "boolean", true)}), "id"))
	tgt := schemaOf(keyed(tblC("public.a",
		[]engine.Column{col2("id", "integer", false), col2("flag", "integer", true)}), "id")) // bool->int
	p := buildPreflight("s", 90600, 170000, src, tgt)

	if !p.Blocked() {
		t.Fatal("incompatible column must block")
	}
	if !hasFinding(p, engine.SevBlock, "type") {
		t.Error("expected a type block finding")
	}
}

func TestPreflightRiskyColumnWarnsOnly(t *testing.T) {
	src := schemaOf(keyed(tblC("public.a",
		[]engine.Column{col2("id", "integer", false), col2("amount", "bigint", true)}), "id"))
	tgt := schemaOf(keyed(tblC("public.a",
		[]engine.Column{col2("id", "integer", false), col2("amount", "integer", true)}), "id")) // int8->int4
	p := buildPreflight("s", 90600, 170000, src, tgt)

	if p.Blocked() {
		t.Fatalf("risky narrowing should warn, not block: %+v", blockFindings(p))
	}
	if !hasFinding(p, engine.SevWarn, "type") {
		t.Error("expected a type warning")
	}
}

func TestPreflightBlockedCyclicFK(t *testing.T) {
	// self-ref NOT NULL non-deferrable -> blocked.
	src := schemaOf(keyed(tblC("public.emp",
		[]engine.Column{col2("id", "integer", false), col2("manager_id", "integer", false)},
		fkN("emp_mgr", "public.emp", "public.emp", []string{"manager_id"}, false),
	), "id"))
	tgt := schemaOf(keyed(tblC("public.emp",
		[]engine.Column{col2("id", "integer", false), col2("manager_id", "integer", false)}), "id"))
	p := buildPreflight("s", 90600, 170000, src, tgt)

	if !p.Blocked() {
		t.Fatal("NOT NULL non-deferrable self-ref FK must block")
	}
	if !hasFinding(p, engine.SevBlock, "cyclic-fk") {
		t.Error("expected a cyclic-fk block finding")
	}
}

func TestPreflightNullableCyclicInfoNotBlock(t *testing.T) {
	src := schemaOf(keyed(tblC("public.emp",
		[]engine.Column{col2("id", "integer", false), col2("manager_id", "integer", true)},
		fkN("emp_mgr", "public.emp", "public.emp", []string{"manager_id"}, false),
	), "id"))
	tgt := schemaOf(keyed(tblC("public.emp",
		[]engine.Column{col2("id", "integer", false), col2("manager_id", "integer", true)}), "id"))
	p := buildPreflight("s", 90600, 170000, src, tgt)

	if p.Blocked() {
		t.Fatalf("nullable self-ref FK must not block: %+v", blockFindings(p))
	}
	if !hasFinding(p, engine.SevInfo, "cyclic-fk") {
		t.Error("expected an info-level cyclic-fk finding describing the strategy")
	}
}

func TestPreflightGiantComponentWarn(t *testing.T) {
	// 5-table chain + 1 solo (6 total) -> giant.
	var srcTs, tgtTs []engine.Table
	cols := []engine.Column{col2("id", "integer", false), col2("parent_id", "integer", true)}
	chain := []string{"a", "b", "c", "d", "e"}
	for i, n := range chain {
		var fks []engine.ForeignKey
		if i > 0 {
			fks = append(fks, fkN(n+"_fk", "public."+n, "public."+chain[i-1], []string{"parent_id"}, false))
		}
		srcTs = append(srcTs, keyed(tblC("public."+n, cols, fks...), "id"))
		tgtTs = append(tgtTs, keyed(tblC("public."+n, cols), "id"))
	}
	srcTs = append(srcTs, keyed(tblC("public.solo", []engine.Column{col2("id", "integer", false)}), "id"))
	tgtTs = append(tgtTs, keyed(tblC("public.solo", []engine.Column{col2("id", "integer", false)}), "id"))

	p := buildPreflight("s", 90600, 170000, schemaOf(srcTs...), schemaOf(tgtTs...))
	if !p.Giant.Present {
		t.Fatal("expected a giant component")
	}
	if !hasFinding(p, engine.SevWarn, "giant-component") {
		t.Error("expected a giant-component warning")
	}
	if p.Blocked() {
		t.Error("giant component is a warning, not a block")
	}
}

func TestPreflightDanglingFKWarn(t *testing.T) {
	// b references a, but a is not selected (only b is).
	src := schemaOf(keyed(tblC("public.b",
		[]engine.Column{col2("id", "integer", false), col2("a_id", "integer", true)},
		fkN("b_to_a", "public.b", "public.a", []string{"a_id"}, false),
	), "id"))
	tgt := schemaOf(keyed(tblC("public.b",
		[]engine.Column{col2("id", "integer", false), col2("a_id", "integer", true)}), "id"))
	p := buildPreflight("s", 90600, 170000, src, tgt)

	if !hasFinding(p, engine.SevWarn, "dangling-fk") {
		t.Error("expected a dangling-fk warning")
	}
	if p.Blocked() {
		t.Error("dangling FK is a warning, not a block")
	}
}

func TestPreflightCounts(t *testing.T) {
	src := schemaOf(
		keyed(tblC("public.a",
			[]engine.Column{col2("id", "integer", false), col2("amount", "bigint", true)}), "id"),
		tblC("public.logs", []engine.Column{col2("msg", "text", true)}),
	)
	tgt := schemaOf(
		keyed(tblC("public.a",
			[]engine.Column{col2("id", "integer", false), col2("amount", "integer", true)}), "id"),
		tblC("public.logs", []engine.Column{col2("msg", "text", true)}),
	)
	p := buildPreflight("s", 90600, 170000, src, tgt)
	_, warn, block := p.Counts()
	if warn < 2 { // no-key + risky type
		t.Errorf("expected >=2 warnings, got %d", warn)
	}
	if block != 0 {
		t.Errorf("expected 0 blocks, got %d", block)
	}
}
