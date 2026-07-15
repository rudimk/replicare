package postgres

import (
	"testing"

	"github.com/rudimk/replicare/internal/engine"
)

func TestClassifyTypes(t *testing.T) {
	cases := []struct {
		src, tgt string
		want     CompatLevel
	}{
		// identical
		{"integer", "integer", CompatIdentical},
		{"character varying(50)", "character varying(50)", CompatIdentical},
		// integer family
		{"integer", "bigint", CompatWiden},
		{"smallint", "integer", CompatWiden},
		{"bigint", "integer", CompatRisky},
		{"bigint", "smallint", CompatRisky},
		// text / varchar length
		{"character varying(30)", "character varying(50)", CompatWiden},
		{"character varying(50)", "character varying(30)", CompatRisky},
		{"character varying(50)", "text", CompatWiden},
		{"integer", "text", CompatWiden},
		{"uuid", "text", CompatWiden},
		{"integer", "character varying(5)", CompatRisky},
		// numeric
		{"numeric(10,2)", "numeric(12,2)", CompatWiden},
		{"numeric(10,2)", "numeric(8,2)", CompatRisky},
		{"numeric(10,2)", "numeric", CompatWiden},
		{"numeric", "numeric(10,2)", CompatRisky},
		// float
		{"real", "double precision", CompatWiden},
		{"double precision", "real", CompatRisky},
		// cross-family temporal
		{"timestamp without time zone", "timestamp with time zone", CompatRisky},
		{"timestamp with time zone", "timestamp without time zone", CompatRisky},
		{"date", "timestamp without time zone", CompatWiden},
		{"date", "timestamp with time zone", CompatRisky},
		{"timestamp without time zone", "date", CompatRisky},
		// int -> numeric
		{"bigint", "numeric", CompatWiden},
		{"bigint", "numeric(4,0)", CompatRisky},
		{"integer", "numeric(20,0)", CompatWiden},
		// int -> float
		{"bigint", "double precision", CompatRisky},
		// json
		{"json", "jsonb", CompatWiden},
		{"jsonb", "json", CompatWiden},
		// incompatible
		{"boolean", "integer", CompatIncompatible},
		{"integer", "boolean", CompatIncompatible},
		{"uuid", "integer", CompatIncompatible},
		{"bytea", "integer", CompatIncompatible},
		// unrecognized different -> risky (can't prove incompatible)
		{"hstore", "citext", CompatRisky},
		{"mytype", "text", CompatWiden}, // target text holds anything
		// temporal precision parsing (parens mid-string)
		{"timestamp(6) without time zone", "timestamp(3) without time zone", CompatRisky},
		{"timestamp(3) without time zone", "timestamp(6) without time zone", CompatWiden},
	}
	for _, c := range cases {
		got, detail := classifyTypes(c.src, c.tgt)
		if got != c.want {
			t.Errorf("classifyTypes(%q, %q) = %s (%q), want %s", c.src, c.tgt, got, detail, c.want)
		}
	}
}

func TestParseType(t *testing.T) {
	cases := []struct {
		in   string
		base string
		mods []int
	}{
		{"integer", "integer", nil},
		{"character varying(50)", "character varying", []int{50}},
		{"numeric(10,2)", "numeric", []int{10, 2}},
		{"timestamp(3) without time zone", "timestamp without time zone", []int{3}},
		{"TEXT", "text", nil},
	}
	for _, c := range cases {
		p := parseType(c.in)
		if p.base != c.base {
			t.Errorf("parseType(%q).base = %q, want %q", c.in, p.base, c.base)
		}
		if !eqInts(p.mods, c.mods) {
			t.Errorf("parseType(%q).mods = %v, want %v", c.in, p.mods, c.mods)
		}
	}
}

func eqInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestCompareTableMatched(t *testing.T) {
	src := engine.Table{
		Ref: ref("public.t"),
		Columns: []engine.Column{
			col2("id", "integer", false),
			col2("name", "character varying(50)", true),
			col2("amount", "bigint", true),
		},
	}
	tgt := engine.Table{
		Ref: ref("public.t"),
		Columns: []engine.Column{
			col2("id", "integer", false),
			col2("name", "text", true),      // widen
			col2("amount", "integer", true), // risky (narrowing)
		},
	}
	tc := compareTable(src, &tgt)
	if !tc.TargetExists || tc.Blocks() {
		t.Fatalf("expected non-blocking compat, got blocks=%v exists=%v", tc.Blocks(), tc.TargetExists)
	}
	byCol := map[string]ColumnCompat{}
	for _, c := range tc.Columns {
		byCol[c.Column] = c
	}
	if byCol["name"].Level != CompatWiden {
		t.Errorf("name level = %s", byCol["name"].Level)
	}
	if byCol["amount"].Level != CompatRisky {
		t.Errorf("amount level = %s", byCol["amount"].Level)
	}
}

func TestCompareTableMissingTarget(t *testing.T) {
	src := engine.Table{Ref: ref("public.t"), Columns: []engine.Column{col2("id", "integer", false)}}
	tc := compareTable(src, nil)
	if tc.TargetExists {
		t.Error("TargetExists should be false")
	}
	if !tc.Blocks() {
		t.Error("missing target table must block")
	}
}

func TestCompareTableMissingColumnBlocks(t *testing.T) {
	src := engine.Table{
		Ref: ref("public.t"),
		Columns: []engine.Column{
			col2("id", "integer", false),
			col2("extra", "integer", true),
		},
	}
	tgt := engine.Table{Ref: ref("public.t"), Columns: []engine.Column{col2("id", "integer", false)}}
	tc := compareTable(src, &tgt)
	if len(tc.MissingInTarget) != 1 || tc.MissingInTarget[0] != "extra" {
		t.Errorf("MissingInTarget = %v", tc.MissingInTarget)
	}
	if !tc.Blocks() {
		t.Error("a source column absent in target must block")
	}
}

func TestCompareTableGeneratedColumnSkipped(t *testing.T) {
	src := engine.Table{
		Ref: ref("public.t"),
		Columns: []engine.Column{
			col2("id", "integer", false),
			{Name: "fullname", DataType: "text", Nullable: true, Generated: true},
		},
	}
	// Target lacks the generated column; that must NOT block (we don't send it).
	tgt := engine.Table{Ref: ref("public.t"), Columns: []engine.Column{col2("id", "integer", false)}}
	tc := compareTable(src, &tgt)
	if len(tc.MissingInTarget) != 0 {
		t.Errorf("generated column should not be required in target: %v", tc.MissingInTarget)
	}
	if tc.Blocks() {
		t.Error("generated-only mismatch must not block")
	}
}

func TestCompareTableExtraNotNullWarn(t *testing.T) {
	src := engine.Table{Ref: ref("public.t"), Columns: []engine.Column{col2("id", "integer", false)}}
	tgt := engine.Table{
		Ref: ref("public.t"),
		Columns: []engine.Column{
			col2("id", "integer", false),
			col2("required", "integer", false), // target NOT NULL, source has no data
			col2("optional", "integer", true),  // nullable extra is fine
		},
	}
	tc := compareTable(src, &tgt)
	if len(tc.ExtraNotNull) != 1 || tc.ExtraNotNull[0] != "required" {
		t.Errorf("ExtraNotNull = %v, want [required]", tc.ExtraNotNull)
	}
	// ExtraNotNull is advisory (warn), not a block.
	if tc.Blocks() {
		t.Error("extra NOT NULL target column should warn, not block")
	}
}

func col2(name, typ string, nullable bool) engine.Column {
	return engine.Column{Name: name, DataType: typ, Nullable: nullable}
}
