package engine

import (
	"strings"
	"testing"
)

func tref(name string) TableRef { return TableRef{Schema: "public", Name: name} }

// oneTable builds a single-table schema for the classifier.
func oneTable(name string, cols []Column, pk *Key, uniques ...Key) *Schema {
	return &Schema{Tables: []Table{{Ref: tref(name), Columns: cols, PrimaryKey: pk, UniqueKeys: uniques}}}
}

func pkey(cols ...string) *Key { return &Key{Name: "pk", Columns: cols, IsPrimary: true} }

func TestMeshIDAllocation_FlagsLocallyAllocatedIntegerKeys(t *testing.T) {
	cases := []struct {
		name       string
		schema     *Schema
		wantBlocks int
		wantSubstr []string // substrings that must appear across the findings
	}{
		{
			name: "identity int PK blocks",
			schema: oneTable("orders",
				[]Column{{Name: "id", DataType: "integer", Identity: true}, {Name: "note", DataType: "text"}},
				pkey("id")),
			wantBlocks: 1,
			wantSubstr: []string{"public.orders", "id", "primary key", "UUID", "32-bit"},
		},
		{
			name: "serial (DefaultSequence) int4 PK blocks, narrow note",
			schema: oneTable("widgets",
				[]Column{{Name: "id", DataType: "integer", DefaultSequence: true}},
				pkey("id")),
			wantBlocks: 1,
			wantSubstr: []string{"public.widgets", "primary key", "32-bit or narrower"},
		},
		{
			name: "bigint identity PK blocks, no narrow note",
			schema: oneTable("events",
				[]Column{{Name: "id", DataType: "bigint", Identity: true}},
				pkey("id")),
			wantBlocks: 1,
			wantSubstr: []string{"public.events"},
		},
		{
			name: "AUTO_INCREMENT unsigned int blocks (MySQL shape)",
			schema: oneTable("mt",
				[]Column{{Name: "id", DataType: "int unsigned", Identity: true}},
				pkey("id")),
			wantBlocks: 1,
			wantSubstr: []string{"public.mt", "32-bit or narrower"},
		},
		{
			name: "secondary unique on a serial column blocks (UUID PK)",
			schema: oneTable("orders",
				[]Column{
					{Name: "id", DataType: "uuid"},
					{Name: "order_number", DataType: "bigint", DefaultSequence: true},
				},
				pkey("id"),
				Key{Name: "orders_order_number_key", Columns: []string{"order_number"}}),
			wantBlocks: 1,
			wantSubstr: []string{"order_number", "unique key"},
		},
		{
			name: "composite PK containing a serial column blocks",
			schema: oneTable("tenant_seq",
				[]Column{
					{Name: "tenant_id", DataType: "uuid"},
					{Name: "local_seq", DataType: "integer", DefaultSequence: true},
				},
				pkey("tenant_id", "local_seq")),
			wantBlocks: 1,
			wantSubstr: []string{"local_seq", "primary key"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fs := MeshIDAllocationFindings(tc.schema)
			blocks := 0
			var all strings.Builder
			for _, f := range fs {
				if f.Severity == SevBlock {
					blocks++
				}
				if f.Category != MeshIDAllocCategory {
					t.Errorf("finding category = %q, want %q", f.Category, MeshIDAllocCategory)
				}
				all.WriteString(f.Message)
				all.WriteByte('\n')
			}
			if blocks != tc.wantBlocks {
				t.Errorf("block findings = %d, want %d: %s", blocks, tc.wantBlocks, all.String())
			}
			for _, sub := range tc.wantSubstr {
				if !strings.Contains(all.String(), sub) {
					t.Errorf("findings missing %q:\n%s", sub, all.String())
				}
			}
		})
	}
}

func TestMeshIDAllocation_AllowsCollisionFreeKeys(t *testing.T) {
	cases := []struct {
		name   string
		schema *Schema
	}{
		{"uuid PK", oneTable("u",
			[]Column{{Name: "id", DataType: "uuid"}}, pkey("id"))},
		{"natural text PK", oneTable("n",
			[]Column{{Name: "code", DataType: "text"}}, pkey("code"))},
		{"app-assigned bigint PK (not DB-allocated)", oneTable("s",
			[]Column{{Name: "id", DataType: "bigint"}}, pkey("id"))},
		{"identity int that is NOT in any key", oneTable("log",
			[]Column{{Name: "seq", DataType: "integer", Identity: true}, {Name: "code", DataType: "text"}},
			pkey("code"))},
		{"no key at all", oneTable("nopk",
			[]Column{{Name: "a", DataType: "integer", Identity: true}}, nil)},
		{"uuid PK + uuid unique", oneTable("uu",
			[]Column{{Name: "id", DataType: "uuid"}, {Name: "ext", DataType: "uuid"}},
			pkey("id"), Key{Name: "uu_ext_key", Columns: []string{"ext"}})},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if fs := MeshIDAllocationFindings(tc.schema); len(fs) != 0 {
				t.Errorf("expected no findings, got %d: %+v", len(fs), fs)
			}
		})
	}
}

// A column in BOTH the PK and a unique key yields exactly one finding (reported once,
// with the primary-key role).
func TestMeshIDAllocation_DedupsColumnAcrossKeys(t *testing.T) {
	s := oneTable("t",
		[]Column{{Name: "id", DataType: "integer", Identity: true}},
		pkey("id"),
		Key{Name: "t_id_key", Columns: []string{"id"}})
	fs := MeshIDAllocationFindings(s)
	if len(fs) != 1 {
		t.Fatalf("want exactly one finding, got %d: %+v", len(fs), fs)
	}
	if !strings.Contains(fs[0].Message, "primary key") {
		t.Errorf("finding should cite the primary key role: %s", fs[0].Message)
	}
}

func TestClassifyIntType(t *testing.T) {
	cases := []struct {
		in         string
		wantInt    bool
		wantNarrow bool
	}{
		{"integer", true, true},
		{"int", true, true},
		{"int4", true, true},
		{"int(11)", true, true},
		{"int unsigned", true, true},
		{"int(10) unsigned", true, true},
		{"smallint", true, true},
		{"mediumint", true, true},
		{"tinyint", true, true},
		{"serial", true, true},
		{"bigint", true, false},
		{"int8", true, false},
		{"bigint unsigned", true, false},
		{"bigserial", true, false},
		{"uuid", false, false},
		{"text", false, false},
		{"numeric(10,2)", false, false},
		{"timestamptz", false, false},
	}
	for _, tc := range cases {
		gotInt, gotNarrow := classifyIntType(tc.in)
		if gotInt != tc.wantInt || gotNarrow != tc.wantNarrow {
			t.Errorf("classifyIntType(%q) = (%v,%v), want (%v,%v)", tc.in, gotInt, gotNarrow, tc.wantInt, tc.wantNarrow)
		}
	}
}

func TestMeshIDAllocation_NilSchema(t *testing.T) {
	if fs := MeshIDAllocationFindings(nil); fs != nil {
		t.Errorf("nil schema should yield nil findings, got %+v", fs)
	}
}
