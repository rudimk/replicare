package pgmigrate

import "testing"

func TestAdvisoryKeyStableAndDistinct(t *testing.T) {
	// Stable across calls.
	if advisoryKey("replicare.schema_version") != advisoryKey("replicare.schema_version") {
		t.Error("advisoryKey should be deterministic")
	}
	// Distinct schemas get distinct keys (state store vs source).
	if advisoryKey("replicare.schema_version") == advisoryKey("replicare_state.schema_version") {
		t.Error("distinct schemas should hash to distinct advisory keys")
	}
}

func TestSplitIdent(t *testing.T) {
	cases := []struct {
		in            string
		schema, table string
		wantErr       bool
	}{
		{"replicare_state.schema_version", "replicare_state", "schema_version", false},
		{"schema_version", "", "schema_version", false},
		{"", "", "", true},
		{".", "", "", true},
		{"a.b.c", "", "", true},
		{"public.t-bad", "", "", true}, // hyphen not allowed
		{"1bad.t", "", "", true},       // schema starts with digit
		{"public.", "", "", true},      // empty table
		{"_ok.t_1", "_ok", "t_1", false},
	}
	for _, c := range cases {
		schema, table, err := splitIdent(c.in)
		if (err != nil) != c.wantErr {
			t.Errorf("splitIdent(%q) err = %v, wantErr = %v", c.in, err, c.wantErr)
			continue
		}
		if !c.wantErr && (schema != c.schema || table != c.table) {
			t.Errorf("splitIdent(%q) = (%q, %q), want (%q, %q)", c.in, schema, table, c.schema, c.table)
		}
	}
}

func TestQualifiedAndQuote(t *testing.T) {
	if got := qualified("replicare_state", "syncs"); got != `"replicare_state"."syncs"` {
		t.Errorf("qualified = %q", got)
	}
	if got := qualified("", "syncs"); got != `"syncs"` {
		t.Errorf("qualified (no schema) = %q", got)
	}
	if got := quoteIdent(`we"ird`); got != `"we""ird"` {
		t.Errorf("quoteIdent = %q", got)
	}
}

func TestIsSafeIdent(t *testing.T) {
	safe := []string{"a", "_x", "replicare_state", "t1", "ABC_9"}
	unsafe := []string{"", "1a", "a-b", "a.b", "a b", `a"b`}
	for _, s := range safe {
		if !isSafeIdent(s) {
			t.Errorf("isSafeIdent(%q) = false, want true", s)
		}
	}
	for _, s := range unsafe {
		if isSafeIdent(s) {
			t.Errorf("isSafeIdent(%q) = true, want false", s)
		}
	}
}
