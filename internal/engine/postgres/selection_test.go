package postgres

import (
	"testing"

	"github.com/rudimk/replicare/internal/engine"
)

func tables(names ...string) []engine.TableRef {
	out := make([]engine.TableRef, 0, len(names))
	for _, n := range names {
		// names are "schema.table"
		schema, table := n, ""
		for i := 0; i < len(n); i++ {
			if n[i] == '.' {
				schema, table = n[:i], n[i+1:]
				break
			}
		}
		out = append(out, engine.TableRef{Schema: schema, Name: table})
	}
	return out
}

func names(ts []engine.TableRef) []string {
	out := make([]string, len(ts))
	for i, t := range ts {
		out[i] = t.String()
	}
	return out
}

func eq(a, b []string) bool {
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

func TestSelectTables(t *testing.T) {
	all := tables(
		"public.orders",
		"public.customers",
		"public.orders_audit",
		"billing.invoices",
		"billing.invoices_audit",
	)

	cases := []struct {
		name string
		sel  engine.Selection
		want []string
	}{
		{
			name: "empty include matches all",
			sel:  engine.Selection{},
			want: []string{"public.orders", "public.customers", "public.orders_audit", "billing.invoices", "billing.invoices_audit"},
		},
		{
			name: "schema glob",
			sel:  engine.Selection{Include: []string{"public.*"}},
			want: []string{"public.orders", "public.customers", "public.orders_audit"},
		},
		{
			name: "exclude audit across schemas",
			sel:  engine.Selection{Exclude: []string{"*_audit"}},
			want: []string{"public.orders", "public.customers", "billing.invoices"},
		},
		{
			name: "include schema then exclude audit",
			sel:  engine.Selection{Include: []string{"public.*"}, Exclude: []string{"*_audit"}},
			want: []string{"public.orders", "public.customers"},
		},
		{
			name: "exact match",
			sel:  engine.Selection{Include: []string{"billing.invoices"}},
			want: []string{"billing.invoices"},
		},
		{
			name: "question mark single char",
			sel:  engine.Selection{Include: []string{"public.order?"}},
			want: []string{"public.orders"},
		},
		{
			name: "multiple includes union",
			sel:  engine.Selection{Include: []string{"public.customers", "billing.*"}},
			want: []string{"public.customers", "billing.invoices", "billing.invoices_audit"},
		},
		{
			name: "no matches",
			sel:  engine.Selection{Include: []string{"nonexistent.*"}},
			want: []string{},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := selectTables(all, tc.sel)
			if err != nil {
				t.Fatalf("selectTables: %v", err)
			}
			if !eq(names(got), tc.want) {
				t.Errorf("got %v, want %v", names(got), tc.want)
			}
		})
	}
}

func TestSelectTablesPreservesOrder(t *testing.T) {
	all := tables("z.a", "a.b", "m.c")
	got, err := selectTables(all, engine.Selection{})
	if err != nil {
		t.Fatalf("selectTables: %v", err)
	}
	if !eq(names(got), []string{"z.a", "a.b", "m.c"}) {
		t.Errorf("order not preserved: %v", names(got))
	}
}

func TestSelectTablesRejectsEmptyPattern(t *testing.T) {
	_, err := selectTables(tables("public.x"), engine.Selection{Include: []string{""}})
	if err == nil {
		t.Fatal("expected error for empty include pattern")
	}
	_, err = selectTables(tables("public.x"), engine.Selection{Exclude: []string{"   "}})
	if err == nil {
		t.Fatal("expected error for blank exclude pattern")
	}
}

func TestGlobDotIsLiteral(t *testing.T) {
	// A dot in the pattern must match a literal dot, not any character: the
	// pattern "publicXorders" (X literal) should NOT be produced by "public.*".
	all := tables("publicXorders.t")
	got, err := selectTables(all, engine.Selection{Include: []string{"public.*"}})
	if err != nil {
		t.Fatalf("selectTables: %v", err)
	}
	// "public.*" = literal "public" + "." + ".*"; "publicXorders.t" has 'X'
	// where the literal '.' is required, so it must not match.
	if len(got) != 0 {
		t.Errorf("dot should be literal; unexpectedly matched %v", names(got))
	}
}
