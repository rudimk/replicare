package live

import (
	"testing"

	"github.com/rudimk/replicare/internal/engine"
)

func TestClassify(t *testing.T) {
	cases := []struct {
		name       string
		src, tgt   engine.Fingerprint
		wantStatus string
		wantConv   bool
	}{
		{"identical", engine.Fingerprint{Rows: 10, Checksum: "abc"}, engine.Fingerprint{Rows: 10, Checksum: "abc"}, "ok", true},
		{"count-drift", engine.Fingerprint{Rows: 10, Checksum: "abc"}, engine.Fingerprint{Rows: 9, Checksum: "abc"}, "drift-count", false},
		{"checksum-drift", engine.Fingerprint{Rows: 10, Checksum: "abc"}, engine.Fingerprint{Rows: 10, Checksum: "xyz"}, "drift-checksum", false},
		{"count-only-match", engine.Fingerprint{Rows: 10}, engine.Fingerprint{Rows: 10}, "count-only-ok", true},
		{"count-only-src-empty", engine.Fingerprint{Rows: 10}, engine.Fingerprint{Rows: 10, Checksum: "x"}, "count-only-ok", true},
		{"count-drift-beats-checksum", engine.Fingerprint{Rows: 10, Checksum: "a"}, engine.Fingerprint{Rows: 5}, "drift-count", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			gotStatus, gotConv := classify(c.src, c.tgt)
			if gotStatus != c.wantStatus || gotConv != c.wantConv {
				t.Errorf("classify(%+v,%+v) = (%q,%v), want (%q,%v)",
					c.src, c.tgt, gotStatus, gotConv, c.wantStatus, c.wantConv)
			}
		})
	}
}

func TestReplicableTables(t *testing.T) {
	pk := &engine.Key{Name: "pk", Columns: []string{"id"}, IsPrimary: true}
	schema := &engine.Schema{Tables: []engine.Table{
		{
			Ref:        engine.TableRef{Schema: "public", Name: "orders"},
			PrimaryKey: pk,
			Columns: []engine.Column{
				{Name: "id"},
				{Name: "total"},
				{Name: "search", Generated: true}, // excluded from the projection
			},
		},
		{
			// No usable key -> skipped entirely (§3.1).
			Ref:     engine.TableRef{Schema: "public", Name: "log"},
			Columns: []engine.Column{{Name: "msg"}},
		},
	}}

	got := replicableTables(schema)
	if len(got) != 1 {
		t.Fatalf("replicableTables returned %d tables, want 1 (keyless skipped)", len(got))
	}
	rt := got[0]
	if rt.ref.Name != "orders" {
		t.Fatalf("kept wrong table: %s", rt.ref)
	}
	if len(rt.cols) != 2 || rt.cols[0] != "id" || rt.cols[1] != "total" {
		t.Errorf("projection = %v, want [id total] (generated excluded)", rt.cols)
	}
}
