package postgres

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

func TestKeysetInPredicate(t *testing.T) {
	// single column
	p, err := keysetInPredicate([]captureCol{{"id", "integer"}}, []engine.KeyValues{{"1"}, {"2"}})
	if err != nil {
		t.Fatal(err)
	}
	if p != `"id" IN ('1'::integer, '2'::integer)` {
		t.Errorf("single = %q", p)
	}
	// composite
	p, err = keysetInPredicate([]captureCol{{"a", "integer"}, {"b", "text"}}, []engine.KeyValues{{"1", "x"}})
	if err != nil {
		t.Fatal(err)
	}
	if p != `("a", "b") IN (('1'::integer, 'x'::text))` {
		t.Errorf("composite = %q", p)
	}
	// empty
	if p, _ := keysetInPredicate([]captureCol{{"id", "integer"}}, nil); p != "FALSE" {
		t.Errorf("empty = %q", p)
	}
}

func TestRereadCurrentIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	src := captureSource(t, ctx)
	setupSourceTables(t, ctx, src, "CREATE TABLE rc_it.orders (id int PRIMARY KEY, note text)")
	mustExec(t, ctx, src.conn, "INSERT INTO rc_it.orders SELECT g, 'note '||g FROM generate_series(1,10) g")

	ref := engine.TableRef{Schema: "rc_it", Name: "orders"}
	// Ask for keys 1, 3, 5 (present) and 999 (absent).
	keys := []engine.KeyValues{{"1"}, {"3"}, {"5"}, {"999"}}

	var buf bytes.Buffer
	if err := src.RereadCurrent(ctx, ref, keys, &buf); err != nil {
		t.Fatalf("RereadCurrent: %v", err)
	}
	lines := nonEmptyLines(buf.String())
	if len(lines) != 3 {
		t.Fatalf("re-read returned %d rows, want 3 (present keys only): %q", len(lines), buf.String())
	}
	// Each COPY line is tab-separated "id\tnote".
	got := map[string]string{}
	for _, ln := range lines {
		parts := strings.SplitN(ln, "\t", 2)
		got[parts[0]] = parts[1]
	}
	for _, id := range []string{"1", "3", "5"} {
		if got[id] != "note "+id {
			t.Errorf("re-read row id=%s = %q, want %q", id, got[id], "note "+id)
		}
	}
	if _, ok := got["999"]; ok {
		t.Error("absent key 999 must not appear in the re-read")
	}
}

func nonEmptyLines(s string) []string {
	var out []string
	for _, ln := range strings.Split(s, "\n") {
		if strings.TrimSpace(ln) != "" {
			out = append(out, ln)
		}
	}
	return out
}
