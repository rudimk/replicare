package apply

import (
	"context"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

// TestStreamingUniqueValueReused: on the source, a row is deleted and a new one is
// created holding the same unique value (an account deleted and re-created under a new
// id with the same email). Both changes land in one pass. Upserts run before deletes, so
// the new row clashes with the old one still on the target (23505). The pass must still
// delete the old row and retry the new one, instead of halting on every pass. Covered for
// both apply paths (acyclic, and the self-referencing table's per-table NULL-fill) and
// both one-connection and pooled apply.
func TestStreamingUniqueValueReused(t *testing.T) {
	for _, tc := range []struct {
		name   string
		ddl    string
		seed   string
		cyclic bool
		nnd    bool // the target's unique treats empty values as equal (link kept real)
	}{
		{"acyclic", "CREATE TABLE rc_it.orders (id int PRIMARY KEY, note text UNIQUE)",
			"INSERT INTO rc_it.orders VALUES (1,'a'), (2,'b')", false, false},
		// Self-link kept real (production's Account): the single-transaction path.
		{"self-ref", "CREATE TABLE rc_it.orders (id int PRIMARY KEY, note text, parent int REFERENCES rc_it.orders(id), UNIQUE (note, parent))",
			"INSERT INTO rc_it.orders VALUES (2,'b',NULL), (1,'a',2)", true, true},
		// Self-link emptied then filled (an ordinary unique elsewhere): the per-table
		// NULL-fill path.
		{"self-ref-nullfill", "CREATE TABLE rc_it.orders (id int PRIMARY KEY, note text UNIQUE, parent int REFERENCES rc_it.orders(id))",
			"INSERT INTO rc_it.orders VALUES (2,'b',NULL), (1,'a',2)", true, false},
	} {
		for _, pooled := range []bool{false, true} {
			name := tc.name + "/single"
			if pooled {
				name = tc.name + "/pooled"
			}
			t.Run(name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
				defer cancel()
				f := newFixture(t, ctx, tc.ddl, tc.seed)
				if tc.nnd {
					// As in production, the TARGET's unique treats empty values as equal
					// (PG15+), so the self-link is kept at its real value and the incoming
					// row really does clash with the old one. The source keeps an ordinary
					// unique (the harness source is too old for NULLS NOT DISTINCT).
					exec(t, ctx, f.rawTgt, "ALTER TABLE rc_it.orders DROP CONSTRAINT orders_note_parent_key")
					exec(t, ctx, f.rawTgt, "CREATE UNIQUE INDEX orders_note_parent_nnd ON rc_it.orders (note, parent) NULLS NOT DISTINCT")
					exec(t, ctx, f.rawSrc, "DELETE FROM rc_it.orders WHERE id = 1")
					exec(t, ctx, f.rawSrc, "INSERT INTO rc_it.orders VALUES (3, 'a', 2)")
				} else {
					exec(t, ctx, f.rawSrc, "DELETE FROM rc_it.orders WHERE id = 1")
					exec(t, ctx, f.rawSrc, "INSERT INTO rc_it.orders VALUES (3, 'a')")
				}
				pool := []Conn{{Src: f.src, Sink: f.sink}}
				conc := 1
				if pooled {
					src2, sink2 := extraPair(t, ctx)
					pool = append(pool, Conn{Src: src2, Sink: sink2})
					conc = 2
				}
				policy := RetryPolicy{MaxAttempts: 4, BaseBackoff: 10 * time.Millisecond, MaxBackoff: 50 * time.Millisecond}
				if _, err := DrainComponentRetryingPool(ctx, pool, conc, []engine.TableRef{f.ref}, f.target, 100, tc.cyclic, policy); err != nil {
					t.Fatalf("drain: %v", err)
				}
				f.assertConverged(t, ctx)
				if n, err := DrainComponentRetryingPool(ctx, pool, conc, []engine.TableRef{f.ref}, f.target, 100, tc.cyclic, policy); err != nil || n != 0 {
					t.Errorf("queue not fully consumed: second drain consumed %d (err %v)", n, err)
				}
			})
		}
	}
}

// extraPair opens a second Source/Sink pair on the harness, for pooled apply.
func extraPair(t *testing.T, ctx context.Context) (engine.Source, engine.Sink) {
	t.Helper()
	eng, err := engine.Get("postgres")
	if err != nil {
		t.Fatal(err)
	}
	src, err := eng.NewSource(srcCfg())
	if err != nil {
		t.Fatal(err)
	}
	if err := src.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	sink, err := eng.NewSink(tgtCfg())
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = src.Close(context.Background()); _ = sink.Close(context.Background()) })
	return src, sink
}
