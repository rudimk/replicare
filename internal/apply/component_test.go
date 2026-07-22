package apply

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/rudimk/replicare/internal/engine"
)

// compFixture sets up a captured parent<-child component on the source and a
// matching target, and returns the refs in topo order.
type compFixture struct {
	src    engine.Source
	sink   engine.Sink
	rawSrc *pgx.Conn
	rawTgt *pgx.Conn
	topo   []engine.TableRef
	target engine.TargetID
}

const (
	parentDDL = "CREATE TABLE rc_it.parent (id int PRIMARY KEY, label text)"
	childDDL  = "CREATE TABLE rc_it.child (id int PRIMARY KEY, parent_id int NOT NULL REFERENCES rc_it.parent(id), note text)"
)

func newCompFixture(t *testing.T, ctx context.Context) *compFixture {
	t.Helper()
	if !integration() {
		t.Skip("integration test; set REPLICARE_INTEGRATION=1 and run `task harness:up`")
	}
	rawSrc, err := pgx.Connect(ctx, srcURL())
	if err != nil {
		t.Fatalf("connect source: %v", err)
	}
	rawTgt, err := pgx.Connect(ctx, tgtURL())
	if err != nil {
		t.Fatalf("connect target: %v", err)
	}
	for _, s := range []string{
		"DROP SCHEMA IF EXISTS rc_it CASCADE", "DROP SCHEMA IF EXISTS replicare CASCADE", "CREATE SCHEMA rc_it",
	} {
		exec(t, ctx, rawSrc, s)
	}
	exec(t, ctx, rawSrc, parentDDL)
	exec(t, ctx, rawSrc, childDDL)
	exec(t, ctx, rawTgt, "DROP SCHEMA IF EXISTS rc_it CASCADE")
	exec(t, ctx, rawTgt, "CREATE SCHEMA rc_it")
	exec(t, ctx, rawTgt, parentDDL)
	exec(t, ctx, rawTgt, childDDL)

	// Seed BEFORE capture (no deltas), then reproduce on the target (initial copy).
	seed := []string{
		"INSERT INTO rc_it.parent VALUES (1,'p1'),(2,'p2'),(3,'p3')",
		"INSERT INTO rc_it.child VALUES (1,1,'c1'),(2,1,'c2'),(3,2,'c3'),(4,2,'c4'),(5,1,'c5')",
	}
	for _, s := range seed {
		exec(t, ctx, rawSrc, s)
	}

	eng, _ := engine.Get("postgres")
	src, _ := eng.NewSource(srcCfg())
	if err := src.Connect(ctx); err != nil {
		t.Fatalf("connect src: %v", err)
	}
	sink, _ := eng.NewSink(tgtCfg())
	if err := sink.Connect(ctx); err != nil {
		t.Fatalf("connect sink: %v", err)
	}
	parent := engine.TableRef{Schema: "rc_it", Name: "parent"}
	child := engine.TableRef{Schema: "rc_it", Name: "child"}
	if err := src.InstallCapture(ctx, []engine.TableRef{parent, child}); err != nil {
		t.Fatalf("install capture: %v", err)
	}
	for _, s := range seed {
		exec(t, ctx, rawTgt, s)
	}

	f := &compFixture{src: src, sink: sink, rawSrc: rawSrc, rawTgt: rawTgt,
		topo: []engine.TableRef{parent, child}, target: "dst"}
	t.Cleanup(func() {
		bg := context.Background()
		_, _ = rawSrc.Exec(bg, "DROP SCHEMA IF EXISTS rc_it CASCADE")
		_, _ = rawSrc.Exec(bg, "DROP SCHEMA IF EXISTS replicare CASCADE")
		_, _ = rawTgt.Exec(bg, "DROP SCHEMA IF EXISTS rc_it CASCADE")
		_ = src.Close(bg)
		_ = sink.Close(bg)
		_ = rawSrc.Close(bg)
		_ = rawTgt.Close(bg)
	})
	return f
}

func integration() bool {
	return env("REPLICARE_INTEGRATION", "") == "1"
}

func (f *compFixture) drainAll(t *testing.T, ctx context.Context) {
	t.Helper()
	for {
		n, err := DrainComponent(ctx, f.src, f.sink, f.topo, f.target, 100)
		if err != nil {
			t.Fatalf("DrainComponent: %v", err)
		}
		if n == 0 {
			return
		}
	}
}

func (f *compFixture) assertConsistent(t *testing.T, ctx context.Context) {
	t.Helper()
	dump := func(c *pgx.Conn, q string) []string {
		rows, err := c.Query(ctx, q)
		if err != nil {
			t.Fatalf("dump: %v", err)
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			vals, _ := rows.Values()
			line := ""
			for i, v := range vals {
				if i > 0 {
					line += "|"
				}
				line += v.(string)
			}
			out = append(out, line)
		}
		return out
	}
	for _, q := range []string{
		"SELECT id::text, coalesce(label,'') FROM rc_it.parent ORDER BY id",
		"SELECT id::text, parent_id::text, coalesce(note,'') FROM rc_it.child ORDER BY id",
	} {
		s, d := dump(f.rawSrc, q), dump(f.rawTgt, q)
		if len(s) != len(d) {
			t.Fatalf("row count mismatch for %q: source %v target %v", q, s, d)
		}
		for i := range s {
			if s[i] != d[i] {
				t.Fatalf("row differs for %q: source %q target %q", q, s[i], d[i])
			}
		}
	}
	// No orphaned children on the target (FK integrity).
	var orphans int
	if err := f.rawTgt.QueryRow(ctx,
		"SELECT count(*) FROM rc_it.child c LEFT JOIN rc_it.parent p ON p.id=c.parent_id WHERE p.id IS NULL").Scan(&orphans); err != nil {
		t.Fatal(err)
	}
	if orphans != 0 {
		t.Errorf("%d orphaned child rows on target", orphans)
	}
}

func TestComponentReferentialConsistency(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	f := newCompFixture(t, ctx)

	// One source transaction mixing: a new parent+child (parent must apply first),
	// a parent+its children deleted (children must delete first), and updates.
	exec(t, ctx, f.rawSrc, "BEGIN")
	exec(t, ctx, f.rawSrc, "INSERT INTO rc_it.parent VALUES (4,'p4')")
	exec(t, ctx, f.rawSrc, "INSERT INTO rc_it.child VALUES (6,4,'c6')")
	exec(t, ctx, f.rawSrc, "DELETE FROM rc_it.child WHERE parent_id=2") // children 3,4
	exec(t, ctx, f.rawSrc, "DELETE FROM rc_it.parent WHERE id=2")
	exec(t, ctx, f.rawSrc, "UPDATE rc_it.child SET note='c1new' WHERE id=1")
	exec(t, ctx, f.rawSrc, "UPDATE rc_it.parent SET label='p3new' WHERE id=3")
	exec(t, ctx, f.rawSrc, "COMMIT")

	f.drainAll(t, ctx)
	f.assertConsistent(t, ctx)

	// A second drain is a no-op.
	n, err := DrainComponent(ctx, f.src, f.sink, f.topo, f.target, 100)
	if err != nil {
		t.Fatalf("second drain: %v", err)
	}
	if n != 0 {
		t.Errorf("expected empty queue, drained %d", n)
	}
}
