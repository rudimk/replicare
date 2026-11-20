package postgres

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

// TestDeferredIndexesIntegration: the target-index capability lists only the plain
// indexes it may drop (never a primary key, a unique index or one behind a
// constraint), marks the ones on linking columns, carries the comment and CLUSTER
// marker along, drops and rebuilds them exactly, cleans up a broken left-over index
// before rebuilding, and leaves alone the indexes of a table it does not own.
func TestDeferredIndexesIntegration(t *testing.T) {
	cc := harnessConn(t, "target") // skips unless REPLICARE_INTEGRATION=1
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	tgt := &Sink{cfg: cc}
	if err := tgt.Connect(ctx); err != nil {
		t.Fatalf("connect target: %v", err)
	}
	t.Cleanup(func() { _ = tgt.Close(context.Background()) })
	mon := &Sink{cfg: cc}
	if err := mon.Connect(ctx); err != nil {
		t.Fatalf("connect monitor: %v", err)
	}
	t.Cleanup(func() { _ = mon.Close(context.Background()) })
	cleanup := func() {
		bg := context.Background()
		_, _ = tgt.conn.Exec(bg, "DROP SCHEMA IF EXISTS rc_it_ix CASCADE")
		_, _ = tgt.conn.Exec(bg, "DROP ROLE IF EXISTS rc_it_ix_owner")
	}
	cleanup()
	t.Cleanup(cleanup)

	mustExec := func(sql string) {
		t.Helper()
		if _, err := tgt.conn.Exec(ctx, sql); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}
	mustExec("CREATE SCHEMA rc_it_ix")
	mustExec("CREATE TABLE rc_it_ix.parent (id int PRIMARY KEY, email text UNIQUE, name text)")
	mustExec("CREATE TABLE rc_it_ix.child (id int PRIMARY KEY, parent_id int REFERENCES rc_it_ix.parent(id), note text, at timestamptz)")
	mustExec("CREATE INDEX child_parent_id_idx ON rc_it_ix.child (parent_id)")
	mustExec("CREATE INDEX child_note_lower_idx ON rc_it_ix.child (lower(note)) WHERE note IS NOT NULL")
	mustExec("CREATE INDEX child_at_idx ON rc_it_ix.child (at DESC)")
	mustExec("COMMENT ON INDEX rc_it_ix.child_at_idx IS 'for the timeline'")
	mustExec("ALTER TABLE rc_it_ix.child CLUSTER ON child_at_idx")
	mustExec("CREATE UNIQUE INDEX parent_name_uq ON rc_it_ix.parent (name)")
	mustExec("CREATE INDEX parent_name_idx ON rc_it_ix.parent (name)")
	mustExec("INSERT INTO rc_it_ix.parent SELECT g, 'e'||g, 'n'||g FROM generate_series(1,200) g")
	mustExec("INSERT INTO rc_it_ix.child SELECT g, 1 + g % 200, 'Note '||g, now() - g * interval '1 minute' FROM generate_series(1,2000) g")

	child := engine.TableRef{Schema: "rc_it_ix", Name: "child"}
	parent := engine.TableRef{Schema: "rc_it_ix", Name: "parent"}
	idx, skips, err := tgt.DeferrableIndexes(ctx, []engine.TableRef{parent, child})
	if err != nil {
		t.Fatalf("DeferrableIndexes: %v", err)
	}
	if len(skips) != 0 {
		t.Fatalf("skips = %+v, want none (the harness role owns these tables)", skips)
	}
	got := map[string]engine.DeferredIndex{}
	for _, ix := range idx {
		got[ix.Name] = ix
	}
	want := []string{"child_at_idx", "child_note_lower_idx", "child_parent_id_idx", "parent_name_idx"}
	if len(got) != len(want) {
		t.Fatalf("deferrable = %v, want exactly %v (no PK, UNIQUE constraint or unique index)", indexNames(idx), want)
	}
	for _, w := range want {
		if _, ok := got[w]; !ok {
			t.Fatalf("deferrable = %v, missing %s", indexNames(idx), w)
		}
	}
	if !got["child_parent_id_idx"].Link || got["child_at_idx"].Link || got["parent_name_idx"].Link {
		t.Errorf("link flags wrong: parent_id=%v at=%v name=%v", got["child_parent_id_idx"].Link, got["child_at_idx"].Link, got["parent_name_idx"].Link)
	}
	if after := strings.Join(got["child_at_idx"].After, ";"); !strings.Contains(after, "COMMENT ON INDEX") || !strings.Contains(after, "CLUSTER ON") {
		t.Errorf("child_at_idx After = %q, want the comment and the CLUSTER marker", after)
	}

	defs := map[string]string{}
	for _, ix := range idx {
		defs[ix.Name] = indexDef(t, ctx, tgt, ix.Name)
		if err := tgt.DropDeferredIndex(ctx, ix); err != nil {
			t.Fatalf("drop %s: %v", ix.Name, err)
		}
		if p, _ := tgt.IndexPresence(ctx, ix); p != engine.IndexMissing {
			t.Fatalf("%s still present after drop", ix.Name)
		}
		// Dropping again is a no-op.
		if err := tgt.DropDeferredIndex(ctx, ix); err != nil {
			t.Fatalf("drop %s again: %v", ix.Name, err)
		}
	}

	// A broken left-over of an interrupted build under one of the names: the build
	// must clear it and build a valid index.
	broken := got["child_note_lower_idx"]
	mustExec("CREATE INDEX child_note_lower_idx ON rc_it_ix.child (note)")
	mustExec("UPDATE pg_index SET indisvalid = false WHERE indexrelid = 'rc_it_ix.child_note_lower_idx'::regclass")
	if p, _ := tgt.IndexPresence(ctx, broken); p != engine.IndexInvalid {
		t.Fatalf("presence of the broken index = %v, want invalid", p)
	}

	session, err := tgt.BuildSession(ctx)
	if err != nil || session == 0 {
		t.Fatalf("BuildSession = %d, %v", session, err)
	}
	if _, ok, err := mon.IndexBuildProgress(ctx, session); err != nil || ok {
		t.Fatalf("progress with no build running = %v, %v; want none", ok, err)
	}
	opts := engine.IndexBuildOptions{MaintenanceWorkMem: "64MB", ParallelWorkers: 2}
	for _, ix := range idx {
		if err := tgt.BuildDeferredIndex(ctx, ix, opts); err != nil {
			t.Fatalf("build %s: %v", ix.Name, err)
		}
		if p, _ := tgt.IndexPresence(ctx, ix); p != engine.IndexValid {
			t.Fatalf("%s not valid after build", ix.Name)
		}
		if d := indexDef(t, ctx, tgt, ix.Name); d != defs[ix.Name] {
			t.Errorf("%s rebuilt as %q, want %q", ix.Name, d, defs[ix.Name])
		}
		// Building a valid index again does nothing.
		if err := tgt.BuildDeferredIndex(ctx, ix, opts); err != nil {
			t.Fatalf("rebuild of a valid %s: %v", ix.Name, err)
		}
	}
	var comment string
	var clustered bool
	if err := tgt.conn.QueryRow(ctx, `SELECT COALESCE(obj_description('rc_it_ix.child_at_idx'::regclass, 'pg_class'), ''),
		(SELECT indisclustered FROM pg_index WHERE indexrelid = 'rc_it_ix.child_at_idx'::regclass)`).Scan(&comment, &clustered); err != nil {
		t.Fatal(err)
	}
	if comment != "for the timeline" || !clustered {
		t.Errorf("child_at_idx comment=%q clustered=%v, want both restored", comment, clustered)
	}
	if err := tgt.AnalyzeTables(ctx, []engine.TableRef{parent, child}); err != nil {
		t.Fatalf("analyze: %v", err)
	}

	// A table owned by a role this one is not a member of keeps its indexes: switch
	// this session to a plain role that owns nothing here.
	mustExec("CREATE ROLE rc_it_ix_owner NOLOGIN")
	mustExec("GRANT USAGE ON SCHEMA rc_it_ix TO rc_it_ix_owner")
	mustExec("SET ROLE rc_it_ix_owner")
	idx2, skips, err := tgt.DeferrableIndexes(ctx, []engine.TableRef{parent, child})
	mustExec("RESET ROLE")
	if err != nil {
		t.Fatalf("DeferrableIndexes as a non-owner: %v", err)
	}
	if len(idx2) != 0 || len(skips) != 2 {
		t.Fatalf("as a non-owner: deferrable=%v skips=%+v, want none deferrable and both tables skipped", indexNames(idx2), skips)
	}
}

func indexNames(idx []engine.DeferredIndex) []string {
	out := make([]string, len(idx))
	for i, ix := range idx {
		out[i] = ix.Name
	}
	return out
}

func indexDef(t *testing.T, ctx context.Context, s *Sink, name string) string {
	t.Helper()
	var def string
	if err := s.conn.QueryRow(ctx, "SELECT pg_get_indexdef(('rc_it_ix.' || quote_ident($1))::regclass)", name).Scan(&def); err != nil {
		t.Fatalf("indexdef %s: %v", name, err)
	}
	return def
}
