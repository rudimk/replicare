package apply

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/rudimk/replicare/internal/engine"
)

// retryFixture models the §8.1 source/target FK mismatch: the SOURCE has no FK
// between parent and child (so a child can exist before its parent), but the
// TARGET enforces a non-deferrable FK. Topo ordering derived from the source
// can't know about the target's edge — only the retry fallback covers it.
type retryFixture struct {
	src    engine.Source
	sink   engine.Sink
	rawSrc *pgx.Conn
	rawTgt *pgx.Conn
	topo   []engine.TableRef
	target engine.TargetID
}

func newRetryFixture(t *testing.T, ctx context.Context) *retryFixture {
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
		"CREATE TABLE rc_it.parent (id int PRIMARY KEY, label text)",
		// No FK on the source: children may reference not-yet-existing parents.
		"CREATE TABLE rc_it.child (id int PRIMARY KEY, parent_id int NOT NULL, note text)",
	} {
		exec(t, ctx, rawSrc, s)
	}
	for _, s := range []string{
		"DROP SCHEMA IF EXISTS rc_it CASCADE", "CREATE SCHEMA rc_it",
		"CREATE TABLE rc_it.parent (id int PRIMARY KEY, label text)",
		// Non-deferrable FK on the target: violations surface at upsert time.
		"CREATE TABLE rc_it.child (id int PRIMARY KEY, parent_id int NOT NULL REFERENCES rc_it.parent(id), note text)",
	} {
		exec(t, ctx, rawTgt, s)
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

	f := &retryFixture{src: src, sink: sink, rawSrc: rawSrc, rawTgt: rawTgt,
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

// fastRetry keeps test wall-clock small.
var fastRetry = RetryPolicy{MaxAttempts: 3, BaseBackoff: 10 * time.Millisecond, MaxBackoff: 50 * time.Millisecond}

func TestRetryFallbackCrossPassDependency(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	f := newRetryFixture(t, ctx)

	// A second source session inserts the parent but does NOT commit yet — its
	// delta is invisible to the drain (the commit-order reality of §3.3).
	held, err := pgx.Connect(ctx, srcURL())
	if err != nil {
		t.Fatalf("connect held session: %v", err)
	}
	defer held.Close(context.Background())
	exec(t, ctx, held, "BEGIN")
	exec(t, ctx, held, "INSERT INTO rc_it.parent VALUES (1,'p1')")

	// The child commits first and becomes dirty; its target upsert must hit the
	// target-only FK.
	exec(t, ctx, f.rawSrc, "INSERT INTO rc_it.child VALUES (1,1,'c1')")

	// Every attempt fails transiently while the parent is uncommitted, so the
	// policy exhausts and the component halts LOUD.
	_, err = DrainComponentRetrying(ctx, f.src, f.sink, f.topo, f.target, 100, fastRetry)
	if err == nil {
		t.Fatal("expected halt error while parent dependency is missing")
	}
	if !strings.Contains(err.Error(), "HALTED") {
		t.Errorf("halt error should be loud, got: %v", err)
	}
	if !engine.IsTransientConstraint(err) {
		t.Errorf("halt error should wrap the transient FK violation, got: %v", err)
	}

	// §4.2 error policy: nothing was consumed — the child's delta stays dirty.
	dirty, err := f.src.ReadDirtyKeys(ctx, f.topo[1], f.target, 10)
	if err != nil {
		t.Fatalf("read dirty after halt: %v", err)
	}
	if len(dirty) == 0 {
		t.Fatal("child delta must remain dirty after halted pass")
	}

	// The dependency lands; a fresh retried drain now converges.
	exec(t, ctx, held, "COMMIT")
	if _, err := DrainComponentRetrying(ctx, f.src, f.sink, f.topo, f.target, 100, fastRetry); err != nil {
		t.Fatalf("drain after dependency landed: %v", err)
	}
	var parents, children int
	if err := f.rawTgt.QueryRow(ctx, "SELECT count(*) FROM rc_it.parent").Scan(&parents); err != nil {
		t.Fatal(err)
	}
	if err := f.rawTgt.QueryRow(ctx, "SELECT count(*) FROM rc_it.child").Scan(&children); err != nil {
		t.Fatal(err)
	}
	if parents != 1 || children != 1 {
		t.Errorf("target rows = %d parents, %d children; want 1, 1", parents, children)
	}

	// And the queue is drained.
	n, err := DrainComponent(ctx, f.src, f.sink, f.topo, f.target, 100)
	if err != nil {
		t.Fatalf("final drain: %v", err)
	}
	if n != 0 {
		t.Errorf("expected empty queue, drained %d", n)
	}
}

// TestComponentDeferrableCycle proves a DEFERRABLE FK cycle converges in a
// single pass: BeginApply defers all constraints, so the pair upserts (and
// later the pair deletes) commit atomically without any retry.
func TestComponentDeferrableCycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
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
	cycleDDL := []string{
		"CREATE TABLE rc_it.a (id int PRIMARY KEY, b_id int NOT NULL)",
		"CREATE TABLE rc_it.b (id int PRIMARY KEY, a_id int NOT NULL REFERENCES rc_it.a(id) DEFERRABLE INITIALLY DEFERRED)",
		"ALTER TABLE rc_it.a ADD CONSTRAINT a_b_fk FOREIGN KEY (b_id) REFERENCES rc_it.b(id) DEFERRABLE INITIALLY DEFERRED",
	}
	for _, s := range append([]string{
		"DROP SCHEMA IF EXISTS rc_it CASCADE", "DROP SCHEMA IF EXISTS replicare CASCADE", "CREATE SCHEMA rc_it",
	}, cycleDDL...) {
		exec(t, ctx, rawSrc, s)
	}
	for _, s := range append([]string{"DROP SCHEMA IF EXISTS rc_it CASCADE", "CREATE SCHEMA rc_it"}, cycleDDL...) {
		exec(t, ctx, rawTgt, s)
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
	a := engine.TableRef{Schema: "rc_it", Name: "a"}
	b := engine.TableRef{Schema: "rc_it", Name: "b"}
	if err := src.InstallCapture(ctx, []engine.TableRef{a, b}); err != nil {
		t.Fatalf("install capture: %v", err)
	}
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

	// Insert a mutually-referencing pair (one source txn; checks defer to commit).
	exec(t, ctx, rawSrc, "BEGIN")
	exec(t, ctx, rawSrc, "INSERT INTO rc_it.a VALUES (1, 10)")
	exec(t, ctx, rawSrc, "INSERT INTO rc_it.b VALUES (10, 1)")
	exec(t, ctx, rawSrc, "COMMIT")

	topo := []engine.TableRef{a, b}
	if _, err := DrainComponentRetrying(ctx, src, sink, topo, "dst", 100, fastRetry); err != nil {
		t.Fatalf("drain cycle insert: %v", err)
	}
	var na, nb int
	if err := rawTgt.QueryRow(ctx, "SELECT count(*) FROM rc_it.a").Scan(&na); err != nil {
		t.Fatal(err)
	}
	if err := rawTgt.QueryRow(ctx, "SELECT count(*) FROM rc_it.b").Scan(&nb); err != nil {
		t.Fatal(err)
	}
	if na != 1 || nb != 1 {
		t.Fatalf("target rows a=%d b=%d; want 1, 1", na, nb)
	}

	// Delete the pair (again one txn) and converge to empty.
	exec(t, ctx, rawSrc, "BEGIN")
	exec(t, ctx, rawSrc, "DELETE FROM rc_it.a WHERE id=1")
	exec(t, ctx, rawSrc, "DELETE FROM rc_it.b WHERE id=10")
	exec(t, ctx, rawSrc, "COMMIT")
	if _, err := DrainComponentRetrying(ctx, src, sink, topo, "dst", 100, fastRetry); err != nil {
		t.Fatalf("drain cycle delete: %v", err)
	}
	if err := rawTgt.QueryRow(ctx, "SELECT count(*) FROM rc_it.a").Scan(&na); err != nil {
		t.Fatal(err)
	}
	if err := rawTgt.QueryRow(ctx, "SELECT count(*) FROM rc_it.b").Scan(&nb); err != nil {
		t.Fatal(err)
	}
	if na != 0 || nb != 0 {
		t.Errorf("target rows after delete a=%d b=%d; want 0, 0", na, nb)
	}
}
