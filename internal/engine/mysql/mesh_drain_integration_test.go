package mysql

import (
	"context"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/apply"
	"github.com/rudimk/replicare/internal/engine"
)

// TestClusterCrossNodeDrain drives a real cross-node cluster drain (node a -> node b)
// through the neutral apply layer, surfacing any error in the cluster streaming path.
// It uses the source (a) and target (b) harness nodes as two mesh members.
func TestClusterCrossNodeDrain(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Node A = source harness. Node B = target harness. Both are cluster members.
	a := &Source{cfg: srcCfg()}
	if err := a.Connect(ctx); err != nil {
		t.Fatalf("connect a: %v", err)
	}
	a.EnableClusterReads()
	t.Cleanup(func() { _ = a.Close(context.Background()) })

	bSink := &Sink{cfg: tgtCfg()}
	if err := bSink.Connect(ctx); err != nil {
		t.Fatalf("connect b sink: %v", err)
	}
	bSink.EnableOriginMarking("b")
	t.Cleanup(func() { _ = bSink.Close(context.Background()) })
	bSrc := &Source{cfg: tgtCfg()}
	if err := bSrc.Connect(ctx); err != nil {
		t.Fatalf("connect b src: %v", err)
	}
	bSrc.EnableClusterReads()
	t.Cleanup(func() { _ = bSrc.Close(context.Background()) })

	ref := engine.TableRef{Schema: "rc_it", Name: "orders"}
	ddl := "CREATE TABLE rc_it.orders (id INT PRIMARY KEY, note VARCHAR(50)) ENGINE=InnoDB"
	for _, s := range []*Source{a, bSrc} {
		mustExec(t, ctx, s.db, "DROP DATABASE IF EXISTS replicare", "DROP DATABASE IF EXISTS rc_it", "CREATE DATABASE rc_it", ddl)
	}
	t.Cleanup(func() {
		for _, s := range []*Source{a, bSrc} {
			_, _ = s.db.Exec("DROP DATABASE IF EXISTS rc_it")
			_, _ = s.db.Exec("DROP DATABASE IF EXISTS replicare")
		}
	})
	// Install origin capture on BOTH members (each is a source for its outbound edge).
	if err := a.InstallOriginCapture(ctx, []engine.TableRef{ref}, "a"); err != nil {
		t.Fatalf("install a: %v", err)
	}
	if err := bSrc.InstallOriginCapture(ctx, []engine.TableRef{ref}, "b"); err != nil {
		t.Fatalf("install b: %v", err)
	}

	// A local write on A.
	mustExec(t, ctx, a.db, "INSERT INTO rc_it.orders (id, note) VALUES (1, 'from-a')")

	// Drive the A->B drain through the neutral cluster apply path.
	policy := apply.RetryPolicy{MaxAttempts: 5, BaseBackoff: 50 * time.Millisecond, MaxBackoff: 200 * time.Millisecond}
	dctx, dcancel := context.WithTimeout(ctx, 20*time.Second)
	defer dcancel()
	for {
		n, err := apply.DrainComponentRetrying(dctx, a, bSink, []engine.TableRef{ref}, "b", 100, false, policy)
		if err != nil {
			t.Fatalf("cluster drain a->b: %v", err)
		}
		if n == 0 {
			break
		}
	}

	// B must now hold A's row.
	var note string
	if err := bSink.db.QueryRowContext(ctx, "SELECT note FROM rc_it.orders WHERE id=1").Scan(&note); err != nil {
		t.Fatalf("read b.orders after drain: %v", err)
	}
	if note != "from-a" {
		t.Fatalf("b did not receive A's write: note=%q, want from-a", note)
	}

	// --- Bidirectional same-key conflict (mirrors the daemon mesh) ---
	// Reset both nodes' data + register for a clean conflict on key 2.
	mustExec(t, ctx, a.db, "INSERT INTO rc_it.orders (id, note) VALUES (2, 'a2')")
	mustExec(t, ctx, bSrc.db, "INSERT INTO rc_it.orders (id, note) VALUES (2, 'b2')")

	aSink := &Sink{cfg: srcCfg()}
	if err := aSink.Connect(ctx); err != nil {
		t.Fatalf("connect a sink: %v", err)
	}
	aSink.EnableOriginMarking("a")
	t.Cleanup(func() { _ = aSink.Close(context.Background()) })

	// Drain both directions to convergence.
	for i := 0; i < 6; i++ {
		if _, err := apply.DrainComponentRetrying(ctx, a, bSink, []engine.TableRef{ref}, "b", 100, false, policy); err != nil {
			t.Fatalf("drain a->b (round %d): %v", i, err)
		}
		if _, err := apply.DrainComponentRetrying(ctx, bSrc, aSink, []engine.TableRef{ref}, "a", 100, false, policy); err != nil {
			t.Fatalf("drain b->a (round %d): %v", i, err)
		}
	}
	var na, nb string
	_ = a.db.QueryRowContext(ctx, "SELECT note FROM rc_it.orders WHERE id=2").Scan(&na)
	_ = bSrc.db.QueryRowContext(ctx, "SELECT note FROM rc_it.orders WHERE id=2").Scan(&nb)
	t.Logf("bidirectional conflict on key 2: a=%q b=%q", na, nb)
	if na == "" || na != nb {
		t.Fatalf("bidirectional conflict did not converge: a=%q b=%q", na, nb)
	}
}
