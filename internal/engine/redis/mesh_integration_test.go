package redis

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/apply"
	"github.com/rudimk/replicare/internal/engine"
)

// MM6 mesh integration tests. Gated on REPLICARE_REDIS=1 (the local Redis gate), so
// they never run in the Postgres-only CI. They validate the active-active mesh over the
// exact neutral apply path (DrainComponent), driving two members as a full mesh.

// meshMember is one node's cluster source + sink (both point at the same Redis), with
// cluster mode enabled and the selection primed (so metadata keys are excluded).
type meshMember struct {
	id   string
	src  *Source
	sink *Sink
}

func newMeshMember(t *testing.T, ctx context.Context, cfg engine.ConnConfig, id string) meshMember {
	t.Helper()
	src := &Source{cfg: cfg}
	if err := src.Connect(ctx); err != nil {
		t.Fatalf("connect src %s: %v", id, err)
	}
	t.Cleanup(func() { _ = src.Close(context.Background()) })
	src.EnableClusterReads(id)
	if _, err := src.Introspect(ctx, engine.Selection{}); err != nil {
		t.Fatalf("introspect src %s: %v", id, err)
	}
	sink := &Sink{cfg: cfg}
	if err := sink.Connect(ctx); err != nil {
		t.Fatalf("connect sink %s: %v", id, err)
	}
	t.Cleanup(func() { _ = sink.Close(context.Background()) })
	sink.EnableOriginMarking(id)
	if _, err := sink.Introspect(ctx, engine.Selection{}); err != nil {
		t.Fatalf("introspect sink %s: %v", id, err)
	}
	return meshMember{id: id, src: src, sink: sink}
}

// fullPass drives one complete rolling reconciliation pass of edge (from.src ->
// to.sink) through the neutral DrainComponent, exactly as the daemon would.
func fullPass(t *testing.T, ctx context.Context, from, to meshMember) {
	t.Helper()
	ref := unitRef(from.src.cfg)
	for i := 0; i < 100000; i++ {
		n, err := apply.DrainComponent(ctx, from.src, to.sink, []engine.TableRef{ref}, engine.TargetID(to.id), 128, false)
		if err != nil {
			t.Fatalf("drain %s->%s: %v", from.id, to.id, err)
		}
		if n == 0 {
			return
		}
	}
	t.Fatalf("edge %s->%s did not complete a pass within guard", from.id, to.id)
}

// meshConverge runs several full mesh rounds (every ordered pair) so writes on any node
// propagate and settle under LWW. A handful of rounds is ample for a small keyspace.
func meshConverge(t *testing.T, ctx context.Context, members ...meshMember) {
	t.Helper()
	for round := 0; round < 4; round++ {
		for _, from := range members {
			for _, to := range members {
				if from.id != to.id {
					fullPass(t, ctx, from, to)
				}
			}
		}
	}
}

func meshFingerprint(t *testing.T, ctx context.Context, m meshMember) (int64, uint64) {
	t.Helper()
	n, sum, err := fingerprintUnit(ctx, m.src.db, m.src.cfg, m.src.sel)
	if err != nil {
		t.Fatalf("fingerprint %s: %v", m.id, err)
	}
	return n, sum
}

// cfgDB returns cfg pointed at a specific logical DB, so several isolated mesh members
// can run against one harness Redis (each DB is a separate keyspace).
func cfgDB(cfg engine.ConnConfig, db int) engine.ConnConfig {
	cfg.Database = strconv.Itoa(db)
	return cfg
}

// TestMeshThreeNodeConverges proves the mesh has no 2-node limit: THREE members (three
// isolated DBs on the harness) form a full mesh (6 directed edges) and converge —
// disjoint writes union on all three, a 3-way same-key conflict settles to one value
// everywhere, and a delete on one reaches both peers. Uses per-member FlushDB (not
// FlushAll) since the three share a server.
func TestMeshThreeNodeConverges(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	a := newMeshMember(t, ctx, cfgDB(srcCfg(), 0), "a")
	b := newMeshMember(t, ctx, cfgDB(srcCfg(), 1), "b")
	c := newMeshMember(t, ctx, cfgDB(srcCfg(), 2), "c")
	for _, m := range []meshMember{a, b, c} {
		must(t, m.src.db.uc.FlushDB(ctx).Err())
	}

	// Disjoint keys on each node → all three converge to the 6-key union.
	must(t, a.src.db.uc.Set(ctx, "ka1", "va", 0).Err())
	must(t, a.src.db.uc.Set(ctx, "ka2", "va", 0).Err())
	must(t, b.src.db.uc.Set(ctx, "kb1", "vb", 0).Err())
	must(t, b.src.db.uc.Set(ctx, "kb2", "vb", 0).Err())
	must(t, c.src.db.uc.Set(ctx, "kc1", "vc", 0).Err())
	must(t, c.src.db.uc.Set(ctx, "kc2", "vc", 0).Err())
	// A 3-way conflict on one shared key.
	must(t, a.src.db.uc.Set(ctx, "shared", "from-a", 0).Err())
	must(t, b.src.db.uc.Set(ctx, "shared", "from-b", 0).Err())
	must(t, c.src.db.uc.Set(ctx, "shared", "from-c", 0).Err())

	meshConverge(t, ctx, a, b, c)

	na, sa := meshFingerprint(t, ctx, a)
	nb, sb := meshFingerprint(t, ctx, b)
	nc, sc := meshFingerprint(t, ctx, c)
	if na != 7 || nb != 7 || nc != 7 { // 6 disjoint + 1 shared
		t.Fatalf("key counts a=%d b=%d c=%d, want 7 each", na, nb, nc)
	}
	if sa != sb || sb != sc {
		t.Fatalf("fingerprints differ across 3 nodes: a=%016x b=%016x c=%016x", sa, sb, sc)
	}
	// The shared key settled to ONE of the three writes on every node.
	shared, err := a.src.db.uc.Get(ctx, "shared").Result()
	must(t, err)
	if shared != "from-a" && shared != "from-b" && shared != "from-c" {
		t.Fatalf("shared converged to a fabricated value %q", shared)
	}

	// A delete on c must reach a and b.
	must(t, c.src.db.uc.Del(ctx, "kc1").Err())
	meshConverge(t, ctx, a, b, c)
	for _, m := range []meshMember{a, b, c} {
		if n, _ := m.src.db.uc.Exists(ctx, "kc1").Result(); n != 0 {
			t.Fatalf("delete of kc1 did not reach node %s", m.id)
		}
	}
}

// TestMeshDisjointConverges: two members writing DISJOINT keys each end up with the
// union, and both keyspaces are byte-for-byte convergent (identical content fingerprint).
func TestMeshDisjointConverges(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	a := newMeshMember(t, ctx, srcCfg(), "a")
	b := newMeshMember(t, ctx, tgtCfg(), "b")
	must(t, a.src.db.uc.FlushAll(ctx).Err())
	must(t, b.src.db.uc.FlushAll(ctx).Err())

	for i := 0; i < 5; i++ {
		must(t, a.src.db.uc.Set(ctx, "a-"+strconv.Itoa(i), "va", 0).Err())
		must(t, b.src.db.uc.Set(ctx, "b-"+strconv.Itoa(i), "vb", 0).Err())
	}
	meshConverge(t, ctx, a, b)

	na, sa := meshFingerprint(t, ctx, a)
	nb, sb := meshFingerprint(t, ctx, b)
	if na != 10 || nb != 10 {
		t.Fatalf("key counts: a=%d b=%d, want 10 each (union)", na, nb)
	}
	if sa != sb {
		t.Fatalf("fingerprints differ after convergence: a=%016x b=%016x", sa, sb)
	}
}

// TestMeshSameKeyConflictConverges: concurrent writes to the SAME key on both members
// converge to ONE value on both, resolved by (hlc, node) LWW — not left divergent.
func TestMeshSameKeyConflictConverges(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	a := newMeshMember(t, ctx, srcCfg(), "a")
	b := newMeshMember(t, ctx, tgtCfg(), "b")
	must(t, a.src.db.uc.FlushAll(ctx).Err())
	must(t, b.src.db.uc.FlushAll(ctx).Err())

	must(t, a.src.db.uc.Set(ctx, "k", "from-a", 0).Err())
	must(t, b.src.db.uc.Set(ctx, "k", "from-b", 0).Err())
	meshConverge(t, ctx, a, b)

	va, err := a.src.db.uc.Get(ctx, "k").Result()
	must(t, err)
	vb, err := b.src.db.uc.Get(ctx, "k").Result()
	must(t, err)
	if va != vb {
		t.Fatalf("same-key conflict did not converge: a=%q b=%q", va, vb)
	}
	if va != "from-a" && va != "from-b" {
		t.Fatalf("converged to a fabricated value %q (must be one of the two writes)", va)
	}
}

// TestMeshDeletePropagates: a delete on one member removes the key on the other via a
// version-guarded tombstone (not the stateless sweep, which is disabled in cluster mode).
func TestMeshDeletePropagates(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	a := newMeshMember(t, ctx, srcCfg(), "a")
	b := newMeshMember(t, ctx, tgtCfg(), "b")
	must(t, a.src.db.uc.FlushAll(ctx).Err())
	must(t, b.src.db.uc.FlushAll(ctx).Err())

	must(t, a.src.db.uc.Set(ctx, "gone", "v", 0).Err())
	meshConverge(t, ctx, a, b)
	if n, _ := b.src.db.uc.Exists(ctx, "gone").Result(); n != 1 {
		t.Fatalf("key did not replicate to b before delete")
	}
	// Delete on a; must propagate to b as a tombstone.
	must(t, a.src.db.uc.Del(ctx, "gone").Err())
	meshConverge(t, ctx, a, b)
	if n, _ := b.src.db.uc.Exists(ctx, "gone").Result(); n != 0 {
		t.Fatalf("delete did not propagate: key still present on b")
	}
}

// TestMeshPeerWriteNotDestroyed is the headline safety property: a fresh write on one
// member must NEVER be destroyed by the other member's reconciliation. Before the mesh
// even syncs, b creates a key a has never seen; after convergence it must survive on
// both, not be swept away because a's keyspace lacked it.
func TestMeshPeerWriteNotDestroyed(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	a := newMeshMember(t, ctx, srcCfg(), "a")
	b := newMeshMember(t, ctx, tgtCfg(), "b")
	must(t, a.src.db.uc.FlushAll(ctx).Err())
	must(t, b.src.db.uc.FlushAll(ctx).Err())

	must(t, a.src.db.uc.Set(ctx, "shared", "v", 0).Err())
	must(t, b.src.db.uc.Set(ctx, "b-only", "peer", 0).Err())
	meshConverge(t, ctx, a, b)

	if n, _ := a.src.db.uc.Exists(ctx, "b-only").Result(); n != 1 {
		t.Fatalf("peer write b-only missing on a (should have replicated in)")
	}
	if n, _ := b.src.db.uc.Exists(ctx, "b-only").Result(); n != 1 {
		t.Fatalf("peer write b-only was destroyed on b by a's reconciliation")
	}
}
