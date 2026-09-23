package mysql

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

// applyVersioned applies one cluster drain pass to the sink for a single table: it
// stages the given versioned LOAD DATA text (user cols + rc_hlc_phys, rc_hlc_log,
// rc_node, rc_deleted) and runs both phases (alive upsert + tombstone delete) in one
// committed apply transaction — the version-guarded HLC-LWW path.
func applyVersioned(t *testing.T, ctx context.Context, sink *Sink, ref engine.TableRef, cols []string, rows string) {
	t.Helper()
	// Warm the sink introspection cache before pinning the apply connection (the pool
	// is MaxOpenConns=1, so a cache-miss introspection while pinned would block).
	if _, err := sink.tableMeta(ctx, ref); err != nil {
		t.Fatalf("warm tableMeta: %v", err)
	}
	tx, err := sink.BeginApply(ctx, false, []engine.TableRef{ref})
	if err != nil {
		t.Fatalf("BeginApply: %v", err)
	}
	if err := tx.StageUpsert(ctx, ref, cols, strings.NewReader(rows)); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("StageUpsert: %v", err)
	}
	if err := tx.DeleteAbsent(ctx, ref, nil); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("DeleteAbsent: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}
}

func noteOf(t *testing.T, ctx context.Context, sink *Sink, id int) (string, bool) {
	t.Helper()
	var note string
	err := sink.db.QueryRowContext(ctx, "SELECT note FROM rc_it.orders WHERE id=?", id).Scan(&note)
	if err != nil {
		return "", false
	}
	return note, true
}

// TestClusterApplyLWWResolvesByVersion is the MM5 conflict-resolution oracle at the
// apply level (mirror of the Postgres test): an incoming change is applied iff its
// (hlc, node) strictly beats the target's — so a lower version arriving later loses,
// an equal HLC is broken by node id, and a tombstone competes with updates under one
// total order.
func TestClusterApplyLWWResolvesByVersion(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// The target node doubles as the applying member B: a Source to create the mesh
	// objects (its own outbound edge would), and a marked Sink to apply.
	src := &Source{cfg: tgtCfg()}
	if err := src.Connect(ctx); err != nil {
		t.Fatalf("connect src(target): %v", err)
	}
	t.Cleanup(func() { _ = src.Close(context.Background()) })
	mustExec(t, ctx, src.db,
		"DROP DATABASE IF EXISTS replicare",
		"DROP DATABASE IF EXISTS rc_it",
		"CREATE DATABASE rc_it",
		"CREATE TABLE rc_it.orders (id INT PRIMARY KEY, note VARCHAR(50)) ENGINE=InnoDB",
	)
	t.Cleanup(func() {
		_, _ = src.db.Exec("DROP DATABASE IF EXISTS rc_it")
		_, _ = src.db.Exec("DROP DATABASE IF EXISTS replicare")
	})
	ref := engine.TableRef{Schema: "rc_it", Name: "orders"}
	if err := src.InstallOriginCapture(ctx, []engine.TableRef{ref}, "nodeB"); err != nil {
		t.Fatalf("InstallOriginCapture: %v", err)
	}

	sink := &Sink{cfg: tgtCfg()}
	if err := sink.Connect(ctx); err != nil {
		t.Fatalf("connect sink: %v", err)
	}
	sink.EnableOriginMarking("nodeB")
	t.Cleanup(func() { _ = sink.Close(context.Background()) })

	cols := []string{"id", "note"}
	// Row format: id \t note \t phys \t log \t node \t deleted
	applyVersioned(t, ctx, sink, ref, cols, "1\tv-1000\t1000\t0\tnodeA\t0\n")
	if n, ok := noteOf(t, ctx, sink, 1); !ok || n != "v-1000" {
		t.Fatalf("after first write: note=%q ok=%v, want v-1000", n, ok)
	}
	// Lower version arriving later loses.
	applyVersioned(t, ctx, sink, ref, cols, "1\tv-500\t500\t0\tnodeA\t0\n")
	if n, _ := noteOf(t, ctx, sink, 1); n != "v-1000" {
		t.Errorf("lower version won: note=%q, want v-1000", n)
	}
	// Higher version wins.
	applyVersioned(t, ctx, sink, ref, cols, "1\tv-2000\t2000\t0\tnodeA\t0\n")
	if n, _ := noteOf(t, ctx, sink, 1); n != "v-2000" {
		t.Errorf("higher version lost: note=%q, want v-2000", n)
	}
	// Equal HLC: higher node id wins the tie, lower loses.
	applyVersioned(t, ctx, sink, ref, cols, "1\tv-zzz\t2000\t0\tzzz\t0\n")
	if n, _ := noteOf(t, ctx, sink, 1); n != "v-zzz" {
		t.Errorf("node-id tiebreak (higher) lost: note=%q, want v-zzz", n)
	}
	applyVersioned(t, ctx, sink, ref, cols, "1\tv-aaa\t2000\t0\taaa\t0\n")
	if n, _ := noteOf(t, ctx, sink, 1); n != "v-zzz" {
		t.Errorf("node-id tiebreak (lower) won: note=%q, want v-zzz", n)
	}
	// Tombstone at (3000,0) beats the live value -> row deleted.
	applyVersioned(t, ctx, sink, ref, cols, "1\t\\N\t3000\t0\tnodeA\t1\n")
	if _, ok := noteOf(t, ctx, sink, 1); ok {
		t.Errorf("tombstone did not delete the row")
	}
	// Older revive (2500,0) loses -> stays deleted.
	applyVersioned(t, ctx, sink, ref, cols, "1\trevive\t2500\t0\tnodeA\t0\n")
	if _, ok := noteOf(t, ctx, sink, 1); ok {
		t.Errorf("older revive resurrected a tombstoned row")
	}
	// Newer revive (3500,0) wins -> row comes back.
	applyVersioned(t, ctx, sink, ref, cols, "1\treborn\t3500\t0\tnodeA\t0\n")
	if n, ok := noteOf(t, ctx, sink, 1); !ok || n != "reborn" {
		t.Errorf("newer revive lost: note=%q ok=%v, want reborn", n, ok)
	}
}
