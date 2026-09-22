package postgres

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

// applyVersioned applies one cluster drain pass to the sink for a single table: it
// stages the given versioned COPY text (user cols + rc_hlc_phys, rc_hlc_log, rc_node,
// rc_deleted) and runs both phases (alive upsert + tombstone delete) in one committed
// apply transaction — exactly the version-guarded HLC-LWW path.
func applyVersioned(t *testing.T, ctx context.Context, sink *Sink, ref engine.TableRef, cols []string, rows string) {
	t.Helper()
	tx, err := sink.BeginApply(ctx, false, nil)
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

// noteOf returns the note column for id=1, or "" if the row is absent.
func noteOf(t *testing.T, ctx context.Context, sink *Sink, id int) (string, bool) {
	t.Helper()
	var note string
	err := sink.conn.QueryRow(ctx, "SELECT note FROM rc_it.orders WHERE id=$1", id).Scan(&note)
	if err != nil {
		return "", false
	}
	return note, true
}

// TestClusterApplyLWWResolvesByVersion is the MM4 conflict-resolution oracle at the
// apply level: an incoming change is applied iff its (hlc, node) strictly beats the
// value the target holds — so a lower version arriving LATER loses (out-of-order
// safety), an equal HLC is broken by node id, and a tombstone competes with updates
// under the same total order.
func TestClusterApplyLWWResolvesByVersion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// The target (17) node is the applying member B.
	src := &Source{cfg: harnessConn(t, "target")}
	if err := src.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	dropCaptureSchema(t, ctx, src.conn)
	mustExec(t, ctx, src.conn, "DROP SCHEMA IF EXISTS rc_it CASCADE")
	mustExec(t, ctx, src.conn, "CREATE SCHEMA rc_it")
	mustExec(t, ctx, src.conn, "CREATE TABLE rc_it.orders (id int PRIMARY KEY, note text)")
	t.Cleanup(func() {
		bg := context.Background()
		dropCaptureSchema(t, bg, src.conn)
		_, _ = src.conn.Exec(bg, "DROP SCHEMA IF EXISTS rc_it CASCADE")
		_ = src.Close(bg)
	})

	ref := engine.TableRef{Schema: "rc_it", Name: "orders"}
	cols := []string{"id", "note"}

	// The member is also a source (its outbound edge), which is what creates the
	// replicare schema + mesh state + register on this node; do that here.
	if err := src.InstallOriginCapture(ctx, []engine.TableRef{ref}, "nodeB"); err != nil {
		t.Fatalf("InstallOriginCapture: %v", err)
	}

	sink := &Sink{cfg: harnessConn(t, "target")}
	if err := sink.Connect(ctx); err != nil {
		t.Fatalf("connect sink: %v", err)
	}
	sink.EnableOriginMarking("nodeB")
	t.Cleanup(func() { _ = sink.Close(context.Background()) })

	// Row format: id \t note \t phys \t log \t node \t deleted
	// 1) First write at hlc (1000,0) from nodeA -> lands.
	applyVersioned(t, ctx, sink, ref, cols, "1\tv-1000\t1000\t0\tnodeA\tf\n")
	if n, ok := noteOf(t, ctx, sink, 1); !ok || n != "v-1000" {
		t.Fatalf("after first write: note=%q ok=%v, want v-1000", n, ok)
	}

	// 2) A LOWER version (500,0) arriving later must LOSE (out-of-order safety).
	applyVersioned(t, ctx, sink, ref, cols, "1\tv-500\t500\t0\tnodeA\tf\n")
	if n, _ := noteOf(t, ctx, sink, 1); n != "v-1000" {
		t.Errorf("lower version won: note=%q, want v-1000 (unchanged)", n)
	}

	// 3) A HIGHER version (2000,0) wins.
	applyVersioned(t, ctx, sink, ref, cols, "1\tv-2000\t2000\t0\tnodeA\tf\n")
	if n, _ := noteOf(t, ctx, sink, 1); n != "v-2000" {
		t.Errorf("higher version lost: note=%q, want v-2000", n)
	}

	// 4) Equal HLC (2000,0): a HIGHER node id ('zzz' > 'nodeA') wins the tie...
	applyVersioned(t, ctx, sink, ref, cols, "1\tv-zzz\t2000\t0\tzzz\tf\n")
	if n, _ := noteOf(t, ctx, sink, 1); n != "v-zzz" {
		t.Errorf("node-id tiebreak (higher) lost: note=%q, want v-zzz", n)
	}
	// ...and a LOWER node id ('aaa' < 'zzz') at the same HLC loses.
	applyVersioned(t, ctx, sink, ref, cols, "1\tv-aaa\t2000\t0\taaa\tf\n")
	if n, _ := noteOf(t, ctx, sink, 1); n != "v-zzz" {
		t.Errorf("node-id tiebreak (lower) won: note=%q, want v-zzz (unchanged)", n)
	}

	// 5) A tombstone at (3000,0) beats the live value -> row deleted.
	applyVersioned(t, ctx, sink, ref, cols, "1\t\\N\t3000\t0\tnodeA\tt\n")
	if _, ok := noteOf(t, ctx, sink, 1); ok {
		t.Errorf("tombstone did not delete the row")
	}

	// 6) A revive (2500,0) OLDER than the tombstone (3000,0) must LOSE — the row
	// stays deleted (delete-vs-update resolves under the same total order).
	applyVersioned(t, ctx, sink, ref, cols, "1\trevive\t2500\t0\tnodeA\tf\n")
	if _, ok := noteOf(t, ctx, sink, 1); ok {
		t.Errorf("older revive resurrected a tombstoned row")
	}

	// 7) A revive (3500,0) NEWER than the tombstone wins — row comes back.
	applyVersioned(t, ctx, sink, ref, cols, "1\treborn\t3500\t0\tnodeA\tf\n")
	if n, ok := noteOf(t, ctx, sink, 1); !ok || n != "reborn" {
		t.Errorf("newer revive lost: note=%q ok=%v, want reborn", n, ok)
	}
}
