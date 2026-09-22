package postgres

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

// Loop-suppression integration tests (CLAUDE.md §6, docs/multi-master.md §5.4).
// They require the multi-version harness (`task harness:up`): the target (17) node
// doubles as a cluster member B — it has origin-aware capture installed AND receives
// replicare applies — so we can prove the trigger suppresses replicare's own writes
// while still capturing ordinary local writes. Gated on REPLICARE_INTEGRATION=1 via
// harnessConn (skips otherwise).

// clusterMember opens a Source (for install + delta inspection) and a marked Sink
// (for apply/copy) against ONE harness node, with a clean capture + rc_it schema.
// Both connect to the same database, so an apply through the Sink is subject to the
// capture the Source installed — exactly a cluster member. When markSink is false the
// Sink is left one-way (no origin marking) to prove the marker is what suppresses.
func clusterMember(t *testing.T, ctx context.Context, role string, markSink bool) (*Source, *Sink) {
	t.Helper()
	src := &Source{cfg: harnessConn(t, role)}
	if err := src.Connect(ctx); err != nil {
		t.Fatalf("connect member source (%s): %v", role, err)
	}
	dropCaptureSchema(t, ctx, src.conn)
	mustExec(t, ctx, src.conn, "DROP SCHEMA IF EXISTS rc_it CASCADE")
	mustExec(t, ctx, src.conn, "CREATE SCHEMA rc_it")

	sink := &Sink{cfg: harnessConn(t, role)}
	if err := sink.Connect(ctx); err != nil {
		t.Fatalf("connect member sink (%s): %v", role, err)
	}
	if markSink {
		sink.EnableOriginMarking("nodeB")
	}
	t.Cleanup(func() {
		bg := context.Background()
		dropCaptureSchema(t, bg, src.conn)
		_, _ = src.conn.Exec(bg, "DROP SCHEMA IF EXISTS rc_it CASCADE")
		_ = sink.Close(bg)
		_ = src.Close(bg)
	})
	return src, sink
}

// TestOriginCaptureSuppressesCrossNodeApply is the MM3 acceptance in miniature: a
// change originating on node A, applied to node B (a cluster member with origin-aware
// capture), is NOT re-captured on B (no echo back around the mesh), while a local
// write on B still IS captured (so B's own changes replicate outward).
func TestOriginCaptureSuppressesCrossNodeApply(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Node A: an ordinary source holding the originating row we re-read from.
	srcA := captureSource(t, ctx) // "source" role (9.6)
	setupSourceTables(t, ctx, srcA, "CREATE TABLE rc_it.orders (id int PRIMARY KEY, note text)")
	mustExec(t, ctx, srcA.conn, "INSERT INTO rc_it.orders (id, note) VALUES (1, 'from-A')")
	refA := engine.TableRef{Schema: "rc_it", Name: "orders"}

	// Node B: a cluster member (origin-aware capture + marked apply sink).
	srcB, sinkB := clusterMember(t, ctx, "target", true)
	mustExec(t, ctx, srcB.conn, "CREATE TABLE rc_it.orders (id int PRIMARY KEY, note text)")
	refB := engine.TableRef{Schema: "rc_it", Name: "orders"}
	if err := srcB.InstallOriginCapture(ctx, []engine.TableRef{refB}, "nodeB"); err != nil {
		t.Fatalf("InstallOriginCapture on B: %v", err)
	}
	relID, ok, err := lookupRegistry(ctx, srcB.conn, refB)
	if err != nil || !ok {
		t.Fatalf("registry lookup on B: ok=%v err=%v", ok, err)
	}

	// The trigger really carries the loop-suppression guard on B.
	if !objectExists(t, ctx, srcB,
		`SELECT EXISTS(SELECT 1 FROM pg_trigger WHERE tgname=$1 AND tgrelid=$2::regclass AND tgqual IS NOT NULL AND NOT tgisinternal)`,
		triggerName(relID), qualifyTable(refB)) {
		t.Fatal("origin trigger on B has no WHEN guard (tgqual is NULL)")
	}

	// Apply A -> B through the MARKED sink (replicare's own write).
	applyCrossNode(t, ctx, srcA, sinkB, refA, refB, []string{"id", "note"}, []engine.KeyValues{{"1"}})

	// The row landed on B ...
	if got := scalarInt(t, ctx, srcB, "SELECT count(*) FROM rc_it.orders WHERE id=1 AND note='from-A'"); got != 1 {
		t.Fatalf("applied row not present on B (count=%d)", got)
	}
	// ... but was NOT re-captured (loop suppressed).
	if got := deltaCount(t, ctx, srcB, relID); got != 0 {
		t.Fatalf("replicare-applied write was re-captured on B: %d delta rows, want 0", got)
	}

	// A LOCAL write on B (ordinary, unmarked connection) IS captured, so B's own
	// changes still replicate outward.
	mustExec(t, ctx, srcB.conn, "INSERT INTO rc_it.orders (id, note) VALUES (2, 'local-B')")
	if got := deltaCount(t, ctx, srcB, relID); got != 1 {
		t.Fatalf("local write on B was not captured: %d delta rows, want 1", got)
	}
}

// TestOriginCopyMarkerSuppressesBootstrap proves the bootstrap-copy path
// (BulkLoad direct) also carries the marker on a cluster member, so a cold copy does
// not self-capture — and that a one-way (unmarked) sink writing the same table WOULD
// capture, i.e. EnableOriginMarking is exactly what gates suppression.
func TestOriginCopyMarkerSuppressesBootstrap(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	src, markedSink := clusterMember(t, ctx, "target", true)
	mustExec(t, ctx, src.conn, "CREATE TABLE rc_it.orders (id int PRIMARY KEY, note text)")
	ref := engine.TableRef{Schema: "rc_it", Name: "orders"}
	if err := src.InstallOriginCapture(ctx, []engine.TableRef{ref}, "nodeB"); err != nil {
		t.Fatalf("InstallOriginCapture: %v", err)
	}
	relID, _, _ := lookupRegistry(ctx, src.conn, ref)

	// Marked bootstrap COPY -> not captured.
	if _, err := markedSink.BulkLoad(ctx, ref, []string{"id", "note"}, strings.NewReader("5\tx\n"), engine.LoadDirect); err != nil {
		t.Fatalf("marked BulkLoad: %v", err)
	}
	if got := deltaCount(t, ctx, src, relID); got != 0 {
		t.Fatalf("marked bootstrap copy was captured: %d delta rows, want 0", got)
	}

	// An UNMARKED sink (one-way) writing the same origin-captured table IS captured —
	// proving the marker, not the trigger's mere presence, is what suppresses.
	oneWaySink := &Sink{cfg: harnessConn(t, "target")}
	if err := oneWaySink.Connect(ctx); err != nil {
		t.Fatalf("connect one-way sink: %v", err)
	}
	defer oneWaySink.Close(context.Background())
	if _, err := oneWaySink.BulkLoad(ctx, ref, []string{"id", "note"}, strings.NewReader("6\ty\n"), engine.LoadDirect); err != nil {
		t.Fatalf("unmarked BulkLoad: %v", err)
	}
	if got := deltaCount(t, ctx, src, relID); got != 1 {
		t.Fatalf("unmarked write was not captured: %d delta rows, want 1", got)
	}
}

// applyCrossNode pipes RereadCurrent (node A) -> ApplyPass (node B) for the given
// keys, mirroring the streaming apply path with A as the change origin and B the
// applying member.
func applyCrossNode(t *testing.T, ctx context.Context, srcA *Source, sinkB *Sink, refA, refB engine.TableRef, cols []string, keys []engine.KeyValues) {
	t.Helper()
	pr, pw := io.Pipe()
	errc := make(chan error, 1)
	go func() {
		err := srcA.RereadCurrent(ctx, refA, keys, pw)
		_ = pw.CloseWithError(err)
		errc <- err
	}()
	applyErr := sinkB.ApplyPass(ctx, refB, cols, keys, pr)
	_ = pr.CloseWithError(applyErr)
	if rereadErr := <-errc; rereadErr != nil {
		t.Fatalf("re-read from A: %v", rereadErr)
	}
	if applyErr != nil {
		t.Fatalf("apply to B: %v", applyErr)
	}
}

func scalarInt(t *testing.T, ctx context.Context, src *Source, query string) int {
	t.Helper()
	var n int
	if err := src.conn.QueryRow(ctx, query).Scan(&n); err != nil {
		t.Fatalf("scalar %q: %v", query, err)
	}
	return n
}
