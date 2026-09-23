package daemon

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/rudimk/replicare/internal/config"
	"github.com/rudimk/replicare/internal/observability/live"
	"github.com/rudimk/replicare/internal/observability/status"
)

// TestLiveSurfaceIntegration is the end-to-end acceptance for the operator live
// surface (`replicare status --live` / `replicare verify`): after the daemon brings
// a sync to convergence, live.Verify reports it CONVERGED with matching counts and
// checksums, live.Enrich reports live source/target counts plus a per-target delta
// backlog, and an out-of-band target drift is then reported DIVERGED. It exercises
// the full config→engine wiring the CLI depends on.
func TestLiveSurfaceIntegration(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	src := dial(t, ctx, envd("RC_SRC_HOST", "127.0.0.1"), envd("RC_SRC_PORT", "5440"), envd("RC_SRC_DB", "replicare_src"))
	defer src.Close(context.Background())
	tgt := dial(t, ctx, envd("RC_DST_HOST", "127.0.0.1"), envd("RC_DST_PORT", "5441"), envd("RC_DST_DB", "replicare_dst"))
	defer tgt.Close(context.Background())

	ddl := "CREATE TABLE rc_it.orders (id int PRIMARY KEY, note text)"
	for _, c := range []*pgx.Conn{src, tgt} {
		mustExec(t, ctx, c, "DROP SCHEMA IF EXISTS rc_it CASCADE")
		mustExec(t, ctx, c, "CREATE SCHEMA rc_it")
		mustExec(t, ctx, c, ddl)
	}
	mustExec(t, ctx, src, "DROP SCHEMA IF EXISTS replicare CASCADE")
	mustExec(t, ctx, tgt, "DROP SCHEMA IF EXISTS replicare_state CASCADE")
	mustExec(t, ctx, src, "INSERT INTO rc_it.orders SELECT g, 'v'||g FROM generate_series(1,25) g")
	t.Cleanup(func() {
		bg := context.Background()
		_, _ = src.Exec(bg, "DROP SCHEMA IF EXISTS rc_it CASCADE")
		_, _ = src.Exec(bg, "DROP SCHEMA IF EXISTS replicare CASCADE")
		_, _ = tgt.Exec(bg, "DROP SCHEMA IF EXISTS rc_it CASCADE")
		_, _ = tgt.Exec(bg, "DROP SCHEMA IF EXISTS replicare_state CASCADE")
	})

	syncBlock := `
syncs:
  - name: s1
    source: src
    targets: [dst]
    include: ["rc_it.*"]
    tuning: { drain_interval: 100ms }
`
	cfg, err := config.Load(writeConfig(t, harnessConfigYAML(syncBlock)))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	d, err := New(cfg, nil)
	if err != nil {
		t.Fatalf("new daemon: %v", err)
	}
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- d.Run(runCtx) }()

	if !pollUntil(t, 30*time.Second, func() bool { return count(t, ctx, tgt, "rc_it.orders") == 25 }) {
		stop()
		t.Fatalf("initial copy did not converge: target has %d rows", count(t, ctx, tgt, "rc_it.orders"))
	}

	enricher := live.New(cfg, 20*time.Second)

	// --- verify: the converged sync reports CONVERGED with an "ok" (count+checksum) table.
	vr := enricher.Verify(ctx, "s1")
	if vr.Error != "" || !vr.Converged {
		stop()
		t.Fatalf("verify of a converged sync: converged=%v err=%q\n%+v", vr.Converged, vr.Error, vr)
	}
	ot := findVerifyTable(vr, "dst", "rc_it.orders")
	if ot == nil {
		stop()
		t.Fatalf("verify report missing rc_it.orders for target dst: %+v", vr)
	}
	if ot.Status != "ok" || ot.SourceRows != 25 || ot.TargetRows != 25 {
		stop()
		t.Fatalf("verify orders = %+v, want status ok, 25/25", *ot)
	}

	// --- enrich: live counts + a per-target delta backlog on a bare base report.
	er := enricher.Enrich(ctx, "s1", status.Report{Sync: "s1"})
	if er.LiveError != "" {
		t.Logf("enrich live note (non-fatal): %s", er.LiveError)
	}
	ts := findTable(er, "rc_it.orders")
	if ts == nil || ts.SourceRows == nil || *ts.SourceRows != 25 {
		stop()
		t.Fatalf("enrich source rows = %v, want 25", srcRowsOf(ts))
	}
	tt := findTarget(ts, "dst")
	if tt == nil || tt.TargetRows == nil || *tt.TargetRows != 25 {
		stop()
		t.Fatalf("enrich target rows = %v, want 25", tgtRowsOf(tt))
	}
	if tt.Backlog == nil {
		stop()
		t.Fatalf("enrich did not report a delta backlog for a capture-installed source")
	}

	// Stop the daemon before drifting, so it cannot re-converge the manual change.
	stop()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("daemon Run returned %v, want nil", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("daemon did not stop within 15s")
	}

	// --- verify catches an out-of-band target drift (count).
	mustExec(t, ctx, tgt, "DELETE FROM rc_it.orders WHERE id = 3")
	vr2 := enricher.Verify(ctx, "s1")
	if vr2.Converged {
		t.Fatalf("verify did not catch a target drift: %+v", vr2)
	}
	ot2 := findVerifyTable(vr2, "dst", "rc_it.orders")
	if ot2 == nil || ot2.Status != "drift-count" {
		t.Fatalf("drift status = %v, want drift-count", statusOf(ot2))
	}
}

func findVerifyTable(r live.VerifyReport, target, table string) *live.VerifyTable {
	for _, tg := range r.Targets {
		if tg.Target != target {
			continue
		}
		for i := range tg.Tables {
			if tg.Tables[i].Table == table {
				return &tg.Tables[i]
			}
		}
	}
	return nil
}

func findTable(r status.Report, table string) *status.TableStatus {
	for i := range r.Tables {
		if r.Tables[i].Table == table {
			return &r.Tables[i]
		}
	}
	return nil
}

func findTarget(ts *status.TableStatus, target string) *status.TargetStatus {
	for i := range ts.Targets {
		if ts.Targets[i].Target == target {
			return &ts.Targets[i]
		}
	}
	return nil
}

func srcRowsOf(ts *status.TableStatus) any {
	if ts == nil || ts.SourceRows == nil {
		return nil
	}
	return *ts.SourceRows
}
func tgtRowsOf(tt *status.TargetStatus) any {
	if tt == nil || tt.TargetRows == nil {
		return nil
	}
	return *tt.TargetRows
}
func statusOf(t *live.VerifyTable) any {
	if t == nil {
		return nil
	}
	return t.Status
}
