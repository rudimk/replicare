package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/copy"
	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/observability"
	"github.com/rudimk/replicare/internal/observability/prom"
	"github.com/rudimk/replicare/internal/observability/telemetry"
)

// openers gives a syncer connections of its own, as the daemon does.
func withOpeners(s *Syncer) {
	eng, _ := engine.Get("postgres")
	s.OpenSource = func(ctx context.Context) (engine.Source, error) {
		src, err := eng.NewSource(srcCfg())
		if err != nil {
			return nil, err
		}
		return src, src.Connect(ctx)
	}
	s.OpenSink = func(ctx context.Context) (engine.Sink, error) {
		sink, err := eng.NewSink(tgtCfg())
		if err != nil {
			return nil, err
		}
		return sink, sink.Connect(ctx)
	}
}

// TestMonitorPublishesLagWithoutAPass: with the monitor, the lag shows from the start of
// streaming even when no pass has run at all.
func TestMonitorPublishesLagWithoutAPass(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	rawSrc, _ := isolationSchema(t, ctx)
	syncer := buildSyncer(t, ctx)
	withOpeners(syncer)
	if err := syncer.Bringup(ctx); err != nil {
		t.Fatalf("Bringup: %v", err)
	}
	mustExecC(t, ctx, rawSrc, "UPDATE rc_it.b_fine SET note = 'queued' WHERE id = 1")

	reg := prom.New()
	syncer.Tel = telemetry.New(reg, nil, nil, nil)
	syncer.DrainInterval = time.Hour // no pass in the life of this test
	old := monitorInterval
	monitorInterval = 100 * time.Millisecond
	t.Cleanup(func() { monitorInterval = old })

	sctx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- syncer.Stream(sctx) }()
	deadline := time.Now().Add(20 * time.Second)
	for !metricPresent(t, reg, observability.MetricReplicationLagSeconds) {
		if time.Now().After(deadline) {
			stop()
			<-done
			t.Fatal("no lag published by the monitor within 20s, with no pass run")
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !metricPresent(t, reg, observability.MetricSourceDBBytes) {
		t.Error("no source size published by the monitor")
	}
	stop()
	<-done
	if syncer.monitorOn.Load() {
		t.Error("monitor still marked on after Stream returned")
	}
}

// TestGroupsApplySideBySide: with a pool of two pairs, independent groups drain in the
// same pass, and a group the target refuses still doesn't stop the other.
func TestGroupsApplySideBySide(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	rawSrc, rawTgt := isolationSchema(t, ctx)
	syncer := buildSyncer(t, ctx)
	syncer.Workers = append(syncer.Workers, copy.Worker{Src: connectedSource(t, ctx), Sink: connectedSink(t, ctx)})
	syncer.ApplyConcurrency = 3
	if err := syncer.Bringup(ctx); err != nil {
		t.Fatalf("Bringup: %v", err)
	}
	if n := len(syncer.applyPool()); n != 3 {
		t.Fatalf("apply pool of %d pairs, want 3", n)
	}
	mustExecC(t, ctx, rawSrc, "UPDATE rc_it.a_refuses SET n = 500 WHERE id = 1")
	mustExecC(t, ctx, rawSrc, "UPDATE rc_it.a_refuses SET n = 7 WHERE id = 2")
	mustExecC(t, ctx, rawSrc, "UPDATE rc_it.b_fine SET note = 'changed' WHERE id = 1")
	mustExecC(t, ctx, rawSrc, "INSERT INTO rc_it.b_fine SELECT g, 'new' FROM generate_series(11, 500) g")

	// The test syncer applies at most 100 changes per table per pass.
	for i := 0; i < 8; i++ {
		if err := syncer.StreamOnce(ctx); err == nil {
			t.Fatal("every pass should fail loud on the refused row")
		}
	}
	if got := cellFor(t, ctx, rawTgt, "SELECT note FROM rc_it.b_fine WHERE id = 1"); got != "changed" {
		t.Errorf("b_fine id 1 = %q", got)
	}
	if countRows(t, ctx, rawTgt, "rc_it.b_fine") != 500 {
		t.Errorf("b_fine has %d rows, want 500", countRows(t, ctx, rawTgt, "rc_it.b_fine"))
	}

	// Fix the cause on the target: the next pass applies the refused group too.
	mustExecC(t, ctx, rawTgt, "ALTER TABLE rc_it.a_refuses DROP CONSTRAINT a_refuses_n_check")
	if err := syncer.StreamOnce(ctx); err != nil {
		t.Fatalf("pass after the fix: %v", err)
	}
	if got := cellFor(t, ctx, rawTgt, "SELECT n::text FROM rc_it.a_refuses WHERE id = 1"); got != "500" {
		t.Errorf("a_refuses id 1 = %s after the fix, want 500", got)
	}
}
