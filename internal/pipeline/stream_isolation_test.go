package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/observability"
	"github.com/rudimk/replicare/internal/observability/prom"
	"github.com/rudimk/replicare/internal/observability/telemetry"
)

// isolationSchema: two unrelated tables, so two groups. The target's a_refuses only
// takes n < 100 (a CHECK the source lacks), so a change to n >= 100 is a value the
// target refuses: a failure of that group alone, with both databases up.
func isolationSchema(t *testing.T, ctx context.Context) (rawSrc, rawTgt *pgx.Conn) {
	t.Helper()
	rawSrc = rawConn(t, ctx, srcCfg())
	rawTgt = rawConn(t, ctx, tgtCfg())
	for _, c := range []*pgx.Conn{rawSrc, rawTgt} {
		mustExecC(t, ctx, c, "DROP SCHEMA IF EXISTS rc_it CASCADE")
		mustExecC(t, ctx, c, "CREATE SCHEMA rc_it")
		mustExecC(t, ctx, c, "CREATE TABLE rc_it.b_fine (id int PRIMARY KEY, note text)")
	}
	mustExecC(t, ctx, rawSrc, "CREATE TABLE rc_it.a_refuses (id int PRIMARY KEY, n int)")
	mustExecC(t, ctx, rawTgt, "CREATE TABLE rc_it.a_refuses (id int PRIMARY KEY, n int CHECK (n < 100))")
	mustExecC(t, ctx, rawSrc, "DROP SCHEMA IF EXISTS replicare CASCADE")
	mustExecC(t, ctx, rawTgt, "DROP SCHEMA IF EXISTS replicare_state CASCADE")
	mustExecC(t, ctx, rawSrc, "INSERT INTO rc_it.a_refuses SELECT g, g FROM generate_series(1, 10) g")
	mustExecC(t, ctx, rawSrc, "INSERT INTO rc_it.b_fine SELECT g, 'n'||g FROM generate_series(1, 10) g")
	t.Cleanup(func() {
		bg := context.Background()
		_, _ = rawSrc.Exec(bg, "DROP SCHEMA IF EXISTS rc_it CASCADE")
		_, _ = rawSrc.Exec(bg, "DROP SCHEMA IF EXISTS replicare CASCADE")
		_, _ = rawTgt.Exec(bg, "DROP SCHEMA IF EXISTS rc_it CASCADE")
		_, _ = rawTgt.Exec(bg, "DROP SCHEMA IF EXISTS replicare_state CASCADE")
		_ = rawSrc.Close(bg)
		_ = rawTgt.Close(bg)
	})
	return rawSrc, rawTgt
}

// TestStreamOneFailingGroupDoesNotStopTheOthers: a group the target refuses fails
// the pass, loud, but the other groups still apply, and the lag is still published.
func TestStreamOneFailingGroupDoesNotStopTheOthers(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	rawSrc, rawTgt := isolationSchema(t, ctx)
	syncer := buildSyncer(t, ctx)
	if len(syncer.Components) != 2 {
		t.Fatalf("want two groups, got %d", len(syncer.Components))
	}
	if err := syncer.Bringup(ctx); err != nil {
		t.Fatalf("Bringup: %v", err)
	}

	// A fresh registry, so the lag below can only come from the failing pass.
	reg := prom.New()
	syncer.Tel = telemetry.New(reg, nil, nil, nil)

	mustExecC(t, ctx, rawSrc, "UPDATE rc_it.a_refuses SET n = 500 WHERE id = 1") // refused by the target
	mustExecC(t, ctx, rawSrc, "UPDATE rc_it.b_fine SET note = 'changed' WHERE id = 1")
	mustExecC(t, ctx, rawSrc, "INSERT INTO rc_it.b_fine VALUES (11, 'new')")

	if err := syncer.StreamOnce(ctx); err == nil {
		t.Fatal("the pass should fail loud on the refused row")
	}
	if got := cellFor(t, ctx, rawTgt, "SELECT note FROM rc_it.b_fine WHERE id = 1"); got != "changed" {
		t.Errorf("b_fine id 1 = %q: the group after the failing one did not apply", got)
	}
	if countRows(t, ctx, rawTgt, "rc_it.b_fine") != 11 {
		t.Errorf("b_fine has %d rows, want 11", countRows(t, ctx, rawTgt, "rc_it.b_fine"))
	}
	if got := cellFor(t, ctx, rawTgt, "SELECT n::text FROM rc_it.a_refuses WHERE id = 1"); got != "1" {
		t.Errorf("a_refuses id 1 = %s on the target, want it left at 1", got)
	}
	if !metricPresent(t, reg, observability.MetricReplicationLagSeconds) {
		t.Error("no lag published after a failed pass")
	}
	cur, err := syncer.Store.LoadCursor(ctx, syncer.Name, syncer.Target, engine.TableRef{Schema: "rc_it", Name: "b_fine"})
	if err != nil {
		t.Fatal(err)
	}
	if cur.LastAppliedRows != 2 || cur.LastAppliedAt.IsZero() {
		t.Errorf("b_fine's last applied = %d rows at %v, want 2 rows recorded by the failed pass", cur.LastAppliedRows, cur.LastAppliedAt)
	}
}

// TestCutoverLoggedOnlyWhenAGroupSwitches: a restart passes every group already
// streaming through the cutover again; only a real switch is logged.
func TestCutoverLoggedOnlyWhenAGroupSwitches(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	isolationSchema(t, ctx)
	syncer := buildSyncer(t, ctx)
	cutovers := func() int {
		events, err := syncer.Store.RecentEvents(ctx, syncer.Name, 1000)
		if err != nil {
			t.Fatal(err)
		}
		n := 0
		for _, e := range events {
			if e.Event == observability.EventCutover {
				n++
			}
		}
		return n
	}
	if err := syncer.Bringup(ctx); err != nil {
		t.Fatalf("Bringup: %v", err)
	}
	if got := cutovers(); got != 2 {
		t.Fatalf("first start: %d cutover events, want one per group (2)", got)
	}
	if err := syncer.Bringup(ctx); err != nil {
		t.Fatalf("second Bringup: %v", err)
	}
	if got := cutovers(); got != 2 {
		t.Fatalf("after a restart: %d cutover events, want still 2", got)
	}
}
