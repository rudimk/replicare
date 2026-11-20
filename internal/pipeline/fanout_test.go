package pipeline

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/rudimk/replicare/internal/copy"
	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/observability/telemetry"
	"github.com/rudimk/replicare/internal/reseed"
	"github.com/rudimk/replicare/internal/state"
	statepg "github.com/rudimk/replicare/internal/state/postgres"
)

// fanoutDB is the second target database for the fan-out tests, on the harness
// target server next to replicare_dst.
const fanoutDB = "replicare_dst_pfan"

// buildFanOutSyncer builds a Syncer for sync "s1" writing to the target database
// tgt as target id, the way the daemon does for each target of a fan-out sync.
func buildFanOutSyncer(t *testing.T, ctx context.Context, tgt engine.ConnConfig, id engine.TargetID, store state.StateStore) *Syncer {
	t.Helper()
	eng, _ := engine.Get("postgres")
	open := func() (engine.Source, engine.Sink) {
		src, err := eng.NewSource(srcCfg())
		if err != nil {
			t.Fatalf("new source: %v", err)
		}
		if err := src.Connect(ctx); err != nil {
			t.Fatalf("connect source: %v", err)
		}
		sink, err := eng.NewSink(tgt)
		if err != nil {
			t.Fatalf("new sink: %v", err)
		}
		if err := sink.Connect(ctx); err != nil {
			t.Fatalf("connect sink: %v", err)
		}
		t.Cleanup(func() {
			_ = src.Close(context.Background())
			_ = sink.Close(context.Background())
		})
		return src, sink
	}
	source, sink := open()
	wsrc, wsink := open()

	sel := engine.Selection{Include: []string{"rc_it.*"}}
	srcSchema, err := source.Introspect(ctx, sel)
	if err != nil {
		t.Fatalf("introspect source: %v", err)
	}
	tgtSchema, err := sink.Introspect(ctx, sel)
	if err != nil {
		t.Fatalf("introspect target: %v", err)
	}
	srcVer, _ := source.ServerVersion(ctx)
	tgtVer, _ := sink.ServerVersion(ctx)
	report := eng.Preflight("s1", srcVer, tgtVer, srcSchema, tgtSchema)
	if report.Blocked() {
		t.Fatalf("pre-flight blocked: %+v", report.Findings)
	}
	return &Syncer{
		Name:          "s1",
		Source:        source,
		Sink:          sink,
		Target:        id,
		Workers:       []copy.Worker{{Src: wsrc, Sink: wsink}},
		Store:         store,
		Tel:           telemetry.New(nil, nil, nil, nil),
		Components:    report.Components,
		Replicable:    report.Replicable,
		ChunkOpts:     engine.ChunkOptions{TargetRows: 10},
		DrainBatch:    100,
		DrainInterval: 50 * time.Millisecond,
	}
}

// deltaRows counts the rows left in a captured table's source delta table.
func deltaRows(t *testing.T, ctx context.Context, rawSrc *pgx.Conn, table string) int {
	t.Helper()
	var relID int
	if err := rawSrc.QueryRow(ctx,
		"SELECT rel_id FROM replicare.captured WHERE schema_name = 'rc_it' AND table_name = $1", table).Scan(&relID); err != nil {
		t.Fatalf("capture row for %s: %v", table, err)
	}
	return countRows(t, ctx, rawSrc, fmt.Sprintf("replicare.delta_%d", relID))
}

// TestFanOutPurgeWaitsForEveryTarget is the regression test for the fan-out purge
// bug behind the flaky TestDaemonFanOutTwoTargets. A fan-out sync runs one Syncer
// per target over the SAME source delta tables. Each streaming pass drains, then
// purges. The purge used to be gated on the syncer's own target only, so whichever
// target streamed first purged deltas the other had not read yet, and that target
// never received those changes. In CI it showed as: dst1 converged, dst2 missing
// both changes, the delta table empty, the track holding only dst1's rows.
//
// The test makes that ordering deterministic: dst runs full streaming passes
// (drain + purge) before dst2 runs any. The deltas must survive until dst2 has
// consumed them, and dst2 must then converge.
func TestFanOutPurgeWaitsForEveryTarget(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	rawSrc, rawTgt := freshFKSchema(t, ctx)
	mustExecC(t, ctx, rawTgt, "DROP DATABASE IF EXISTS "+fanoutDB+" WITH (FORCE)")
	mustExecC(t, ctx, rawTgt, "CREATE DATABASE "+fanoutDB)
	tgt2Cfg := tgtCfg()
	tgt2Cfg.Database = fanoutDB
	rawTgt2 := rawConn(t, ctx, tgt2Cfg)
	t.Cleanup(func() {
		bg := context.Background()
		_ = rawTgt2.Close(bg)
		_, _ = rawTgt.Exec(bg, "DROP DATABASE IF EXISTS "+fanoutDB+" WITH (FORCE)")
	})
	mustExecC(t, ctx, rawTgt2, "CREATE SCHEMA rc_it")
	mustExecC(t, ctx, rawTgt2, "CREATE TABLE rc_it.customers (id int PRIMARY KEY, name text)")
	mustExecC(t, ctx, rawTgt2, "CREATE TABLE rc_it.orders (id int PRIMARY KEY, customer_id int NOT NULL REFERENCES rc_it.customers(id), note text)")

	store := statepg.New(tgtCfg())
	if err := store.Open(ctx); err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close(context.Background()) })
	if err := store.PutSync(ctx, state.SyncDef{Name: "s1", Source: "src", Targets: []engine.TargetID{"dst", "dst2"}}); err != nil {
		t.Fatalf("put sync: %v", err)
	}

	// What the daemon wires for a two-target sync: both syncers gate on both targets.
	consumers := []reseed.Consumer{{Sync: "s1", Target: "dst"}, {Sync: "s1", Target: "dst2"}}
	s1 := buildFanOutSyncer(t, ctx, tgtCfg(), "dst", store)
	s2 := buildFanOutSyncer(t, ctx, tgt2Cfg, "dst2", store)
	s1.Consumers, s2.Consumers = consumers, consumers

	for _, s := range []*Syncer{s1, s2} {
		if err := s.Bringup(ctx); err != nil {
			t.Fatalf("Bringup %s: %v", s.Target, err)
		}
	}

	// The test's live mutation: an insert and a delete, after both cut over.
	mustExecC(t, ctx, rawSrc, "INSERT INTO rc_it.orders VALUES (51, 1, 'o51')")
	mustExecC(t, ctx, rawSrc, "DELETE FROM rc_it.orders WHERE id = 2")

	// dst streams first, twice: drain, purge, and a pass with nothing to drain.
	for i := 0; i < 2; i++ {
		if err := s1.StreamOnce(ctx); err != nil {
			t.Fatalf("dst StreamOnce %d: %v", i, err)
		}
	}
	if got := countRows(t, ctx, rawTgt, "rc_it.orders WHERE id = 51"); got != 1 {
		t.Fatalf("dst: id 51 not applied")
	}
	// dst2 has not consumed them, so they must still be queued.
	if n := deltaRows(t, ctx, rawSrc, "orders"); n != 2 {
		t.Fatalf("orders deltas after dst's passes = %d, want 2 (dst2 has not consumed them; purging now loses them for dst2)", n)
	}

	// Now dst2 streams: it must see both changes.
	for i := 0; i < 2; i++ {
		if err := s2.StreamOnce(ctx); err != nil {
			t.Fatalf("dst2 StreamOnce %d: %v", i, err)
		}
	}
	src := dumpOrders(t, ctx, rawSrc)
	for name, c := range map[string]*pgx.Conn{"dst": rawTgt, "dst2": rawTgt2} {
		if got := dumpOrders(t, ctx, c); fmt.Sprint(got) != fmt.Sprint(src) {
			t.Errorf("%s not converged with source:\n source=%v\n target=%v", name, src, got)
		}
	}

	// Both have consumed them now, so the next pass of either syncer purges them.
	if err := s1.StreamOnce(ctx); err != nil {
		t.Fatalf("dst final StreamOnce: %v", err)
	}
	if n := deltaRows(t, ctx, rawSrc, "orders"); n != 0 {
		t.Errorf("orders deltas after both consumed = %d, want 0 (purge must still run)", n)
	}
}

// TestSyncerConsumersIncludesSelf: a syncer always gates its purge on its own target,
// even if a caller leaves Consumers empty (single target) or omits it.
func TestSyncerConsumersIncludesSelf(t *testing.T) {
	s := &Syncer{Name: "s1", Target: "dst"}
	if got := s.consumers(); len(got) != 1 || got[0] != (reseed.Consumer{Sync: "s1", Target: "dst"}) {
		t.Fatalf("empty Consumers: got %v, want [s1/dst]", got)
	}
	s.Consumers = []reseed.Consumer{{Sync: "s1", Target: "dst2"}}
	got := s.consumers()
	if len(got) != 2 || got[1] != (reseed.Consumer{Sync: "s1", Target: "dst"}) {
		t.Fatalf("Consumers without self: got %v, want [s1/dst2 s1/dst]", got)
	}
	if len(s.Consumers) != 1 {
		t.Fatalf("consumers() must not mutate s.Consumers: %v", s.Consumers)
	}
	s.Consumers = []reseed.Consumer{{Sync: "s1", Target: "dst"}, {Sync: "s1", Target: "dst2"}}
	if got := s.consumers(); len(got) != 2 {
		t.Fatalf("Consumers with self: got %v, want unchanged", got)
	}
}
