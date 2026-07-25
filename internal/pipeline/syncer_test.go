package pipeline

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/rudimk/replicare/internal/copy"
	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/observability/telemetry"
	"github.com/rudimk/replicare/internal/state"
	statepg "github.com/rudimk/replicare/internal/state/postgres"
)

// integration skips a test unless the harness is enabled, returning whether it
// should run.
func integration(t *testing.T) bool {
	t.Helper()
	if os.Getenv("REPLICARE_INTEGRATION") != "1" {
		t.Skip("integration test; set REPLICARE_INTEGRATION=1 and run `task harness:up`")
		return false
	}
	return true
}

func rawConn(t *testing.T, ctx context.Context, cfg engine.ConnConfig) *pgx.Conn {
	t.Helper()
	c, err := pgx.Connect(ctx, fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
		cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database))
	if err != nil {
		t.Fatalf("raw connect: %v", err)
	}
	return c
}

func connectedSource(t *testing.T, ctx context.Context) engine.Source {
	t.Helper()
	eng, _ := engine.Get("postgres")
	s, err := eng.NewSource(srcCfg())
	if err != nil {
		t.Fatalf("new source: %v", err)
	}
	if err := s.Connect(ctx); err != nil {
		t.Fatalf("connect source: %v", err)
	}
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	return s
}

func connectedSink(t *testing.T, ctx context.Context) engine.Sink {
	t.Helper()
	eng, _ := engine.Get("postgres")
	s, err := eng.NewSink(tgtCfg())
	if err != nil {
		t.Fatalf("new sink: %v", err)
	}
	if err := s.Connect(ctx); err != nil {
		t.Fatalf("connect sink: %v", err)
	}
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	return s
}

// freshFKSchema creates a clean parent<-child FK schema (customers <- orders) on
// both source and target, seeds the source (20 customers, 50 orders), and clears
// any prior capture/state schema. Returns raw conns for setup/verification.
func freshFKSchema(t *testing.T, ctx context.Context) (rawSrc, rawTgt *pgx.Conn) {
	t.Helper()
	rawSrc = rawConn(t, ctx, srcCfg())
	rawTgt = rawConn(t, ctx, tgtCfg())
	parentDDL := "CREATE TABLE rc_it.customers (id int PRIMARY KEY, name text)"
	childDDL := "CREATE TABLE rc_it.orders (id int PRIMARY KEY, customer_id int NOT NULL REFERENCES rc_it.customers(id), note text)"
	setup := func(c *pgx.Conn) {
		mustExecC(t, ctx, c, "DROP SCHEMA IF EXISTS rc_it CASCADE")
		mustExecC(t, ctx, c, "CREATE SCHEMA rc_it")
		mustExecC(t, ctx, c, parentDDL)
		mustExecC(t, ctx, c, childDDL)
	}
	setup(rawSrc)
	setup(rawTgt)
	mustExecC(t, ctx, rawSrc, "DROP SCHEMA IF EXISTS replicare CASCADE")
	mustExecC(t, ctx, rawTgt, "DROP SCHEMA IF EXISTS replicare_state CASCADE")
	mustExecC(t, ctx, rawSrc, "INSERT INTO rc_it.customers SELECT g, 'c'||g FROM generate_series(1,20) g")
	mustExecC(t, ctx, rawSrc, "INSERT INTO rc_it.orders SELECT g, (g%20)+1, 'o'||g FROM generate_series(1,50) g")
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

// buildSyncer connects a source, target, copy worker pair, and state store to the
// harness, runs pre-flight for the topo-ordered components (the daemon's real
// path), and returns a ready single-target Syncer for sync "s1" / target "dst".
func buildSyncer(t *testing.T, ctx context.Context) *Syncer {
	t.Helper()
	source := connectedSource(t, ctx)
	sink := connectedSink(t, ctx)
	wsrc := connectedSource(t, ctx)
	wsink := connectedSink(t, ctx)

	store := statepg.New(tgtCfg())
	if err := store.Open(ctx); err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close(context.Background()) })
	if err := store.PutSync(ctx, state.SyncDef{Name: "s1", Source: "src", Targets: []engine.TargetID{"dst"}}); err != nil {
		t.Fatalf("put sync: %v", err)
	}

	sel := engine.Selection{Include: []string{"rc_it.*"}}
	eng, _ := engine.Get("postgres")
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
		Target:        "dst",
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

// TestSyncerBringupColdToStreaming is the first M7 lifecycle acceptance: a cold
// sync installs capture, copies a multi-table FK schema parents-first, and cuts
// every table over to streaming — leaving the target converged with the source
// and all cursors in the streaming phase.
func TestSyncerBringupColdToStreaming(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	_, rawTgt := freshFKSchema(t, ctx)
	syncer := buildSyncer(t, ctx)
	if err := syncer.Bringup(ctx); err != nil {
		t.Fatalf("Bringup: %v", err)
	}

	// Data copied faithfully.
	if got := countRows(t, ctx, rawTgt, "rc_it.customers"); got != 20 {
		t.Errorf("target customers = %d, want 20", got)
	}
	if got := countRows(t, ctx, rawTgt, "rc_it.orders"); got != 50 {
		t.Errorf("target orders = %d, want 50", got)
	}

	// Every table cut over to streaming.
	for _, tbl := range []string{"customers", "orders"} {
		cur, err := syncer.Store.LoadCursor(ctx, "s1", "dst", engine.TableRef{Schema: "rc_it", Name: tbl})
		if err != nil {
			t.Fatalf("load cursor %s: %v", tbl, err)
		}
		if cur.Phase != state.PhaseStreaming {
			t.Errorf("%s phase = %q, want streaming", tbl, cur.Phase)
		}
	}
}

func mustExecC(t *testing.T, ctx context.Context, c *pgx.Conn, sql string) {
	t.Helper()
	if _, err := c.Exec(ctx, sql); err != nil {
		t.Fatalf("exec: %v\nSQL: %s", err, sql)
	}
}

func countRows(t *testing.T, ctx context.Context, c *pgx.Conn, table string) int {
	t.Helper()
	var n int
	if err := c.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}
