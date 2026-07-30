package reseed

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/rudimk/replicare/internal/copy"
	"github.com/rudimk/replicare/internal/engine"
	_ "github.com/rudimk/replicare/internal/engine/postgres" // register the engine
	"github.com/rudimk/replicare/internal/state"
	statepg "github.com/rudimk/replicare/internal/state/postgres"
)

// Integration tests for the M5c reseed orchestration. Gated on
// REPLICARE_INTEGRATION=1 (harness: old source :5440, modern target :5441).

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func srcCfg() engine.ConnConfig {
	return engine.ConnConfig{Host: env("RC_SRC_HOST", "127.0.0.1"), Port: 5440, Database: env("RC_SRC_DB", "replicare_src"), User: env("RC_USER", "postgres"), Password: env("RC_PASSWORD", "postgres"), TLS: engine.TLSDisable}
}

func tgtCfg() engine.ConnConfig {
	return engine.ConnConfig{Host: env("RC_DST_HOST", "127.0.0.1"), Port: 5441, Database: env("RC_DST_DB", "replicare_dst"), User: env("RC_USER", "postgres"), Password: env("RC_PASSWORD", "postgres"), TLS: engine.TLSDisable}
}

func url(cfg engine.ConnConfig) string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
		cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database)
}

func exec(t *testing.T, ctx context.Context, c *pgx.Conn, sql string) {
	t.Helper()
	if _, err := c.Exec(ctx, sql); err != nil {
		t.Fatalf("exec: %v\nSQL: %s", err, sql)
	}
}

type fixture struct {
	deps     Deps
	rawSrc   *pgx.Conn
	rawTgt   *pgx.Conn
	syncName string
}

func newFixture(t *testing.T, ctx context.Context) *fixture {
	t.Helper()
	if os.Getenv("REPLICARE_INTEGRATION") != "1" {
		t.Skip("integration test; set REPLICARE_INTEGRATION=1 and run `task harness:up`")
	}

	rawSrc, err := pgx.Connect(ctx, url(srcCfg()))
	if err != nil {
		t.Fatalf("raw src connect: %v", err)
	}
	rawTgt, err := pgx.Connect(ctx, url(tgtCfg()))
	if err != nil {
		t.Fatalf("raw tgt connect: %v", err)
	}
	exec(t, ctx, rawSrc, "DROP SCHEMA IF EXISTS rc_it CASCADE")
	exec(t, ctx, rawSrc, "DROP SCHEMA IF EXISTS replicare CASCADE")
	exec(t, ctx, rawSrc, "CREATE SCHEMA rc_it")
	exec(t, ctx, rawTgt, "DROP SCHEMA IF EXISTS rc_it CASCADE")
	exec(t, ctx, rawTgt, "DROP SCHEMA IF EXISTS replicare_state CASCADE")
	exec(t, ctx, rawTgt, "CREATE SCHEMA rc_it")

	eng, err := engine.Get("postgres")
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	src, err := eng.NewSource(srcCfg())
	if err != nil {
		t.Fatalf("new source: %v", err)
	}
	if err := src.Connect(ctx); err != nil {
		t.Fatalf("connect source: %v", err)
	}
	sink, err := eng.NewSink(tgtCfg())
	if err != nil {
		t.Fatalf("new sink: %v", err)
	}
	if err := sink.Connect(ctx); err != nil {
		t.Fatalf("connect sink: %v", err)
	}
	// A separate worker pair for the re-copy (Source/Sink are not concurrency-safe).
	wsrc, _ := eng.NewSource(srcCfg())
	_ = wsrc.Connect(ctx)
	wsink, _ := eng.NewSink(tgtCfg())
	_ = wsink.Connect(ctx)

	store := statepg.New(tgtCfg())
	if err := store.Open(ctx); err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := store.PutSync(ctx, state.SyncDef{Name: "s1", Source: "src", Targets: []engine.TargetID{"dst"}}); err != nil {
		t.Fatalf("put sync: %v", err)
	}

	f := &fixture{
		deps: Deps{
			Src:     src,
			Sink:    sink,
			Workers: []copy.Worker{{Src: wsrc, Sink: wsink}},
			Store:   store,
		},
		rawSrc:   rawSrc,
		rawTgt:   rawTgt,
		syncName: "s1",
	}
	t.Cleanup(func() {
		bg := context.Background()
		_, _ = rawTgt.Exec(bg, "DROP SCHEMA IF EXISTS replicare_state CASCADE")
		_, _ = rawSrc.Exec(bg, "DROP SCHEMA IF EXISTS rc_it CASCADE")
		_, _ = rawSrc.Exec(bg, "DROP SCHEMA IF EXISTS replicare CASCADE")
		_, _ = rawTgt.Exec(bg, "DROP SCHEMA IF EXISTS rc_it CASCADE")
		_ = src.Close(bg)
		_ = sink.Close(bg)
		_ = wsrc.Close(bg)
		_ = wsink.Close(bg)
		_ = store.Close(bg)
		_ = rawSrc.Close(bg)
		_ = rawTgt.Close(bg)
	})
	return f
}

// ageAllDeltas pushes every captured table's deltas past a retention age so the
// backlog reads as over-cap without waiting real time.
func (f *fixture) ageAllDeltas(t *testing.T, ctx context.Context) {
	t.Helper()
	rows, err := f.rawSrc.Query(ctx, "SELECT rel_id FROM replicare.captured")
	if err != nil {
		t.Fatalf("list captured: %v", err)
	}
	var relIDs []int
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan rel_id: %v", err)
		}
		relIDs = append(relIDs, id)
	}
	rows.Close()
	for _, id := range relIDs {
		exec(t, ctx, f.rawSrc, fmt.Sprintf(`UPDATE replicare."delta_%d" SET rc_at = now() - interval '48 hours'`, id))
	}
}

// dump returns "id|note" rows ordered by id from a table on the given conn.
func dump(t *testing.T, ctx context.Context, c *pgx.Conn) []string {
	t.Helper()
	rows, err := c.Query(ctx, "SELECT id::text, coalesce(note,'<n>') FROM rc_it.orders ORDER BY id")
	if err != nil {
		t.Fatalf("dump: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id, note string
		if err := rows.Scan(&id, &note); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, id+"|"+note)
	}
	return out
}

func (f *fixture) assertConverged(t *testing.T, ctx context.Context) {
	t.Helper()
	s, d := dump(t, ctx, f.rawSrc), dump(t, ctx, f.rawTgt)
	if fmt.Sprint(s) != fmt.Sprint(d) {
		t.Fatalf("not converged:\n source=%v\n target=%v", s, d)
	}
}

const ordersDDL = "CREATE TABLE rc_it.orders (id int PRIMARY KEY, note text)"

// TestReseedConvergenceUnderWrites is the M5c keystone: an over-cap target is
// forced to reseed, and — with source writes continuing throughout — the target
// re-derives and converges to current source state, with no delta lost across the
// handoff (docs/reseed-state-machine.md §4.3).
func TestReseedConvergenceUnderWrites(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	f := newFixture(t, ctx)
	ref := engine.TableRef{Schema: "rc_it", Name: "orders"}
	comp := []engine.TableRef{ref}
	targets := []engine.TargetID{"dst"}

	exec(t, ctx, f.rawSrc, ordersDDL)
	exec(t, ctx, f.rawTgt, ordersDDL)

	// Capture-first, then queue an unconsumed backlog.
	if err := f.deps.Src.InstallCapture(ctx, comp); err != nil {
		t.Fatalf("install capture: %v", err)
	}
	exec(t, ctx, f.rawSrc, "INSERT INTO rc_it.orders SELECT g, 'v'||g FROM generate_series(1,5) g")

	// Age the backlog past the cap; Enforce must sacrifice dst.
	f.ageAllDeltas(t, ctx)
	res, err := Enforce(ctx, f.deps, f.syncName, comp, targets, engine.RetentionPolicy{MaxAgeSeconds: 3600})
	if err != nil {
		t.Fatalf("Enforce: %v", err)
	}
	if len(res.Reseeded) != 1 || res.Reseeded[0] != "dst" {
		t.Fatalf("reseeded = %v, want [dst]", res.Reseeded)
	}
	cur, _ := f.deps.Store.LoadCursor(ctx, f.syncName, "dst", ref)
	if !cur.NeedsReseed {
		t.Fatalf("cursor should be marked needs-reseed after Enforce")
	}

	// Source keeps changing during the reseed window.
	exec(t, ctx, f.rawSrc, "INSERT INTO rc_it.orders SELECT g, 'v'||g FROM generate_series(6,7) g")

	// Recover the target: re-copy current source, cut back to streaming.
	if err := Run(ctx, f.deps, f.syncName, "dst", comp, engine.ChunkOptions{TargetRows: 2}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	cur, _ = f.deps.Store.LoadCursor(ctx, f.syncName, "dst", ref)
	if cur.NeedsReseed || cur.Phase != state.PhaseStreaming {
		t.Fatalf("cursor after Run = %+v, want streaming + not needs-reseed", cur)
	}

	// More writes after cutover: an insert, an update, and a delete.
	exec(t, ctx, f.rawSrc, "INSERT INTO rc_it.orders VALUES (8, 'v8')")
	exec(t, ctx, f.rawSrc, "UPDATE rc_it.orders SET note = 'v1-updated' WHERE id = 1")
	exec(t, ctx, f.rawSrc, "DELETE FROM rc_it.orders WHERE id = 2")

	// Resumed streaming drains the retained + in-flight deltas to convergence.
	if _, err := DrainToConvergence(ctx, f.deps, comp, "dst", 100); err != nil {
		t.Fatalf("DrainToConvergence: %v", err)
	}
	f.assertConverged(t, ctx)
}

// TestEnforceWithinCapNoReseed: a fresh backlog under the cap sacrifices nobody
// and marks no cursor (retention must not fire early).
func TestEnforceWithinCapNoReseed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	f := newFixture(t, ctx)
	ref := engine.TableRef{Schema: "rc_it", Name: "orders"}
	comp := []engine.TableRef{ref}

	exec(t, ctx, f.rawSrc, ordersDDL)
	exec(t, ctx, f.rawTgt, ordersDDL)
	if err := f.deps.Src.InstallCapture(ctx, comp); err != nil {
		t.Fatalf("install capture: %v", err)
	}
	exec(t, ctx, f.rawSrc, "INSERT INTO rc_it.orders SELECT g, 'v'||g FROM generate_series(1,3) g")

	res, err := Enforce(ctx, f.deps, f.syncName, comp, []engine.TargetID{"dst"},
		engine.RetentionPolicy{MaxAgeSeconds: 86400})
	if err != nil {
		t.Fatalf("Enforce: %v", err)
	}
	if len(res.Reseeded) != 0 {
		t.Fatalf("reseeded = %v, want none within cap", res.Reseeded)
	}
	cur, _ := f.deps.Store.LoadCursor(ctx, f.syncName, "dst", ref)
	if cur.NeedsReseed {
		t.Fatalf("cursor should not be marked needs-reseed within cap")
	}
}
