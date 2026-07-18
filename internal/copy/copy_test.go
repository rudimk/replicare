package copy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/rudimk/replicare/internal/engine"
	_ "github.com/rudimk/replicare/internal/engine/postgres" // register the engine
	"github.com/rudimk/replicare/internal/state"
	statepg "github.com/rudimk/replicare/internal/state/postgres"
)

func skipUnlessIntegration(t *testing.T) {
	if os.Getenv("REPLICARE_INTEGRATION") != "1" {
		t.Skip("integration test; set REPLICARE_INTEGRATION=1 and run `task harness:up`")
	}
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func srcURL() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		env("RC_USER", "postgres"), env("RC_PASSWORD", "postgres"),
		env("RC_SRC_HOST", "127.0.0.1"), env("RC_SRC_PORT", "5440"), env("RC_SRC_DB", "replicare_src"))
}

func tgtURL() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		env("RC_USER", "postgres"), env("RC_PASSWORD", "postgres"),
		env("RC_DST_HOST", "127.0.0.1"), env("RC_DST_PORT", "5441"), env("RC_DST_DB", "replicare_dst"))
}

func srcCfg() engine.ConnConfig {
	return engine.ConnConfig{Host: env("RC_SRC_HOST", "127.0.0.1"), Port: 5440, Database: env("RC_SRC_DB", "replicare_src"), User: env("RC_USER", "postgres"), Password: env("RC_PASSWORD", "postgres"), TLS: engine.TLSDisable}
}

func tgtCfg() engine.ConnConfig {
	return engine.ConnConfig{Host: env("RC_DST_HOST", "127.0.0.1"), Port: 5441, Database: env("RC_DST_DB", "replicare_dst"), User: env("RC_USER", "postgres"), Password: env("RC_PASSWORD", "postgres"), TLS: engine.TLSDisable}
}

func rawConn(t *testing.T, ctx context.Context, url string) *pgx.Conn {
	t.Helper()
	c, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatalf("raw connect: %v", err)
	}
	return c
}

func exec(t *testing.T, ctx context.Context, c *pgx.Conn, sql string) {
	t.Helper()
	if _, err := c.Exec(ctx, sql); err != nil {
		t.Fatalf("exec: %v\nSQL: %s", err, sql)
	}
}

// fixture builds a source (Source), target (Sink), and state store against the
// harness, with a clean rc_it schema on both and a registered sync. Returns raw
// conns for setup/verification too.
type fixture struct {
	src      engine.Source
	sink     engine.Sink
	store    *statepg.Store
	rawSrc   *pgx.Conn
	rawTgt   *pgx.Conn
	syncName string
}

func newFixture(t *testing.T, ctx context.Context) *fixture {
	t.Helper()
	skipUnlessIntegration(t)

	rawSrc := rawConn(t, ctx, srcURL())
	rawTgt := rawConn(t, ctx, tgtURL())
	exec(t, ctx, rawSrc, "DROP SCHEMA IF EXISTS rc_it CASCADE")
	exec(t, ctx, rawSrc, "CREATE SCHEMA rc_it")
	exec(t, ctx, rawTgt, "DROP SCHEMA IF EXISTS rc_it CASCADE")
	exec(t, ctx, rawTgt, "CREATE SCHEMA rc_it")
	exec(t, ctx, rawTgt, "DROP SCHEMA IF EXISTS replicare_state CASCADE")

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
	store := statepg.New(tgtCfg())
	if err := store.Open(ctx); err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := store.PutSync(ctx, state.SyncDef{Name: "s1", Source: "src", Targets: []engine.TargetID{"dst"}}); err != nil {
		t.Fatalf("put sync: %v", err)
	}

	f := &fixture{src: src, sink: sink, store: store, rawSrc: rawSrc, rawTgt: rawTgt, syncName: "s1"}
	t.Cleanup(func() {
		bg := context.Background()
		_, _ = rawTgt.Exec(bg, "DROP SCHEMA IF EXISTS replicare_state CASCADE")
		_, _ = rawSrc.Exec(bg, "DROP SCHEMA IF EXISTS rc_it CASCADE")
		_, _ = rawTgt.Exec(bg, "DROP SCHEMA IF EXISTS rc_it CASCADE")
		_ = src.Close(bg)
		_ = sink.Close(bg)
		_ = store.Close(bg)
		_ = rawSrc.Close(bg)
		_ = rawTgt.Close(bg)
	})
	return f
}

func (f *fixture) rowCount(t *testing.T, ctx context.Context, c *pgx.Conn) int {
	t.Helper()
	var n int
	if err := c.QueryRow(ctx, "SELECT count(*) FROM rc_it.orders").Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	return n
}

func (f *fixture) faithful(t *testing.T, ctx context.Context) {
	t.Helper()
	dump := func(c *pgx.Conn) []string {
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
	s, d := dump(f.rawSrc), dump(f.rawTgt)
	if len(s) != len(d) {
		t.Fatalf("row count mismatch: source %d, target %d", len(s), len(d))
	}
	for i := range s {
		if s[i] != d[i] {
			t.Fatalf("row %d differs: source %q target %q", i, s[i], d[i])
		}
	}
}

const ordersDDL = "CREATE TABLE rc_it.orders (id int PRIMARY KEY, note text)"

func TestCopyTableFaithful(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	f := newFixture(t, ctx)
	exec(t, ctx, f.rawSrc, ordersDDL)
	exec(t, ctx, f.rawTgt, ordersDDL)
	exec(t, ctx, f.rawSrc, "INSERT INTO rc_it.orders SELECT g, 'note '||g FROM generate_series(1,50) g")

	ref := engine.TableRef{Schema: "rc_it", Name: "orders"}
	if err := Table(ctx, f.src, f.sink, f.store, f.syncName, ref, engine.ChunkOptions{TargetRows: 10}); err != nil {
		t.Fatalf("copy: %v", err)
	}
	if f.rowCount(t, ctx, f.rawTgt) != 50 {
		t.Fatalf("target has %d rows, want 50", f.rowCount(t, ctx, f.rawTgt))
	}
	f.faithful(t, ctx)

	// Progress marks the table done; re-running is a no-op.
	if err := Table(ctx, f.src, f.sink, f.store, f.syncName, ref, engine.ChunkOptions{TargetRows: 10}); err != nil {
		t.Fatalf("re-copy: %v", err)
	}
	if f.rowCount(t, ctx, f.rawTgt) != 50 {
		t.Error("re-run should be a no-op (table already done)")
	}
}

// flakySource fails CopyChunk once the counter exceeds failAfter, simulating a
// crash partway through the initial copy.
type flakySource struct {
	engine.Source
	failAfter int
	n         int
}

func (f *flakySource) CopyChunk(ctx context.Context, c engine.Chunk, w io.Writer) error {
	f.n++
	if f.n > f.failAfter {
		return errors.New("injected crash")
	}
	return f.Source.CopyChunk(ctx, c, w)
}

func TestCopyTableResumeAfterCrash(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	f := newFixture(t, ctx)
	exec(t, ctx, f.rawSrc, ordersDDL)
	exec(t, ctx, f.rawTgt, ordersDDL)
	exec(t, ctx, f.rawSrc, "INSERT INTO rc_it.orders SELECT g, 'note '||g FROM generate_series(1,100) g")

	ref := engine.TableRef{Schema: "rc_it", Name: "orders"}

	// First attempt crashes after 3 chunks.
	flaky := &flakySource{Source: f.src, failAfter: 3}
	err := Table(ctx, flaky, f.sink, f.store, f.syncName, ref, engine.ChunkOptions{TargetRows: 10})
	if err == nil {
		t.Fatal("expected the injected crash to fail the copy")
	}
	partial := f.rowCount(t, ctx, f.rawTgt)
	if partial == 0 || partial == 100 {
		t.Fatalf("expected a partial copy, got %d rows", partial)
	}

	// Resume with a healthy source: skips completed chunks, finishes the rest.
	if err := Table(ctx, f.src, f.sink, f.store, f.syncName, ref, engine.ChunkOptions{TargetRows: 10}); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if got := f.rowCount(t, ctx, f.rawTgt); got != 100 {
		t.Fatalf("after resume target has %d rows, want 100 (no dupes, no gaps)", got)
	}
	f.faithful(t, ctx)
}

func TestCopyTableResumeClearsIncompleteTail(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	f := newFixture(t, ctx)
	exec(t, ctx, f.rawSrc, ordersDDL)
	exec(t, ctx, f.rawTgt, ordersDDL)
	exec(t, ctx, f.rawSrc, "INSERT INTO rc_it.orders SELECT g, 'note '||g FROM generate_series(1,40) g")

	ref := engine.TableRef{Schema: "rc_it", Name: "orders"}

	// Simulate a crash mid-chunk: rows below the watermark (1..19) are already
	// on the target (completed chunks); progress is at watermark id=20; and a
	// bogus half-applied row sits above the watermark.
	exec(t, ctx, f.rawTgt, "INSERT INTO rc_it.orders SELECT g, 'note '||g FROM generate_series(1,19) g")
	if err := f.store.SaveCopyProgress(ctx, f.syncName, state.CopyProgress{
		Table: ref, Watermark: engine.KeyValues{"20"},
	}); err != nil {
		t.Fatalf("seed progress: %v", err)
	}
	exec(t, ctx, f.rawTgt, "INSERT INTO rc_it.orders VALUES (25, 'STALE PARTIAL')")

	// Resume must DELETE the tail (>= 20), clearing the stale row, then re-copy.
	if err := Table(ctx, f.src, f.sink, f.store, f.syncName, ref, engine.ChunkOptions{TargetRows: 10}); err != nil {
		t.Fatalf("resume: %v", err)
	}
	// The stale row is gone; the real row 25 is present and correct.
	var note string
	if err := f.rawTgt.QueryRow(ctx, "SELECT note FROM rc_it.orders WHERE id=25").Scan(&note); err != nil {
		t.Fatalf("read id=25: %v", err)
	}
	if note != "note 25" {
		t.Errorf("id=25 note = %q, want 'note 25' (stale partial should be cleaned)", note)
	}
	if got := f.rowCount(t, ctx, f.rawTgt); got != 40 {
		t.Fatalf("target has %d rows, want 40", got)
	}
	f.faithful(t, ctx)
}
