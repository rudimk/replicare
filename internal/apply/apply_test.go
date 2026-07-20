package apply

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/rudimk/replicare/internal/engine"
	_ "github.com/rudimk/replicare/internal/engine/postgres" // register the engine
)

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

type fixture struct {
	src    engine.Source
	sink   engine.Sink
	rawSrc *pgx.Conn
	rawTgt *pgx.Conn
	ref    engine.TableRef
	target engine.TargetID
}

func exec(t *testing.T, ctx context.Context, c *pgx.Conn, sql string) {
	t.Helper()
	if _, err := c.Exec(ctx, sql); err != nil {
		t.Fatalf("exec: %v\nSQL: %s", err, sql)
	}
}

// newFixture sets up a captured source table and a matching (initial-copied)
// target, ready to stream deltas.
func newFixture(t *testing.T, ctx context.Context, ddl string, seed string) *fixture {
	t.Helper()
	if os.Getenv("REPLICARE_INTEGRATION") != "1" {
		t.Skip("integration test; set REPLICARE_INTEGRATION=1 and run `task harness:up`")
	}
	rawSrc, err := pgx.Connect(ctx, srcURL())
	if err != nil {
		t.Fatalf("connect source: %v", err)
	}
	rawTgt, err := pgx.Connect(ctx, tgtURL())
	if err != nil {
		t.Fatalf("connect target: %v", err)
	}
	exec(t, ctx, rawSrc, "DROP SCHEMA IF EXISTS rc_it CASCADE")
	exec(t, ctx, rawSrc, "DROP SCHEMA IF EXISTS replicare CASCADE")
	exec(t, ctx, rawSrc, "CREATE SCHEMA rc_it")
	exec(t, ctx, rawSrc, ddl)
	exec(t, ctx, rawTgt, "DROP SCHEMA IF EXISTS rc_it CASCADE")
	exec(t, ctx, rawTgt, "CREATE SCHEMA rc_it")
	exec(t, ctx, rawTgt, ddl)
	if seed != "" {
		exec(t, ctx, rawSrc, seed) // seed BEFORE capture, so no deltas for it
	}

	eng, err := engine.Get("postgres")
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	src, err := eng.NewSource(srcCfg())
	if err != nil {
		t.Fatalf("new source: %v", err)
	}
	if err := src.Connect(ctx); err != nil {
		t.Fatalf("connect src: %v", err)
	}
	sink, err := eng.NewSink(tgtCfg())
	if err != nil {
		t.Fatalf("new sink: %v", err)
	}
	if err := sink.Connect(ctx); err != nil {
		t.Fatalf("connect sink: %v", err)
	}

	ref := engine.TableRef{Schema: "rc_it", Name: "orders"}
	if err := src.InstallCapture(ctx, []engine.TableRef{ref}); err != nil {
		t.Fatalf("install capture: %v", err)
	}
	// Initial copy: reproduce the seeded state on the target.
	if seed != "" {
		exec(t, ctx, rawTgt, seed)
	}

	f := &fixture{src: src, sink: sink, rawSrc: rawSrc, rawTgt: rawTgt, ref: ref, target: "dst"}
	t.Cleanup(func() {
		bg := context.Background()
		_, _ = rawSrc.Exec(bg, "DROP SCHEMA IF EXISTS rc_it CASCADE")
		_, _ = rawSrc.Exec(bg, "DROP SCHEMA IF EXISTS replicare CASCADE")
		_, _ = rawTgt.Exec(bg, "DROP SCHEMA IF EXISTS rc_it CASCADE")
		_ = src.Close(bg)
		_ = sink.Close(bg)
		_ = rawSrc.Close(bg)
		_ = rawTgt.Close(bg)
	})
	return f
}

// assertConverged checks the target matches the source exactly.
func (f *fixture) assertConverged(t *testing.T, ctx context.Context) {
	t.Helper()
	dump := func(c *pgx.Conn) []string {
		rows, err := c.Query(ctx, "SELECT id::text, coalesce(note,'<n>') FROM rc_it.orders ORDER BY id")
		if err != nil {
			t.Fatalf("dump: %v", err)
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			var a, b string
			if err := rows.Scan(&a, &b); err != nil {
				t.Fatalf("scan: %v", err)
			}
			out = append(out, a+"|"+b)
		}
		return out
	}
	s, d := dump(f.rawSrc), dump(f.rawTgt)
	if len(s) != len(d) {
		t.Fatalf("row count: source %d target %d\n source=%v\n target=%v", len(s), len(d), s, d)
	}
	for i := range s {
		if s[i] != d[i] {
			t.Fatalf("row %d differs: source %q target %q", i, s[i], d[i])
		}
	}
}

const ordersDDL = "CREATE TABLE rc_it.orders (id int PRIMARY KEY, note text)"

func TestStreamConvergence(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	f := newFixture(t, ctx, ordersDDL,
		"INSERT INTO rc_it.orders SELECT g, 'n'||g FROM generate_series(1,5) g")

	// A mix of every operation on the captured source.
	exec(t, ctx, f.rawSrc, "INSERT INTO rc_it.orders VALUES (6, 'six')") // insert
	exec(t, ctx, f.rawSrc, "UPDATE rc_it.orders SET note='one-new' WHERE id=1")
	exec(t, ctx, f.rawSrc, "DELETE FROM rc_it.orders WHERE id=2")      // delete
	exec(t, ctx, f.rawSrc, "UPDATE rc_it.orders SET id=30 WHERE id=3") // PK change

	if _, err := DrainAll(ctx, f.src, f.sink, f.ref, f.target, 100); err != nil {
		t.Fatalf("DrainAll: %v", err)
	}
	f.assertConverged(t, ctx)

	// A second drain with no new changes is a no-op.
	n, err := DrainAll(ctx, f.src, f.sink, f.ref, f.target, 100)
	if err != nil {
		t.Fatalf("second DrainAll: %v", err)
	}
	if n != 0 {
		t.Errorf("expected an empty queue on the second drain, consumed %d", n)
	}
}
