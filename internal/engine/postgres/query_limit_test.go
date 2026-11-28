package postgres

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

// A read with a deadline that runs too long is cancelled by the server, and the
// connection stays usable. A deadline on the context alone makes pgx close the
// connection, which failed every later table of status and verify (production).
func TestQueryLimitKeepsTheConnection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	src := captureSource(t, ctx)

	qctx, qcancel := context.WithTimeout(ctx, 3*time.Second) // the server gives up after about 1s
	start := time.Now()
	err := withQueryLimit(qctx, src.conn, func(q pgQuerier) error {
		var x string
		return q.QueryRow(qctx, "SELECT pg_sleep(10)::text").Scan(&x)
	})
	qcancel()
	if err == nil || !isQueryCanceled(err) || !strings.Contains(err.Error(), "still usable") {
		t.Fatalf("slow read: err = %v, want the server's statement timeout", err)
	}
	if took := time.Since(start); took > 2500*time.Millisecond {
		t.Fatalf("slow read took %s, want the server to stop it before the 3s deadline", took)
	}
	var one int
	if err := src.conn.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil || one != 1 {
		t.Fatalf("connection after the slow read: %v, want it still usable", err)
	}
	// And the limit is only for that read: the session has none afterwards.
	var st string
	if err := src.conn.QueryRow(ctx, "SHOW statement_timeout").Scan(&st); err != nil || st != "0" {
		t.Fatalf("statement_timeout after = %q (%v), want 0", st, err)
	}

	// Without a deadline the read runs as is.
	if err := withQueryLimit(context.Background(), src.conn, func(q pgQuerier) error {
		return q.QueryRow(ctx, "SELECT 1").Scan(&one)
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := src.CountRows(ctx, engine.TableRef{Schema: "pg_catalog", Name: "pg_class"}); err != nil {
		t.Fatal(err)
	}
}
