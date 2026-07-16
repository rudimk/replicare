package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// dropCaptureSchema removes the source `replicare` schema so capture tests start
// from a clean slate.
func dropCaptureSchema(t *testing.T, ctx context.Context, conn *pgx.Conn) {
	t.Helper()
	if _, err := conn.Exec(ctx, "DROP SCHEMA IF EXISTS "+captureSchema+" CASCADE"); err != nil {
		t.Fatalf("drop capture schema: %v", err)
	}
}

// captureSource opens a Source against the harness source (9.6) with a clean
// capture schema, and registers cleanup.
func captureSource(t *testing.T, ctx context.Context) *Source {
	t.Helper()
	src := &Source{cfg: harnessConn(t, "source")}
	if err := src.Connect(ctx); err != nil {
		t.Fatalf("connect source: %v", err)
	}
	dropCaptureSchema(t, ctx, src.conn)
	t.Cleanup(func() {
		bg := context.Background()
		dropCaptureSchema(t, bg, src.conn)
		_ = src.Close(bg)
	})
	return src
}

func TestEnsureCaptureSchemaIdempotent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	src := captureSource(t, ctx)

	if err := src.ensureCaptureSchema(ctx); err != nil {
		t.Fatalf("ensureCaptureSchema: %v", err)
	}

	// Version recorded at the set's target.
	var version int
	if err := src.conn.QueryRow(ctx, "SELECT COALESCE(MAX(version),0) FROM "+captureVersionTable).Scan(&version); err != nil {
		t.Fatalf("read version: %v", err)
	}
	if version != sourceSchemaSet.Target() {
		t.Errorf("capture schema version = %d, want %d", version, sourceSchemaSet.Target())
	}

	// Registry table and delta sequence exist.
	var hasRegistry bool
	if err := src.conn.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM information_schema.tables
		WHERE table_schema=$1 AND table_name='captured')`, captureSchema).Scan(&hasRegistry); err != nil {
		t.Fatalf("check registry: %v", err)
	}
	if !hasRegistry {
		t.Error("expected replicare.captured registry table")
	}
	var hasSeq bool
	if err := src.conn.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM information_schema.sequences
		WHERE sequence_schema=$1 AND sequence_name='delta_seq')`, captureSchema).Scan(&hasSeq); err != nil {
		t.Fatalf("check sequence: %v", err)
	}
	if !hasSeq {
		t.Error("expected replicare.delta_seq sequence")
	}

	// Re-running is a no-op.
	if err := src.ensureCaptureSchema(ctx); err != nil {
		t.Fatalf("second ensureCaptureSchema should be idempotent: %v", err)
	}
}
