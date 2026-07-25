package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// dumpOrders returns "id|customer_id|note" rows ordered by id.
func dumpOrders(t *testing.T, ctx context.Context, c *pgx.Conn) []string {
	t.Helper()
	rows, err := c.Query(ctx, "SELECT id::text, customer_id::text, coalesce(note,'') FROM rc_it.orders ORDER BY id")
	if err != nil {
		t.Fatalf("dump orders: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id, cust, note string
		if err := rows.Scan(&id, &cust, &note); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, id+"|"+cust+"|"+note)
	}
	return out
}

// cellFor reads a single string cell.
func cellFor(t *testing.T, ctx context.Context, c *pgx.Conn, q string) string {
	t.Helper()
	var s string
	if err := c.QueryRow(ctx, q).Scan(&s); err != nil {
		t.Fatalf("query %q: %v", q, err)
	}
	return s
}

// TestSyncerStreamConverges brings a sync up, then — with inserts, updates, and a
// delete applied to the source afterward — a streaming pass converges the target
// with the source (insert/update/delete/PK-stable all reconciled).
func TestSyncerStreamConverges(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	rawSrc, rawTgt := freshFKSchema(t, ctx)
	syncer := buildSyncer(t, ctx)
	if err := syncer.Bringup(ctx); err != nil {
		t.Fatalf("Bringup: %v", err)
	}

	// Mutate the source after cutover: new parent+children, an update, a delete.
	mustExecC(t, ctx, rawSrc, "INSERT INTO rc_it.customers VALUES (21, 'c21')")
	mustExecC(t, ctx, rawSrc, "INSERT INTO rc_it.orders VALUES (51, 21, 'o51'), (52, 21, 'o52')")
	mustExecC(t, ctx, rawSrc, "UPDATE rc_it.orders SET note = 'o1-updated' WHERE id = 1")
	mustExecC(t, ctx, rawSrc, "DELETE FROM rc_it.orders WHERE id = 2")

	// Drain to convergence (a couple of passes covers coalescing + FK ordering).
	for i := 0; i < 3; i++ {
		if err := syncer.StreamOnce(ctx); err != nil {
			t.Fatalf("StreamOnce %d: %v", i, err)
		}
	}

	src := dumpOrders(t, ctx, rawSrc)
	dst := dumpOrders(t, ctx, rawTgt)
	if len(src) != len(dst) {
		t.Fatalf("row count mismatch: source %d, target %d", len(src), len(dst))
	}
	for i := range src {
		if src[i] != dst[i] {
			t.Fatalf("row %d differs: source %q target %q", i, src[i], dst[i])
		}
	}
	// Spot-check the specific mutations landed.
	if got := cellFor(t, ctx, rawTgt, "SELECT note FROM rc_it.orders WHERE id=1"); got != "o1-updated" {
		t.Errorf("update not applied: id=1 note=%q", got)
	}
	if countRows(t, ctx, rawTgt, "rc_it.orders WHERE id=2") != 0 {
		t.Errorf("delete not applied: id=2 still present")
	}
	if countRows(t, ctx, rawTgt, "rc_it.orders") != 51 {
		t.Errorf("target orders = %d, want 51 (50 + 2 inserts - 1 delete)", countRows(t, ctx, rawTgt, "rc_it.orders"))
	}
}

// TestSyncerStreamGracefulStop confirms Stream returns cleanly when its context is
// cancelled between passes — the basis for SIGTERM drain+checkpoint shutdown.
func TestSyncerStreamGracefulStop(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	_, _ = freshFKSchema(t, ctx)
	syncer := buildSyncer(t, ctx)
	if err := syncer.Bringup(ctx); err != nil {
		t.Fatalf("Bringup: %v", err)
	}

	streamCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- syncer.Stream(streamCtx) }()

	time.Sleep(200 * time.Millisecond) // let a few ticks run
	stop()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Stream returned %v, want context.Canceled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Stream did not stop within 10s of cancellation")
	}
}
