package postgres

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

// TestDeltaBacklogLongQueueIsEstimated: a queue past exactBacklogRows is read without
// a full pass over it: an estimate of the rows, the exact oldest change, and the oldest
// found by walking the queue's key, not by reading it all.
func TestDeltaBacklogLongQueueIsEstimated(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	src := captureSource(t, ctx)
	setupSourceTables(t, ctx, src, "CREATE TABLE rc_it.busy (id int PRIMARY KEY, v int)")
	ref := engine.TableRef{Schema: "rc_it", Name: "busy"}
	if err := src.InstallCapture(ctx, []engine.TableRef{ref}); err != nil {
		t.Fatal(err)
	}
	const n = 150_000
	mustExec(t, ctx, src.conn, fmt.Sprintf("INSERT INTO rc_it.busy SELECT g, g FROM generate_series(1, %d) g", n))

	// The live-row statistic is flushed shortly after the commit.
	var bl engine.DeltaBacklog
	deadline := time.Now().Add(30 * time.Second)
	for {
		var err error
		if bl, err = src.DeltaBacklog(ctx, ref, "dst"); err != nil {
			t.Fatal(err)
		}
		if bl.Estimated || time.Now().After(deadline) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if !bl.Estimated || !bl.HasBacklog || bl.Rows < n*9/10 || bl.Rows > n*11/10 || bl.OldestAge <= 0 || bl.Bytes <= 0 {
		t.Fatalf("long queue: %+v, want an estimate of about %d rows with an oldest age and a size", bl, n)
	}

	// Applied but not yet purged changes come off the estimate.
	dirty, err := src.ReadDirtyKeys(ctx, ref, "dst", 20_000)
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]engine.DeltaID, len(dirty))
	for i, d := range dirty {
		ids[i] = d.DeltaID
	}
	if err := src.ConfirmConsumed(ctx, ref, "dst", ids); err != nil {
		t.Fatal(err)
	}
	after, err := src.DeltaBacklog(ctx, ref, "dst")
	if err != nil {
		t.Fatal(err)
	}
	if after.Rows > bl.Rows-15_000 {
		t.Errorf("after applying 20000: %d rows, want about %d", after.Rows, bl.Rows-20_000)
	}

	// The oldest is found from the front of the queue, not by reading all of it.
	relID, _, _, err := lookupCapture(ctx, src.conn, ref)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := src.conn.Query(ctx, fmt.Sprintf(`EXPLAIN SELECT d.rc_at FROM %s d
		WHERE NOT EXISTS (SELECT 1 FROM %s tr WHERE tr.target = 'dst' AND tr.delta_id = d.delta_id)
		ORDER BY d.delta_id LIMIT 1`, qualifiedCapture(deltaTableName(relID)), qualifiedCapture(trackTableName(relID))))
	if err != nil {
		t.Fatal(err)
	}
	var plan []string
	for rows.Next() {
		var line string
		_ = rows.Scan(&line)
		plan = append(plan, line)
	}
	rows.Close()
	if p := strings.Join(plan, "\n"); !strings.Contains(p, "Index") || strings.Contains(p, "Sort") {
		t.Errorf("oldest-change query should walk the queue's index, got:\n%s", p)
	}
}
