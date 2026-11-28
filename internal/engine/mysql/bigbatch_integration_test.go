package mysql

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/apply"
	"github.com/rudimk/replicare/internal/engine"
)

// TestBigBatchDrain: capture on, 25k changes on a three-column key (5k of them
// deletes), drained in one pass of a batch far past one statement's limit (MySQL
// caps a statement at 65,535 placeholders: 25k keys x 3 columns is over it), through
// the per-table component drain the daemon uses.
func TestBigBatchDrain(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	src := connectSource(t, ctx)
	sink := connectSink(t, ctx)
	t.Cleanup(func() { _, _ = src.db.Exec("DROP DATABASE IF EXISTS replicare") })

	ddl := "CREATE TABLE rc_it.chain (underlying VARCHAR(10), snap DATE, contract VARCHAR(10), v VARCHAR(10), " +
		"PRIMARY KEY (underlying, snap, contract)) ENGINE=InnoDB"
	mustExec(t, ctx, src.db, ddl)
	mustExec(t, ctx, sink.db, "DROP DATABASE IF EXISTS rc_it", "CREATE DATABASE rc_it", ddl)
	t.Cleanup(func() { _, _ = sink.db.Exec("DROP DATABASE IF EXISTS rc_it") })

	ref := engine.TableRef{Schema: "rc_it", Name: "chain"}
	if err := src.InstallCapture(ctx, []engine.TableRef{ref}); err != nil {
		t.Fatalf("install capture: %v", err)
	}
	rows := func(from, to int, v string) string {
		parts := make([]string, 0, to-from+1)
		for g := from; g <= to; g++ {
			parts = append(parts, fmt.Sprintf("('U%d', DATE_ADD('2020-01-01', INTERVAL %d DAY), 'C%d', '%s')", g%97, g/97, g, v))
		}
		return strings.Join(parts, ", ")
	}
	for lo := 1; lo <= 25_000; lo += 1000 {
		mustExec(t, ctx, src.db, "INSERT INTO rc_it.chain VALUES "+rows(lo, lo+999, "new"))
	}
	// The target already has the rows the source is about to delete (an earlier copy).
	for lo := 20_001; lo <= 25_000; lo += 1000 {
		mustExec(t, ctx, sink.db, "INSERT INTO rc_it.chain VALUES "+rows(lo, lo+999, "old"))
	}
	mustExec(t, ctx, src.db, "DELETE FROM rc_it.chain WHERE CAST(SUBSTRING(contract, 2) AS UNSIGNED) > 20000")

	start := time.Now()
	n, err := apply.DrainComponentRetrying(ctx, src, sink, []engine.TableRef{ref}, "dst", 100_000, false, apply.DefaultRetryPolicy)
	if err != nil {
		t.Fatalf("drain with a batch of 100000: %v", err)
	}
	if n == 0 {
		t.Fatal("the drain applied nothing")
	}
	t.Logf("drained %d changes in %s", n, time.Since(start))
	var got, fresh int
	if err := sink.db.QueryRowContext(ctx,
		"SELECT COUNT(*), SUM(v = 'new') FROM rc_it.chain").Scan(&got, &fresh); err != nil {
		t.Fatal(err)
	}
	if got != 20_000 || fresh != 20_000 {
		t.Fatalf("target: %d rows (%d current), want 20000", got, fresh)
	}
	left, err := src.ReadDirtyKeys(ctx, ref, "dst", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(left) != 0 {
		t.Fatalf("%d changes still queued after the drain", len(left))
	}
}
