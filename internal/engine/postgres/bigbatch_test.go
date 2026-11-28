package postgres

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/apply"
	"github.com/rudimk/replicare/internal/engine"
)

// bigBatchKeys is well past where one statement listing every key fails on Postgres
// (a row-value IN list of ~10k three-column keys: "stack depth limit exceeded").
const bigBatchKeys = 20_000

// bigBatchFixture: rc_it.chain with a three-column key, the shape that broke in
// production. The source holds keys 1..bigBatchKeys with value 'new'; the target
// holds every key with value 'old', plus as many again that the source has deleted.
// The dirty keys are all of them, so one pass must upsert 20k rows and delete 20k.
func bigBatchFixture(t *testing.T, ctx context.Context) (*Source, *Sink, engine.TableRef, []string, []engine.KeyValues) {
	t.Helper()
	src := captureSource(t, ctx)
	sink := sinkTarget(t, ctx)
	ddl := "CREATE TABLE rc_it.chain (underlying text, snap date, contract text, v text, PRIMARY KEY (underlying, snap, contract))"
	setupSourceTables(t, ctx, src, ddl)
	mustExecTarget(t, ctx, sink, "CREATE SCHEMA rc_it")
	mustExecTarget(t, ctx, sink, ddl)
	row := func(g string) string {
		return fmt.Sprintf("'U'||(%[1]s %% 97), date '2020-01-01' + (%[1]s / 97), 'C'||%[1]s", g)
	}
	mustExec(t, ctx, src.conn, fmt.Sprintf(
		"INSERT INTO rc_it.chain SELECT %s, 'new' FROM generate_series(1, %d) g", row("g"), bigBatchKeys))
	mustExecTarget(t, ctx, sink, fmt.Sprintf(
		"INSERT INTO rc_it.chain SELECT %s, 'old' FROM generate_series(1, %d) g", row("g"), 2*bigBatchKeys))

	rows, err := sink.conn.Query(ctx, "SELECT underlying, snap::text, contract FROM rc_it.chain")
	if err != nil {
		t.Fatal(err)
	}
	var keys []engine.KeyValues
	for rows.Next() {
		var u, s, c string
		if err := rows.Scan(&u, &s, &c); err != nil {
			t.Fatal(err)
		}
		keys = append(keys, engine.KeyValues{u, s, c})
	}
	rows.Close()
	if len(keys) != 2*bigBatchKeys {
		t.Fatalf("fixture: %d dirty keys, want %d", len(keys), 2*bigBatchKeys)
	}
	ref := engine.TableRef{Schema: "rc_it", Name: "chain"}
	table, err := src.tableMeta(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	return src, sink, ref, transportColumns(table), keys
}

func checkBigBatch(t *testing.T, ctx context.Context, sink *Sink) {
	t.Helper()
	var total, fresh int
	if err := sink.conn.QueryRow(ctx,
		"SELECT count(*), count(*) FILTER (WHERE v = 'new') FROM rc_it.chain").Scan(&total, &fresh); err != nil {
		t.Fatal(err)
	}
	if total != bigBatchKeys || fresh != bigBatchKeys {
		t.Fatalf("target after the pass: %d rows, %d current; want %d and %d (upserts and deletes both applied)",
			total, fresh, bigBatchKeys, bigBatchKeys)
	}
}

// TestBigBatchReread: re-reading far more keys than one statement can hold returns
// exactly the present rows.
func TestBigBatchReread(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	src, _, ref, _, keys := bigBatchFixture(t, ctx)
	var buf bytes.Buffer
	if err := src.RereadCurrent(ctx, ref, keys, &buf); err != nil {
		t.Fatalf("RereadCurrent over %d keys: %v", len(keys), err)
	}
	if n := len(nonEmptyLines(buf.String())); n != bigBatchKeys {
		t.Fatalf("re-read returned %d rows, want %d", n, bigBatchKeys)
	}
}

// TestBigBatchApplyPass: the one-transaction apply upserts and deletes a batch far
// past one statement's limit.
func TestBigBatchApplyPass(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	src, sink, ref, cols, keys := bigBatchFixture(t, ctx)
	applyOnePass(t, ctx, src, sink, ref, cols, keys)
	checkBigBatch(t, ctx, sink)
}

// TestBigBatchApplyTx: the per-table apply the streaming drain uses (StageUpsert then
// DeleteAbsent) does the same.
func TestBigBatchApplyTx(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	src, sink, ref, cols, keys := bigBatchFixture(t, ctx)
	var buf bytes.Buffer
	if err := src.RereadCurrent(ctx, ref, keys, &buf); err != nil {
		t.Fatalf("RereadCurrent: %v", err)
	}
	tx, err := sink.BeginApply(ctx, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.StageUpsert(ctx, ref, cols, &buf); err != nil {
		t.Fatalf("StageUpsert: %v", err)
	}
	if err := tx.DeleteAbsent(ctx, ref, keys); err != nil {
		t.Fatalf("DeleteAbsent over %d keys: %v", len(keys), err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	checkBigBatch(t, ctx, sink)
}

// TestBigBatchCatalogLookups: the look-ups by table name take any number of tables.
func TestBigBatchCatalogLookups(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	src, _, ref, _, _ := bigBatchFixture(t, ctx)
	tables := []engine.TableRef{ref}
	for i := 0; i < 3*engine.MaxKeysPerStatement; i++ {
		tables = append(tables, engine.TableRef{Schema: "rc_it", Name: fmt.Sprintf("missing_%d", i)})
	}
	mustExec(t, ctx, src.conn, "ANALYZE rc_it.chain")
	rows, err := src.EstimateRows(ctx, tables)
	if err != nil || rows[ref] <= 0 {
		t.Fatalf("EstimateRows over %d tables: %v, %v", len(tables), rows, err)
	}
	bytes, err := src.EstimateTableBytes(ctx, tables)
	if err != nil || bytes[ref] <= 0 {
		t.Fatalf("EstimateTableBytes over %d tables: %v, %v", len(tables), bytes, err)
	}
	size, err := replicatedSize(ctx, src.conn, tables)
	if err != nil || size <= 0 {
		t.Fatalf("replicatedSize over %d tables: %d, %v", len(tables), size, err)
	}
}

// TestBigBatchDrain is the production path end to end: capture on, 25k changes on
// a three-column key (5k of them deletes), drained in one pass of a batch far past
// one statement's limit, through the per-table component drain the daemon uses.
func TestBigBatchDrain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	src := captureSource(t, ctx)
	sink := sinkTarget(t, ctx)
	ddl := "CREATE TABLE rc_it.chain (underlying text, snap date, contract text, v text, PRIMARY KEY (underlying, snap, contract))"
	setupSourceTables(t, ctx, src, ddl)
	mustExecTarget(t, ctx, sink, "CREATE SCHEMA rc_it")
	mustExecTarget(t, ctx, sink, ddl)
	ref := engine.TableRef{Schema: "rc_it", Name: "chain"}
	if err := src.InstallCapture(ctx, []engine.TableRef{ref}); err != nil {
		t.Fatalf("install capture: %v", err)
	}
	row := "'U'||(g % 97), date '2020-01-01' + (g / 97), 'C'||g"
	// The target already has the rows the source is about to delete (an earlier copy).
	mustExecTarget(t, ctx, sink, "INSERT INTO rc_it.chain SELECT "+row+", 'old' FROM generate_series(20001, 25000) g")
	mustExec(t, ctx, src.conn, "INSERT INTO rc_it.chain SELECT "+row+", 'new' FROM generate_series(1, 25000) g")
	mustExec(t, ctx, src.conn, "DELETE FROM rc_it.chain WHERE contract IN (SELECT 'C'||g FROM generate_series(20001, 25000) g)")
	const want = 20_000

	n, err := apply.DrainComponentRetrying(ctx, src, sink, []engine.TableRef{ref}, "dst", 100_000, false, apply.DefaultRetryPolicy)
	if err != nil {
		t.Fatalf("drain with a batch of 100000: %v", err)
	}
	if n == 0 {
		t.Fatal("the drain applied nothing")
	}
	var got, fresh int
	if err := sink.conn.QueryRow(ctx,
		"SELECT count(*), count(*) FILTER (WHERE v = 'new') FROM rc_it.chain").Scan(&got, &fresh); err != nil {
		t.Fatal(err)
	}
	if got != want || fresh != want {
		t.Fatalf("target: %d rows (%d current), source: %d", got, fresh, want)
	}
	left, err := src.ReadDirtyKeys(ctx, ref, "dst", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(left) != 0 {
		t.Fatalf("%d changes still queued after the drain", len(left))
	}
}
