package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

func dirtyIDs(ks []engine.DirtyKey) []engine.DeltaID {
	out := make([]engine.DeltaID, len(ks))
	for i, k := range ks {
		out[i] = k.DeltaID
	}
	return out
}

func TestReadDirtyKeysSetDifference(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	src := captureSource(t, ctx)
	setupSourceTables(t, ctx, src, "CREATE TABLE rc_it.orders (id int PRIMARY KEY)")
	ref := engine.TableRef{Schema: "rc_it", Name: "orders"}
	if err := src.InstallCapture(ctx, []engine.TableRef{ref}); err != nil {
		t.Fatalf("install: %v", err)
	}
	mustExec(t, ctx, src.conn, "INSERT INTO rc_it.orders VALUES (1),(2),(3)")

	keys, err := src.ReadDirtyKeys(ctx, ref, "dst", 0)
	if err != nil {
		t.Fatalf("ReadDirtyKeys: %v", err)
	}
	if len(keys) != 3 {
		t.Fatalf("expected 3 dirty keys, got %d", len(keys))
	}
	if k, _ := keys[0].Key[0].(string); k != "1" {
		t.Errorf("first key = %v, want \"1\"", keys[0].Key[0])
	}
	if keys[0].Op != engine.OpInsert {
		t.Errorf("op = %q, want I", keys[0].Op)
	}

	// Confirm the first two; only the third remains dirty (set difference).
	if err := src.ConfirmConsumed(ctx, ref, "dst", dirtyIDs(keys[:2])); err != nil {
		t.Fatalf("ConfirmConsumed: %v", err)
	}
	remaining, err := src.ReadDirtyKeys(ctx, ref, "dst", 0)
	if err != nil {
		t.Fatalf("ReadDirtyKeys after confirm: %v", err)
	}
	if len(remaining) != 1 || remaining[0].DeltaID != keys[2].DeltaID {
		t.Errorf("remaining = %v, want just the third delta", dirtyIDs(remaining))
	}
}

func TestReadDirtyKeysPerTarget(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	src := captureSource(t, ctx)
	setupSourceTables(t, ctx, src, "CREATE TABLE rc_it.orders (id int PRIMARY KEY)")
	ref := engine.TableRef{Schema: "rc_it", Name: "orders"}
	if err := src.InstallCapture(ctx, []engine.TableRef{ref}); err != nil {
		t.Fatalf("install: %v", err)
	}
	mustExec(t, ctx, src.conn, "INSERT INTO rc_it.orders VALUES (1),(2)")

	all, _ := src.ReadDirtyKeys(ctx, ref, "a", 0)
	// Target "a" consumes everything; target "b" is untouched.
	if err := src.ConfirmConsumed(ctx, ref, "a", dirtyIDs(all)); err != nil {
		t.Fatalf("confirm a: %v", err)
	}
	aLeft, _ := src.ReadDirtyKeys(ctx, ref, "a", 0)
	bLeft, _ := src.ReadDirtyKeys(ctx, ref, "b", 0)
	if len(aLeft) != 0 {
		t.Errorf("target a should be drained, got %d", len(aLeft))
	}
	if len(bLeft) != 2 {
		t.Errorf("target b should still see all deltas, got %d", len(bLeft))
	}
}

func TestReadDirtyKeysMaxLimit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	src := captureSource(t, ctx)
	setupSourceTables(t, ctx, src, "CREATE TABLE rc_it.orders (id int PRIMARY KEY)")
	ref := engine.TableRef{Schema: "rc_it", Name: "orders"}
	if err := src.InstallCapture(ctx, []engine.TableRef{ref}); err != nil {
		t.Fatalf("install: %v", err)
	}
	mustExec(t, ctx, src.conn, "INSERT INTO rc_it.orders SELECT generate_series(1,10)")

	keys, err := src.ReadDirtyKeys(ctx, ref, "dst", 4)
	if err != nil {
		t.Fatalf("ReadDirtyKeys: %v", err)
	}
	if len(keys) != 4 {
		t.Errorf("max=4 should cap results, got %d", len(keys))
	}
}

func TestConfirmConsumedIdempotent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	src := captureSource(t, ctx)
	setupSourceTables(t, ctx, src, "CREATE TABLE rc_it.orders (id int PRIMARY KEY)")
	ref := engine.TableRef{Schema: "rc_it", Name: "orders"}
	if err := src.InstallCapture(ctx, []engine.TableRef{ref}); err != nil {
		t.Fatalf("install: %v", err)
	}
	mustExec(t, ctx, src.conn, "INSERT INTO rc_it.orders VALUES (1)")
	keys, _ := src.ReadDirtyKeys(ctx, ref, "dst", 0)
	ids := dirtyIDs(keys)

	// Confirm twice — the second is a harmless replay.
	if err := src.ConfirmConsumed(ctx, ref, "dst", ids); err != nil {
		t.Fatalf("confirm 1: %v", err)
	}
	if err := src.ConfirmConsumed(ctx, ref, "dst", ids); err != nil {
		t.Fatalf("confirm 2 (replay): %v", err)
	}
	relID, _, _, _ := lookupCapture(ctx, src.conn, ref)
	var trackRows int
	if err := src.conn.QueryRow(ctx, "SELECT count(*) FROM "+qualifiedCapture(trackTableName(relID))).Scan(&trackRows); err != nil {
		t.Fatalf("count track: %v", err)
	}
	if trackRows != 1 {
		t.Errorf("track rows = %d, want 1 (idempotent)", trackRows)
	}
}

func TestConsumeDeleteByDeltaIDRace(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	src := captureSource(t, ctx)
	setupSourceTables(t, ctx, src, "CREATE TABLE rc_it.orders (id int PRIMARY KEY, note text)")
	ref := engine.TableRef{Schema: "rc_it", Name: "orders"}
	if err := src.InstallCapture(ctx, []engine.TableRef{ref}); err != nil {
		t.Fatalf("install: %v", err)
	}

	// Insert X -> delta1 (observed).
	mustExec(t, ctx, src.conn, "INSERT INTO rc_it.orders VALUES (1, 'v1')")
	observed, _ := src.ReadDirtyKeys(ctx, ref, "dst", 0)
	if len(observed) != 1 {
		t.Fatalf("expected 1 observed delta, got %d", len(observed))
	}

	// While processing, X is updated -> delta2 (a NEW delta row for the same PK).
	mustExec(t, ctx, src.conn, "UPDATE rc_it.orders SET note='v2' WHERE id=1")

	// Confirm ONLY the observed delta_id (delete-by-id, not by PK).
	if err := src.ConfirmConsumed(ctx, ref, "dst", dirtyIDs(observed)); err != nil {
		t.Fatalf("confirm: %v", err)
	}

	// delta2 must survive and remain dirty, so X will be reprocessed and converge.
	remaining, _ := src.ReadDirtyKeys(ctx, ref, "dst", 0)
	if len(remaining) != 1 {
		t.Fatalf("expected the second delta to survive, got %d", len(remaining))
	}
	if remaining[0].DeltaID == observed[0].DeltaID {
		t.Error("surviving delta should be the newer one, not the confirmed one")
	}
	if k, _ := remaining[0].Key[0].(string); k != "1" {
		t.Errorf("surviving key = %v, want PK \"1\"", remaining[0].Key[0])
	}
}

func TestReadDirtyKeysCompositeKey(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	src := captureSource(t, ctx)
	setupSourceTables(t, ctx, src,
		"CREATE TABLE rc_it.items (order_id bigint, sku text, qty int, PRIMARY KEY (order_id, sku))")
	ref := engine.TableRef{Schema: "rc_it", Name: "items"}
	if err := src.InstallCapture(ctx, []engine.TableRef{ref}); err != nil {
		t.Fatalf("install: %v", err)
	}
	mustExec(t, ctx, src.conn, "INSERT INTO rc_it.items VALUES (10, 'abc', 2)")

	keys, err := src.ReadDirtyKeys(ctx, ref, "dst", 0)
	if err != nil {
		t.Fatalf("ReadDirtyKeys: %v", err)
	}
	if len(keys) != 1 || len(keys[0].Key) != 2 {
		t.Fatalf("expected 1 key with 2 columns, got %+v", keys)
	}
	if oid, _ := keys[0].Key[0].(string); oid != "10" {
		t.Errorf("order_id = %v, want \"10\"", keys[0].Key[0])
	}
	if sku, _ := keys[0].Key[1].(string); sku != "abc" {
		t.Errorf("sku = %v, want abc", keys[0].Key[1])
	}
}
