package postgres

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

func TestQuotedKeyList(t *testing.T) {
	if got := quotedKeyList([]captureCol{{"id", "integer"}}); got != `"id"` {
		t.Errorf("single = %q", got)
	}
	if got := quotedKeyList([]captureCol{{"order_id", "bigint"}, {"sku", "text"}}); got != `"order_id", "sku"` {
		t.Errorf("composite = %q", got)
	}
}

func TestBuildKeysetRangesNoBoundaries(t *testing.T) {
	ref := engine.TableRef{Schema: "public", Name: "t"}
	chunks := buildKeysetRanges(ref, nil)
	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(chunks))
	}
	if chunks[0].Lo != nil || chunks[0].Hi != nil {
		t.Errorf("single chunk should be fully unbounded, got %+v", chunks[0])
	}
	if chunks[0].Method != engine.ChunkKeyset {
		t.Errorf("method = %q", chunks[0].Method)
	}
}

func TestBuildKeysetRangesContiguous(t *testing.T) {
	ref := engine.TableRef{Schema: "public", Name: "t"}
	boundaries := []engine.KeyValues{{10}, {20}}
	chunks := buildKeysetRanges(ref, boundaries)

	if len(chunks) != 3 {
		t.Fatalf("expected 3 chunks, got %d", len(chunks))
	}
	// [nil,10), [10,20), [20,nil)
	if chunks[0].Lo != nil {
		t.Errorf("first Lo should be nil, got %v", chunks[0].Lo)
	}
	if chunks[len(chunks)-1].Hi != nil {
		t.Errorf("last Hi should be nil, got %v", chunks[len(chunks)-1].Hi)
	}
	// Contiguity: each Hi equals the next Lo (gap-free, no overlap).
	for i := 0; i < len(chunks)-1; i++ {
		if !reflect.DeepEqual(chunks[i].Hi, chunks[i+1].Lo) {
			t.Errorf("chunk %d Hi=%v != chunk %d Lo=%v", i, chunks[i].Hi, i+1, chunks[i+1].Lo)
		}
	}
	if !reflect.DeepEqual(chunks[1].Lo, engine.KeyValues{10}) || !reflect.DeepEqual(chunks[1].Hi, engine.KeyValues{20}) {
		t.Errorf("middle chunk = %+v", chunks[1])
	}
}

// --- integration ---

func TestPlanKeysetChunksIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	src := captureSource(t, ctx)
	setupSourceTables(t, ctx, src, "CREATE TABLE rc_it.orders (id int PRIMARY KEY)")
	mustExec(t, ctx, src.conn, "INSERT INTO rc_it.orders SELECT generate_series(1, 25)")

	ref := engine.TableRef{Schema: "rc_it", Name: "orders"}
	chunks, err := src.PlanChunks(ctx, ref, engine.ChunkOptions{TargetRows: 10})
	if err != nil {
		t.Fatalf("PlanChunks: %v", err)
	}
	// 25 rows, N=10 -> boundaries at rn=11,21 -> 3 chunks.
	if len(chunks) != 3 {
		t.Fatalf("expected 3 chunks for 25 rows / 10, got %d: %+v", len(chunks), chunks)
	}
	// Unbounded ends and contiguity.
	if chunks[0].Lo != nil || chunks[2].Hi != nil {
		t.Error("ends should be unbounded")
	}
	for i := 0; i < len(chunks)-1; i++ {
		if !reflect.DeepEqual(chunks[i].Hi, chunks[i+1].Lo) {
			t.Errorf("non-contiguous at %d: %v vs %v", i, chunks[i].Hi, chunks[i+1].Lo)
		}
	}
	// Boundary keys ascending, in faithful text form: "11" then "21".
	if b, _ := chunks[0].Hi[0].(string); b != "11" {
		t.Errorf("first boundary = %v, want \"11\"", chunks[0].Hi[0])
	}
	if b, _ := chunks[1].Hi[0].(string); b != "21" {
		t.Errorf("second boundary = %v, want \"21\"", chunks[1].Hi[0])
	}
}

func TestPlanKeysetChunksEmptyTable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	src := captureSource(t, ctx)
	setupSourceTables(t, ctx, src, "CREATE TABLE rc_it.empty (id int PRIMARY KEY)")

	chunks, err := src.PlanChunks(ctx, engine.TableRef{Schema: "rc_it", Name: "empty"}, engine.ChunkOptions{TargetRows: 10})
	if err != nil {
		t.Fatalf("PlanChunks: %v", err)
	}
	if len(chunks) != 1 || chunks[0].Lo != nil || chunks[0].Hi != nil {
		t.Errorf("empty table should yield one unbounded chunk, got %+v", chunks)
	}
}

func TestPlanKeysetChunksCompositeKey(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	src := captureSource(t, ctx)
	setupSourceTables(t, ctx, src,
		"CREATE TABLE rc_it.items (order_id bigint, sku text, PRIMARY KEY (order_id, sku))")
	mustExec(t, ctx, src.conn, "INSERT INTO rc_it.items SELECT g/3, 'sku'||(g%3) FROM generate_series(1,30) g")

	chunks, err := src.PlanChunks(ctx, engine.TableRef{Schema: "rc_it", Name: "items"}, engine.ChunkOptions{TargetRows: 10})
	if err != nil {
		t.Fatalf("PlanChunks composite: %v", err)
	}
	if len(chunks) < 2 {
		t.Fatalf("expected multiple chunks, got %d", len(chunks))
	}
	// Each internal boundary carries both key columns.
	for i := 0; i < len(chunks)-1; i++ {
		if len(chunks[i].Hi) != 2 {
			t.Errorf("boundary %d should have 2 key columns, got %v", i, chunks[i].Hi)
		}
	}
}

// (Keyless tables no longer error on the keyset path — they auto-fall back to
// ctid; see TestPlanChunksAutoFallbackToCtid.)
