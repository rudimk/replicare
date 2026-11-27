package postgres

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

func TestDecideStorageOrder(t *testing.T) {
	low, high, negHigh := 0.07, 0.95, -0.93
	const gib = int64(1) << 30
	cases := []struct {
		name    string
		relkind string
		file    int64
		size    int64
		corr    *float64
		pref    engine.CopyOrder
		want    bool
		reason  string
	}{
		{"large and scattered", "r", 7, 32 * gib, &low, engine.CopyOrderAuto, true, "correlation 0.07"},
		{"large but in key order", "r", 7, 32 * gib, &high, engine.CopyOrderAuto, false, "close to key order"},
		{"large, in reverse key order", "r", 7, 32 * gib, &negHigh, engine.CopyOrderAuto, false, "close to key order"},
		{"small and scattered", "r", 7, gib - 1, &low, engine.CopyOrderAuto, false, "smaller than 1 GiB"},
		{"never analysed", "r", 7, 32 * gib, nil, engine.CopyOrderAuto, false, "not been analysed"},
		{"partitioned", "p", 0, 0, &low, engine.CopyOrderStorage, false, "not an ordinary table"},
		{"asked for, small", "r", 7, 1, &high, engine.CopyOrderStorage, true, "asked for"},
	}
	for _, c := range cases {
		got := decideStorageOrder(c.relkind, c.file, c.size, c.corr, c.pref)
		if got.Use != c.want || !strings.Contains(got.Reason, c.reason) {
			t.Errorf("%s: got %+v, want use=%v with reason containing %q", c.name, got, c.want, c.reason)
		}
		if got.Use && got.File != c.file {
			t.Errorf("%s: file = %d, want %d", c.name, got.File, c.file)
		}
	}
}

func TestStorageChunks(t *testing.T) {
	ref := engine.TableRef{Schema: "public", Name: "t"}

	// 10 rows per block, 100k rows per chunk -> 10k blocks per chunk; 25k blocks.
	chunks := storageChunks(ref, 0, 25_000, 25_000, 250_000, 100_000)
	if len(chunks) != 3 {
		t.Fatalf("got %d chunks, want 3: %+v", len(chunks), chunks)
	}
	if chunks[0].Lo != nil || chunks[2].Hi != nil {
		t.Errorf("ends should be unbounded: %+v", chunks)
	}
	if b := chunkHi(chunks[0]); b != 10_000 {
		t.Errorf("first chunk ends at %d, want 10000", b)
	}
	for _, c := range chunks {
		if c.Method != engine.ChunkCTID {
			t.Errorf("method = %q", c.Method)
		}
	}

	// Many rows per block: never under the minimum chunk size.
	chunks = storageChunks(ref, 0, 5000, 5000, 5_000_000, 10_000)
	if b := chunkHi(chunks[0]); b != storageChunkMinBlocks {
		t.Errorf("first chunk ends at %d, want the minimum %d", b, storageChunkMinBlocks)
	}

	// Resuming from a block: the first chunk starts there.
	chunks = storageChunks(ref, 3000, 5000, 5000, 5_000_000, 10_000)
	if lo, _ := chunks[0].Lo[0].(int); lo != 3000 {
		t.Errorf("resumed first chunk starts at %v, want 3000", chunks[0].Lo)
	}

	// No statistics, or past the end: one chunk from `from` to the end.
	chunks = storageChunks(ref, 0, 0, 0, -1, 10_000)
	if len(chunks) != 1 || chunks[0].Lo != nil || chunks[0].Hi != nil {
		t.Errorf("empty table: %+v", chunks)
	}
	chunks = storageChunks(ref, 9000, 5000, 5000, 50_000, 10_000)
	if len(chunks) != 1 || chunks[0].Hi != nil {
		t.Errorf("from past the end: %+v", chunks)
	}
}

func chunkHi(c engine.Chunk) int {
	if len(c.Hi) != 1 {
		return -1
	}
	b, _ := c.Hi[0].(int)
	return b
}

func TestHumanBytes(t *testing.T) {
	for n, want := range map[int64]string{512: "512 B", 2048: "2.0 KiB", 32 << 30: "32.0 GiB"} {
		if got := humanBytes(n); got != want {
			t.Errorf("humanBytes(%d) = %q, want %q", n, got, want)
		}
	}
}

// TestStorageOrderIntegration checks the decision against a real server, that the
// storage identity changes on a rewrite, and that a storage-order chunk is read with
// a TID range scan (not a scan of the whole table) on 14+.
func TestStorageOrderIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	src := captureSource(t, ctx)
	setupSourceTables(t, ctx, src, "CREATE TABLE rc_it.events (id int PRIMARY KEY, note text)")
	mustExec(t, ctx, src.conn, "INSERT INTO rc_it.events SELECT g, 'v'||g FROM generate_series(1,5000) g ORDER BY random()")
	mustExec(t, ctx, src.conn, "ANALYZE rc_it.events")
	ref := engine.TableRef{Schema: "rc_it", Name: "events"}

	version, err := serverVersion(ctx, src.conn)
	if err != nil {
		t.Fatal(err)
	}
	auto, err := src.StorageOrder(ctx, ref, engine.CopyOrderAuto)
	if err != nil {
		t.Fatal(err)
	}
	if auto.Use {
		t.Errorf("a small table should be read in key order under auto: %+v", auto)
	}
	asked, err := src.StorageOrder(ctx, ref, engine.CopyOrderStorage)
	if err != nil {
		t.Fatal(err)
	}
	if version < storageOrderMinVersion {
		if asked.Use {
			t.Fatalf("storage order must never be used before Postgres 14: %+v", asked)
		}
		return
	}
	if !asked.Use || asked.File == 0 {
		t.Fatalf("asked for storage order on %d: %+v", version, asked)
	}

	file, err := src.StorageFile(ctx, ref)
	if err != nil || file != asked.File {
		t.Fatalf("StorageFile = %d, %v; want %d", file, err, asked.File)
	}
	mustExec(t, ctx, src.conn, "VACUUM FULL rc_it.events")
	after, err := src.StorageFile(ctx, ref)
	if err != nil || after == file {
		t.Fatalf("StorageFile after VACUUM FULL = %d, %v; want it to change from %d", after, err, file)
	}

	chunks, err := src.PlanStorageChunks(ctx, ref, 0, 1000)
	if err != nil || len(chunks) == 0 {
		t.Fatalf("plan: %v, %v", chunks, err)
	}
	pred, err := ctidPredicate(engine.KeyValues{10}, engine.KeyValues{20})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := src.conn.Query(ctx, "EXPLAIN SELECT * FROM rc_it.events WHERE "+pred)
	if err != nil {
		t.Fatal(err)
	}
	var plan []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		plan = append(plan, line)
	}
	rows.Close()
	if !strings.Contains(strings.Join(plan, "\n"), "Tid Range Scan") {
		t.Errorf("a block range should be read with a TID range scan, got:\n%s", strings.Join(plan, "\n"))
	}
}
