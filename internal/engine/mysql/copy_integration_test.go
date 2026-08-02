package mysql

import (
	"context"
	"database/sql"
	"io"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

func mustExecArgs(t *testing.T, ctx context.Context, db *sql.DB, q string, args ...any) {
	t.Helper()
	if _, err := db.ExecContext(ctx, q, args...); err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
}

// assertHexEqual compares every row of a table on source and target by HEX of the
// escape-sensitive columns (v, b) plus the generated column (up, recomputed on
// the target), proving byte-identical transport.
func assertHexEqual(t *testing.T, ctx context.Context, src *Source, sink *Sink, ref engine.TableRef) {
	t.Helper()
	q := "SELECT id, HEX(v), HEX(b), IFNULL(HEX(note),'NULL'), HEX(up) FROM " + qualify(ref.Schema, ref.Name) + " ORDER BY id"
	dump := func(db *sql.DB) []string {
		rows, err := db.QueryContext(ctx, q)
		if err != nil {
			t.Fatalf("dump: %v", err)
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			var id int
			var v, b, note, up string
			if err := rows.Scan(&id, &v, &b, &note, &up); err != nil {
				t.Fatalf("scan: %v", err)
			}
			out = append(out, v+"|"+b+"|"+note+"|"+up)
		}
		return out
	}
	s, d := dump(src.db), dump(sink.db)
	if len(s) != len(d) {
		t.Fatalf("row count: source %d, target %d", len(s), len(d))
	}
	for i := range s {
		if s[i] != d[i] {
			t.Errorf("row %d differs (byte-level):\n  source: %s\n  target: %s", i, s[i], d[i])
		}
	}
}

// copyChunkManual mimics the neutral copy driver's per-chunk pipe: CopyChunk on
// the source feeds BulkLoad on the sink through an io.Pipe.
func copyChunkManual(t *testing.T, ctx context.Context, src *Source, sink *Sink, c engine.Chunk, cols []string) int64 {
	t.Helper()
	pr, pw := io.Pipe()
	errc := make(chan error, 1)
	go func() {
		err := src.CopyChunk(ctx, c, pw)
		_ = pw.CloseWithError(err)
		errc <- err
	}()
	n, loadErr := sink.BulkLoad(ctx, c.Table, cols, pr, engine.LoadDirect)
	_ = pr.CloseWithError(loadErr)
	if copyErr := <-errc; copyErr != nil {
		t.Fatalf("CopyChunk: %v", copyErr)
	}
	if loadErr != nil {
		t.Fatalf("BulkLoad: %v", loadErr)
	}
	return n
}

func connectSink(t *testing.T, ctx context.Context) *Sink {
	t.Helper()
	s := &Sink{cfg: tgtCfg()}
	if err := s.Connect(ctx); err != nil {
		t.Fatalf("connect sink: %v", err)
	}
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	return s
}

// TestCopyByteFaithful is the MM4a crux: a fidelity corpus with escape-sensitive
// values (tab, newline, backslash, NUL, the literal bytes "\N", a 4-byte utf8mb4
// character, NULL) copies byte-identically source->target through the LOAD DATA
// transport, and a generated column is excluded (target regenerates it).
func TestCopyByteFaithful(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	src := connectSource(t, ctx)
	sink := connectSink(t, ctx)

	ddl := `CREATE TABLE rc_it.fid (
		id INT PRIMARY KEY,
		v VARCHAR(200) CHARACTER SET utf8mb4,
		b VARBINARY(64),
		note VARCHAR(64) NULL,
		up VARCHAR(220) GENERATED ALWAYS AS (UPPER(v)) STORED
	) ENGINE=InnoDB`
	mustExec(t, ctx, src.db, ddl)
	mustExec(t, ctx, sink.db, "DROP DATABASE IF EXISTS rc_it", "CREATE DATABASE rc_it", ddl)
	t.Cleanup(func() { _, _ = sink.db.Exec("DROP DATABASE IF EXISTS rc_it") })

	// Seed the source with escape-sensitive values.
	seed := []struct {
		id      int
		v, note string
		b       []byte
		hasNote bool
	}{
		{1, "plain", "ok", []byte{0x01, 0x02}, true},
		{2, "tab\tnl\nbackslash\\end", "", []byte{0x00, 0x09, 0x0A, 0x1A, 0x5C, 0x4E}, false}, // NUL, tab, nl, ^Z, backslash, N
		{3, "emoji \U0001F600 4byte", "q\tt", []byte("\\N"), true},                            // literal "\N" bytes
	}
	for _, r := range seed {
		if r.hasNote {
			mustExecArgs(t, ctx, src.db, "INSERT INTO rc_it.fid (id, v, b, note) VALUES (?, ?, ?, ?)", r.id, r.v, r.b, r.note)
		} else {
			mustExecArgs(t, ctx, src.db, "INSERT INTO rc_it.fid (id, v, b, note) VALUES (?, ?, ?, NULL)", r.id, r.v, r.b)
		}
	}

	ref := engine.TableRef{Schema: "rc_it", Name: "fid"}
	chunks, err := src.PlanChunks(ctx, ref, engine.ChunkOptions{TargetRows: 2})
	if err != nil {
		t.Fatalf("plan chunks: %v", err)
	}
	tbl, _ := src.tableMeta(ctx, ref)
	cols := transportCols(tbl) // excludes the generated `up` column
	for _, c := range cols {
		if c == "up" {
			t.Fatal("generated column `up` must be excluded from transport")
		}
	}

	var total int64
	for _, c := range chunks {
		total += copyChunkManual(t, ctx, src, sink, c, cols)
	}
	if total != 3 {
		t.Fatalf("copied %d rows, want 3", total)
	}

	// Byte-identity check via HEX on the escape-sensitive columns, plus the
	// generated column recomputed on the target.
	assertHexEqual(t, ctx, src, sink, ref)
}

// TestCopyResumeDeleteRange copies, then simulates a resume by DELETE-range +
// re-copy of a chunk, asserting no duplicates and unchanged fidelity.
func TestCopyResumeDeleteRange(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	src := connectSource(t, ctx)
	sink := connectSink(t, ctx)

	ddl := "CREATE TABLE rc_it.nums (id INT PRIMARY KEY, v VARCHAR(20)) ENGINE=InnoDB"
	mustExec(t, ctx, src.db, ddl)
	mustExec(t, ctx, sink.db, "DROP DATABASE IF EXISTS rc_it", "CREATE DATABASE rc_it", ddl)
	t.Cleanup(func() { _, _ = sink.db.Exec("DROP DATABASE IF EXISTS rc_it") })
	mustExec(t, ctx, src.db, "INSERT INTO rc_it.nums SELECT n, CONCAT('v', n) FROM (SELECT 1 n UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) x")

	ref := engine.TableRef{Schema: "rc_it", Name: "nums"}
	tbl, _ := src.tableMeta(ctx, ref)
	cols := transportCols(tbl)
	chunks, _ := src.PlanChunks(ctx, ref, engine.ChunkOptions{TargetRows: 2})
	for _, c := range chunks {
		copyChunkManual(t, ctx, src, sink, c, cols)
	}
	// Resume: clear the first chunk's range on the target and re-copy it.
	if err := sink.DeleteRange(ctx, ref, chunks[0].Lo, chunks[0].Hi); err != nil {
		t.Fatalf("delete range: %v", err)
	}
	copyChunkManual(t, ctx, src, sink, chunks[0], cols)

	var n int
	if err := sink.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM rc_it.nums").Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 5 {
		t.Errorf("after resume, target has %d rows, want 5 (no duplicates)", n)
	}
}
