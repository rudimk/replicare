package daemon

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/rudimk/replicare/internal/config"
	"github.com/rudimk/replicare/internal/observability/live"
)

// TestVerifyChunkedIntegration checks the chunked verify against real source and
// target servers, with no daemon running (verify is read-only and needs nothing
// installed): a matching table reads ok over several ranges; missing, extra and
// changed rows are each found and named by key; a sampled run reads only part of the
// table; a composite text key works; --table narrows the check; and a row that is
// fixed before the re-check is not reported.
func TestVerifyChunkedIntegration(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	src := dial(t, ctx, envd("RC_SRC_HOST", "127.0.0.1"), envd("RC_SRC_PORT", "5440"), envd("RC_SRC_DB", "replicare_src"))
	defer src.Close(context.Background())
	tgt := dial(t, ctx, envd("RC_DST_HOST", "127.0.0.1"), envd("RC_DST_PORT", "5441"), envd("RC_DST_DB", "replicare_dst"))
	defer tgt.Close(context.Background())

	for _, c := range []*pgx.Conn{src, tgt} {
		mustExec(t, ctx, c, "DROP SCHEMA IF EXISTS rc_vc CASCADE")
		mustExec(t, ctx, c, "CREATE SCHEMA rc_vc")
		mustExec(t, ctx, c, "CREATE TABLE rc_vc.items (id int PRIMARY KEY, note text, amt numeric)")
		mustExec(t, ctx, c, "CREATE TABLE rc_vc.pairs (region text, code text, val int, PRIMARY KEY (region, code))")
		mustExec(t, ctx, c, "INSERT INTO rc_vc.items SELECT g, 'n'||g, g*1.5 FROM generate_series(1,2500) g")
		mustExec(t, ctx, c, "INSERT INTO rc_vc.pairs SELECT 'r'||(g%7), 'c'||g, g FROM generate_series(1,700) g")
		mustExec(t, ctx, c, "ANALYZE rc_vc.items")
	}
	t.Cleanup(func() {
		bg := context.Background()
		_, _ = src.Exec(bg, "DROP SCHEMA IF EXISTS rc_vc CASCADE")
		_, _ = tgt.Exec(bg, "DROP SCHEMA IF EXISTS rc_vc CASCADE")
	})

	cfg, err := config.Load(writeConfig(t, harnessConfigYAML(`
syncs:
  - name: s1
    source: src
    targets: [dst]
    include: ["rc_vc.*"]
`)))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	e := live.New(cfg, 20*time.Second)
	fast := live.VerifyOptions{ChunkRows: 300, Pause: -1, RecheckAfter: -1}

	// --- matching data: ok, every row covered, several ranges.
	vr := e.VerifyWith(ctx, "s1", fast)
	if vr.Error != "" || !vr.Converged {
		t.Fatalf("matching data: converged=%v err=%q %+v", vr.Converged, vr.Error, vr)
	}
	it := findVerifyTable(vr, "dst", "rc_vc.items")
	if it == nil || it.Status != "ok" || it.Mode != "chunked" || it.SourceRows != 2500 || it.TargetRows != 2500 {
		t.Fatalf("items = %+v, want ok/chunked 2500/2500", it)
	}
	if it.Chunks < 9 {
		t.Fatalf("items checked in %d ranges, want at least 9 for 2500 rows at 300 a range", it.Chunks)
	}
	if p := findVerifyTable(vr, "dst", "rc_vc.pairs"); p == nil || p.Status != "ok" || p.SourceRows != 700 {
		t.Fatalf("pairs (composite text key) = %+v, want ok with 700 rows", p)
	}

	// --- drift: one row missing, one extra (also past the source's last key), one changed.
	mustExec(t, ctx, tgt, "DELETE FROM rc_vc.items WHERE id = 1234")
	mustExec(t, ctx, tgt, "INSERT INTO rc_vc.items VALUES (9999, 'stray', 0)")
	mustExec(t, ctx, tgt, "UPDATE rc_vc.items SET note = 'changed' WHERE id = 42")
	mustExec(t, ctx, tgt, "UPDATE rc_vc.pairs SET val = -1 WHERE region = 'r3' AND code = 'c10'")
	vr = e.VerifyWith(ctx, "s1", fast)
	if vr.Converged {
		t.Fatalf("drift not caught: %+v", vr)
	}
	it = findVerifyTable(vr, "dst", "rc_vc.items")
	if it == nil || it.Status != "drift-count" || it.Missing != 1 || it.Extra != 1 || it.Different != 1 {
		t.Fatalf("items drift = %+v, want drift-count with 1 missing, 1 extra, 1 different", it)
	}
	want := map[string]string{"1234": "missing", "9999": "extra", "42": "different"}
	for _, k := range it.Keys {
		if want[k.Key] != k.Problem {
			t.Fatalf("unexpected key %+v in %+v", k, it.Keys)
		}
		delete(want, k.Key)
	}
	if len(want) != 0 {
		t.Fatalf("keys not reported: %v (got %+v)", want, it.Keys)
	}
	p := findVerifyTable(vr, "dst", "rc_vc.pairs")
	if p == nil || p.Status != "drift-checksum" || p.Different != 1 || len(p.Keys) != 1 || p.Keys[0].Key != "r3,c10" {
		t.Fatalf("pairs drift = %+v, want one different row r3,c10", p)
	}

	// --- --table narrows the check; an unknown table is an error.
	only := fast
	only.Tables = []string{"rc_vc.pairs"}
	vr = e.VerifyWith(ctx, "s1", only)
	if len(vr.Targets) != 1 || len(vr.Targets[0].Tables) != 1 || vr.Targets[0].Tables[0].Table != "rc_vc.pairs" {
		t.Fatalf("--table rc_vc.pairs checked %+v", vr.Targets)
	}
	only.Tables = []string{"rc_vc.nope"}
	if vr = e.VerifyWith(ctx, "s1", only); vr.Error == "" {
		t.Fatalf("unknown table: want an error, got %+v", vr)
	}

	// --- a row fixed before the re-check is not reported: the re-check reads by key
	// after the wait, so the hook below repairs id 42 between the scan and the re-check.
	mustExec(t, ctx, tgt, "INSERT INTO rc_vc.items VALUES (1234, 'n1234', 1234*1.5)")
	mustExec(t, ctx, tgt, "DELETE FROM rc_vc.items WHERE id = 9999")
	mustExec(t, ctx, tgt, "UPDATE rc_vc.pairs SET val = 10 WHERE region = 'r3' AND code = 'c10'")
	healing := fast
	healing.RecheckAfter = 2 * time.Second
	healing.Tables = []string{"rc_vc.items"}
	go func() {
		time.Sleep(500 * time.Millisecond)
		_, _ = tgt.Exec(context.Background(), "UPDATE rc_vc.items SET note = 'n42' WHERE id = 42")
	}()
	vr = e.VerifyWith(ctx, "s1", healing)
	time.Sleep(100 * time.Millisecond) // the goroutine's Exec has finished with tgt
	if it = findVerifyTable(vr, "dst", "rc_vc.items"); it == nil || it.Status != "ok" {
		t.Fatalf("row fixed before the re-check still reported: %+v", it)
	}

	// --- sampled: a few ranges only, and still catches a difference inside one.
	sampled := fast
	sampled.Sample = 3
	vr = e.VerifyWith(ctx, "s1", sampled)
	it = findVerifyTable(vr, "dst", "rc_vc.items")
	if it == nil || it.Mode != "sampled" || it.Status != "ok" || it.Chunks != 3 || it.SourceRows >= 2500 {
		t.Fatalf("sampled = %+v, want ok over 3 ranges covering part of the table", it)
	}
	mustExec(t, ctx, tgt, "UPDATE rc_vc.items SET note = 'x'")
	vr = e.VerifyWith(ctx, "s1", sampled)
	if it = findVerifyTable(vr, "dst", "rc_vc.items"); it == nil || it.Status != "drift-checksum" || !it.Truncated && it.Different == 0 {
		t.Fatalf("sampled after changing every row = %+v, want drift-checksum", it)
	}
}
