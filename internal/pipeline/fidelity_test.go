package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// fidelityDDL exercises types beyond the M4 copy corpus — an enum and range
// types — plus the usual tricky ones, through the FULL pipeline (copy AND the
// delta/staging-upsert apply path). The enum type is created on both ends (we
// never install target types; the target schema pre-exists, §7).
const fidelityDDL = `CREATE TYPE rc_it.mood AS ENUM ('sad','ok','happy');
CREATE TABLE rc_it.fidelity (
	id           int PRIMARY KEY,
	c_text       text,
	c_numeric    numeric(24,8),
	c_uuid       uuid,
	c_jsonb      jsonb,
	c_bytea      bytea,
	c_tstz       timestamptz,
	c_arr        int[],
	c_mood       rc_it.mood,
	c_i4range    int4range,
	c_numrange   numrange,
	c_tstzrange  tstzrange
)`

const fidelitySelect = `SELECT id::text, coalesce(c_text,'<n>'), c_numeric::text, c_uuid::text,
	c_jsonb::text, c_bytea::text, c_tstz::text, c_arr::text, c_mood::text,
	c_i4range::text, c_numrange::text, c_tstzrange::text
	FROM rc_it.fidelity ORDER BY id`

func setupFidelity(t *testing.T, ctx context.Context) (rawSrc, rawTgt *pgx.Conn) {
	t.Helper()
	rawSrc = rawConn(t, ctx, srcCfg())
	rawTgt = rawConn(t, ctx, tgtCfg())
	for _, c := range []*pgx.Conn{rawSrc, rawTgt} {
		mustExecC(t, ctx, c, "DROP SCHEMA IF EXISTS rc_it CASCADE")
		mustExecC(t, ctx, c, "CREATE SCHEMA rc_it")
		mustExecC(t, ctx, c, fidelityDDL)
	}
	mustExecC(t, ctx, rawSrc, "DROP SCHEMA IF EXISTS replicare CASCADE")
	mustExecC(t, ctx, rawTgt, "DROP SCHEMA IF EXISTS replicare_state CASCADE")
	t.Cleanup(func() {
		bg := context.Background()
		_, _ = rawSrc.Exec(bg, "DROP SCHEMA IF EXISTS rc_it CASCADE")
		_, _ = rawSrc.Exec(bg, "DROP SCHEMA IF EXISTS replicare CASCADE")
		_, _ = rawTgt.Exec(bg, "DROP SCHEMA IF EXISTS rc_it CASCADE")
		_, _ = rawTgt.Exec(bg, "DROP SCHEMA IF EXISTS replicare_state CASCADE")
		_ = rawSrc.Close(bg)
		_ = rawTgt.Close(bg)
	})
	return rawSrc, rawTgt
}

func dumpRows(t *testing.T, ctx context.Context, c *pgx.Conn, sel string) []string {
	t.Helper()
	rows, err := c.Query(ctx, sel)
	if err != nil {
		t.Fatalf("dump: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			t.Fatalf("scan: %v", err)
		}
		line := ""
		for _, v := range vals {
			s, _ := v.(string)
			line += s + "\x1f"
		}
		out = append(out, line)
	}
	return out
}

func assertFidelity(t *testing.T, ctx context.Context, rawSrc, rawTgt *pgx.Conn, phase string) {
	t.Helper()
	s := dumpRows(t, ctx, rawSrc, fidelitySelect)
	d := dumpRows(t, ctx, rawTgt, fidelitySelect)
	if len(s) != len(d) {
		t.Fatalf("%s: row count mismatch: source %d, target %d", phase, len(s), len(d))
	}
	for i := range s {
		if s[i] != d[i] {
			t.Errorf("%s: row %d not byte-faithful:\n source: %q\n target: %q", phase, i, s[i], d[i])
		}
	}
}

// TestFidelityThroughPipeline proves faithful transport across BOTH paths and
// including enum + range types: the initial copy, then the delta/staging-upsert
// apply path (update → stream), then a delete. This is the §4.2 promise that the
// delta path is as faithful as copy, exercised end-to-end through the daemon's
// Syncer against the 9.6 → 17 version gap.
func TestFidelityThroughPipeline(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	rawSrc, rawTgt := setupFidelity(t, ctx)

	// Row 1: rich values across every column.
	mustExecC(t, ctx, rawSrc, `INSERT INTO rc_it.fidelity VALUES (
		1, e'tab\there '' quote', 12345678901234.87654321,
		'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '{"k":"é","n":[1,2.5,null]}',
		e'\\xdeadbeef00'::bytea, '2021-03-14 15:09:26.535+05:30', '{1,2,3}',
		'happy', '[1,10)', numrange(1.5, 9.5, '[]'), tstzrange('2021-01-01', '2021-12-31'))`)
	// Row 2: NULLs, empty array, empty range.
	mustExecC(t, ctx, rawSrc, `INSERT INTO rc_it.fidelity (id, c_arr, c_i4range, c_mood) VALUES (2, '{}', 'empty', 'sad')`)

	syncer := buildSyncer(t, ctx)
	if err := syncer.Bringup(ctx); err != nil {
		t.Fatalf("Bringup: %v", err)
	}
	assertFidelity(t, ctx, rawSrc, rawTgt, "copy")

	// Delta path: update every row with new tricky values, add a row, delete a row.
	mustExecC(t, ctx, rawSrc, `UPDATE rc_it.fidelity SET
		c_text = e'changed\nline', c_numeric = -0.00000001, c_mood = 'ok',
		c_i4range = '(,5)', c_jsonb = '{"z":[true,false,null]}' WHERE id = 1`)
	mustExecC(t, ctx, rawSrc, `INSERT INTO rc_it.fidelity VALUES (
		3, 'new', 3.14, '11111111-1111-1111-1111-111111111111', '[]', ''::bytea,
		'2020-06-15 12:00:00+00', '{9,8,7}', 'ok', '[100,200)', numrange(0,1), tstzrange('2020-01-01', NULL))`)
	mustExecC(t, ctx, rawSrc, "DELETE FROM rc_it.fidelity WHERE id = 2")

	for i := 0; i < 2; i++ {
		if err := syncer.StreamOnce(ctx); err != nil {
			t.Fatalf("StreamOnce: %v", err)
		}
	}
	assertFidelity(t, ctx, rawSrc, rawTgt, "delta")
}
