package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

// TestFidelityCorpus copies a table exercising every tricky Postgres type from
// the 9.6 source to the 17 target and asserts every value is byte-faithful in
// text form. This is the end-to-end proof of the §4.2 faithful-transport promise
// and the session-GUC canonicalization (float precision via extra_float_digits,
// timestamptz via TimeZone=UTC, bytea hex, interval style, ...).
func TestFidelityCorpus(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	src := captureSource(t, ctx)
	sink := sinkTarget(t, ctx)

	ddl := `CREATE TABLE rc_it.fidelity (
		id             int PRIMARY KEY,
		c_smallint     smallint,
		c_int          int,
		c_bigint       bigint,
		c_numeric      numeric,
		c_numeric_s    numeric(30,10),
		c_real         real,
		c_double       double precision,
		c_text         text,
		c_varchar      varchar(50),
		c_bool         boolean,
		c_uuid         uuid,
		c_json         json,
		c_jsonb        jsonb,
		c_int_arr      int[],
		c_text_arr     text[],
		c_bytea        bytea,
		c_date         date,
		c_timestamp    timestamp,
		c_timestamptz  timestamptz,
		c_time         time,
		c_interval     interval,
		c_inet         inet
	)`
	setupSourceTables(t, ctx, src, ddl)
	mustExecTarget(t, ctx, sink, "CREATE SCHEMA rc_it")
	mustExecTarget(t, ctx, sink, ddl)

	// Row 1: edge values stressing precision, special characters, and formats.
	mustExec(t, ctx, src.conn, `INSERT INTO rc_it.fidelity VALUES (
		1,
		-32768, -2147483648, -9223372036854775808,
		123456789012345678901234567890.123456789,
		1234567890.0123456789,
		(1.0/3.0)::real, (1.0/3.0)::double precision,
		e'tab\there, quote '' and newline\nend', 'weird: '',"\',
		true,
		'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',
		'{"k": "v", "u": "é", "n": [1,2,3]}',
		'{"b": true, "z": null, "arr": [1, 2.5, "x"]}',
		'{1,2,3}', array['a,b', 'c"d', NULL],
		e'\\xdeadbeef00ff'::bytea,
		'2021-03-14', '2021-03-14 15:09:26.535', '2021-03-14 15:09:26.535+05:30',
		'13:14:15.5', '1 year 2 mons 3 days 04:05:06',
		'192.168.1.0/24'
	)`)
	// Row 2: mostly NULL, plus float specials and an empty array.
	mustExec(t, ctx, src.conn, `INSERT INTO rc_it.fidelity (id, c_double, c_real, c_int_arr, c_text_arr, c_text) VALUES (
		2, 'Infinity'::float8, '-Infinity'::float4, '{}', '{}', ''
	)`)
	// Row 3: NaN and a negative interval.
	mustExec(t, ctx, src.conn, `INSERT INTO rc_it.fidelity (id, c_double, c_interval) VALUES (
		3, 'NaN'::float8, '-2 days -03:04:05'
	)`)

	ref := engine.TableRef{Schema: "rc_it", Name: "fidelity"}
	table, err := src.tableMeta(ctx, ref)
	if err != nil {
		t.Fatalf("tableMeta: %v", err)
	}
	cols := transportColumns(table)
	chunks, err := src.PlanChunks(ctx, ref, engine.ChunkOptions{TargetRows: 2})
	if err != nil {
		t.Fatalf("PlanChunks: %v", err)
	}
	for _, c := range chunks {
		copyOneChunk(t, ctx, src, sink, ref, cols, c)
	}

	// Compare every non-float column, rendered as text, ordered by id. Floats are
	// checked separately by VALUE: their text rendering legitimately differs
	// across the version gap (PG12+ emits the shortest round-trip form), but
	// extra_float_digits=3 guarantees the transported text parses to the exact
	// same bits — so a value comparison, not a text comparison, is correct.
	sel := `SELECT
		id::text, c_smallint::text, c_int::text, c_bigint::text,
		c_numeric::text, c_numeric_s::text,
		coalesce(c_text,'<n>'), coalesce(c_varchar,'<n>'), c_bool::text,
		c_uuid::text, c_json::text, c_jsonb::text,
		c_int_arr::text, c_text_arr::text, c_bytea::text,
		c_date::text, c_timestamp::text, c_timestamptz::text,
		c_time::text, c_interval::text, c_inet::text
		FROM rc_it.fidelity ORDER BY id`
	srcDump := dumpText(t, ctx, src.conn, sel)
	tgtDump := dumpText(t, ctx, sink.conn, sel)
	if len(srcDump) != 3 {
		t.Fatalf("source has %d rows, want 3", len(srcDump))
	}
	for i := range srcDump {
		if srcDump[i] != tgtDump[i] {
			t.Errorf("row %d not byte-faithful:\n source: %s\n target: %s", i, srcDump[i], tgtDump[i])
		}
	}

	// Float fidelity: the target must hold the EXACT source bits (IS NOT DISTINCT
	// FROM handles Infinity/NaN). If extra_float_digits weren't pinned to 3, the
	// old source would emit too few digits and these would fail.
	floatChecks := []struct {
		id   int
		expr string
	}{
		{1, "c_real IS NOT DISTINCT FROM (1.0/3.0)::real AND c_double IS NOT DISTINCT FROM (1.0/3.0)::double precision"},
		{2, "c_real IS NOT DISTINCT FROM '-Infinity'::real AND c_double IS NOT DISTINCT FROM 'Infinity'::double precision"},
		{3, "c_double IS NOT DISTINCT FROM 'NaN'::double precision"},
	}
	for _, fc := range floatChecks {
		var ok bool
		if err := sink.conn.QueryRow(ctx,
			"SELECT "+fc.expr+" FROM rc_it.fidelity WHERE id=$1", fc.id).Scan(&ok); err != nil {
			t.Fatalf("float check id=%d: %v", fc.id, err)
		}
		if !ok {
			t.Errorf("float value not preserved for id=%d", fc.id)
		}
	}
}
