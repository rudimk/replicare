package postgres

import (
	"context"
	"fmt"

	"github.com/rudimk/replicare/internal/engine"
)

// ReseedSequences implements engine.SequenceReseeder for the Postgres Sink: it advances
// every owned identity/serial sequence on table t to MAX(col)+1 on THIS endpoint, so a
// promoted one-way (DR) target never re-issues an id already present in the replicated
// data (CLAUDE.md §7 — the counter is schema-object state the data path does not carry).
//
// It is PASSIVE/DR ONLY; the caller (reseed-sequences CLI) refuses to run it against a
// mesh member. `setval(seq, next, false)` sets is_called=false so the NEXT nextval
// returns exactly `next` (= Max+1, or 1 for an empty table). Dry-run reads MAX and
// computes `next` but issues no setval.
func (s *Sink) ReseedSequences(ctx context.Context, t engine.TableRef, dryRun bool) (engine.SequenceReseedResult, error) {
	if s.conn == nil {
		return engine.SequenceReseedResult{}, fmt.Errorf("postgres: sink not connected")
	}
	res := engine.SequenceReseedResult{Table: t}

	schema := t.Schema
	if schema == "" {
		schema = "public"
	}

	// Owned sequences for the table, covering BOTH `serial` defaults and
	// `GENERATED … AS IDENTITY` (pg_get_serial_sequence resolves both; a column with no
	// owned sequence — UUID/natural/composite PK — returns NULL and is excluded).
	// $1 is the already-quoted schema.table (pg_get_serial_sequence parses it as a
	// regclass-like name); the ::text casts pin every parameter's type so Postgres never
	// fails with "could not determine data type of parameter" (SQLSTATE 42P08) — the
	// params feed only functions/comparisons whose argument type it won't infer alone.
	qualified := quoteIdentifier(schema) + "." + quoteIdentifier(t.Name)
	rows, err := s.conn.Query(ctx, `
		SELECT a.attname,
		       pg_get_serial_sequence($1::text, a.attname)
		FROM pg_attribute a
		JOIN pg_class c ON c.oid = a.attrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $2::text AND c.relname = $3::text
		  AND a.attnum > 0 AND NOT a.attisdropped
		  AND pg_get_serial_sequence($1::text, a.attname) IS NOT NULL
		ORDER BY a.attnum`, qualified, schema, t.Name)
	if err != nil {
		return res, fmt.Errorf("postgres: discover sequences for %s: %w", t, err)
	}
	type owned struct{ col, seq string }
	var seqs []owned
	for rows.Next() {
		var o owned
		if err := rows.Scan(&o.col, &o.seq); err != nil {
			rows.Close()
			return res, fmt.Errorf("postgres: scan sequence for %s: %w", t, err)
		}
		seqs = append(seqs, o)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return res, fmt.Errorf("postgres: discover sequences for %s: %w", t, err)
	}

	for _, o := range seqs {
		// Identifiers cannot be parameterized; quote them. The sequence name comes from
		// pg_get_serial_sequence already schema-qualified and quoted as needed.
		var max int64
		q := fmt.Sprintf(`SELECT COALESCE(MAX(%s), 0) FROM %s`,
			quoteIdentifier(o.col), qualifyTable(t))
		if err := s.conn.QueryRow(ctx, q).Scan(&max); err != nil {
			return res, fmt.Errorf("postgres: max(%s) on %s: %w", o.col, t, err)
		}
		next := max + 1
		if !dryRun {
			// is_called=false → the next nextval() returns exactly `next`.
			if _, err := s.conn.Exec(ctx, `SELECT setval($1::regclass, $2, false)`, o.seq, next); err != nil {
				return res, fmt.Errorf("postgres: setval %s to %d: %w", o.seq, next, err)
			}
		}
		res.Columns = append(res.Columns, engine.SequenceColumnReseed{Column: o.col, Max: max, SetTo: next})
	}
	return res, nil
}

// compile-time assertion that the Sink satisfies the capability.
var _ engine.SequenceReseeder = (*Sink)(nil)
