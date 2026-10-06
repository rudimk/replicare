package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/rudimk/replicare/internal/engine"
)

// ReseedSequences implements engine.SequenceReseeder for the MySQL Sink: it advances the
// table's AUTO_INCREMENT counter to MAX(id)+1 on THIS endpoint, so a promoted one-way (DR)
// target never re-issues an id already present in the replicated data (CLAUDE.md §7 — the
// counter is table metadata the data path does not carry).
//
// It is PASSIVE/DR ONLY; the caller (reseed-sequences CLI) refuses to run it against a
// mesh member. MySQL has no sequence objects — the counter is the table's own
// AUTO_INCREMENT — and it only ever moves forward (an ALTER below the current max is
// clamped), so `AUTO_INCREMENT = Max+1` is both correct and inherently safe. A table with
// no AUTO_INCREMENT column (UUID/natural/composite PK) returns an empty result. Dry-run
// reads MAX and computes the target but issues no ALTER.
func (s *Sink) ReseedSequences(ctx context.Context, t engine.TableRef, dryRun bool) (engine.SequenceReseedResult, error) {
	if s.db == nil {
		return engine.SequenceReseedResult{}, fmt.Errorf("mysql: sink not connected")
	}
	res := engine.SequenceReseedResult{Table: t}

	// At most one AUTO_INCREMENT column per table; absent → nothing to reseed.
	var col string
	err := s.db.QueryRowContext(ctx, `
		SELECT column_name FROM information_schema.columns
		WHERE table_schema = ? AND table_name = ? AND EXTRA LIKE '%auto_increment%'
		LIMIT 1`, t.Schema, t.Name).Scan(&col)
	if errors.Is(err, sql.ErrNoRows) {
		return res, nil
	}
	if err != nil {
		return res, fmt.Errorf("mysql: discover auto_increment for %s: %w", t, err)
	}

	var max int64
	q := fmt.Sprintf("SELECT COALESCE(MAX(%s), 0) FROM %s", bq(col), qualify(t.Schema, t.Name))
	if err := s.db.QueryRowContext(ctx, q).Scan(&max); err != nil {
		return res, fmt.Errorf("mysql: max(%s) on %s: %w", col, t, err)
	}
	next := max + 1
	if !dryRun {
		alter := fmt.Sprintf("ALTER TABLE %s AUTO_INCREMENT = %d", qualify(t.Schema, t.Name), next)
		if _, err := s.db.ExecContext(ctx, alter); err != nil {
			return res, fmt.Errorf("mysql: set %s AUTO_INCREMENT to %d: %w", t, next, err)
		}
	}
	res.Columns = append(res.Columns, engine.SequenceColumnReseed{Column: col, Max: max, SetTo: next})
	return res, nil
}

// compile-time assertion that the Sink satisfies the capability.
var _ engine.SequenceReseeder = (*Sink)(nil)
