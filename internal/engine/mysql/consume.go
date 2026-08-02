package mysql

import (
	"context"
	"fmt"
	"strings"

	"github.com/rudimk/replicare/internal/engine"
)

// ReadDirtyKeys returns up to max unconsumed dirty keys for (target, table): the
// delta rows not yet present in the target's track, via set-difference (§3.3) —
// never a high-water mark (the commit-order hazard). Ordered by delta_id (an
// ordering hint only). Key columns are read as raw bytes (character_set_results=
// binary) and returned as string values for faithful re-read/apply.
func (s *Source) ReadDirtyKeys(ctx context.Context, t engine.TableRef, target engine.TargetID, max int) ([]engine.DirtyKey, error) {
	if s.db == nil {
		return nil, errNotConnected
	}
	relID, pkCols, ok, err := s.lookupRegistry(ctx, t)
	if err != nil {
		return nil, fmt.Errorf("mysql: read dirty keys for %s: %w", t, err)
	}
	if !ok {
		return nil, fmt.Errorf("mysql: read dirty keys: table %s is not captured", t)
	}

	sel := "d.delta_id, d.rc_op"
	for _, k := range deltaColumns(len(pkCols)) {
		sel += ", d." + bq(k)
	}
	q := fmt.Sprintf(`
		SELECT %s
		FROM %s d
		LEFT JOIN %s tr ON tr.target = ? AND tr.delta_id = d.delta_id
		WHERE tr.delta_id IS NULL
		ORDER BY d.delta_id`,
		sel, captureRef(deltaTableName(relID)), captureRef(trackTableName(relID)))
	if max > 0 {
		q += fmt.Sprintf(" LIMIT %d", max)
	}

	rows, err := s.db.QueryContext(ctx, q, string(target))
	if err != nil {
		return nil, fmt.Errorf("mysql: read dirty keys for %s: %w", t, err)
	}
	defer func() { _ = rows.Close() }()

	var out []engine.DirtyKey
	for rows.Next() {
		// delta_id, rc_op, then n key columns (scanned as raw bytes for fidelity).
		scan := make([]any, 2+len(pkCols))
		var deltaID int64
		var op string
		scan[0], scan[1] = &deltaID, &op
		keyBytes := make([]*[]byte, len(pkCols))
		for i := range pkCols {
			var b []byte
			keyBytes[i] = &b
			scan[2+i] = &b
		}
		if err := rows.Scan(scan...); err != nil {
			return nil, fmt.Errorf("mysql: scan dirty key %s: %w", t, err)
		}
		key := make(engine.KeyValues, len(pkCols))
		for i := range pkCols {
			if b := *keyBytes[i]; b != nil {
				key[i] = string(b)
			} else {
				key[i] = nil
			}
		}
		out = append(out, engine.DirtyKey{DeltaID: engine.DeltaID(deltaID), Op: engine.Op(op), Key: key})
	}
	return out, rows.Err()
}

// ConfirmConsumed records the given delta ids as consumed for (target, table),
// which later enables purge. Delete-by-delta_id discipline (§3.3): idempotent via
// ON DUPLICATE KEY, so a replayed confirm is harmless.
func (s *Source) ConfirmConsumed(ctx context.Context, t engine.TableRef, target engine.TargetID, ids []engine.DeltaID) error {
	if s.db == nil {
		return errNotConnected
	}
	if len(ids) == 0 {
		return nil
	}
	relID, _, ok, err := s.lookupRegistry(ctx, t)
	if err != nil {
		return fmt.Errorf("mysql: confirm consumed for %s: %w", t, err)
	}
	if !ok {
		return fmt.Errorf("mysql: confirm consumed: table %s is not captured", t)
	}

	placeholders := make([]string, len(ids))
	args := make([]any, 0, 1+len(ids))
	for i, id := range ids {
		placeholders[i] = "(?, ?)"
		args = append(args, string(target), int64(id))
	}
	q := fmt.Sprintf("INSERT INTO %s (target, delta_id) VALUES %s ON DUPLICATE KEY UPDATE delta_id = delta_id",
		captureRef(trackTableName(relID)), strings.Join(placeholders, ", "))
	if _, err := s.db.ExecContext(ctx, q, args...); err != nil {
		return fmt.Errorf("mysql: confirm consumed for %s: %w", t, err)
	}
	return nil
}
