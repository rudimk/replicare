package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/rudimk/replicare/internal/engine"
)

// Delta consumption (CLAUDE.md §3.3): the dirty-key-set model. A pass reads
// `delta MINUS track` for a target (set difference, never a scalar high-water
// mark — the commit-order hazard is why), returning the dirty PKs to re-read;
// ConfirmConsumed then records the exact observed delta_ids in the track table.
// Deletion is always by delta_id, never by PK, to survive the read-and-clear
// race. Purge (M5c) later removes delta rows consumed by all targets.

// ReadDirtyKeys returns up to max unconsumed dirty keys for (target, table): the
// delta rows not yet present in the target's track. Ordered by delta_id (an
// ordering hint only). max <= 0 means no limit.
func (s *Source) ReadDirtyKeys(ctx context.Context, t engine.TableRef, target engine.TargetID, max int) ([]engine.DirtyKey, error) {
	if err := s.requireConn(); err != nil {
		return nil, err
	}
	relID, pkCols, ok, err := lookupCapture(ctx, s.conn, t)
	if err != nil {
		return nil, fmt.Errorf("postgres: read dirty keys for %s: %w", t, err)
	}
	if !ok {
		return nil, fmt.Errorf("postgres: read dirty keys: table %s is not captured", t)
	}

	sel := "d.delta_id, d.rc_op"
	for _, k := range deltaColumns(len(pkCols)) {
		sel += ", d." + k
	}
	q := fmt.Sprintf(`
		SELECT %s
		FROM %s d
		LEFT JOIN %s tr ON tr.target = $1 AND tr.delta_id = d.delta_id
		WHERE tr.delta_id IS NULL
		ORDER BY d.delta_id`,
		sel, qualifiedCapture(deltaTableName(relID)), qualifiedCapture(trackTableName(relID)))
	if max > 0 {
		q += fmt.Sprintf("\n\t\tLIMIT %d", max)
	}

	rows, err := s.conn.Query(ctx, q, string(target))
	if err != nil {
		return nil, fmt.Errorf("postgres: read dirty keys for %s: %w", t, err)
	}
	defer rows.Close()

	var out []engine.DirtyKey
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, fmt.Errorf("postgres: scan dirty key: %w", err)
		}
		deltaID, ok := vals[0].(int64)
		if !ok {
			return nil, fmt.Errorf("postgres: unexpected delta_id type %T", vals[0])
		}
		op, _ := vals[1].(string)
		key := engine.KeyValues(vals[2:])
		out = append(out, engine.DirtyKey{
			DeltaID: engine.DeltaID(deltaID),
			Op:      engine.Op(strings.TrimSpace(op)),
			Key:     key,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: read dirty keys for %s: %w", t, err)
	}
	return out, nil
}

// ConfirmConsumed records the given delta_ids as consumed by target in the
// table's track (delete-by-delta_id discipline, CLAUDE.md §3.3). It is
// idempotent (ON CONFLICT DO NOTHING) so a replay after a crash is harmless.
func (s *Source) ConfirmConsumed(ctx context.Context, t engine.TableRef, target engine.TargetID, ids []engine.DeltaID) error {
	if err := s.requireConn(); err != nil {
		return err
	}
	if len(ids) == 0 {
		return nil
	}
	relID, _, ok, err := lookupCapture(ctx, s.conn, t)
	if err != nil {
		return fmt.Errorf("postgres: confirm consumed for %s: %w", t, err)
	}
	if !ok {
		return fmt.Errorf("postgres: confirm consumed: table %s is not captured", t)
	}
	raw := make([]int64, len(ids))
	for i, id := range ids {
		raw[i] = int64(id)
	}
	_, err = s.conn.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s (target, delta_id)
		SELECT $1, unnest($2::bigint[])
		ON CONFLICT (target, delta_id) DO NOTHING`,
		qualifiedCapture(trackTableName(relID))), string(target), raw)
	if err != nil {
		return fmt.Errorf("postgres: confirm consumed for %s: %w", t, err)
	}
	return nil
}

// lookupCapture returns a captured table's rel_id and ordered PK column names, or
// ok=false if the table is not captured.
func lookupCapture(ctx context.Context, conn *pgx.Conn, ref engine.TableRef) (relID int, pkCols []string, ok bool, err error) {
	err = conn.QueryRow(ctx, fmt.Sprintf(
		`SELECT rel_id, pk_cols FROM %s.captured WHERE schema_name = $1 AND table_name = $2`,
		quoteIdentifier(captureSchema)), ref.Schema, ref.Name).Scan(&relID, &pkCols)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil, false, nil
	}
	if err != nil {
		return 0, nil, false, err
	}
	return relID, pkCols, true, nil
}
