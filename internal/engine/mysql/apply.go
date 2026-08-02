package mysql

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/rudimk/replicare/internal/engine"
)

// ApplyPass applies one streaming drain pass for a single table (§3.3): in one
// transaction, stage the faithful re-read into a TEMP table, upsert the present
// rows, then delete the dirty keys that are ABSENT from the re-read (deleted at
// the source). Idempotent: re-running after a crash re-applies harmlessly (§3.3
// at-least-once). An `ON UPDATE CURRENT_TIMESTAMP` column is carried in the
// upsert's SET clause (col = VALUES(col)), so it lands the verbatim source value
// rather than being auto-mutated to "now" (§0.4/Momus M2).
func (s *Sink) ApplyPass(ctx context.Context, t engine.TableRef, cols []string, dirtyKeys []engine.KeyValues, reread io.Reader) error {
	if s.db == nil {
		return errNotConnected
	}
	tbl, err := s.tableMeta(ctx, t)
	if err != nil {
		return err
	}
	key := replicationKey(tbl)
	if key == nil {
		return fmt.Errorf("mysql: apply: target %s has no usable key", t)
	}
	keySet := map[string]bool{}
	for _, c := range key.Columns {
		keySet[c] = true
	}
	charset, err := s.loadCharset(ctx, t)
	if err != nil {
		return err
	}
	typeDDL, err := tempColumnDDL(tbl, cols)
	if err != nil {
		return err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("mysql: apply: begin: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	const stg = "rc_apply_stg"
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("CREATE TEMPORARY TABLE %s (%s) ENGINE=InnoDB", bq(stg), typeDDL)); err != nil {
		return fmt.Errorf("mysql: apply: staging: %w", err)
	}
	if _, err := runLoad(ctx, tx, bq(stg), cols, reread, charset); err != nil {
		return fmt.Errorf("mysql: apply: stage re-read %s: %w", t, err)
	}

	// Upsert the present (re-read) rows.
	quoted := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = bq(c)
	}
	var setParts []string
	for _, c := range cols {
		if keySet[c] {
			continue
		}
		setParts = append(setParts, fmt.Sprintf("%s = VALUES(%s)", bq(c), bq(c)))
	}
	if len(setParts) == 0 {
		setParts = []string{fmt.Sprintf("%s = %s", bq(key.Columns[0]), bq(key.Columns[0]))}
	}
	ins := fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s ON DUPLICATE KEY UPDATE %s",
		qualify(t.Schema, t.Name), strings.Join(quoted, ", "), strings.Join(quoted, ", "), bq(stg), strings.Join(setParts, ", "))
	if _, err := tx.ExecContext(ctx, ins); err != nil {
		return fmt.Errorf("mysql: apply: upsert %s: %w", t, err)
	}

	// Delete the dirty keys absent from the re-read (deleted at source).
	keyCols := captureColsFor(tbl)
	keyNames := make([]string, len(keyCols))
	for i, c := range keyCols {
		keyNames[i] = bq(c.Name)
	}
	inPred, args := keyInPredicate(keyCols, dirtyKeys)
	keyTuple := strings.Join(keyNames, ", ")
	del := fmt.Sprintf("DELETE FROM %s WHERE (%s) AND (%s) NOT IN (SELECT %s FROM %s)",
		qualify(t.Schema, t.Name), inPred, keyTuple, keyTuple, bq(stg))
	if _, err := tx.ExecContext(ctx, del, args...); err != nil {
		return fmt.Errorf("mysql: apply: delete-absent %s: %w", t, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("mysql: apply: commit: %w", err)
	}
	committed = true
	return nil
}
