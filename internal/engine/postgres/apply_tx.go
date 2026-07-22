package postgres

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/rudimk/replicare/internal/engine"
)

// pgApplyTx is a per-FK-component apply transaction (CLAUDE.md §8.1). The caller
// stages+upserts every dirty table in topo order (parent->child), then
// deletes-absent every dirty table in reverse (child->parent), then commits — so
// the component is referentially consistent within the pass. FK checks are
// deferred to commit (set in BeginApply) so deferrable cyclic FKs succeed.
type pgApplyTx struct {
	sink      *Sink
	staging   map[engine.TableRef]stagingInfo
	nextStg   int
	committed bool
}

type stagingInfo struct {
	name    string
	keyCols []captureCol
}

var _ engine.ApplyTx = (*pgApplyTx)(nil)

// StageUpsert stages a table's re-read present rows into a per-table TEMP table
// and upserts them, keeping the staging for a later DeleteAbsent in the same tx.
func (tx *pgApplyTx) StageUpsert(ctx context.Context, t engine.TableRef, cols []string, reread io.Reader) error {
	conn := tx.sink.conn
	table, err := tx.sink.tableMeta(ctx, t)
	if err != nil {
		return err
	}
	keyCols := captureColsFor(table)
	if len(keyCols) == 0 {
		return fmt.Errorf("postgres: apply: target %s has no usable key", t)
	}
	typeByName := make(map[string]string, len(table.Columns))
	identity := false
	colSet := make(map[string]bool, len(cols))
	for _, c := range cols {
		colSet[c] = true
	}
	for _, c := range table.Columns {
		typeByName[c.Name] = c.DataType
		if c.Identity && colSet[c.Name] {
			identity = true
		}
	}

	stg := fmt.Sprintf("replicare_stg_%d", tx.nextStg)
	tx.nextStg++
	stgCols := make([]string, len(cols))
	for i, c := range cols {
		stgCols[i] = quoteIdentifier(c) + " " + typeByName[c]
	}
	if _, err := conn.Exec(ctx, fmt.Sprintf("CREATE TEMP TABLE %s (%s) ON COMMIT DROP",
		quoteIdentifier(stg), strings.Join(stgCols, ", "))); err != nil {
		return fmt.Errorf("postgres: apply: create staging for %s: %w", t, err)
	}
	copySQL := fmt.Sprintf("COPY %s (%s) FROM STDIN", quoteIdentifier(stg), quotedColumnList(cols))
	if _, err := conn.PgConn().CopyFrom(ctx, reread, copySQL); err != nil {
		return fmt.Errorf("postgres: apply: stage re-read for %s: %w", t, err)
	}
	if _, err := conn.Exec(ctx, mergeInsertSQL(t, stg, cols, keyCols, colSetOf(keyCols), identity)); err != nil {
		return fmt.Errorf("postgres: apply: upsert %s: %w", t, classifyFKViolation(err))
	}
	tx.staging[t] = stagingInfo{name: stg, keyCols: keyCols}
	return nil
}

// DeleteAbsent deletes the dirty keys absent from the table's staging.
func (tx *pgApplyTx) DeleteAbsent(ctx context.Context, t engine.TableRef, dirtyKeys []engine.KeyValues) error {
	info, ok := tx.staging[t]
	if !ok {
		return fmt.Errorf("postgres: apply: DeleteAbsent before StageUpsert for %s", t)
	}
	if len(dirtyKeys) == 0 {
		return nil
	}
	inPred, err := keysetInPredicate(info.keyCols, dirtyKeys)
	if err != nil {
		return err
	}
	keyTuple := "(" + quotedKeyList(info.keyCols) + ")"
	del := fmt.Sprintf("DELETE FROM %s WHERE %s AND %s NOT IN (SELECT %s FROM %s)",
		qualifyTable(t), inPred, keyTuple, quotedKeyList(info.keyCols), quoteIdentifier(info.name))
	if _, err := tx.sink.conn.Exec(ctx, del); err != nil {
		return fmt.Errorf("postgres: apply: delete absent %s: %w", t, classifyFKViolation(err))
	}
	return nil
}

// Commit commits the apply transaction (FK checks fire here for deferred ones).
func (tx *pgApplyTx) Commit(ctx context.Context) error {
	if _, err := tx.sink.conn.Exec(ctx, "COMMIT"); err != nil {
		return fmt.Errorf("postgres: apply: commit: %w", classifyFKViolation(err))
	}
	tx.committed = true
	return nil
}

// classifyFKViolation marks Postgres FK violations (SQLSTATE 23503) as
// transient so the drain loop's retry fallback handles cycles/self-refs/
// cross-pass dependencies (§3.3). Deferred FK checks surface at COMMIT, so
// Commit classifies too. Every other error stays as-is and halts loud.
func classifyFKViolation(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23503" {
		return &engine.TransientConstraintError{Err: err}
	}
	return err
}

// Rollback rolls back the apply transaction if it has not committed.
func (tx *pgApplyTx) Rollback(ctx context.Context) error {
	if tx.committed {
		return nil
	}
	if _, err := tx.sink.conn.Exec(ctx, "ROLLBACK"); err != nil {
		return fmt.Errorf("postgres: apply: rollback: %w", err)
	}
	return nil
}
