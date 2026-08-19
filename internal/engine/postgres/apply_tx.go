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

// pgApplyTx is a Postgres apply transaction for a drain pass (CLAUDE.md §8.1). It
// backs two drain shapes the neutral apply layer drives:
//
//   - The single-component transaction (acyclic per-table, and all-DEFERRABLE
//     cyclic): stage+upsert in topo order (parent->child), delete-absent in reverse
//     (child->parent), commit. SET CONSTRAINTS ALL DEFERRED (set in BeginApply) lets
//     a DEFERRABLE cyclic FK commit atomically.
//   - The per-table NULL-then-fill cyclic drain: each phase (upsert / delete / fill)
//     is its own committed pgApplyTx. StageUpsert loads the nullable cyclic FK
//     columns (cyclicCols) NULL so a non-DEFERRABLE FK check passes immediately;
//     FillCyclic sets them from staging once every referenced row is present.
type pgApplyTx struct {
	sink    *Sink
	staging map[engine.TableRef]stagingInfo
	nextStg int
	// cyclicCols are the nullable cyclic FK child columns per table (non-empty only
	// for a cyclic component drained via NULL-then-fill). Those columns are omitted
	// from the per-table upsert (loaded NULL, breaking the cycle so a non-DEFERRABLE
	// FK check passes immediately) and filled from staging by an explicit FillCyclic
	// phase once every referenced row is present. This is the streaming analogue of
	// the cyclic initial copy's NULL-then-fill (§4.1).
	cyclicCols map[engine.TableRef][]string
	committed  bool
}

type stagingInfo struct {
	name    string
	keyCols []captureCol
}

var (
	_ engine.ApplyTx      = (*pgApplyTx)(nil)
	_ engine.CyclicFiller = (*pgApplyTx)(nil)
)

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
	// In a cyclic component, omit the cyclic FK columns from the upsert (they load
	// NULL on new rows, keep their prior value on existing rows) so a non-DEFERRABLE
	// FK check passes now; they are filled from the staging by an explicit FillCyclic
	// phase once every referenced row is present. The staging keeps ALL columns for
	// that fill.
	upsertCols := cols
	if nc := tx.cyclicCols[t]; len(nc) > 0 {
		upsertCols = subtractCols(cols, nc)
	}
	if _, err := conn.Exec(ctx, mergeInsertSQL(t, stg, upsertCols, keyCols, colSetOf(keyCols), identity)); err != nil {
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

// FillCyclic implements engine.CyclicFiller: it sets each staged table's cyclic FK
// columns (loaded NULL during StageUpsert) from that table's staging, now that
// every referenced row in the component is present, so the non-DEFERRABLE FK holds.
// A 23503 here is a cross-batch cyclic dependency not yet landed and is classified
// transient so the drain retries (§3.3). FillCyclic is an explicit phase of the
// per-table NULL-then-fill cyclic drain — it is NOT run at Commit, so a landing
// pass can commit rows with their cyclic columns still NULL.
func (tx *pgApplyTx) FillCyclic(ctx context.Context) error {
	for ref, info := range tx.staging {
		cols := tx.cyclicCols[ref]
		if len(cols) == 0 {
			continue
		}
		if _, err := tx.sink.conn.Exec(ctx, cyclicFillSQL(ref, info, cols)); err != nil {
			return fmt.Errorf("postgres: apply: fill cyclic FK columns %s: %w", ref, classifyFKViolation(err))
		}
	}
	return nil
}

// Commit commits the apply transaction. Cyclic FK columns are filled by an explicit
// FillCyclic phase (the per-table NULL-then-fill drain), not here, so a landing pass
// commits rows with those columns still NULL and the cycle is closed by a later fill.
func (tx *pgApplyTx) Commit(ctx context.Context) error {
	if _, err := tx.sink.conn.Exec(ctx, "COMMIT"); err != nil {
		return fmt.Errorf("postgres: apply: commit: %w", classifyFKViolation(err))
	}
	tx.committed = true
	return nil
}

// cyclicFillSQL updates a table's cyclic FK columns from its staging, joined by
// the table's key: UPDATE t SET c = s.c ... FROM stg s WHERE t.k = s.k ...
func cyclicFillSQL(ref engine.TableRef, info stagingInfo, cols []string) string {
	set := make([]string, len(cols))
	for i, c := range cols {
		set[i] = fmt.Sprintf("%s = s.%s", quoteIdentifier(c), quoteIdentifier(c))
	}
	join := make([]string, len(info.keyCols))
	for i, k := range info.keyCols {
		join[i] = fmt.Sprintf("t.%s = s.%s", quoteIdentifier(k.Name), quoteIdentifier(k.Name))
	}
	return fmt.Sprintf("UPDATE %s t SET %s FROM %s s WHERE %s",
		qualifyTable(ref), strings.Join(set, ", "), quoteIdentifier(info.name), strings.Join(join, " AND "))
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
