package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"strings"

	driver "github.com/go-sql-driver/mysql"

	"github.com/rudimk/replicare/internal/engine"
)

// mysqlApplyTx is a per-FK-component apply transaction (§8.1, MM5b). The neutral
// apply layer stages+upserts every dirty table in topo order (parent->child),
// deletes-absent every dirty table in reverse (child->parent), then commits — so
// the component is referentially consistent within the pass.
//
// The whole pass runs on a single PINNED connection (the Sink's pool is
// MaxOpenConns=1). A CYCLIC component runs with FOREIGN_KEY_CHECKS=0 for the load
// (InnoDB has no deferrable constraints) and re-enables + verifies referential
// integrity over the WHOLE component BEFORE commit, so an orphan halts loud and
// rolls back — nothing referentially broken is ever committed (mysql-plan §0.2,
// Momus 2nd-pass B1/m1). An ACYCLIC component keeps checks ON and lets the
// neutral retry fallback resolve cross-pass FK deps (errno 1451/1452 -> transient).
type mysqlApplyTx struct {
	sink       *Sink
	conn       *sql.Conn
	tx         *sql.Tx
	cyclic     bool
	compTables []engine.TableRef
	staging    map[engine.TableRef]applyStg
	nextStg    int
	committed  bool
}

type applyStg struct {
	name    string
	keyCols []captureCol
}

var _ engine.ApplyTx = (*mysqlApplyTx)(nil)

// BeginApply starts a component drain-pass transaction on a pinned connection
// (MM5b). For a cyclic component it disables FK checks for the load; the
// pre-commit verify in Commit restores loud-before-corrupt safety. componentTables
// is the full topo-ordered member set — needed because a delete under
// FK_CHECKS=0 can strand a NON-dirty child, so the verify must scan every
// component edge, not just this pass's staged tables (mysql-plan §0.2 m1).
func (s *Sink) BeginApply(ctx context.Context, cyclic bool, componentTables []engine.TableRef) (engine.ApplyTx, error) {
	if s.db == nil {
		return nil, errNotConnected
	}
	// Pre-populate the introspection cache for every component table BEFORE pinning
	// the single connection: with MaxOpenConns=1, a cache-miss introspection while
	// the connection is pinned to the apply transaction would deadlock (mirrors
	// cyclic.go's pre-fetch). Covers StageUpsert (dirty tables) AND the whole-
	// component pre-commit verify (which touches non-dirty tables too).
	for _, ref := range componentTables {
		if _, err := s.tableMeta(ctx, ref); err != nil {
			return nil, err
		}
	}

	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("mysql: begin apply: pin connection: %w", err)
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("mysql: begin apply: begin: %w", err)
	}
	if cyclic {
		if _, err := tx.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS = 0"); err != nil {
			_ = tx.Rollback()
			_ = conn.Close()
			return nil, fmt.Errorf("mysql: begin apply: disable fk checks: %w", err)
		}
	}
	return &mysqlApplyTx{
		sink: s, conn: conn, tx: tx, cyclic: cyclic,
		compTables: componentTables, staging: map[engine.TableRef]applyStg{},
	}, nil
}

// StageUpsert stages a table's faithful re-read (present rows) into a per-table
// TEMP table and upserts it (INSERT ... SELECT ... ON DUPLICATE KEY UPDATE). An
// ON UPDATE CURRENT_TIMESTAMP column is carried in the SET clause (col =
// VALUES(col)) so it lands the verbatim source value, not the target's apply-time
// now() (§0.4/Momus M2). The staging is kept for a later DeleteAbsent in the same tx.
func (t *mysqlApplyTx) StageUpsert(ctx context.Context, ref engine.TableRef, cols []string, reread io.Reader) error {
	tbl, err := t.sink.tableMeta(ctx, ref) // cache hit (pre-populated in BeginApply)
	if err != nil {
		return err
	}
	key := replicationKey(tbl)
	if key == nil {
		return fmt.Errorf("mysql: apply: target %s has no usable key", ref)
	}
	keySet := map[string]bool{}
	for _, c := range key.Columns {
		keySet[c] = true
	}
	charset, err := t.sink.loadCharset(ctx, ref)
	if err != nil {
		return err
	}
	typeDDL, err := tempColumnDDL(tbl, cols)
	if err != nil {
		return err
	}

	stg := fmt.Sprintf("rc_apply_stg_%d", t.nextStg)
	t.nextStg++
	if _, err := t.tx.ExecContext(ctx, fmt.Sprintf("CREATE TEMPORARY TABLE %s (%s) ENGINE=InnoDB", bq(stg), typeDDL)); err != nil {
		return fmt.Errorf("mysql: apply: staging %s: %w", ref, err)
	}
	if _, err := runLoad(ctx, t.tx, bq(stg), cols, reread, charset, t.sink.localInfile); err != nil {
		return fmt.Errorf("mysql: apply: stage re-read %s: %w", ref, err)
	}

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
		// Key-only table: nothing to update. VALUES(key) is a no-op that avoids the
		// `id = id` ambiguity INSERT ... SELECT ... ODKU raises (both the target and
		// the SELECT expose the column name).
		setParts = []string{fmt.Sprintf("%s = VALUES(%s)", bq(key.Columns[0]), bq(key.Columns[0]))}
	}
	ins := fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s ON DUPLICATE KEY UPDATE %s",
		qualify(ref.Schema, ref.Name), strings.Join(quoted, ", "), strings.Join(quoted, ", "), bq(stg), strings.Join(setParts, ", "))
	if _, err := t.tx.ExecContext(ctx, ins); err != nil {
		return fmt.Errorf("mysql: apply: upsert %s: %w", ref, classifyMySQLFK(err, t.cyclic))
	}
	t.staging[ref] = applyStg{name: stg, keyCols: captureColsFor(tbl)}
	return nil
}

// DeleteAbsent deletes the dirty keys absent from the table's staging (deleted at
// the source). Requires a prior StageUpsert for ref.
func (t *mysqlApplyTx) DeleteAbsent(ctx context.Context, ref engine.TableRef, dirtyKeys []engine.KeyValues) error {
	info, ok := t.staging[ref]
	if !ok {
		return fmt.Errorf("mysql: apply: DeleteAbsent before StageUpsert for %s", ref)
	}
	if len(dirtyKeys) == 0 {
		return nil
	}
	keyNames := make([]string, len(info.keyCols))
	for i, c := range info.keyCols {
		keyNames[i] = bq(c.Name)
	}
	inPred, args := keyInPredicate(info.keyCols, dirtyKeys)
	keyTuple := strings.Join(keyNames, ", ")
	del := fmt.Sprintf("DELETE FROM %s WHERE (%s) AND (%s) NOT IN (SELECT %s FROM %s)",
		qualify(ref.Schema, ref.Name), inPred, keyTuple, keyTuple, bq(info.name))
	if _, err := t.tx.ExecContext(ctx, del, args...); err != nil {
		return fmt.Errorf("mysql: apply: delete-absent %s: %w", ref, classifyMySQLFK(err, t.cyclic))
	}
	return nil
}

// Commit finalizes the pass. For a cyclic component it re-enables FK checks and
// runs the whole-component orphan verify INSIDE the still-open transaction; any
// orphan rolls back with a loud error and commits nothing (mysql-plan §0.2 B1).
func (t *mysqlApplyTx) Commit(ctx context.Context) error {
	if t.committed {
		return nil
	}
	if t.cyclic {
		if _, err := t.tx.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS = 1"); err != nil {
			_ = t.tx.Rollback()
			_ = t.closeConn(ctx)
			return fmt.Errorf("mysql: apply: re-enable fk checks: %w", err)
		}
		if err := t.verifyNoOrphans(ctx); err != nil {
			_ = t.tx.Rollback()
			_ = t.closeConn(ctx)
			return err // rolled back — nothing committed/visible
		}
	}
	if err := t.tx.Commit(); err != nil {
		_ = t.closeConn(ctx)
		return fmt.Errorf("mysql: apply: commit: %w", classifyMySQLFK(err, t.cyclic))
	}
	t.committed = true
	return t.closeConn(ctx)
}

// Rollback aborts the pass if it has not committed and releases the connection.
func (t *mysqlApplyTx) Rollback(ctx context.Context) error {
	if t.committed {
		return nil
	}
	rbErr := t.tx.Rollback()
	ccErr := t.closeConn(ctx)
	if rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
		return fmt.Errorf("mysql: apply: rollback: %w", rbErr)
	}
	return ccErr
}

// closeConn releases the pinned connection, first restoring the session state the
// pass mutated. Both cleanups matter because the Sink pool is MaxOpenConns=1, so
// (*sql.Conn).Close returns the SAME physical connection for the next pass:
//   - TEMP staging tables are session-scoped and survive Close, so a later pass
//     would collide on the name — drop them explicitly.
//   - SET FOREIGN_KEY_CHECKS is session-level and NOT transactional, so a
//     rolled-back cyclic pass leaves FK_CHECKS=0 on the connection — reset it.
func (t *mysqlApplyTx) closeConn(ctx context.Context) error {
	for _, s := range t.staging {
		_, _ = t.conn.ExecContext(ctx, "DROP TEMPORARY TABLE IF EXISTS "+bq(s.name))
	}
	if t.cyclic {
		_, _ = t.conn.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS = 1")
	}
	return t.conn.Close()
}

// verifyNoOrphans runs an anti-join for every FK edge of every component table
// (target-side edges) INSIDE the open transaction, so it sees the pass's own
// uncommitted writes and validates pre-commit. Whole-component scope is required:
// a DeleteAbsent under FK_CHECKS=0 can strand a NON-dirty child, which a
// dirty-scoped check would miss (Momus 2nd-pass m1). A found orphan is a loud,
// actionable, NON-transient error (§1.7 fail loud, never mangle).
func (t *mysqlApplyTx) verifyNoOrphans(ctx context.Context) error {
	for _, ref := range t.compTables {
		tbl, err := t.sink.tableMeta(ctx, ref) // cache hit
		if err != nil {
			return err
		}
		for _, fk := range tbl.ForeignKeys {
			var joinOn, notNull []string
			for i := range fk.ChildCols {
				joinOn = append(joinOn, fmt.Sprintf("c.%s = p.%s", bq(fk.ChildCols[i]), bq(fk.ParentCols[i])))
				notNull = append(notNull, fmt.Sprintf("c.%s IS NOT NULL", bq(fk.ChildCols[i])))
			}
			q := fmt.Sprintf("SELECT 1 FROM %s c LEFT JOIN %s p ON %s WHERE (%s) AND p.%s IS NULL LIMIT 1",
				qualify(fk.Child.Schema, fk.Child.Name), qualify(fk.Parent.Schema, fk.Parent.Name),
				strings.Join(joinOn, " AND "), strings.Join(notNull, " AND "), bq(fk.ParentCols[0]))
			var one int
			err := t.tx.QueryRowContext(ctx, q).Scan(&one)
			if err == nil {
				return fmt.Errorf("mysql: apply: FK %s (%s) has an orphan referencing a missing parent in %s — "+
					"drain pass rolled back, nothing committed (mysql-plan §0.2)", fk.Name, fk.Child, fk.Parent)
			}
			if !errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("mysql: apply: verify FK %s: %w", fk.Name, err)
			}
		}
	}
	return nil
}

// classifyMySQLFK marks InnoDB FK violations (errno 1451/1452, SQLSTATE 23000) on
// an ACYCLIC component as transient, so the neutral retry fallback resolves
// cross-pass FK dependencies (the MySQL analog of Postgres' 23503 in apply_tx.go).
// A CYCLIC component runs with FOREIGN_KEY_CHECKS=0 and never raises 1452 mid-pass;
// its only referential failure is the pre-commit verify's loud halt, which must
// NOT be transient (retrying the identical pass would thrash — Momus 2nd-pass m2).
// Every other error halts loud unchanged.
func classifyMySQLFK(err error, cyclic bool) error {
	if cyclic {
		return err
	}
	var myErr *driver.MySQLError
	if errors.As(err, &myErr) && (myErr.Number == 1451 || myErr.Number == 1452) {
		return &engine.TransientConstraintError{Err: err}
	}
	return err
}
