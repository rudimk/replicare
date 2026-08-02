package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/rudimk/replicare/internal/engine"
)

// Cyclic-FK initial load (mysql-plan §0.2, §4.1). MySQL has no DEFERRABLE
// constraints, so the two strategies are: (1) NULL-then-fill for a nullable
// cyclic FK, and (2) scoped FOREIGN_KEY_CHECKS=0 + PRE-COMMIT orphan verification
// for a NOT NULL cyclic FK. Both are engine-level building blocks (unit/
// integration-tested here), mirroring the Postgres engine's cyclic.go, which is
// likewise not yet wired into the neutral copy pipeline.

// LoadCyclicNullFill loads a self-referential table whose cyclic FK columns are
// nullable, via NULL-then-fill: pass 1 loads every row with the FK column(s)
// omitted (NULL on the target); pass 2 fills them from the source once all rows
// exist. FK checks stay ON throughout — nothing violates them.
func LoadCyclicNullFill(ctx context.Context, src *Source, sink *Sink, ref engine.TableRef) error {
	if src.db == nil || sink.db == nil {
		return errNotConnected
	}
	table, err := src.tableMeta(ctx, ref)
	if err != nil {
		return err
	}
	fkCols := selfRefFKCols(table)
	if len(fkCols) == 0 {
		return fmt.Errorf("mysql: null-fill: %s has no self-referential FK columns", ref)
	}
	pk := captureColsFor(table)
	if len(pk) == 0 {
		return fmt.Errorf("mysql: null-fill: %s has no usable key", ref)
	}
	charset, err := sink.loadCharset(ctx, ref)
	if err != nil {
		return err
	}

	// Pass 1: load every row with the FK column(s) omitted (NULL on target).
	pass1 := subtractCols(transportCols(table), fkCols)
	if err := pipeLoad(ctx, src, ref, pass1, sink.db, qualify(ref.Schema, ref.Name), charset, sink.localInfile); err != nil {
		return fmt.Errorf("mysql: null-fill pass 1 (%s): %w", ref, err)
	}

	// Pass 2: stage (pk + fk) into a TEMP table and UPDATE the FK columns.
	pkNames := make([]string, len(pk))
	for i, c := range pk {
		pkNames[i] = c.Name
	}
	fillCols := append(append([]string{}, pkNames...), fkCols...)
	tgt, err := sink.tableMeta(ctx, ref)
	if err != nil {
		return err
	}
	typeDDL, err := tempColumnDDL(tgt, fillCols)
	if err != nil {
		return err
	}

	tx, err := sink.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("mysql: null-fill pass 2: begin: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	// Drop any leftover staging first (session-scoped TEMP on the single-conn pool).
	const stg = "rc_fill_stg"
	if _, err := tx.ExecContext(ctx, "DROP TEMPORARY TABLE IF EXISTS "+bq(stg)); err != nil {
		return fmt.Errorf("mysql: null-fill pass 2: drop stale staging: %w", err)
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("CREATE TEMPORARY TABLE %s (%s) ENGINE=InnoDB", bq(stg), typeDDL)); err != nil {
		return fmt.Errorf("mysql: null-fill pass 2: staging: %w", err)
	}
	if err := pipeLoad(ctx, src, ref, fillCols, tx, bq(stg), charset, sink.localInfile); err != nil {
		return fmt.Errorf("mysql: null-fill pass 2: stage: %w", err)
	}
	setParts := make([]string, len(fkCols))
	for i, c := range fkCols {
		setParts[i] = fmt.Sprintf("t.%s = s.%s", bq(c), bq(c))
	}
	joinParts := make([]string, len(pkNames))
	for i, c := range pkNames {
		joinParts[i] = fmt.Sprintf("t.%s = s.%s", bq(c), bq(c))
	}
	upd := fmt.Sprintf("UPDATE %s t JOIN %s s ON %s SET %s",
		qualify(ref.Schema, ref.Name), bq(stg), strings.Join(joinParts, " AND "), strings.Join(setParts, ", "))
	if _, err := tx.ExecContext(ctx, upd); err != nil {
		return fmt.Errorf("mysql: null-fill pass 2: update: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("mysql: null-fill pass 2: commit: %w", err)
	}
	committed = true
	return nil
}

// LoadCyclicFKChecks loads a component whose cyclic FK is NOT NULL, via scoped
// FOREIGN_KEY_CHECKS=0 + PRE-COMMIT orphan verification (§0.2/Momus 2nd-pass B1).
// It pins a dedicated connection, loads every table with checks off inside one
// transaction, re-enables checks, then runs an orphan anti-join over the
// component's FK edges BEFORE COMMIT: any dangling reference rolls the whole load
// back with a loud error, so nothing referentially broken is ever committed —
// restoring the loud-before-corrupt guarantee plain FK_CHECKS=0 loses. The pinned
// connection is CLOSED at the end so the FOREIGN_KEY_CHECKS=0 session state never
// leaks back to the pool.
func LoadCyclicFKChecks(ctx context.Context, src *Source, sink *Sink, tables []engine.TableRef) error {
	if src.db == nil || sink.db == nil {
		return errNotConnected
	}
	// Pre-fetch per-table cols + charset BEFORE pinning the sink connection: with
	// a single-connection pool, doing sink introspection while the connection is
	// pinned to the load transaction would deadlock.
	type tinfo struct {
		ref     engine.TableRef
		cols    []string
		charset string
	}
	infos := make([]tinfo, 0, len(tables))
	for _, ref := range tables {
		tbl, err := src.tableMeta(ctx, ref)
		if err != nil {
			return err
		}
		charset, err := sink.loadCharset(ctx, ref)
		if err != nil {
			return err
		}
		infos = append(infos, tinfo{ref: ref, cols: transportCols(tbl), charset: charset})
	}

	conn, err := sink.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("mysql: cyclic load: pin connection: %w", err)
	}
	defer func() { _ = conn.Close() }() // discard the session (FK_CHECKS state) from the pool

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("mysql: cyclic load: begin: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	if _, err := tx.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS = 0"); err != nil {
		return fmt.Errorf("mysql: cyclic load: disable fk checks: %w", err)
	}
	for _, info := range infos {
		if err := pipeLoad(ctx, src, info.ref, info.cols, tx, qualify(info.ref.Schema, info.ref.Name), info.charset, sink.localInfile); err != nil {
			return fmt.Errorf("mysql: cyclic load %s: %w", info.ref, err)
		}
	}
	// Re-enable checks and VERIFY before commit: an orphan rolls everything back.
	if _, err := tx.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS = 1"); err != nil {
		return fmt.Errorf("mysql: cyclic load: re-enable fk checks: %w", err)
	}
	if err := verifyNoOrphans(ctx, tx, src, tables); err != nil {
		return err // rolled back by defer — nothing committed/visible
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("mysql: cyclic load: commit: %w", err)
	}
	committed = true
	return nil
}

// verifyNoOrphans runs an anti-join for every FK edge of the component tables,
// checking each child row references an existing parent. It sees the
// transaction's own uncommitted rows (same session), so it validates the load
// pre-commit. A found orphan is a loud, actionable error.
func verifyNoOrphans(ctx context.Context, ex execQuerier, src *Source, tables []engine.TableRef) error {
	for _, ref := range tables {
		tbl, err := src.tableMeta(ctx, ref)
		if err != nil {
			return err
		}
		for _, fk := range tbl.ForeignKeys {
			var joinOn, notNull []string
			for i := range fk.ChildCols {
				joinOn = append(joinOn, fmt.Sprintf("c.%s = p.%s", bq(fk.ChildCols[i]), bq(fk.ParentCols[i])))
				notNull = append(notNull, fmt.Sprintf("c.%s IS NOT NULL", bq(fk.ChildCols[i])))
			}
			// A NULL parent-key column after the LEFT JOIN means no matching parent.
			q := fmt.Sprintf("SELECT 1 FROM %s c LEFT JOIN %s p ON %s WHERE (%s) AND p.%s IS NULL LIMIT 1",
				qualify(fk.Child.Schema, fk.Child.Name), qualify(fk.Parent.Schema, fk.Parent.Name),
				strings.Join(joinOn, " AND "), strings.Join(notNull, " AND "), bq(fk.ParentCols[0]))
			var one int
			err := ex.QueryRowContext(ctx, q).Scan(&one)
			if err == nil {
				return fmt.Errorf("mysql: cyclic load: FK %s (%s) has an orphan referencing a missing parent in %s — "+
					"load rolled back, nothing committed (mysql-plan §0.2)", fk.Name, fk.Child, fk.Parent)
			}
			if !errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("mysql: cyclic load: verify FK %s: %w", fk.Name, err)
			}
		}
	}
	return nil
}

// pipeLoad streams an explicit column subset of a whole source table into `into`
// (a real or TEMP target) via an io.Pipe: CopyChunk-style serialization feeds
// runLoad on the given execer.
func pipeLoad(ctx context.Context, src *Source, ref engine.TableRef, cols []string, ex execQuerier, into, charset string, localInfile bool) error {
	pr, pw := io.Pipe()
	errc := make(chan error, 1)
	go func() {
		err := streamCols(ctx, src.db, ref, cols, pw)
		_ = pw.CloseWithError(err)
		errc <- err
	}()
	_, loadErr := runLoad(ctx, ex, into, cols, pr, charset, localInfile)
	_ = pr.CloseWithError(loadErr)
	copyErr := <-errc
	if copyErr != nil {
		return copyErr
	}
	return loadErr
}

// streamCols selects an explicit column list from a whole source table and
// serializes each row as byte-faithful LOAD DATA text (values read as raw bytes).
func streamCols(ctx context.Context, db *sql.DB, ref engine.TableRef, cols []string, w io.Writer) error {
	sel := make([]string, len(cols))
	for i, c := range cols {
		sel[i] = bq(c)
	}
	rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT %s FROM %s", strings.Join(sel, ", "), qualify(ref.Schema, ref.Name)))
	if err != nil {
		return fmt.Errorf("mysql: stream cols %s: %w", ref, err)
	}
	defer func() { _ = rows.Close() }()
	bw := newRowWriter(w)
	raw := make([][]byte, len(cols))
	dest := make([]any, len(cols))
	for i := range raw {
		dest[i] = &raw[i]
	}
	for rows.Next() {
		for i := range raw {
			raw[i] = nil
		}
		if err := rows.Scan(dest...); err != nil {
			return fmt.Errorf("mysql: stream cols scan %s: %w", ref, err)
		}
		writeRow(bw, raw)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return bw.Flush()
}

// selfRefFKCols returns the child columns of self-referential FKs (child ==
// parent) on a table — the columns NULL-fill omits then fills.
func selfRefFKCols(t engine.Table) []string {
	seen := map[string]bool{}
	var out []string
	for _, fk := range t.ForeignKeys {
		if fk.Child == fk.Parent {
			for _, c := range fk.ChildCols {
				if !seen[c] {
					seen[c] = true
					out = append(out, c)
				}
			}
		}
	}
	return out
}

// subtractCols returns cols with any member of remove excluded, preserving order.
func subtractCols(cols, remove []string) []string {
	rm := map[string]bool{}
	for _, c := range remove {
		rm[c] = true
	}
	out := make([]string, 0, len(cols))
	for _, c := range cols {
		if !rm[c] {
			out = append(out, c)
		}
	}
	return out
}
