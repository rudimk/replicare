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

// Version-guarded (HLC last-write-wins) apply for MySQL cluster members (CLAUDE.md
// §5.3), mirroring internal/engine/postgres/apply_mesh.go. It parallels the one-way
// StageUpsert/DeleteAbsent but applies an incoming change only when its version
// (rc_hlc_phys, rc_hlc_log, rc_node) strictly beats the value the target holds
// (recorded in the target's version register):
//
//   - StageUpsert (phase 1): apply ALIVE winners — upsert value + register.
//   - DeleteAbsent (phase 2): apply TOMBSTONE winners — delete + tombstone register.
//
// MySQL cannot read the register table inside an INSERT ... SELECT that also targets
// it (error 1093), so the winner set is computed ONCE into a staging column (rc_win)
// against the current register, and every value/register statement reads that flag —
// which also guarantees the value and register statements agree on the same winners.
// Both phases advance the target HLC past the max version seen (the receive step).

// stageUpsertCluster stages the versioned re-read, marks winners, and applies the
// alive winners under HLC-LWW.
func (t *mysqlApplyTx) stageUpsertCluster(ctx context.Context, ref engine.TableRef, cols []string, reread io.Reader) error {
	tbl, err := t.sink.tableMeta(ctx, ref)
	if err != nil {
		return err
	}
	keyCols := captureColsFor(tbl)
	if len(keyCols) == 0 {
		return fmt.Errorf("mysql: cluster apply: target %s has no usable key", ref)
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
	// Staging = user transport columns + the four version columns + a computed winner
	// flag (rc_win, not loaded).
	fullDDL := typeDDL + ", rc_hlc_phys BIGINT, rc_hlc_log INT, rc_node VARCHAR(255), rc_deleted TINYINT(1), rc_win TINYINT NOT NULL DEFAULT 0"
	if _, err := t.tx.ExecContext(ctx, fmt.Sprintf("CREATE TEMPORARY TABLE %s (%s) ENGINE=InnoDB", bq(stg), fullDDL)); err != nil {
		return fmt.Errorf("mysql: cluster apply: staging %s: %w", ref, err)
	}
	loadCols := append(append([]string{}, cols...), meshVersionCols...)
	if _, err := runLoad(ctx, t.tx, bq(stg), loadCols, reread, charset, t.sink.localInfile); err != nil {
		return fmt.Errorf("mysql: cluster apply: stage re-read %s: %w", ref, err)
	}

	reg := captureRef(registerTableName(ref))
	// Mark winners ONCE against the current register (before any register write). This
	// is the first statement to touch the target register; if the peer member's own
	// capture install has not yet created it (the startup ordering window between
	// reciprocal edges), the failure is classified transient so the drain retries.
	if _, err := t.tx.ExecContext(ctx, clusterMarkWinnersSQL(stg, reg, keyCols)); err != nil {
		return fmt.Errorf("mysql: cluster apply: mark winners %s: %w", ref, classifyMeshNotReady(err))
	}
	// Alive winners -> user table, then the register.
	if _, err := t.tx.ExecContext(ctx, clusterAliveUpsertSQL(ref, stg, cols, keyCols)); err != nil {
		return fmt.Errorf("mysql: cluster apply: upsert %s: %w", ref, classifyMySQLFK(err, t.cyclic))
	}
	if _, err := t.tx.ExecContext(ctx, clusterRegUpsertSQL(reg, stg, keyCols, false)); err != nil {
		return fmt.Errorf("mysql: cluster apply: register %s: %w", ref, err)
	}
	if err := t.observeFromStaging(ctx, stg); err != nil {
		return err
	}
	t.staging[ref] = applyStg{name: stg, keyCols: keyCols}
	return nil
}

// deleteTombstonesCluster applies the tombstone winners staged by stageUpsertCluster:
// version-guarded deletes of the user row plus a tombstone in the register. Reuses the
// staging (and its computed rc_win), so the passed dirtyKeys are unused.
func (t *mysqlApplyTx) deleteTombstonesCluster(ctx context.Context, ref engine.TableRef) error {
	info, ok := t.staging[ref]
	if !ok {
		return fmt.Errorf("mysql: cluster apply: DeleteAbsent before StageUpsert for %s", ref)
	}
	reg := captureRef(registerTableName(ref))
	if _, err := t.tx.ExecContext(ctx, clusterTombstoneDeleteSQL(ref, info.name, info.keyCols)); err != nil {
		return fmt.Errorf("mysql: cluster apply: delete %s: %w", ref, classifyMySQLFK(err, t.cyclic))
	}
	if _, err := t.tx.ExecContext(ctx, clusterRegUpsertSQL(reg, info.name, info.keyCols, true)); err != nil {
		return fmt.Errorf("mysql: cluster apply: tombstone register %s: %w", ref, err)
	}
	return t.observeFromStaging(ctx, info.name)
}

// observeFromStaging advances the target HLC past the max version in the staging (the
// HLC receive step). A no-op when the staging is empty.
func (t *mysqlApplyTx) observeFromStaging(ctx context.Context, stg string) error {
	var phys int64
	var log int
	err := t.tx.QueryRowContext(ctx,
		fmt.Sprintf("SELECT rc_hlc_phys, rc_hlc_log FROM %s ORDER BY rc_hlc_phys DESC, rc_hlc_log DESC LIMIT 1", bq(stg))).
		Scan(&phys, &log)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("mysql: cluster apply: read max version: %w", err)
	}
	if _, err := t.tx.ExecContext(ctx, hlcObserveSQL(phys, log)); err != nil {
		return fmt.Errorf("mysql: cluster apply: advance clock: %w", err)
	}
	return nil
}

// classifyMeshNotReady marks a "database/table not there yet" failure (errno 1049
// unknown database, 1146 unknown table) transient so the drain retries once the peer
// member's capture install has created the replicare database.
func classifyMeshNotReady(err error) error {
	var myErr *driver.MySQLError
	if errors.As(err, &myErr) && (myErr.Number == 1049 || myErr.Number == 1146) {
		return &engine.TransientConstraintError{Err: err}
	}
	return err
}

// meshVersionGreater is the row-comparison predicate "staged version strictly beats
// the target register version", with a NULL register (never seen) always losing.
func meshVersionGreater(sAlias, rAlias string) string {
	return fmt.Sprintf(
		"(%[2]s.rc_hlc_phys IS NULL OR (%[1]s.rc_hlc_phys, %[1]s.rc_hlc_log, %[1]s.rc_node) > (%[2]s.rc_hlc_phys, %[2]s.rc_hlc_log, %[2]s.rc_node))",
		sAlias, rAlias)
}

func meshKeyJoin(a, b string, keyCols []captureCol) string {
	conds := make([]string, len(keyCols))
	for i, k := range keyCols {
		q := bq(k.Name)
		conds[i] = fmt.Sprintf("%s.%s = %s.%s", a, q, b, q)
	}
	return strings.Join(conds, " AND ")
}

// clusterMarkWinnersSQL sets rc_win=1 on staged rows whose version beats the target
// register (or the register has no row). Computed ONCE, before any register write, so
// every subsequent value/register statement agrees on the winners without re-reading
// the register (which MySQL forbids while inserting into it — error 1093).
func clusterMarkWinnersSQL(stg, reg string, keyCols []captureCol) string {
	return fmt.Sprintf(
		"UPDATE %s s LEFT JOIN %s r ON %s SET s.rc_win = IF(%s, 1, 0)",
		bq(stg), reg, meshKeyJoin("s", "r", keyCols), meshVersionGreater("s", "r"))
}

// clusterAliveUpsertSQL upserts the alive winners (rc_deleted=0, rc_win=1) into the
// user table. Non-key columns update on conflict.
func clusterAliveUpsertSQL(ref engine.TableRef, stg string, cols []string, keyCols []captureCol) string {
	keySet := map[string]bool{}
	for _, c := range keyCols {
		keySet[c.Name] = true
	}
	quoted := make([]string, len(cols))
	sel := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = bq(c)
		sel[i] = "s." + bq(c)
	}
	var setParts []string
	for _, c := range cols {
		if !keySet[c] {
			setParts = append(setParts, fmt.Sprintf("%s = VALUES(%s)", bq(c), bq(c)))
		}
	}
	if len(setParts) == 0 {
		setParts = []string{fmt.Sprintf("%s = VALUES(%s)", bq(keyCols[0].Name), bq(keyCols[0].Name))}
	}
	return fmt.Sprintf(
		"INSERT INTO %s (%s) SELECT %s FROM %s s WHERE s.rc_deleted = 0 AND s.rc_win = 1 ON DUPLICATE KEY UPDATE %s",
		qualify(ref.Schema, ref.Name), strings.Join(quoted, ", "), strings.Join(sel, ", "), bq(stg), strings.Join(setParts, ", "))
}

// clusterRegUpsertSQL upserts the winners' versions into the register. deleted selects
// the phase: false for the alive upsert, true for the tombstone delete. It reads only
// the staging (rc_win already set), so it does not re-read the register (avoiding 1093).
func clusterRegUpsertSQL(reg, stg string, keyCols []captureCol, deleted bool) string {
	keyNames := make([]string, len(keyCols))
	selKeys := make([]string, len(keyCols))
	for i, k := range keyCols {
		q := bq(k.Name)
		keyNames[i] = q
		selKeys[i] = "s." + q
	}
	del := "0"
	filter := "s.rc_deleted = 0"
	if deleted {
		del, filter = "1", "s.rc_deleted = 1"
	}
	return fmt.Sprintf(
		"INSERT INTO %s (%s, rc_hlc_phys, rc_hlc_log, rc_node, rc_deleted) "+
			"SELECT %s, s.rc_hlc_phys, s.rc_hlc_log, s.rc_node, %s FROM %s s WHERE %s AND s.rc_win = 1 "+
			"ON DUPLICATE KEY UPDATE rc_hlc_phys = VALUES(rc_hlc_phys), rc_hlc_log = VALUES(rc_hlc_log), "+
			"rc_node = VALUES(rc_node), rc_deleted = VALUES(rc_deleted), rc_updated_at = NOW(6)",
		reg, strings.Join(keyNames, ", "), strings.Join(selKeys, ", "), del, bq(stg), filter)
}

// clusterTombstoneDeleteSQL deletes user rows for tombstone winners (rc_deleted=1,
// rc_win=1) via a MySQL multi-table DELETE joined to the staging.
func clusterTombstoneDeleteSQL(ref engine.TableRef, stg string, keyCols []captureCol) string {
	return fmt.Sprintf(
		"DELETE u FROM %s u JOIN %s s ON %s WHERE s.rc_deleted = 1 AND s.rc_win = 1",
		qualify(ref.Schema, ref.Name), bq(stg), meshKeyJoin("u", "s", keyCols))
}
