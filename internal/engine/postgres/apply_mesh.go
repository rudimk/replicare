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

// Version-guarded (HLC last-write-wins) apply for cluster members (CLAUDE.md §5.3,
// docs/multi-master.md §5.3). It parallels the one-way StageUpsert/DeleteAbsent but,
// instead of blindly overwriting, applies an incoming change only when its version
// (rc_hlc_phys, rc_hlc_log, rc_node) strictly beats the value the target already
// holds — recorded in the target's version register. The incoming version rides the
// re-read (rereadVersioned appends meshVersionCols), so it is compared against the
// target register in SQL:
//
//   - StageUpsert (phase 1, upserts parent->child): apply ALIVE winners — upsert the
//     value into the user table and the (hlc, node) into the register.
//   - DeleteAbsent (phase 2, deletes child->parent): apply TOMBSTONE winners — delete
//     the user row and record the tombstone (deleted=true) in the register.
//
// Both phases advance the target HLC past the max version seen (hlc_observe), so a
// subsequent local write is ordered after everything applied — the HLC receive step
// that makes LWW skew-tolerant. Deletes are explicit tombstone rows (not "absent from
// staging"), so DeleteAbsent ignores the passed dirtyKeys here.

// stageUpsertCluster is the cluster (multi-master) StageUpsert: it stages the
// versioned re-read and applies the alive winners under HLC-LWW.
func (tx *pgApplyTx) stageUpsertCluster(ctx context.Context, t engine.TableRef, cols []string, reread io.Reader) error {
	conn := tx.sink.conn
	table, err := tx.sink.tableMeta(ctx, t)
	if err != nil {
		return err
	}
	keyCols := captureColsFor(table)
	if len(keyCols) == 0 {
		return fmt.Errorf("postgres: cluster apply: target %s has no usable key", t)
	}
	if err := tx.ensureTargetMesh(ctx, t, keyCols); err != nil {
		return err
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
	// Staging = the user transport columns plus the four appended version columns
	// (matching rereadVersioned's output order).
	stgCols := make([]string, 0, len(cols)+4)
	for _, c := range cols {
		stgCols = append(stgCols, quoteIdentifier(c)+" "+typeByName[c])
	}
	stgCols = append(stgCols, "rc_hlc_phys bigint", "rc_hlc_log int", "rc_node text", "rc_deleted boolean")
	if _, err := conn.Exec(ctx, fmt.Sprintf("CREATE TEMP TABLE %s (%s) ON COMMIT DROP",
		quoteIdentifier(stg), strings.Join(stgCols, ", "))); err != nil {
		return fmt.Errorf("postgres: cluster apply: create staging for %s: %w", t, err)
	}
	copyCols := append(append([]string{}, cols...), meshVersionCols...)
	copySQL := fmt.Sprintf("COPY %s (%s) FROM STDIN", quoteIdentifier(stg), quotedColumnList(copyCols))
	if _, err := conn.PgConn().CopyFrom(ctx, reread, copySQL); err != nil {
		return fmt.Errorf("postgres: cluster apply: stage re-read for %s: %w", t, err)
	}

	// Alive winners -> user table, then the register (both filtered against the
	// pre-statement register, so they agree on the same winner set).
	if _, err := conn.Exec(ctx, clusterAliveUpsertSQL(t, stg, cols, keyCols, identity)); err != nil {
		return fmt.Errorf("postgres: cluster apply: upsert %s: %w", t, classifyFKViolation(err))
	}
	if _, err := conn.Exec(ctx, clusterRegisterUpsertSQL(t, stg, keyCols, false)); err != nil {
		return fmt.Errorf("postgres: cluster apply: register %s: %w", t, err)
	}
	if _, err := conn.Exec(ctx, clusterObserveSQL(stg)); err != nil {
		return fmt.Errorf("postgres: cluster apply: advance clock %s: %w", t, err)
	}
	tx.staging[t] = stagingInfo{name: stg, keyCols: keyCols}
	return nil
}

// deleteTombstonesCluster is the cluster DeleteAbsent: it applies the tombstone
// winners staged by stageUpsertCluster — version-guarded deletes of the user row plus
// a tombstone in the register. It reuses the same staging (re-created by the delete
// phase's StageUpsert), so the passed dirtyKeys are unused (deletes are explicit
// tombstone rows, not "absent" keys).
func (tx *pgApplyTx) deleteTombstonesCluster(ctx context.Context, t engine.TableRef) error {
	info, ok := tx.staging[t]
	if !ok {
		return fmt.Errorf("postgres: cluster apply: DeleteAbsent before StageUpsert for %s", t)
	}
	conn := tx.sink.conn
	if _, err := conn.Exec(ctx, clusterTombstoneDeleteSQL(t, info.name, info.keyCols)); err != nil {
		return fmt.Errorf("postgres: cluster apply: delete %s: %w", t, classifyFKViolation(err))
	}
	if _, err := conn.Exec(ctx, clusterRegisterUpsertSQL(t, info.name, info.keyCols, true)); err != nil {
		return fmt.Errorf("postgres: cluster apply: tombstone register %s: %w", t, err)
	}
	if _, err := conn.Exec(ctx, clusterObserveSQL(info.name)); err != nil {
		return fmt.Errorf("postgres: cluster apply: advance clock %s: %w", t, err)
	}
	return nil
}

// ensureTargetMesh makes sure the target has the mesh objects version-guarded apply
// needs — the HLC state (keyed to this member's node id) and the table's version
// register — creating them idempotently. A member's own capture install normally
// creates them, but an apply edge may reach a table first; the create is IF NOT EXISTS,
// so whoever runs first wins. If the `replicare` schema is not present yet (the peer's
// own capture install has not run), the failure is classified transient so the drain
// retries rather than halting.
func (tx *pgApplyTx) ensureTargetMesh(ctx context.Context, t engine.TableRef, keyCols []captureCol) error {
	if tx.sink.meshReady[t] || (tx.ensured != nil && tx.ensured[t]) {
		return nil
	}
	stmts := []string{hlcStateDDL, hlcTickFnDDL, hlcObserveFnDDL, hlcSeedDDL(tx.sink.nodeID), registerTableDDL(t, keyCols)}
	for _, stmt := range stmts {
		if _, err := tx.sink.conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("postgres: cluster apply: ensure target mesh for %s: %w", t, classifyMeshNotReady(err))
		}
	}
	if tx.ensured == nil {
		tx.ensured = make(map[engine.TableRef]bool)
	}
	tx.ensured[t] = true
	return nil
}

// classifyMeshNotReady marks a "schema/relation not there yet" failure transient so
// the drain retries once the peer member's capture install has created the replicare
// schema — the startup ordering window between reciprocal cluster edges. Every other
// error halts loud.
func classifyMeshNotReady(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && (pgErr.Code == "3F000" || pgErr.Code == "42P01") {
		return &engine.TransientConstraintError{Err: err}
	}
	return err
}

// meshVersionGreater is the SQL predicate "staged version strictly beats the target
// register version" — a row-wise (phys, log, node) comparison, with a NULL register
// (never seen) always losing. node breaks equal-HLC ties so the order is total.
func meshVersionGreater(sAlias, rAlias string) string {
	return fmt.Sprintf(
		"(%[2]s.rc_hlc_phys IS NULL OR (%[1]s.rc_hlc_phys, %[1]s.rc_hlc_log, %[1]s.rc_node) > (%[2]s.rc_hlc_phys, %[2]s.rc_hlc_log, %[2]s.rc_node))",
		sAlias, rAlias)
}

// meshKeyJoin builds the join/equality conditions between two aliases on the key cols.
func meshKeyJoin(aAlias, bAlias string, keyCols []captureCol) string {
	conds := make([]string, len(keyCols))
	for i, k := range keyCols {
		q := quoteIdentifier(k.Name)
		conds[i] = fmt.Sprintf("%s.%s = %s.%s", aAlias, q, bAlias, q)
	}
	return strings.Join(conds, " AND ")
}

// clusterAliveUpsertSQL upserts the alive winners from staging into the user table:
// staged non-deleted rows whose version beats the target register. Non-key columns are
// updated on conflict; identity columns carry the source value (OVERRIDING SYSTEM
// VALUE).
func clusterAliveUpsertSQL(t engine.TableRef, stg string, cols []string, keyCols []captureCol, identity bool) string {
	reg := qualifiedCapture(registerTableName(t))
	overriding := ""
	if identity {
		overriding = " OVERRIDING SYSTEM VALUE"
	}
	sel := make([]string, len(cols))
	for i, c := range cols {
		sel[i] = "s." + quoteIdentifier(c)
	}
	pkNames := make([]string, len(keyCols))
	pkSet := colSetOf(keyCols)
	for i, k := range keyCols {
		pkNames[i] = quoteIdentifier(k.Name)
	}
	var setParts []string
	for _, c := range cols {
		if !pkSet[c] {
			setParts = append(setParts, fmt.Sprintf("%s = EXCLUDED.%s", quoteIdentifier(c), quoteIdentifier(c)))
		}
	}
	action := "DO NOTHING"
	if len(setParts) > 0 {
		action = "DO UPDATE SET " + strings.Join(setParts, ", ")
	}
	return fmt.Sprintf(
		"INSERT INTO %s (%s)%s SELECT %s FROM %s s LEFT JOIN %s r ON %s "+
			"WHERE s.rc_deleted = false AND %s ON CONFLICT (%s) %s",
		qualifyTable(t), quotedColumnList(cols), overriding, strings.Join(sel, ", "),
		quoteIdentifier(stg), reg, meshKeyJoin("s", "r", keyCols),
		meshVersionGreater("s", "r"), strings.Join(pkNames, ", "), action)
}

// clusterRegisterUpsertSQL upserts the winners' versions into the target register.
// deleted selects the phase: false for the alive upsert (records the live version),
// true for the tombstone delete (records the delete's version so a later older upsert
// loses). It filters against the pre-statement register, so it agrees with the value
// statement's winner set.
func clusterRegisterUpsertSQL(t engine.TableRef, stg string, keyCols []captureCol, deleted bool) string {
	reg := qualifiedCapture(registerTableName(t))
	keyNames := make([]string, len(keyCols))
	selKeys := make([]string, len(keyCols))
	for i, k := range keyCols {
		q := quoteIdentifier(k.Name)
		keyNames[i] = q
		selKeys[i] = "s." + q
	}
	return fmt.Sprintf(
		"INSERT INTO %s (%s, rc_hlc_phys, rc_hlc_log, rc_node, rc_deleted) "+
			"SELECT %s, s.rc_hlc_phys, s.rc_hlc_log, s.rc_node, %t FROM %s s LEFT JOIN %s r ON %s "+
			"WHERE s.rc_deleted = %t AND %s "+
			"ON CONFLICT (%s) DO UPDATE SET rc_hlc_phys = EXCLUDED.rc_hlc_phys, rc_hlc_log = EXCLUDED.rc_hlc_log, "+
			"rc_node = EXCLUDED.rc_node, rc_deleted = EXCLUDED.rc_deleted, rc_updated_at = now()",
		reg, strings.Join(keyNames, ", "), strings.Join(selKeys, ", "), deleted,
		quoteIdentifier(stg), reg, meshKeyJoin("s", "r", keyCols),
		deleted, meshVersionGreater("s", "r"), strings.Join(keyNames, ", "))
}

// clusterTombstoneDeleteSQL deletes user rows for the tombstone winners: staged
// deleted rows whose version beats the target register.
func clusterTombstoneDeleteSQL(t engine.TableRef, stg string, keyCols []captureCol) string {
	reg := qualifiedCapture(registerTableName(t))
	return fmt.Sprintf(
		"DELETE FROM %s u USING %s s LEFT JOIN %s r ON %s "+
			"WHERE s.rc_deleted = true AND %s AND %s",
		qualifyTable(t), quoteIdentifier(stg), reg, meshKeyJoin("s", "r", keyCols),
		meshVersionGreater("s", "r"), meshKeyJoin("u", "s", keyCols))
}

// clusterObserveSQL advances the target HLC past the max version in the staging (the
// HLC receive step), so a subsequent local write is ordered after everything applied.
// A no-op when the staging is empty (the SELECT returns no row).
func clusterObserveSQL(stg string) string {
	return fmt.Sprintf(
		"SELECT replicare.hlc_observe(rc_hlc_phys, rc_hlc_log) FROM %s ORDER BY rc_hlc_phys DESC, rc_hlc_log DESC LIMIT 1",
		quoteIdentifier(stg))
}
