package mysql

import (
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/rudimk/replicare/internal/engine"
)

// Multi-master mesh machinery for MySQL (CLAUDE.md §6, docs/multi-master.md §5.3),
// mirroring internal/engine/postgres/mesh.go. It lives in the source `replicare`
// DATABASE alongside the delta/track tables and is created ONLY for cluster members
// (InstallOriginCapture) — a one-way source gets none of it, so the one-way path is
// byte-identical.
//
// Two MySQL-specific choices differ from Postgres:
//   - The HLC is a table + INLINE SQL (a tick in the trigger body, an observe in the
//     apply), NOT a stored function: creating a data-modifying MySQL function needs
//     SUPER/CREATE ROUTINE under the managed-provider default (log_bin_trust_function
//     _creators=0), which violates the no-superuser mandate. Triggers already run as
//     DEFINER=CURRENT_USER, so inline UPDATE+SELECT..INTO in the trigger body needs no
//     routine privilege.
//   - Capture is three inline trigger bodies (no trigger function), so the mesh
//     trigger interleaves the HLC tick + register stamp into each op's body.

// registerTableName is the per-table version-register name — a hash of the fully
// qualified name so every cluster member computes the same name (unlike the per-DB
// rel_id). Matches the Postgres scheme so the two engines stay parallel.
func registerTableName(ref engine.TableRef) string {
	sum := sha1.Sum([]byte(ref.Schema + "\x00" + ref.Name))
	return "reg_" + hex.EncodeToString(sum[:10])
}

// hlcStateDDL creates the single-row hybrid-logical-clock state: one physical/logical
// pair per database, shared by this member's capture triggers (tick on every local
// write) and the daemon's apply (observe past every incoming version). node_id is this
// member's replication-origin identity, stamped into every locally-originated version.
const hlcStateDDL = "CREATE TABLE IF NOT EXISTS " + captureDB_hlc + ` (
	id       TINYINT NOT NULL PRIMARY KEY,
	physical BIGINT  NOT NULL DEFAULT 0,
	logical  INT     NOT NULL DEFAULT 0,
	node_id  VARCHAR(255) NOT NULL
) ENGINE=InnoDB`

// captureDB_hlc is the qualified hlc_state name; a const so the DDL can be a const.
const captureDB_hlc = "`" + captureDB + "`.`hlc_state`"

// hlcSeedSQL seeds the singleton hlc_state row with this node's id if absent (INSERT
// IGNORE = no-op when present), so node_id is set once and stays stable. Both a
// member's own capture install and a peer's apply-side ensure run it with this DB's
// OWN node id, so they never disagree.
func hlcSeedSQL(nodeID string) string {
	return fmt.Sprintf("INSERT IGNORE INTO %s (id, physical, logical, node_id) VALUES (1, 0, 0, %s)",
		captureDB_hlc, sqlStringLiteral(nodeID))
}

// nowMillis is the wall-clock-ms expression (millisecond precision). NOW(3) is
// constant within a single statement, so every use in one statement agrees.
const nowMillis = "FLOOR(UNIX_TIMESTAMP(NOW(3)) * 1000)"

// hlcTickBody is the inline HLC send step for a trigger body: advance the clock and
// read the new (physical, logical, node_id) into the trigger locals. The `logical`
// assignment is written FIRST so it reads the OLD physical (MySQL single-table UPDATE
// evaluates assignments left-to-right, using updated values only for columns set
// earlier — physical is set second, so both reads are of the old physical).
var hlcTickBody = fmt.Sprintf(
	"SET rc_ts = %[1]s;\n"+
		"\t\tUPDATE %[2]s SET logical = IF(rc_ts > physical, 0, logical + 1), physical = IF(rc_ts > physical, rc_ts, physical) WHERE id = 1;\n"+
		"\t\tSELECT physical, logical, node_id INTO rc_hp, rc_hl, rc_nid FROM %[2]s WHERE id = 1;",
	nowMillis, captureDB_hlc)

// hlcObserveSQL is the inline HLC receive step for the apply: advance the clock past
// an incoming (phys, log) so a subsequent local write is ordered after it. Single-
// table UPDATE, so `logical` (first) reads the old physical. phys/log are integers
// read from the staging, inlined as literals (no injection risk).
func hlcObserveSQL(phys int64, log int) string {
	g := fmt.Sprintf("GREATEST(%s, physical, %d)", nowMillis, phys)
	return fmt.Sprintf(
		"UPDATE %s SET logical = CASE "+
			"WHEN %s = physical AND physical = %d THEN GREATEST(logical, %d) + 1 "+
			"WHEN %s = physical THEN logical + 1 "+
			"WHEN %s = %d THEN %d + 1 "+
			"ELSE 0 END, physical = %s WHERE id = 1",
		captureDB_hlc, g, phys, log, g, g, phys, log, g)
}

// registerTableDDL builds a table's version register: its primary-key columns (real
// names + MySQL types, so it joins the user table naturally) plus the current value's
// version (rc_hlc_phys, rc_hlc_log, rc_node) and a tombstone flag rc_deleted.
func registerTableDDL(ref engine.TableRef, pk []captureCol) string {
	var b strings.Builder
	fmt.Fprintf(&b, "CREATE TABLE IF NOT EXISTS %s (\n", captureRef(registerTableName(ref)))
	for _, c := range pk {
		fmt.Fprintf(&b, "\t%s %s NOT NULL,\n", bq(c.Name), c.Type)
	}
	b.WriteString("\trc_hlc_phys BIGINT NOT NULL,\n")
	b.WriteString("\trc_hlc_log  INT NOT NULL,\n")
	b.WriteString("\trc_node     VARCHAR(255) NOT NULL,\n")
	b.WriteString("\trc_deleted  TINYINT(1) NOT NULL DEFAULT 0,\n")
	b.WriteString("\trc_updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),\n")
	keyNames := make([]string, len(pk))
	for i, c := range pk {
		keyNames[i] = bq(c.Name)
	}
	fmt.Fprintf(&b, "\tPRIMARY KEY (%s)\n", strings.Join(keyNames, ", "))
	b.WriteString(") ENGINE=InnoDB")
	return b.String()
}

// registerStampSQL builds one register upsert for the trigger body: set (pk) ->
// (hlc, node, deleted) from the NEW/OLD row's key columns and the trigger-local HLC
// vars. A local write always wins its own node's register (its fresh tick exceeds any
// stored version), so this is an unconditional upsert.
func registerStampSQL(ref engine.TableRef, pk []captureCol, rowVar string, deleted bool) string {
	reg := captureRef(registerTableName(ref))
	keyCols := make([]string, len(pk))
	keyVals := make([]string, len(pk))
	for i, c := range pk {
		q := bq(c.Name)
		keyCols[i] = q
		keyVals[i] = rowVar + "." + q
	}
	del := "0"
	if deleted {
		del = "1"
	}
	return fmt.Sprintf(
		"INSERT INTO %s (%s, rc_hlc_phys, rc_hlc_log, rc_node, rc_deleted) VALUES (%s, rc_hp, rc_hl, rc_nid, %s) "+
			"ON DUPLICATE KEY UPDATE rc_hlc_phys = VALUES(rc_hlc_phys), rc_hlc_log = VALUES(rc_hlc_log), "+
			"rc_node = VALUES(rc_node), rc_deleted = VALUES(rc_deleted), rc_updated_at = NOW(6);",
		reg, strings.Join(keyCols, ", "), strings.Join(keyVals, ", "), del)
}

// meshTriggerDDL builds one AFTER trigger for a CLUSTER member: like triggerDDL it
// records the changed PK into the delta table, but it (1) wraps the whole body in
// `IF @replicare_apply IS NULL THEN … END IF` so replicare's own applies are not
// re-captured (loop suppression — the MySQL analog of Postgres's WHEN guard, which
// MySQL lacks), and (2) stamps the version register with a fresh inline HLC tick and
// this node's id, writing a tombstone on a delete (and the old key of a PK-change).
// The one-way triggerDDL is untouched, so the one-way path is byte-identical.
func meshTriggerDDL(relID int, schema, table string, op byte, pk []captureCol) string {
	ref := engine.TableRef{Schema: schema, Name: table}
	delta := captureRef(deltaTableName(relID))
	cols := make([]string, len(pk))
	for i := range pk {
		cols[i] = bq(deltaColumns(len(pk))[i])
	}
	colList := strings.Join(cols, ", ")
	valList := func(row string) string {
		vals := make([]string, len(pk))
		for i, c := range pk {
			vals[i] = row + "." + bq(c.Name)
		}
		return strings.Join(vals, ", ")
	}
	insert := func(row, opChar string) string {
		return fmt.Sprintf("INSERT INTO %s (rc_op, %s) VALUES ('%s', %s);", delta, colList, opChar, valList(row))
	}
	stampNew := registerStampSQL(ref, pk, "NEW", false)
	stampOldDel := registerStampSQL(ref, pk, "OLD", true)

	var timingEvent, opBody string
	switch op {
	case 'I':
		timingEvent = "AFTER INSERT"
		opBody = insert("NEW", "I") + "\n\t\t" + stampNew
	case 'D':
		timingEvent = "AFTER DELETE"
		opBody = insert("OLD", "D") + "\n\t\t" + stampOldDel
	case 'U':
		timingEvent = "AFTER UPDATE"
		changed := make([]string, len(pk))
		for i, c := range pk {
			changed[i] = fmt.Sprintf("NOT (NEW.%s <=> OLD.%s)", bq(c.Name), bq(c.Name))
		}
		var sb strings.Builder
		sb.WriteString("IF " + strings.Join(changed, " OR ") + " THEN\n")
		sb.WriteString("\t\t\t" + insert("OLD", "D") + "\n")
		sb.WriteString("\t\t\t" + insert("NEW", "U") + "\n")
		sb.WriteString("\t\t\t" + stampOldDel + "\n")
		sb.WriteString("\t\t\t" + stampNew + "\n")
		sb.WriteString("\t\tELSE\n")
		sb.WriteString("\t\t\t" + insert("NEW", "U") + "\n")
		sb.WriteString("\t\t\t" + stampNew + "\n")
		sb.WriteString("\t\tEND IF;")
		opBody = sb.String()
	}

	return fmt.Sprintf(
		"CREATE DEFINER = CURRENT_USER TRIGGER %s.%s %s ON %s FOR EACH ROW\n"+
			"BEGIN\n"+
			"\tDECLARE rc_hp BIGINT; DECLARE rc_hl INT; DECLARE rc_nid VARCHAR(255); DECLARE rc_ts BIGINT;\n"+
			"\tIF @replicare_apply IS NULL THEN\n"+
			"\t\t%s\n"+
			"\t\t%s\n"+
			"\tEND IF;\n"+
			"END",
		bq(schema), bq(triggerName(relID, op)), timingEvent, qualify(schema, table), hlcTickBody, opBody)
}

// ensureMeshState creates the per-database mesh objects (hlc_state) and seeds this
// node's id, idempotently. Called by a cluster member's capture install and by a peer
// sink before its first apply, so the objects exist regardless of edge start order.
func ensureMeshState(ctx context.Context, ex interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}, nodeID string) error {
	if nodeID == "" {
		return fmt.Errorf("mysql: mesh: empty node_id")
	}
	for _, stmt := range []string{hlcStateDDL, hlcSeedSQL(nodeID)} {
		if _, err := ex.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("mysql: ensure mesh state: %w", err)
		}
	}
	return nil
}

// sqlStringLiteral single-quotes a MySQL string literal, escaping quotes and
// backslashes. Used only for fixed/config values (node ids).
func sqlStringLiteral(s string) string {
	r := strings.NewReplacer(`\`, `\\`, `'`, `\'`)
	return "'" + r.Replace(s) + "'"
}
