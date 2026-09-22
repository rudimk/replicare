package postgres

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/rudimk/replicare/internal/engine"
)

// Multi-master mesh machinery (CLAUDE.md §6, docs/multi-master.md §5.3): the HLC
// version register that makes HLC last-write-wins possible. These objects live in
// the source `replicare` schema alongside the delta/track tables, but are created
// ONLY for cluster members (InstallOriginCapture) — a one-way source never gets them,
// so the one-way path is byte-identical (the backward-compatibility invariant).
//
// Two kinds of object:
//   - Per database: a single-row `hlc_state` (the node's hybrid logical clock + its
//     node_id) and the `hlc_tick()` / `hlc_observe()` functions.
//   - Per replicated table: a `reg_<hash>` version register holding, per primary key,
//     the (hlc, node) of the value the node currently holds — or a tombstone
//     (rc_deleted) for a deleted key, so a delete competes with a concurrent update
//     under the same total order.
//
// The register is named by a STABLE hash of schema.table (not the per-node rel_id),
// so every member computes the same register name for the same table — the apply on a
// target references the same register the source's trigger stamps.

// registerTableName is the version-register table name for a replicated table. It is
// a hash of the fully-qualified name so it is identical on every cluster member
// (unlike rel_id, which is assigned per database) and short/collision-free regardless
// of the source table's name.
func registerTableName(ref engine.TableRef) string {
	sum := sha1.Sum([]byte(ref.Schema + "\x00" + ref.Name))
	return "reg_" + hex.EncodeToString(sum[:10])
}

// hlcStateDDL creates the single-row hybrid-logical-clock state (CLAUDE.md §5.3): one
// physical/logical pair per database, shared by this node's capture triggers (which
// tick it on every local write) and the daemon's apply (which advances it past every
// incoming version). node_id is this member's replication-origin identity, stamped
// into every locally-originated version. The CHECK+fixed id keep it a singleton.
const hlcStateDDL = `CREATE TABLE IF NOT EXISTS replicare.hlc_state (
	id smallint PRIMARY KEY DEFAULT 1 CHECK (id = 1),
	physical bigint NOT NULL DEFAULT 0,
	logical  int    NOT NULL DEFAULT 0,
	node_id  text   NOT NULL
)`

// hlcSeedDDL seeds the singleton hlc_state row with this node's id, if absent. Both a
// member's own capture install and a peer's apply-side ensure run it; whichever runs
// first sets node_id, and DO NOTHING keeps it stable thereafter. Both pass this DB's
// OWN node id (a source install with its node id; a peer's sink with the target node
// id — which is this same DB's id), so they never disagree.
func hlcSeedDDL(nodeID string) string {
	return fmt.Sprintf("INSERT INTO replicare.hlc_state (id, node_id) VALUES (1, %s) ON CONFLICT (id) DO NOTHING",
		quoteLiteral(nodeID))
}

// hlcTickFnDDL advances the clock for a LOCAL change and returns the new (physical,
// logical): if wall-clock ms exceeds the stored physical it resets logical to 0, else
// it increments logical. Evaluated against the pre-update row (Postgres evaluates SET
// right-hand sides on the old tuple), so it is the standard HLC send step. Every
// captured local row bumps it, so locally-originated versions are strictly ordered.
const hlcTickFnDDL = `CREATE OR REPLACE FUNCTION replicare.hlc_tick(OUT phys bigint, OUT logi int)
LANGUAGE plpgsql AS $rc$
DECLARE pt bigint;
BEGIN
	pt := floor(extract(epoch from clock_timestamp()) * 1000)::bigint;
	UPDATE replicare.hlc_state SET
		physical = CASE WHEN pt > physical THEN pt ELSE physical END,
		logical  = CASE WHEN pt > physical THEN 0 ELSE logical + 1 END
	WHERE id = 1
	RETURNING physical, logical INTO phys, logi;
END;
$rc$`

// hlcObserveFnDDL advances the clock past an INCOMING version at apply time (the HLC
// receive step): the new physical is the max of the local physical, the incoming
// physical, and wall-clock now; logical is bumped so a subsequent local tick is
// strictly greater than anything seen. This is what makes a laggard node's next write
// beat an older remote write despite clock skew (docs/multi-master.md §9).
const hlcObserveFnDDL = `CREATE OR REPLACE FUNCTION replicare.hlc_observe(in_phys bigint, in_log int)
RETURNS void LANGUAGE plpgsql AS $rc$
DECLARE pt bigint; new_phys bigint;
BEGIN
	pt := floor(extract(epoch from clock_timestamp()) * 1000)::bigint;
	new_phys := GREATEST(pt, in_phys, (SELECT physical FROM replicare.hlc_state WHERE id = 1));
	UPDATE replicare.hlc_state SET
		physical = new_phys,
		logical = CASE
			WHEN new_phys = physical AND new_phys = in_phys THEN GREATEST(logical, in_log) + 1
			WHEN new_phys = physical THEN logical + 1
			WHEN new_phys = in_phys THEN in_log + 1
			ELSE 0
		END
	WHERE id = 1;
END;
$rc$`

// registerTableDDL builds a table's version register: its primary-key columns (real
// names + types, so it joins the user table naturally on apply) plus the current
// value's version (rc_hlc_phys, rc_hlc_log, rc_node) and a tombstone flag rc_deleted.
// A row here is the (hlc, node) of the value this node holds for that key — or, when
// rc_deleted, a tombstone recording a delete's version so it competes with a
// concurrent update under the same total order (CLAUDE.md §5.3).
func registerTableDDL(ref engine.TableRef, pk []captureCol) string {
	var b strings.Builder
	fmt.Fprintf(&b, "CREATE TABLE IF NOT EXISTS %s (\n", qualifiedCapture(registerTableName(ref)))
	for _, c := range pk {
		fmt.Fprintf(&b, "\t%s %s NOT NULL,\n", quoteIdentifier(c.Name), c.Type)
	}
	b.WriteString("\trc_hlc_phys bigint NOT NULL,\n")
	b.WriteString("\trc_hlc_log  int    NOT NULL,\n")
	b.WriteString("\trc_node     text   NOT NULL,\n")
	b.WriteString("\trc_deleted  boolean NOT NULL DEFAULT false,\n")
	b.WriteString("\trc_updated_at timestamptz NOT NULL DEFAULT now(),\n")
	fmt.Fprintf(&b, "\tPRIMARY KEY (%s)\n", quotedKeyList(pk))
	b.WriteString(")")
	return b.String()
}

// meshTriggerFunctionDDL builds the cluster-member capture trigger function: like the
// one-way function it records the changed PK into the delta table (PK-only), but it
// ALSO stamps the version register with a fresh HLC tick and this node's id — so every
// locally-originated change carries an ordered (hlc, node) for HLC-LWW. A delete (and
// the old key of a PK-change) writes a tombstone (rc_deleted). Like the one-way
// function it is SECURITY DEFINER with a pinned search_path and fully-qualified
// references. It is installed only for cluster tables, so the one-way function is
// unchanged.
func meshTriggerFunctionDDL(relID int, ref engine.TableRef, pk []captureCol) string {
	delta := qualifiedCapture(deltaTableName(relID))
	cols := strings.Join(deltaColumns(len(pk)), ", ")

	newVals := make([]string, len(pk))
	oldVals := make([]string, len(pk))
	distinct := make([]string, len(pk))
	for i, c := range pk {
		q := quoteIdentifier(c.Name)
		newVals[i] = "NEW." + q
		oldVals[i] = "OLD." + q
		distinct[i] = fmt.Sprintf("NEW.%s IS DISTINCT FROM OLD.%s", q, q)
	}
	insertNew := fmt.Sprintf("INSERT INTO %s (rc_op, %s) VALUES ('%%s', %s);", delta, cols, strings.Join(newVals, ", "))
	insertOld := fmt.Sprintf("INSERT INTO %s (rc_op, %s) VALUES ('D', %s);", delta, cols, strings.Join(oldVals, ", "))
	stampNew := registerStampSQL(ref, pk, "NEW", "h_phys", "h_log", "nid", false)
	stampOldDeleted := registerStampSQL(ref, pk, "OLD", "h_phys", "h_log", "nid", true)

	var b strings.Builder
	fmt.Fprintf(&b, "CREATE OR REPLACE FUNCTION %s() RETURNS trigger\n", qualifiedCapture(triggerFnName(relID)))
	b.WriteString("LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $rc$\n")
	b.WriteString("DECLARE h_phys bigint; h_log int; nid text;\n")
	b.WriteString("BEGIN\n")
	b.WriteString("\tSELECT phys, logi INTO h_phys, h_log FROM replicare.hlc_tick();\n")
	b.WriteString("\tSELECT node_id INTO nid FROM replicare.hlc_state WHERE id = 1;\n")
	b.WriteString("\tIF TG_OP = 'INSERT' THEN\n")
	fmt.Fprintf(&b, "\t\t%s\n", fmt.Sprintf(insertNew, "I"))
	fmt.Fprintf(&b, "\t\t%s\n", stampNew)
	b.WriteString("\tELSIF TG_OP = 'UPDATE' THEN\n")
	fmt.Fprintf(&b, "\t\tIF %s THEN\n", strings.Join(distinct, " OR "))
	fmt.Fprintf(&b, "\t\t\t%s\n", insertOld)
	fmt.Fprintf(&b, "\t\t\t%s\n", fmt.Sprintf(insertNew, "U"))
	fmt.Fprintf(&b, "\t\t\t%s\n", stampOldDeleted)
	fmt.Fprintf(&b, "\t\t\t%s\n", stampNew)
	b.WriteString("\t\tELSE\n")
	fmt.Fprintf(&b, "\t\t\t%s\n", fmt.Sprintf(insertNew, "U"))
	fmt.Fprintf(&b, "\t\t\t%s\n", stampNew)
	b.WriteString("\t\tEND IF;\n")
	b.WriteString("\tELSIF TG_OP = 'DELETE' THEN\n")
	fmt.Fprintf(&b, "\t\t%s\n", insertOld)
	fmt.Fprintf(&b, "\t\t%s\n", stampOldDeleted)
	b.WriteString("\tEND IF;\n")
	b.WriteString("\tRETURN NULL;\n")
	b.WriteString("END;\n")
	b.WriteString("$rc$")
	return b.String()
}

// registerStampSQL builds one register upsert for the trigger: set (pk) -> (hlc, node,
// deleted) using the NEW/OLD row's key columns and the trigger-local HLC/node
// variables. A local write always wins its own node's register (its fresh tick
// exceeds any stored version), so this is an unconditional upsert.
func registerStampSQL(ref engine.TableRef, pk []captureCol, rowVar, physVar, logVar, nodeVar string, deleted bool) string {
	reg := qualifiedCapture(registerTableName(ref))
	keyCols := make([]string, len(pk))
	keyVals := make([]string, len(pk))
	for i, c := range pk {
		q := quoteIdentifier(c.Name)
		keyCols[i] = q
		keyVals[i] = rowVar + "." + q
	}
	return fmt.Sprintf(
		"INSERT INTO %s (%s, rc_hlc_phys, rc_hlc_log, rc_node, rc_deleted) VALUES (%s, %s, %s, %s, %t) "+
			"ON CONFLICT (%s) DO UPDATE SET rc_hlc_phys = EXCLUDED.rc_hlc_phys, rc_hlc_log = EXCLUDED.rc_hlc_log, "+
			"rc_node = EXCLUDED.rc_node, rc_deleted = EXCLUDED.rc_deleted, rc_updated_at = now();",
		reg, strings.Join(keyCols, ", "), strings.Join(keyVals, ", "),
		physVar, logVar, nodeVar, deleted, strings.Join(keyCols, ", "))
}

// ensureMeshState creates the per-database mesh objects (hlc_state + the HLC
// functions) and seeds this node's id, idempotently. Called by a cluster member's
// capture install and by a peer sink before its first apply, so the objects exist
// regardless of the order cluster edges start in.
func ensureMeshState(ctx context.Context, conn *pgx.Conn, nodeID string) error {
	if nodeID == "" {
		return fmt.Errorf("postgres: mesh: empty node_id")
	}
	for _, stmt := range []string{hlcStateDDL, hlcTickFnDDL, hlcObserveFnDDL, hlcSeedDDL(nodeID)} {
		if _, err := conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("postgres: ensure mesh state: %w", err)
		}
	}
	return nil
}
