package postgres

import (
	"fmt"
	"strings"
)

// Capture DDL generators (CLAUDE.md §3.1, §3.4). These are pure string builders,
// unit-tested for shape; InstallCapture executes them. All generated SQL is
// conservative and old-PG-safe (down to the 9.6 floor): triggers use
// EXECUTE PROCEDURE (not the PG11+ EXECUTE FUNCTION), and the trigger function
// is SECURITY DEFINER with a pinned search_path and fully-qualified object
// references so application writes enqueue deltas without the writing role
// needing any grant on the replicare schema.

// captureCol is a source primary/unique-key column: its name and its type (as
// reported by format_type), used to type the matching delta column.
type captureCol struct {
	Name string
	Type string
}

// deltaColumns returns the positional delta key column names k1..kn.
func deltaColumns(n int) []string {
	out := make([]string, n)
	for i := 0; i < n; i++ {
		out[i] = fmt.Sprintf("k%d", i+1)
	}
	return out
}

// Capture object names are derived deterministically from a table's rel_id so
// they stay short (well under the 63-char identifier limit) and collision-free
// regardless of the source table's name.

func deltaTableName(relID int) string { return fmt.Sprintf("delta_%d", relID) }
func trackTableName(relID int) string { return fmt.Sprintf("track_%d", relID) }
func triggerFnName(relID int) string  { return fmt.Sprintf("tf_%d", relID) }
func triggerName(relID int) string    { return fmt.Sprintf("replicare_trg_%d", relID) }

// qualifiedCapture returns a schema-qualified, quoted capture object name.
func qualifiedCapture(object string) string {
	return quoteIdentifier(captureSchema) + "." + quoteIdentifier(object)
}

// quoteIdentifier double-quotes a SQL identifier, doubling embedded quotes.
func quoteIdentifier(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// deltaTableDDL builds the per-table delta table (CLAUDE.md §3.1): one row per
// captured change carrying only the primary key (positional k1..kn typed to
// match the source), the op, and ordering/timing hints. delta_id is the unique
// id used for delete-by-id consumption (§3.3); rc_seq/rc_txid/rc_at are lag and
// ordering hints only, never the resume cursor. Key columns are nullable so a
// nullable unique-key column never breaks the trigger.
func deltaTableDDL(relID int, pk []captureCol) string {
	var b strings.Builder
	fmt.Fprintf(&b, "CREATE TABLE %s (\n", qualifiedCapture(deltaTableName(relID)))
	b.WriteString("\tdelta_id bigserial PRIMARY KEY,\n")
	b.WriteString("\trc_op char(1) NOT NULL,\n")
	for i, c := range pk {
		fmt.Fprintf(&b, "\tk%d %s,\n", i+1, c.Type)
	}
	b.WriteString("\trc_txid bigint NOT NULL DEFAULT txid_current(),\n")
	fmt.Fprintf(&b, "\trc_seq bigint NOT NULL DEFAULT nextval('%s.delta_seq'),\n", captureSchema)
	b.WriteString("\trc_at timestamptz NOT NULL DEFAULT clock_timestamp()\n")
	b.WriteString(")")
	return b.String()
}

// trackTableDDL builds the per-captured-table track table (CLAUDE.md §3.3, §3.4).
// Consumption state is per (target, table): one track table per captured table
// (isolating its bloat alongside the delta table), keyed by target so all
// targets share it. A row here means that (target, delta_id) has been consumed.
func trackTableDDL(relID int) string {
	return fmt.Sprintf(`CREATE TABLE %s (
	target text NOT NULL,
	delta_id bigint NOT NULL,
	consumed_at timestamptz NOT NULL DEFAULT now(),
	PRIMARY KEY (target, delta_id)
)`, qualifiedCapture(trackTableName(relID)))
}

// deltaAutovacuumDDL sets aggressive per-table autovacuum on the delta table so
// dead tuples from purge are reclaimed promptly, without touching the source's
// global settings (CLAUDE.md §3.4). Owner-level ALTER TABLE, no superuser.
func deltaAutovacuumDDL(relID int) string {
	return fmt.Sprintf(`ALTER TABLE %s SET (
	autovacuum_vacuum_scale_factor = 0,
	autovacuum_vacuum_threshold = 1000,
	autovacuum_vacuum_cost_delay = 0
)`, qualifiedCapture(deltaTableName(relID)))
}

// triggerFunctionDDL builds the SECURITY DEFINER PL/pgSQL trigger function
// (CLAUDE.md §3.1). It records only the changed row's primary key. A PK-changing
// UPDATE enqueues BOTH the old key (as a delete) and the new key (as an upsert)
// — PK change = delete(old) + upsert(new). It is an AFTER trigger and returns
// NULL. search_path is pinned and every object reference is fully qualified, so
// the function is safe to run as its definer regardless of the caller's session.
func triggerFunctionDDL(relID int, pk []captureCol) string {
	delta := qualifiedCapture(deltaTableName(relID))
	cols := strings.Join(deltaColumns(len(pk)), ", ")

	// NEW/OLD value lists for the source PK columns, quoted.
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

	var b strings.Builder
	fmt.Fprintf(&b, "CREATE OR REPLACE FUNCTION %s() RETURNS trigger\n", qualifiedCapture(triggerFnName(relID)))
	b.WriteString("LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $rc$\n")
	b.WriteString("BEGIN\n")
	b.WriteString("\tIF TG_OP = 'INSERT' THEN\n")
	fmt.Fprintf(&b, "\t\t%s\n", fmt.Sprintf(insertNew, "I"))
	b.WriteString("\tELSIF TG_OP = 'UPDATE' THEN\n")
	fmt.Fprintf(&b, "\t\tIF %s THEN\n", strings.Join(distinct, " OR "))
	fmt.Fprintf(&b, "\t\t\t%s\n", insertOld)
	fmt.Fprintf(&b, "\t\t\t%s\n", fmt.Sprintf(insertNew, "U"))
	b.WriteString("\t\tELSE\n")
	fmt.Fprintf(&b, "\t\t\t%s\n", fmt.Sprintf(insertNew, "U"))
	b.WriteString("\t\tEND IF;\n")
	b.WriteString("\tELSIF TG_OP = 'DELETE' THEN\n")
	fmt.Fprintf(&b, "\t\t%s\n", insertOld)
	b.WriteString("\tEND IF;\n")
	b.WriteString("\tRETURN NULL;\n")
	b.WriteString("END;\n")
	b.WriteString("$rc$")
	return b.String()
}

// createTriggerDDL attaches the capture trigger to a source table. It uses
// EXECUTE PROCEDURE (not the PG11+ EXECUTE FUNCTION) for old-server portability.
func createTriggerDDL(relID int, table string) string {
	return fmt.Sprintf(
		"CREATE TRIGGER %s AFTER INSERT OR UPDATE OR DELETE ON %s FOR EACH ROW EXECUTE PROCEDURE %s()",
		quoteIdentifier(triggerName(relID)), table, qualifiedCapture(triggerFnName(relID)))
}

// dropTriggerDDL detaches the capture trigger from a source table.
func dropTriggerDDL(relID int, table string) string {
	return fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON %s", quoteIdentifier(triggerName(relID)), table)
}
