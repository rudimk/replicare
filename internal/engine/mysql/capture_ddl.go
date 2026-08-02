package mysql

import (
	"fmt"
	"strings"
)

// Capture DDL generators (CLAUDE.md §3.1). Pure string builders, unit-tested for
// shape; InstallCapture executes them. MySQL differs from Postgres in three ways
// that shape this file:
//   - the capture "schema" is a MySQL DATABASE (`replicare`);
//   - there are no trigger functions — logic is inline in the trigger body, and
//     MySQL (< 8.0) allows only ONE trigger per (timing, event), so capture uses
//     THREE triggers per table (AFTER INSERT / UPDATE / DELETE);
//   - delta_id is BIGINT AUTO_INCREMENT (no sequences), serving as both the
//     delete-by-id handle (§3.3) and the ordering hint.
// Triggers are created with DEFINER = CURRENT_USER so they run as the daemon role
// (which has INSERT on `replicare`); application writers fire them without needing
// any grant on `replicare` (mysql-plan §MM3 / Momus m2).

const captureDB = "replicare"

// captureCol is a source key column: its name and its MySQL COLUMN_TYPE, used to
// type the matching delta column so the key value stores faithfully.
type captureCol struct {
	Name string
	Type string
}

func deltaColumns(n int) []string {
	out := make([]string, n)
	for i := 0; i < n; i++ {
		out[i] = fmt.Sprintf("k%d", i+1)
	}
	return out
}

func deltaTableName(relID int) string { return fmt.Sprintf("delta_%d", relID) }
func trackTableName(relID int) string { return fmt.Sprintf("track_%d", relID) }

// triggerName returns the trigger name for one op ('I'/'U'/'D'). MySQL triggers
// live in the table's own database (not `replicare`), so names are scoped by op
// and rel_id to stay unique and short (<64 chars).
func triggerName(relID int, op byte) string {
	return fmt.Sprintf("rc_trg_%c_%d", op|0x20, relID) // lowercase op letter
}

// bq back-quotes a MySQL identifier, doubling embedded back-quotes.
func bq(s string) string { return "`" + strings.ReplaceAll(s, "`", "``") + "`" }

// captureRef qualifies a capture object in the `replicare` database.
func captureRef(object string) string { return bq(captureDB) + "." + bq(object) }

// qualify qualifies a source table `db`.`table`.
func qualify(schema, name string) string { return bq(schema) + "." + bq(name) }

// deltaTableDDL builds the per-table delta table: one row per captured change
// carrying only the key (positional k1..kn typed to the source), the op, the
// delete-by-id handle (delta_id AUTO_INCREMENT), and a timing hint. Key columns
// are nullable so a nullable unique-key column never breaks the trigger.
func deltaTableDDL(relID int, pk []captureCol) string {
	var b strings.Builder
	fmt.Fprintf(&b, "CREATE TABLE IF NOT EXISTS %s (\n", captureRef(deltaTableName(relID)))
	b.WriteString("\tdelta_id BIGINT AUTO_INCREMENT PRIMARY KEY,\n")
	b.WriteString("\trc_op CHAR(1) NOT NULL,\n")
	for i, c := range pk {
		fmt.Fprintf(&b, "\t%s %s NULL,\n", bq(deltaColumns(len(pk))[i]), c.Type)
	}
	b.WriteString("\trc_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)\n")
	b.WriteString(") ENGINE=InnoDB")
	return b.String()
}

// trackTableDDL builds the per-captured-table track table, keyed by (target,
// delta_id): a row means that (target, delta_id) has been consumed (§3.3).
func trackTableDDL(relID int) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
	target      VARCHAR(255) NOT NULL,
	delta_id    BIGINT NOT NULL,
	consumed_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
	PRIMARY KEY (target, delta_id)
) ENGINE=InnoDB`, captureRef(trackTableName(relID)))
}

// triggerDDL builds one AFTER trigger for the given op. INSERT enqueues NEW as
// 'I'; DELETE enqueues OLD as 'D'; UPDATE enqueues NEW as 'U', and if the key
// changed also OLD as 'D' first (PK change = delete(old) + upsert(new), §3.1),
// detected with the NULL-safe <=> operator.
func triggerDDL(relID int, schema, table string, op byte, pk []captureCol) string {
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

	var body string
	timingEvent := ""
	switch op {
	case 'I':
		timingEvent = "AFTER INSERT"
		body = insert("NEW", "I")
	case 'D':
		timingEvent = "AFTER DELETE"
		body = insert("OLD", "D")
	case 'U':
		timingEvent = "AFTER UPDATE"
		changed := make([]string, len(pk))
		for i, c := range pk {
			changed[i] = fmt.Sprintf("NOT (NEW.%s <=> OLD.%s)", bq(c.Name), bq(c.Name))
		}
		var sb strings.Builder
		sb.WriteString("IF " + strings.Join(changed, " OR ") + " THEN\n")
		sb.WriteString("\t\t" + insert("OLD", "D") + "\n")
		sb.WriteString("\t\t" + insert("NEW", "U") + "\n")
		sb.WriteString("\tELSE\n")
		sb.WriteString("\t\t" + insert("NEW", "U") + "\n")
		sb.WriteString("\tEND IF;")
		body = sb.String()
	}

	// The trigger name is schema-qualified so MySQL creates it in the table's
	// schema (not the connection's default database) — an unqualified name
	// triggers "Error 1435: Trigger in wrong schema".
	return fmt.Sprintf("CREATE DEFINER = CURRENT_USER TRIGGER %s.%s %s ON %s FOR EACH ROW\nBEGIN\n\t%s\nEND",
		bq(schema), bq(triggerName(relID, op)), timingEvent, qualify(schema, table), body)
}

// dropTriggerDDL detaches one capture trigger (IF EXISTS, so uninstall/reinstall
// are idempotent even though MySQL auto-commits DDL). A MySQL trigger lives in
// the schema of the table it is attached to.
func dropTriggerDDL(relID int, schema string, op byte) string {
	return fmt.Sprintf("DROP TRIGGER IF EXISTS %s.%s", bq(schema), bq(triggerName(relID, op)))
}
