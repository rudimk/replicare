package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"strings"

	"github.com/rudimk/replicare/internal/engine"
)

// txBeginner is the BeginTx surface shared by *sql.DB and a pinned *sql.Conn, so the
// merge load runs on the pool (one-way) or on a marker-carrying pinned connection
// (a cluster member's copy).
type txBeginner interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}

// mergeLoad implements the non-empty-target initial-copy path (§4.1): LOAD DATA
// into a TEMPORARY staging table, then INSERT … SELECT … ON DUPLICATE KEY UPDATE
// into the target. Idempotent and privilege-light (needs CREATE TEMPORARY
// TABLES). Runs in one transaction on a pinned connection (a TEMP table is
// session-scoped). Upsert is keyed on the replication key; secondary-unique
// target tables are already blocked at pre-flight (Momus B1), so ON DUPLICATE
// KEY cannot silently hit the wrong row here.
func (s *Sink) mergeLoad(ctx context.Context, beginner txBeginner, t engine.TableRef, cols []string, r io.Reader, charset string) (int64, error) {
	tbl, err := s.tableMeta(ctx, t)
	if err != nil {
		return 0, err
	}
	key := replicationKey(tbl)
	if key == nil {
		return 0, fmt.Errorf("mysql: merge load: target %s has no usable key for ON DUPLICATE KEY", t)
	}
	keySet := map[string]bool{}
	for _, c := range key.Columns {
		keySet[c] = true
	}
	typeDDL, err := tempColumnDDL(tbl, cols)
	if err != nil {
		return 0, err
	}

	tx, err := beginner.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("mysql: merge load: begin: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	// Drop any leftover staging first: TEMP tables are session-scoped and the pool
	// is single-connection, so a multi-chunk merge copy reuses the same connection
	// and would otherwise collide on the constant staging name.
	const stg = "rc_merge_stg"
	if _, err := tx.ExecContext(ctx, "DROP TEMPORARY TABLE IF EXISTS "+bq(stg)); err != nil {
		return 0, fmt.Errorf("mysql: merge load: drop stale staging: %w", err)
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("CREATE TEMPORARY TABLE %s (%s) ENGINE=InnoDB", bq(stg), typeDDL)); err != nil {
		return 0, fmt.Errorf("mysql: merge load: create staging: %w", err)
	}
	n, err := runLoad(ctx, tx, bq(stg), cols, r, charset, s.localInfile)
	if err != nil {
		return 0, fmt.Errorf("mysql: merge load: stage: %w", err)
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
		// Key-only table: no non-key column to update; make the upsert a no-op.
		// VALUES(key) avoids the `id = id` ambiguity INSERT ... SELECT ... ODKU
		// raises (target and SELECT both expose the column name).
		setParts = []string{fmt.Sprintf("%s = VALUES(%s)", bq(key.Columns[0]), bq(key.Columns[0]))}
	}
	ins := fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s ON DUPLICATE KEY UPDATE %s",
		qualify(t.Schema, t.Name), strings.Join(quoted, ", "), strings.Join(quoted, ", "), bq(stg), strings.Join(setParts, ", "))
	if _, err := tx.ExecContext(ctx, ins); err != nil {
		return 0, fmt.Errorf("mysql: merge load: upsert %s: %w", t, err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("mysql: merge load: commit: %w", err)
	}
	committed = true
	return n, nil
}

// tempColumnDDL builds the column definitions for a staging table matching the
// transport columns' target types (and charset for character columns), so staged
// bytes are held faithfully before the upsert.
func tempColumnDDL(tbl engine.Table, cols []string) (string, error) {
	byName := map[string]engine.Column{}
	for _, c := range tbl.Columns {
		byName[c.Name] = c
	}
	parts := make([]string, 0, len(cols))
	for _, name := range cols {
		c, ok := byName[name]
		if !ok {
			return "", fmt.Errorf("mysql: merge load: column %q not found on target %s", name, tbl.Ref)
		}
		def := bq(name) + " " + c.DataType
		if c.Charset != "" {
			def += " CHARACTER SET " + c.Charset
		}
		parts = append(parts, def)
	}
	return strings.Join(parts, ", "), nil
}

// stagingKeyIndex returns an index clause on the table's key columns for an apply
// staging table (", INDEX `rc_key` (...)"), or "" when one cannot be built (a key
// column missing from the staging, or a TEXT/BLOB-style column, which MySQL can only
// index with a prefix length). The apply deletes target rows whose key is NOT IN the
// staging; without an index MySQL checks each candidate row against the whole staging,
// so the delete grows with the square of the batch (30k changes took 94s). With it,
// each check is one index look-up. It is part of the CREATE TEMPORARY TABLE, since an
// ALTER TABLE would commit the open transaction.
func stagingKeyIndex(tbl engine.Table, cols []string) string {
	key := replicationKey(tbl)
	if key == nil || len(key.Columns) == 0 {
		return ""
	}
	inStaging := make(map[string]bool, len(cols))
	for _, c := range cols {
		inStaging[c] = true
	}
	types := make(map[string]string, len(tbl.Columns))
	for _, c := range tbl.Columns {
		types[c.Name] = strings.ToLower(c.DataType)
	}
	names := make([]string, len(key.Columns))
	for i, name := range key.Columns {
		t := types[name]
		if !inStaging[name] || t == "" ||
			strings.Contains(t, "text") || strings.Contains(t, "blob") ||
			strings.Contains(t, "json") || strings.Contains(t, "geometry") {
			return ""
		}
		names[i] = bq(name)
	}
	return ", INDEX `rc_key` (" + strings.Join(names, ", ") + ")"
}
