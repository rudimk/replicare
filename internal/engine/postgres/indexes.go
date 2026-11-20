package postgres

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/rudimk/replicare/internal/engine"
)

// *Sink takes a target's plain indexes down for the initial copy and builds them
// again afterwards (CLAUDE.md §4.1 "deferred target indexes").
var _ engine.IndexDeferrer = (*Sink)(nil)

// deferrableIndexesSQL lists the plain indexes on the given tables: not a primary key,
// not unique, not an exclusion index, not behind any constraint, and valid (an invalid
// index is someone's interrupted build; it is left alone). For each it reports what the
// rebuild needs to put it back exactly: its CREATE statement, its tablespace, comment,
// CLUSTER marker and per-column statistics targets, and whether its leading column is a
// foreign-key column of the table. owned says whether this role may drop it (a member
// of the table owner's role); partitioned marks indexes that belong to a partitioned
// table or are a partition of a parent index, which cannot be rebuilt concurrently in
// one step and are left alone.
const deferrableIndexesSQL = `
SELECT n.nspname, t.relname, i.relname,
       pg_get_indexdef(x.indexrelid),
       COALESCE(ts.spcname, ''),
       pg_has_role(t.relowner, 'USAGE'),
       (t.relkind = 'p' OR i.relispartition),
       EXISTS (SELECT 1 FROM pg_constraint f
                WHERE f.conrelid = t.oid AND f.contype = 'f' AND f.conkey[1] = x.indkey[0]),
       COALESCE(obj_description(i.oid, 'pg_class'), ''),
       x.indisclustered,
       ARRAY(SELECT a.attnum::int || '=' || a.attstattarget::int
               FROM pg_attribute a
              WHERE a.attrelid = i.oid AND a.attstattarget > 0
              ORDER BY a.attnum)
FROM pg_index x
JOIN pg_class i     ON i.oid = x.indexrelid
JOIN pg_class t     ON t.oid = x.indrelid
JOIN pg_namespace n ON n.oid = t.relnamespace
LEFT JOIN pg_tablespace ts ON ts.oid = i.reltablespace
JOIN unnest($1::text[], $2::text[]) AS sel(s, r) ON sel.s = n.nspname AND sel.r = t.relname
WHERE NOT x.indisprimary
  AND NOT x.indisunique
  AND NOT x.indisexclusion
  AND x.indisvalid
  AND NOT EXISTS (SELECT 1 FROM pg_constraint c WHERE c.conindid = x.indexrelid)
ORDER BY n.nspname, t.relname, i.relname`

// DeferrableIndexes implements engine.IndexDeferrer.
func (s *Sink) DeferrableIndexes(ctx context.Context, tables []engine.TableRef) ([]engine.DeferredIndex, []engine.IndexSkip, error) {
	if len(tables) == 0 {
		return nil, nil, nil
	}
	schemas := make([]string, len(tables))
	names := make([]string, len(tables))
	for i, t := range tables {
		schemas[i], names[i] = t.Schema, t.Name
	}
	rows, err := s.conn.Query(ctx, deferrableIndexesSQL, schemas, names)
	if err != nil {
		return nil, nil, fmt.Errorf("postgres: list deferrable indexes: %w", err)
	}
	defer rows.Close()

	var out []engine.DeferredIndex
	skipped := map[engine.TableRef]string{}
	for rows.Next() {
		var (
			ref                engine.TableRef
			idx                engine.DeferredIndex
			owned, partitioned bool
			comment            string
			clustered          bool
			stats              []string
		)
		if err := rows.Scan(&ref.Schema, &ref.Name, &idx.Name, &idx.Definition, &idx.Tablespace,
			&owned, &partitioned, &idx.Link, &comment, &clustered, &stats); err != nil {
			return nil, nil, fmt.Errorf("postgres: scan deferrable index: %w", err)
		}
		idx.Table = ref
		switch {
		case !owned:
			skipped[ref] = "the target role does not own this table, so it may not drop or create its indexes"
			continue
		case partitioned:
			skipped[ref] = "indexes on a partitioned table cannot be rebuilt without blocking writes in one step"
			continue
		}
		if !strings.HasPrefix(idx.Definition, "CREATE INDEX ") {
			skipped[ref] = fmt.Sprintf("index %s has an unexpected definition", idx.Name)
			continue
		}
		qi := qualifyIndex(ref.Schema, idx.Name)
		if comment != "" {
			idx.After = append(idx.After, "COMMENT ON INDEX "+qi+" IS "+quoteLiteral(comment))
		}
		for _, st := range stats {
			col, target, ok := strings.Cut(st, "=")
			if !ok {
				continue
			}
			idx.After = append(idx.After, fmt.Sprintf("ALTER INDEX %s ALTER COLUMN %s SET STATISTICS %s", qi, col, target))
		}
		if clustered {
			idx.After = append(idx.After, "ALTER TABLE "+qualifyTable(ref)+" CLUSTER ON "+quoteIdentifier(idx.Name))
		}
		out = append(out, idx)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("postgres: list deferrable indexes: %w", err)
	}
	// A table is either deferred whole or left whole: if any of its indexes may not be
	// dropped, keep them all, so one table never ends up half-indexed by our hand.
	if len(skipped) > 0 {
		kept := out[:0]
		for _, idx := range out {
			if _, skip := skipped[idx.Table]; !skip {
				kept = append(kept, idx)
			}
		}
		out = kept
	}
	skips := make([]engine.IndexSkip, 0, len(skipped))
	for t, why := range skipped {
		skips = append(skips, engine.IndexSkip{Table: t, Reason: why})
	}
	sort.Slice(skips, func(i, j int) bool { return skips[i].Table.String() < skips[j].Table.String() })
	return out, skips, nil
}

// qualifyIndex returns a schema-qualified, quoted index name.
func qualifyIndex(schema, name string) string {
	return quoteIdentifier(schema) + "." + quoteIdentifier(name)
}

// DropDeferredIndex implements engine.IndexDeferrer. A plain DROP INDEX takes a brief
// exclusive lock on the table; like installing capture, it waits at most
// captureLockWait for it, backs off and retries, and after captureLockAttempts fails
// rather than ever queueing the application's queries behind a waiting lock request.
func (s *Sink) DropDeferredIndex(ctx context.Context, idx engine.DeferredIndex) error {
	stmt := "DROP INDEX IF EXISTS " + qualifyIndex(idx.Table.Schema, idx.Name)
	var err error
	for attempt := 0; attempt < captureLockAttempts; attempt++ {
		err = s.dropIndexOnce(ctx, stmt)
		if err == nil || !isLockWaitTimeout(err) {
			break
		}
		if attempt == captureLockAttempts-1 {
			return fmt.Errorf("postgres: drop index %s: could not get a lock on %s within %s on any of %d attempts; "+
				"another session is holding it: %w", idx.Name, idx.Table, captureLockWait, captureLockAttempts, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(captureLockBackoff(attempt)):
		}
	}
	if err != nil {
		return fmt.Errorf("postgres: drop index %s on %s: %w", idx.Name, idx.Table, err)
	}
	return nil
}

func (s *Sink) dropIndexOnce(ctx context.Context, stmt string) error {
	tx, err := s.conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL lock_timeout = '%dms'", captureLockWait.Milliseconds())); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, stmt); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// IndexPresence implements engine.IndexDeferrer.
func (s *Sink) IndexPresence(ctx context.Context, idx engine.DeferredIndex) (engine.IndexPresence, error) {
	var valid bool
	err := s.conn.QueryRow(ctx, `
SELECT x.indisvalid
FROM pg_class i
JOIN pg_namespace n ON n.oid = i.relnamespace
JOIN pg_index x     ON x.indexrelid = i.oid
WHERE n.nspname = $1 AND i.relname = $2`, idx.Table.Schema, idx.Name).Scan(&valid)
	if errors.Is(err, pgx.ErrNoRows) {
		return engine.IndexMissing, nil
	}
	if err != nil {
		return engine.IndexMissing, fmt.Errorf("postgres: look up index %s: %w", idx.Name, err)
	}
	if valid {
		return engine.IndexValid, nil
	}
	return engine.IndexInvalid, nil
}

// BuildDeferredIndex implements engine.IndexDeferrer. It runs CREATE INDEX
// CONCURRENTLY, which lets reads and writes to the table carry on while it builds,
// on this Sink's own connection with no statement or lock timeout (a build of a big
// table takes as long as it takes, and its waits are for transactions to finish, not
// for locks the application needs). An interrupted earlier build leaves an invalid
// index behind; it is dropped first. A valid index of that name means the work is
// already done (an earlier run, or an operator by hand), so nothing is built.
func (s *Sink) BuildDeferredIndex(ctx context.Context, idx engine.DeferredIndex, opts engine.IndexBuildOptions) error {
	presence, err := s.IndexPresence(ctx, idx)
	if err != nil {
		return err
	}
	switch presence {
	case engine.IndexValid:
		return nil
	case engine.IndexInvalid:
		if _, err := s.conn.Exec(ctx, "DROP INDEX CONCURRENTLY IF EXISTS "+qualifyIndex(idx.Table.Schema, idx.Name)); err != nil {
			return fmt.Errorf("postgres: drop broken index %s: %w", idx.Name, err)
		}
	}
	create, ok := strings.CutPrefix(idx.Definition, "CREATE INDEX ")
	if !ok {
		return fmt.Errorf("postgres: index %s: unexpected definition %q", idx.Name, idx.Definition)
	}
	session := []string{
		"SET statement_timeout = 0",
		"SET lock_timeout = 0",
		"SET default_tablespace = " + quoteLiteral(idx.Tablespace),
	}
	if opts.MaintenanceWorkMem != "" {
		session = append(session, "SET maintenance_work_mem = "+quoteLiteral(opts.MaintenanceWorkMem))
	}
	if opts.ParallelWorkers > 0 {
		session = append(session, fmt.Sprintf("SET max_parallel_maintenance_workers = %d", opts.ParallelWorkers))
	}
	for _, stmt := range session {
		if _, err := s.conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("postgres: index build session (%s): %w", stmt, err)
		}
	}
	if _, err := s.conn.Exec(ctx, "CREATE INDEX CONCURRENTLY "+create); err != nil {
		return fmt.Errorf("postgres: build index %s on %s: %w", idx.Name, idx.Table, err)
	}
	for _, stmt := range idx.After {
		if _, err := s.conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("postgres: index %s: %s: %w", idx.Name, stmt, err)
		}
	}
	return nil
}

// BuildSession implements engine.IndexDeferrer: this connection's backend pid.
func (s *Sink) BuildSession(ctx context.Context) (int64, error) {
	var pid int64
	if err := s.conn.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid); err != nil {
		return 0, fmt.Errorf("postgres: backend pid: %w", err)
	}
	return pid, nil
}

// IndexBuildProgress implements engine.IndexDeferrer from pg_stat_progress_create_index.
func (s *Sink) IndexBuildProgress(ctx context.Context, session int64) (engine.IndexBuildProgress, bool, error) {
	var p engine.IndexBuildProgress
	err := s.conn.QueryRow(ctx, `
SELECT phase,
       CASE WHEN blocks_total > 0 THEN blocks_done ELSE tuples_done END,
       CASE WHEN blocks_total > 0 THEN blocks_total ELSE tuples_total END
FROM pg_stat_progress_create_index
WHERE pid = $1`, session).Scan(&p.Phase, &p.Done, &p.Total)
	if errors.Is(err, pgx.ErrNoRows) {
		return p, false, nil
	}
	if err != nil {
		return p, false, fmt.Errorf("postgres: index build progress: %w", err)
	}
	return p, true, nil
}

// AnalyzeTables implements engine.IndexDeferrer.
func (s *Sink) AnalyzeTables(ctx context.Context, tables []engine.TableRef) error {
	for _, t := range tables {
		if _, err := s.conn.Exec(ctx, "ANALYZE "+qualifyTable(t)); err != nil {
			return fmt.Errorf("postgres: analyze %s: %w", t, err)
		}
	}
	return nil
}
