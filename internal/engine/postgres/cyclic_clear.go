package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/rudimk/replicare/internal/engine"
)

// clearTryTruncate lets tests take the DELETE path, as a target role without the
// TRUNCATE privilege (the documented grants are SELECT/INSERT/UPDATE/DELETE) would.
var clearTryTruncate = true

// clearLockWait bounds how long clearing waits for a lock on a target table before
// falling back from TRUNCATE (ACCESS EXCLUSIVE) to DELETE (ROW EXCLUSIVE).
const clearLockWait = "10s"

// clearComponentForReload empties a cyclic component's tables on the target before its
// copy, when any of them holds rows from an earlier, unfinished attempt.
//
// A cyclic component is copied as one unit and only checkpointed once the whole unit is
// done, so every (re)start copies it from scratch. Loading over what an earlier attempt
// left behind used to merge (stage + upsert on the primary key), which cannot get past
// a row the source has since moved: an account deleted and re-created under a new id with
// the same unique (email, parent), or two rows that swapped a unique value. The stale
// target row holds the value under another id, and the upsert fails with a duplicate key
// (23505). Clearing first is the component-level form of the chunked copy's resume rule:
// delete the unfinished part, then copy it again. It is safe for the same reason: during a
// table's initial copy only the copier writes it.
//
// It does not clear, and the copy falls back to merging, when:
//   - the sink is an active-active cluster member: the target's rows are a peer's own
//     data, not leftovers;
//   - a table outside the component references one of its tables: emptying it would fail
//     the FK, or cascade into data replicare does not own.
//
// TRUNCATE is tried first (fast, no dead rows); without the privilege, or if a table's
// lock is not free within clearLockWait, it falls back to a single DELETE statement over
// every table: FK checks fire at the end of a statement, so one statement can empty a
// component whatever its loops (self-references, RESTRICT, NOT NULL cycles).
func (s *Sink) clearComponentForReload(ctx context.Context, tables []engine.TableRef) (bool, error) {
	if s.origin || len(tables) == 0 {
		return false, nil
	}
	occupied := false
	for _, t := range tables {
		empty, err := targetEmpty(ctx, s, t)
		if err != nil {
			return false, err
		}
		if !empty {
			occupied = true
			break
		}
	}
	if !occupied {
		return false, nil
	}
	outside, err := s.outsideReferences(ctx, tables)
	if err != nil {
		return false, err
	}
	if len(outside) > 0 {
		return false, nil
	}

	names := make([]string, len(tables))
	for i, t := range tables {
		names[i] = qualifyTable(t)
	}
	if _, err := s.conn.Exec(ctx, "BEGIN"); err != nil {
		return false, fmt.Errorf("postgres: clear for reload: begin: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = s.conn.Exec(context.Background(), "ROLLBACK")
		}
	}()
	if _, err := s.conn.Exec(ctx, "SET LOCAL lock_timeout = '"+clearLockWait+"'"); err != nil {
		return false, fmt.Errorf("postgres: clear for reload: lock_timeout: %w", err)
	}
	truncated := false
	if clearTryTruncate {
		if _, err := s.conn.Exec(ctx, "SAVEPOINT replicare_clear"); err != nil {
			return false, fmt.Errorf("postgres: clear for reload: savepoint: %w", err)
		}
		_, err := s.conn.Exec(ctx, "TRUNCATE "+strings.Join(names, ", "))
		switch {
		case err == nil:
			truncated = true
		case isInsufficientPrivilege(err) || isLockWaitTimeout(err):
			if _, rerr := s.conn.Exec(ctx, "ROLLBACK TO SAVEPOINT replicare_clear"); rerr != nil {
				return false, fmt.Errorf("postgres: clear for reload: rollback to savepoint: %w", rerr)
			}
		default:
			return false, fmt.Errorf("postgres: clear for reload: truncate: %w", classifyFKViolation(err))
		}
	}
	if !truncated {
		if _, err := s.conn.Exec(ctx, clearDeleteSQL(names)); err != nil {
			return false, fmt.Errorf("postgres: clear for reload: delete: %w", classifyFKViolation(err))
		}
	}
	if _, err := s.conn.Exec(ctx, "COMMIT"); err != nil {
		return false, fmt.Errorf("postgres: clear for reload: commit: %w", err)
	}
	committed = true
	return true, nil
}

// clearDeleteSQL empties every table in one statement: a DELETE per table, all but the
// last in data-modifying CTEs. FK checks run at the end of the statement, by which time
// every row of every table is gone, so no ordering is needed and loops are fine.
func clearDeleteSQL(names []string) string {
	if len(names) == 1 {
		return "DELETE FROM " + names[0]
	}
	ctes := make([]string, 0, len(names)-1)
	for i, n := range names[:len(names)-1] {
		ctes = append(ctes, fmt.Sprintf("d%d AS (DELETE FROM %s)", i, n))
	}
	return "WITH " + strings.Join(ctes, ", ") + " DELETE FROM " + names[len(names)-1]
}

// outsideReferences lists target FKs from tables outside the set into tables in it.
func (s *Sink) outsideReferences(ctx context.Context, tables []engine.TableRef) ([]string, error) {
	names := make([]string, len(tables))
	for i, t := range tables {
		names[i] = qualifyTable(t)
	}
	rows, err := s.conn.Query(ctx, `
SELECT c.conname || ' on ' || c.conrelid::regclass::text
  FROM pg_constraint c
 WHERE c.contype = 'f'
   AND c.confrelid = ANY ($1::text[]::regclass[])
   AND NOT (c.conrelid = ANY ($1::text[]::regclass[]))
 ORDER BY 1`, names)
	if err != nil {
		return nil, fmt.Errorf("postgres: clear for reload: find outside references: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

func isInsufficientPrivilege(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "42501"
}

// withPgDetail appends a Postgres error's DETAIL line to the message. pgx's Error()
// leaves it out, and it is the line that names the offending key, e.g. "Key (email,
// parent)=(a@x, 51) already exists." The wrapped error stays matchable with errors.As.
func withPgDetail(err error) error {
	var pgErr *pgconn.PgError
	if err == nil || !errors.As(err, &pgErr) || pgErr.Detail == "" || strings.Contains(err.Error(), pgErr.Detail) {
		return err
	}
	return fmt.Errorf("%w; %s", err, pgErr.Detail)
}
