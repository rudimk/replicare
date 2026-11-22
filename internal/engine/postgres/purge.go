package postgres

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/rudimk/replicare/internal/engine"
)

// Delta purge, bounded retention & forced reseed (CLAUDE.md §3.4;
// .sisyphus/reseed-state-machine.md). Source-side only: Purge touches the delta and
// track tables, never the StateStore (§9). It returns which targets it
// sacrificed so the engine-neutral reseed orchestration can mark needs-reseed.

// defaultPurgeBatch bounds a single purge DELETE so transaction churn on the
// source stays small; the purge loops until a pass deletes fewer than a batch.
const defaultPurgeBatch = 10000

// purgeStatementTimeout caps each purge statement on the source. The purge runs on
// every streaming pass against a database we may not own, so a purge that turns slow
// must give up and try again next pass, never hold the source's disk for minutes.
const purgeStatementTimeout = 30 * time.Second

// trackSweepWindow bounds the leftover-track sweep: one sweep statement covers at most
// this many delta ids per target, so clearing a large backlog of leftovers (from a
// version that never removed them) is spread over many passes in small deletes.
var trackSweepWindow int64 = 100000

// Purge removes deltas consumed by all configured targets, subject to retention.
// If a target's pinned backlog exceeds a retention cap it is sacrificed: its
// track is reset (so it will be reseeded from scratch) and the now-unpinned
// deltas are purged. See .sisyphus/reseed-state-machine.md §3.
func (s *Source) Purge(ctx context.Context, t engine.TableRef, targets []engine.TargetID, ret engine.RetentionPolicy) (engine.PurgeStats, error) {
	if err := s.requireConn(); err != nil {
		return engine.PurgeStats{}, err
	}
	relID, _, ok, err := lookupCapture(ctx, s.conn, t)
	if err != nil {
		return engine.PurgeStats{}, fmt.Errorf("postgres: purge %s: %w", t, err)
	}
	if !ok {
		return engine.PurgeStats{}, fmt.Errorf("postgres: purge: table %s is not captured", t)
	}

	reseed, err := s.overCapTargets(ctx, relID, targets, ret)
	if err != nil {
		return engine.PurgeStats{}, fmt.Errorf("postgres: purge %s: %w", t, err)
	}

	// No sacrifice needed: plain consumption-gated purge across all targets.
	if len(reseed) == 0 {
		n, err := s.purgeConsumedBy(ctx, relID, targets, defaultPurgeBatch)
		if err != nil {
			return engine.PurgeStats{DeltasPurged: n}, fmt.Errorf("postgres: purge %s: %w", t, err)
		}
		return engine.PurgeStats{DeltasPurged: n}, nil
	}

	// Reset the sacrificed targets' track (they are reseeded from scratch, so
	// their partial consumption record is void — reset to EMPTY, never a
	// high-water mark, .sisyphus/reseed-state-machine.md §4.5), then purge the deltas
	// now consumed by all remaining targets (treating the reseeding ones as
	// satisfied). For a single-target sync, remaining is empty and the queue
	// drains fully.
	reseedStrs := targetStrings(reseed)
	if _, err := s.conn.Exec(ctx,
		fmt.Sprintf("DELETE FROM %s WHERE target = ANY($1)", qualifiedCapture(trackTableName(relID))),
		reseedStrs); err != nil {
		return engine.PurgeStats{}, fmt.Errorf("postgres: purge %s: reset reseed track: %w", t, err)
	}
	remaining := subtractTargets(targets, reseed)
	n, err := s.purgeConsumedBy(ctx, relID, remaining, defaultPurgeBatch)
	if err != nil {
		return engine.PurgeStats{TargetsReseeded: reseed}, fmt.Errorf("postgres: purge %s: %w", t, err)
	}
	return engine.PurgeStats{DeltasPurged: n, TargetsReseeded: reseed}, nil
}

// overCapTargets returns the sorted set of targets whose pinned backlog exceeds a
// retention cap. Age is evaluated per target; the size cap is on the delta
// table's on-disk footprint and is attributed to the laggard (the target with
// the oldest unconsumed delta), since that is what pins the queue.
func (s *Source) overCapTargets(ctx context.Context, relID int, targets []engine.TargetID, ret engine.RetentionPolicy) ([]engine.TargetID, error) {
	if ret.MaxAgeSeconds <= 0 && ret.MaxBytes <= 0 {
		return nil, nil
	}
	type tgBacklog struct {
		target engine.TargetID
		bl     engine.DeltaBacklog
	}
	backlogs := make([]tgBacklog, 0, len(targets))
	for _, tg := range targets {
		bl, err := s.deltaBacklogByRel(ctx, relID, tg)
		if err != nil {
			return nil, err
		}
		backlogs = append(backlogs, tgBacklog{tg, bl})
	}

	over := map[engine.TargetID]bool{}
	if ret.MaxAgeSeconds > 0 {
		ageCap := time.Duration(ret.MaxAgeSeconds) * time.Second
		for _, x := range backlogs {
			if x.bl.HasBacklog && x.bl.OldestAge > ageCap {
				over[x.target] = true
			}
		}
	}
	if ret.MaxBytes > 0 {
		totalBytes, err := s.deltaTotalBytes(ctx, relID)
		if err != nil {
			return nil, err
		}
		if totalBytes > ret.MaxBytes {
			// Sacrifice the laggard: the target with the oldest unconsumed delta.
			var laggard engine.TargetID
			var oldest time.Duration
			found := false
			for _, x := range backlogs {
				if x.bl.HasBacklog && (!found || x.bl.OldestAge > oldest) {
					laggard, oldest, found = x.target, x.bl.OldestAge, true
				}
			}
			if found {
				over[laggard] = true
			}
		}
	}
	if len(over) == 0 {
		return nil, nil
	}
	out := make([]engine.TargetID, 0, len(over))
	for tg := range over {
		out = append(out, tg)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

// purgeConsumedBy deletes, in batches, delta rows consumed by every target in
// the set (present in each target's track). With an empty set every row is
// purgeable — used when a single-target sync's only target is being reseeded.
// Loops until a pass deletes fewer than batch rows.
//
// A delta's track rows are deleted in the same statement as the delta itself. A
// track row only matters while its delta exists (it records that the target applied
// it), and delta ids are never reused, so once the delta is gone its track rows are
// dead weight. Leaving them behind (as earlier versions did) grew the track
// table by one row per change per target, forever, and since the consumed set below
// is read from the track table, every purge got slower: on a busy production source
// the purge reached minutes and starved the application's own queries of disk.
// Deleting them here keeps the track table no bigger than the live delta queue
// (times the targets), so reading it stays cheap.
//
// The consumed set is derived set-based from the track table via its (target,
// delta_id) PK — GROUP BY delta_id, one row per target — rather than a per-delta
// correlated subquery. The original correlated form was O(delta × track): under a
// large partially-consumed backlog it scanned the entire delta table running a track
// scan per row, stalling the (synchronous) streaming pass that calls it every tick.
//
// Each statement runs under purgeStatementTimeout. Leftover track rows from earlier
// versions are swept afterwards, a bounded window per call (sweepLeftoverTrack).
func (s *Source) purgeConsumedBy(ctx context.Context, relID int, targets []engine.TargetID, batch int) (int64, error) {
	deltaTbl := qualifiedCapture(deltaTableName(relID))
	trackTbl := qualifiedCapture(trackTableName(relID))

	var q string
	var args []any
	if len(targets) == 0 {
		// No consumers to gate on: every delta row is purgeable. Every target's track
		// was reset by the caller, so there are no track rows to remove with them.
		q = fmt.Sprintf(`
			WITH gone AS (
				DELETE FROM %s WHERE delta_id IN (
					SELECT delta_id FROM %s ORDER BY delta_id LIMIT $1
				) RETURNING delta_id
			)
			SELECT count(*) FROM gone`, deltaTbl, deltaTbl)
		args = []any{batch}
	} else {
		q = fmt.Sprintf(`
			WITH purgeable AS (
				SELECT d.delta_id
				FROM %[1]s d
				WHERE d.delta_id IN (
					SELECT tr.delta_id FROM %[2]s tr
					WHERE tr.target = ANY($1)
					GROUP BY tr.delta_id
					HAVING count(*) = $2
				)
				ORDER BY d.delta_id
				LIMIT $3
			),
			gone AS (
				DELETE FROM %[1]s WHERE delta_id IN (SELECT delta_id FROM purgeable)
				RETURNING delta_id
			),
			untracked AS (
				DELETE FROM %[2]s
				WHERE target = ANY($1) AND delta_id IN (SELECT delta_id FROM gone)
				RETURNING 1
			)
			-- untracked always runs to completion, even though the result does not read
			-- it: Postgres executes every data-modifying WITH exactly once.
			SELECT count(*) FROM gone`,
			deltaTbl, trackTbl)
		args = []any{targetStrings(targets), len(targets), batch}
	}

	var total int64
	for {
		var n int64
		err := s.withStatementTimeout(ctx, purgeStatementTimeout, func(tx pgx.Tx) error {
			return tx.QueryRow(ctx, q, args...).Scan(&n)
		})
		if err != nil {
			return total, err
		}
		total += n
		if n < int64(batch) {
			break
		}
	}
	for _, tg := range targets {
		if err := s.sweepLeftoverTrack(ctx, relID, tg); err != nil {
			return total, err
		}
	}
	return total, nil
}

// sweepLeftoverTrack removes, for one target, track rows whose delta no longer exists
// and lies below the oldest delta still queued: rows left behind by versions that did
// not delete them with their delta. Delta ids are never reused, so nothing will look
// those rows up again, and removing them can never make a change look applied when it
// is not (a missing track row only ever means "not applied yet"). Each call covers at
// most trackSweepWindow ids, using the track table's (target, delta_id) key, so a large
// backlog of leftovers is cleared over many passes in small, quick deletes. An empty
// queue sweeps nothing (a change being committed right now may not be visible yet).
func (s *Source) sweepLeftoverTrack(ctx context.Context, relID int, target engine.TargetID) error {
	q := fmt.Sprintf(`
		DELETE FROM %[2]s
		WHERE target = $1
		  AND delta_id < LEAST(
		        (SELECT coalesce(min(delta_id), 0) FROM %[1]s),
		        (SELECT min(delta_id) FROM %[2]s WHERE target = $1) + $2)`,
		qualifiedCapture(deltaTableName(relID)), qualifiedCapture(trackTableName(relID)))
	err := s.withStatementTimeout(ctx, purgeStatementTimeout, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, q, string(target), trackSweepWindow)
		return err
	})
	if err != nil {
		return fmt.Errorf("sweep leftover track rows for %s: %w", target, err)
	}
	return nil
}

// withStatementTimeout runs fn in a transaction whose statements may each run for at
// most d (SET LOCAL, so the session's own setting is untouched afterwards).
func (s *Source) withStatementTimeout(ctx context.Context, d time.Duration, fn func(pgx.Tx) error) error {
	tx, err := s.conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL statement_timeout = '%dms'", d.Milliseconds())); err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// DeltaBacklog reports a target's unconsumed-delta footprint for a table (M5c
// telemetry + retention). It resolves the capture then delegates to the by-rel_id
// helper.
func (s *Source) DeltaBacklog(ctx context.Context, t engine.TableRef, target engine.TargetID) (engine.DeltaBacklog, error) {
	if err := s.requireConn(); err != nil {
		return engine.DeltaBacklog{}, err
	}
	relID, _, ok, err := lookupCapture(ctx, s.conn, t)
	if err != nil {
		return engine.DeltaBacklog{}, fmt.Errorf("postgres: delta backlog %s: %w", t, err)
	}
	if !ok {
		return engine.DeltaBacklog{}, fmt.Errorf("postgres: delta backlog: table %s is not captured", t)
	}
	return s.deltaBacklogByRel(ctx, relID, target)
}

// deltaBacklogByRel computes the backlog for a captured table's rel_id: the count
// and estimated bytes of delta rows not yet in the target's track, plus the age
// of the oldest such delta. Bytes is prorated from the delta table's total
// on-disk size by the backlog's row fraction. FILTER aggregates are old-PG-safe
// (9.4+), within the 9.6 floor.
func (s *Source) deltaBacklogByRel(ctx context.Context, relID int, target engine.TargetID) (engine.DeltaBacklog, error) {
	var (
		backlogRows int64
		totalRows   int64
		oldestSec   float64
		totalBytes  int64
	)
	err := s.conn.QueryRow(ctx, fmt.Sprintf(`
		SELECT
			count(*) FILTER (WHERE tr.delta_id IS NULL),
			count(*),
			COALESCE(EXTRACT(EPOCH FROM now() - min(d.rc_at) FILTER (WHERE tr.delta_id IS NULL)), 0),
			pg_total_relation_size($2::regclass)
		FROM %s d
		LEFT JOIN %s tr ON tr.target = $1 AND tr.delta_id = d.delta_id`,
		qualifiedCapture(deltaTableName(relID)), qualifiedCapture(trackTableName(relID))),
		string(target), qualifiedCapture(deltaTableName(relID))).
		Scan(&backlogRows, &totalRows, &oldestSec, &totalBytes)
	if err != nil {
		return engine.DeltaBacklog{}, fmt.Errorf("postgres: delta backlog (rel %d): %w", relID, err)
	}
	bytes := int64(0)
	if totalRows > 0 {
		bytes = totalBytes * backlogRows / totalRows
	}
	return engine.DeltaBacklog{
		Rows:       backlogRows,
		Bytes:      bytes,
		OldestAge:  time.Duration(oldestSec * float64(time.Second)),
		HasBacklog: backlogRows > 0,
	}, nil
}

// deltaTotalBytes returns the delta table's total on-disk size (heap + indexes +
// toast), the source-footprint signal for the size retention cap.
func (s *Source) deltaTotalBytes(ctx context.Context, relID int) (int64, error) {
	var bytes int64
	err := s.conn.QueryRow(ctx, "SELECT pg_total_relation_size($1::regclass)",
		qualifiedCapture(deltaTableName(relID))).Scan(&bytes)
	if err != nil {
		return 0, fmt.Errorf("postgres: delta size (rel %d): %w", relID, err)
	}
	return bytes, nil
}

// targetStrings converts target ids to the text[] form used in ANY() predicates.
func targetStrings(targets []engine.TargetID) []string {
	out := make([]string, len(targets))
	for i, t := range targets {
		out[i] = string(t)
	}
	return out
}

// subtractTargets returns the targets not in remove, preserving order.
func subtractTargets(targets, remove []engine.TargetID) []engine.TargetID {
	drop := make(map[engine.TargetID]bool, len(remove))
	for _, r := range remove {
		drop[r] = true
	}
	out := make([]engine.TargetID, 0, len(targets))
	for _, t := range targets {
		if !drop[t] {
			out = append(out, t)
		}
	}
	return out
}
