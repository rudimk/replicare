package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

// Delta purge, bounded retention & forced reseed for MySQL (CLAUDE.md §3.4;
// .sisyphus/reseed-state-machine.md), the MM5c bloat-control path. Source-side only:
// Purge touches the `replicare` delta and track tables, never the StateStore (§9),
// and returns which targets it sacrificed so the neutral reseed orchestration can
// mark needs-reseed. MySQL has no autovacuum reloptions — bloat control is
// entirely the batched consumption-gated DELETE here.
//
// MySQL bloat physics differ from Postgres and shape the telemetry: InnoDB does
// NOT shrink the tablespace on DELETE (DATA_LENGTH stays flat until OPTIMIZE), and
// a long-running source transaction inflates the InnoDB history list, blocking
// purge of rows it can still see — the direct xmin-horizon analog. Both are
// surfaced via DeltaBacklog rather than silently tolerated.

// defaultPurgeBatch bounds a single purge DELETE so transaction churn on the
// source stays small; the purge loops until a pass deletes fewer than a batch.
const defaultPurgeBatch = 10000

// Purge removes deltas consumed by all configured targets, subject to retention.
// If a target's pinned backlog exceeds a retention cap it is sacrificed: its
// track is reset (so it will be reseeded from scratch) and the now-unpinned
// deltas are purged. See .sisyphus/reseed-state-machine.md §3.
func (s *Source) Purge(ctx context.Context, t engine.TableRef, targets []engine.TargetID, ret engine.RetentionPolicy) (engine.PurgeStats, error) {
	if s.db == nil {
		return engine.PurgeStats{}, errNotConnected
	}
	relID, _, ok, err := s.lookupRegistry(ctx, t)
	if err != nil {
		return engine.PurgeStats{}, fmt.Errorf("mysql: purge %s: %w", t, err)
	}
	if !ok {
		return engine.PurgeStats{}, fmt.Errorf("mysql: purge: table %s is not captured", t)
	}

	reseed, err := s.overCapTargets(ctx, relID, targets, ret)
	if err != nil {
		return engine.PurgeStats{}, fmt.Errorf("mysql: purge %s: %w", t, err)
	}

	// No sacrifice needed: plain consumption-gated purge across all targets.
	if len(reseed) == 0 {
		n, err := s.purgeConsumedBy(ctx, relID, targets, defaultPurgeBatch)
		if err != nil {
			return engine.PurgeStats{DeltasPurged: n}, fmt.Errorf("mysql: purge %s: %w", t, err)
		}
		return engine.PurgeStats{DeltasPurged: n}, nil
	}

	// Reset the sacrificed targets' track (they are reseeded from scratch, so their
	// partial consumption record is void — reset to EMPTY, never a high-water mark,
	// .sisyphus/reseed-state-machine.md §4.5), then purge the deltas now consumed by all
	// remaining targets (treating the reseeding ones as satisfied). For a
	// single-target sync, remaining is empty and the queue drains fully.
	inClause, inArgs := targetInClause(reseed)
	if _, err := s.db.ExecContext(ctx,
		fmt.Sprintf("DELETE FROM %s WHERE target IN (%s)", captureRef(trackTableName(relID)), inClause),
		inArgs...); err != nil {
		return engine.PurgeStats{}, fmt.Errorf("mysql: purge %s: reset reseed track: %w", t, err)
	}
	remaining := subtractTargets(targets, reseed)
	n, err := s.purgeConsumedBy(ctx, relID, remaining, defaultPurgeBatch)
	if err != nil {
		return engine.PurgeStats{TargetsReseeded: reseed}, fmt.Errorf("mysql: purge %s: %w", t, err)
	}
	return engine.PurgeStats{DeltasPurged: n, TargetsReseeded: reseed}, nil
}

// overCapTargets returns the sorted set of targets whose pinned backlog exceeds a
// retention cap. Age is evaluated per target; the size cap is on the delta table's
// on-disk footprint and is attributed to the laggard (the target with the oldest
// unconsumed delta), since that is what pins the queue.
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

// trackSweepWindow bounds the leftover-track sweep: one sweep covers at most this many
// delta ids per target, so a large backlog of leftovers is cleared over many passes.
var trackSweepWindow int64 = 100000

// purgeConsumedBy deletes, in batches, delta rows consumed by every target in the
// set (present in each target's track), together with their track rows. With an
// empty set every row is purgeable — used when a single-target sync's only target is
// being reseeded (its track was reset by the caller). Loops until a pass deletes
// fewer than batch rows.
//
// A delta's track rows are only needed while the delta exists, and delta ids are
// never reused, so they are deleted with it. Earlier versions left them behind, and
// the track table grew by one row per change per target forever (the Postgres purge
// slowed to minutes on a busy source because of it). MySQL 5.7 has no RETURNING, so
// each batch first picks its delta ids (a correlated COUNT per delta over the track
// table's (target, delta_id) key), then deletes those deltas and their track rows in
// one transaction. Leftovers from earlier versions are swept afterwards, a bounded
// window per call.
func (s *Source) purgeConsumedBy(ctx context.Context, relID int, targets []engine.TargetID, batch int) (int64, error) {
	delta := captureRef(deltaTableName(relID))
	track := captureRef(trackTableName(relID))

	if len(targets) == 0 {
		// No pinning targets: every delta is purgeable.
		q := fmt.Sprintf("DELETE FROM %s ORDER BY delta_id LIMIT ?", delta)
		var total int64
		for {
			res, err := s.db.ExecContext(ctx, q, batch)
			if err != nil {
				return total, err
			}
			n, _ := res.RowsAffected()
			total += n
			if n < int64(batch) {
				return total, nil
			}
		}
	}

	inClause, inArgs := targetInClause(targets)
	pick := fmt.Sprintf(
		"SELECT d.delta_id FROM %s d WHERE (SELECT COUNT(*) FROM %s tr WHERE tr.delta_id = d.delta_id AND tr.target IN (%s)) = ? ORDER BY d.delta_id LIMIT ?",
		delta, track, inClause)
	pickArgs := append(append([]any{}, inArgs...), len(targets), batch)

	var total int64
	for {
		ids, err := s.pickDeltaIDs(ctx, pick, pickArgs)
		if err != nil {
			return total, err
		}
		if len(ids) > 0 {
			if err := s.deleteDeltasAndTrack(ctx, delta, track, inClause, inArgs, ids); err != nil {
				return total, err
			}
		}
		total += int64(len(ids))
		if len(ids) < batch {
			break
		}
	}
	for _, tg := range targets {
		if err := s.sweepLeftoverTrack(ctx, delta, track, tg); err != nil {
			return total, err
		}
	}
	return total, nil
}

// pickDeltaIDs runs the batch-picking query and returns the delta ids it found.
func (s *Source) pickDeltaIDs(ctx context.Context, q string, args []any) ([]int64, error) {
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// deleteDeltasAndTrack deletes the given deltas and the targets' track rows for them,
// in one transaction.
func (s *Source) deleteDeltasAndTrack(ctx context.Context, delta, track, inClause string, inArgs []any, ids []int64) error {
	idPh := make([]string, len(ids))
	idArgs := make([]any, len(ids))
	for i, id := range ids {
		idPh[i] = "?"
		idArgs[i] = id
	}
	idList := strings.Join(idPh, ", ")
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE delta_id IN (%s)", delta, idList), idArgs...); err != nil {
		return err
	}
	trackArgs := append(append([]any{}, inArgs...), idArgs...)
	if _, err := tx.ExecContext(ctx,
		fmt.Sprintf("DELETE FROM %s WHERE target IN (%s) AND delta_id IN (%s)", track, inClause, idList),
		trackArgs...); err != nil {
		return err
	}
	return tx.Commit()
}

// sweepLeftoverTrack removes, for one target, track rows below the oldest delta still
// queued: rows earlier versions left behind when their delta was purged. Delta ids
// are never reused, so nothing looks those rows up again, and a missing track row can
// only ever mean "not applied yet", never the reverse. Each call covers at most
// trackSweepWindow ids from the target's oldest track row, on the (target, delta_id)
// key. An empty queue sweeps nothing (a change being committed right now may not be
// visible yet). The bounds are read first: MySQL cannot reference the table being
// deleted from in a subquery (error 1093).
func (s *Source) sweepLeftoverTrack(ctx context.Context, delta, track string, target engine.TargetID) error {
	var minLive, minTrack sql.NullInt64
	if err := s.db.QueryRowContext(ctx, fmt.Sprintf("SELECT MIN(delta_id) FROM %s", delta)).Scan(&minLive); err != nil {
		return fmt.Errorf("sweep leftover track rows for %s: %w", target, err)
	}
	if !minLive.Valid {
		return nil
	}
	if err := s.db.QueryRowContext(ctx, fmt.Sprintf("SELECT MIN(delta_id) FROM %s WHERE target = ?", track),
		string(target)).Scan(&minTrack); err != nil {
		return fmt.Errorf("sweep leftover track rows for %s: %w", target, err)
	}
	if !minTrack.Valid || minTrack.Int64 >= minLive.Int64 {
		return nil
	}
	bound := minLive.Int64
	if w := minTrack.Int64 + trackSweepWindow; w < bound {
		bound = w
	}
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE target = ? AND delta_id < ?", track),
		string(target), bound); err != nil {
		return fmt.Errorf("sweep leftover track rows for %s: %w", target, err)
	}
	return nil
}

// DeltaBacklog reports a target's unconsumed-delta footprint for a table (MM5c
// telemetry + retention). It resolves the capture then delegates to the by-rel_id
// helper.
func (s *Source) DeltaBacklog(ctx context.Context, t engine.TableRef, target engine.TargetID) (engine.DeltaBacklog, error) {
	if s.db == nil {
		return engine.DeltaBacklog{}, errNotConnected
	}
	relID, _, ok, err := s.lookupRegistry(ctx, t)
	if err != nil {
		return engine.DeltaBacklog{}, fmt.Errorf("mysql: delta backlog %s: %w", t, err)
	}
	if !ok {
		return engine.DeltaBacklog{}, fmt.Errorf("mysql: delta backlog: table %s is not captured", t)
	}
	return s.deltaBacklogByRel(ctx, relID, target)
}

// deltaBacklogByRel computes the backlog for a captured table's rel_id: the count
// and estimated bytes of delta rows not yet in the target's track, plus the age of
// the oldest such delta. Bytes is prorated from the delta table's on-disk size by
// the backlog's row fraction. MySQL has no FILTER, so conditional counts use
// SUM(cond) / MIN(CASE ...); age is TIMESTAMPDIFF in microseconds. (DATETIME age
// is only tz-exact when writer and reader sessions share a time zone — the harness
// runs UTC throughout, the documented MySQL caveat.)
func (s *Source) deltaBacklogByRel(ctx context.Context, relID int, target engine.TargetID) (engine.DeltaBacklog, error) {
	var (
		backlogRows int64
		totalRows   int64
		oldestMicro int64
	)
	q := fmt.Sprintf(`
		SELECT
			COALESCE(SUM(tr.delta_id IS NULL), 0),
			COUNT(*),
			COALESCE(TIMESTAMPDIFF(MICROSECOND, MIN(CASE WHEN tr.delta_id IS NULL THEN d.rc_at END), NOW(6)), 0)
		FROM %s d
		LEFT JOIN %s tr ON tr.target = ? AND tr.delta_id = d.delta_id`,
		captureRef(deltaTableName(relID)), captureRef(trackTableName(relID)))
	if err := s.db.QueryRowContext(ctx, q, string(target)).Scan(&backlogRows, &totalRows, &oldestMicro); err != nil {
		return engine.DeltaBacklog{}, fmt.Errorf("mysql: delta backlog (rel %d): %w", relID, err)
	}

	totalBytes, err := s.deltaTotalBytes(ctx, relID)
	if err != nil {
		return engine.DeltaBacklog{}, err
	}
	bytes := int64(0)
	if totalRows > 0 {
		bytes = totalBytes * backlogRows / totalRows
	}
	if oldestMicro < 0 {
		oldestMicro = 0 // clock skew guard
	}
	return engine.DeltaBacklog{
		Rows:       backlogRows,
		Bytes:      bytes,
		OldestAge:  time.Duration(oldestMicro) * time.Microsecond,
		HasBacklog: backlogRows > 0,
	}, nil
}

// deltaTotalBytes returns the delta table's on-disk size (data + indexes) from
// information_schema — the source-footprint signal for the size retention cap.
// InnoDB DATA_LENGTH is an approximate, page-granular figure that does NOT shrink
// on DELETE, which is exactly the bloat signal we want to surface.
func (s *Source) deltaTotalBytes(ctx context.Context, relID int) (int64, error) {
	var bytes int64
	err := s.db.QueryRowContext(ctx,
		"SELECT COALESCE(DATA_LENGTH + INDEX_LENGTH, 0) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
		captureDB, deltaTableName(relID)).Scan(&bytes)
	if err != nil {
		return 0, fmt.Errorf("mysql: delta size (rel %d): %w", relID, err)
	}
	return bytes, nil
}

// targetInClause builds a `?, ?, ...` placeholder list and its args for a
// `target IN (...)` predicate.
func targetInClause(targets []engine.TargetID) (string, []any) {
	ph := make([]string, len(targets))
	args := make([]any, len(targets))
	for i, t := range targets {
		ph[i] = "?"
		args[i] = string(t)
	}
	return strings.Join(ph, ", "), args
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
