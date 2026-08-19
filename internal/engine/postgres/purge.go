package postgres

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

// Delta purge, bounded retention & forced reseed (CLAUDE.md §3.4;
// docs/reseed-state-machine.md). Source-side only: Purge touches the delta and
// track tables, never the StateStore (§9). It returns which targets it
// sacrificed so the engine-neutral reseed orchestration can mark needs-reseed.

// defaultPurgeBatch bounds a single purge DELETE so transaction churn on the
// source stays small; the purge loops until a pass deletes fewer than a batch.
const defaultPurgeBatch = 10000

// Purge removes deltas consumed by all configured targets, subject to retention.
// If a target's pinned backlog exceeds a retention cap it is sacrificed: its
// track is reset (so it will be reseeded from scratch) and the now-unpinned
// deltas are purged. See docs/reseed-state-machine.md §3.
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
	// high-water mark, docs/reseed-state-machine.md §4.5), then purge the deltas
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
// The consumed set is derived set-based from the (small) track table via its
// (target, delta_id) PK — GROUP BY delta_id, one row per target — rather than a
// per-delta correlated subquery. The original correlated form was O(delta ×
// track): under a large partially-consumed backlog it scanned the entire delta
// table running a track scan per row, stalling the (synchronous) streaming pass
// that calls it every tick. Driving from delta with a semijoin to that set keeps
// each pass O(track + batch) and lets batches make progress (already-deleted
// deltas cannot reappear).
func (s *Source) purgeConsumedBy(ctx context.Context, relID int, targets []engine.TargetID, batch int) (int64, error) {
	deltaTbl := qualifiedCapture(deltaTableName(relID))
	trackTbl := qualifiedCapture(trackTableName(relID))

	var q string
	var args []any
	if len(targets) == 0 {
		// No consumers to gate on: every delta row is purgeable.
		q = fmt.Sprintf(`
			DELETE FROM %s WHERE delta_id IN (
				SELECT delta_id FROM %s ORDER BY delta_id LIMIT $1
			)`, deltaTbl, deltaTbl)
		args = []any{batch}
	} else {
		q = fmt.Sprintf(`
			WITH purgeable AS (
				SELECT d.delta_id
				FROM %s d
				WHERE d.delta_id IN (
					SELECT tr.delta_id FROM %s tr
					WHERE tr.target = ANY($1)
					GROUP BY tr.delta_id
					HAVING count(*) = $2
				)
				ORDER BY d.delta_id
				LIMIT $3
			)
			DELETE FROM %s WHERE delta_id IN (SELECT delta_id FROM purgeable)`,
			deltaTbl, trackTbl, deltaTbl)
		args = []any{targetStrings(targets), len(targets), batch}
	}

	var total int64
	for {
		ct, err := s.conn.Exec(ctx, q, args...)
		if err != nil {
			return total, err
		}
		n := ct.RowsAffected()
		total += n
		if n < int64(batch) {
			break
		}
	}
	return total, nil
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
