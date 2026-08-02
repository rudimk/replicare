package mysql

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

// Delta purge, bounded retention & forced reseed for MySQL (CLAUDE.md §3.4;
// docs/reseed-state-machine.md), the MM5c bloat-control path. Source-side only:
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
// deltas are purged. See docs/reseed-state-machine.md §3.
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
	// docs/reseed-state-machine.md §4.5), then purge the deltas now consumed by all
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

// purgeConsumedBy deletes, in batches, delta rows consumed by every target in the
// set (present in each target's track). With an empty set every row is purgeable —
// used when a single-target sync's only target is being reseeded. Loops until a
// pass deletes fewer than batch rows. MySQL 5.7 has no CTEs, so this is a plain
// single-table DELETE with ORDER BY + LIMIT and a correlated COUNT subquery over
// the (different) track table — the delta table is only referenced by column, not
// in the subquery FROM, so the 1093 self-delete restriction does not apply.
func (s *Source) purgeConsumedBy(ctx context.Context, relID int, targets []engine.TargetID, batch int) (int64, error) {
	delta := captureRef(deltaTableName(relID))
	track := captureRef(trackTableName(relID))

	var q string
	var baseArgs []any
	if len(targets) == 0 {
		// No pinning targets: every delta is purgeable.
		q = fmt.Sprintf("DELETE FROM %s ORDER BY delta_id LIMIT ?", delta)
	} else {
		inClause, inArgs := targetInClause(targets)
		q = fmt.Sprintf(
			"DELETE FROM %s WHERE (SELECT COUNT(*) FROM %s tr WHERE tr.delta_id = %s.delta_id AND tr.target IN (%s)) = ? ORDER BY delta_id LIMIT ?",
			delta, track, delta, inClause)
		baseArgs = append(baseArgs, inArgs...)
		baseArgs = append(baseArgs, len(targets))
	}

	var total int64
	for {
		args := append(append([]any{}, baseArgs...), batch)
		res, err := s.db.ExecContext(ctx, q, args...)
		if err != nil {
			return total, err
		}
		n, _ := res.RowsAffected()
		total += n
		if n < int64(batch) {
			break
		}
	}
	return total, nil
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
