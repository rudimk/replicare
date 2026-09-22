package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/rudimk/replicare/internal/engine"
)

// Tombstone / version-register GC (CLAUDE.md §5.3, docs/multi-master.md §5.3). A
// tombstone (a register row with rc_deleted=true) must outlive the moment a delete is
// captured so it can reject a resurrecting OLDER write from a peer that has not yet
// applied the delete. It becomes safe to remove once EVERY peer has applied the
// delete — at which point no peer can produce a write for that key older than the
// tombstone (each peer re-reads the tombstone, not the stale value).
//
// We do not need a bespoke cross-node cursor exchange to know that: the delete's delta
// row is purged only when it has been consumed by ALL target edges, and consumption
// (ConfirmConsumed) runs only AFTER the apply commits on the target. So "the delete
// delta for this key is gone" already means "every peer has committed the delete".
// GC therefore removes a tombstone exactly when its key has no remaining delta row —
// reusing the existing consume+purge machinery as the distributed watermark. A key
// re-created after a delete has a fresh (alive) register row and unconsumed deltas, so
// it is never GC'd by mistake; and a tombstone whose delete is still pending on a down
// peer keeps its delta (unpurged), so it correctly survives until that peer catches up
// (the retention/reseed path bounds a permanently-down peer, §3.4).

// GCTombstones removes version-register tombstones for a cluster table whose delete
// has been fully consumed (no delta row remains for the key). It returns the number of
// tombstones reclaimed. A no-op for a one-way source (no register) or an uncaptured
// table.
func (s *Source) GCTombstones(ctx context.Context, t engine.TableRef) (int64, error) {
	if !s.cluster {
		return 0, nil
	}
	if err := s.requireConn(); err != nil {
		return 0, err
	}
	relID, ok, err := lookupRegistry(ctx, s.conn, t)
	if err != nil {
		return 0, fmt.Errorf("postgres: gc tombstones %s: %w", t, err)
	}
	if !ok {
		return 0, nil
	}
	table, err := s.tableMeta(ctx, t)
	if err != nil {
		return 0, err
	}
	keyCols := captureColsFor(table)
	if len(keyCols) == 0 {
		return 0, nil
	}

	reg := qualifiedCapture(registerTableName(t))
	delta := qualifiedCapture(deltaTableName(relID))
	// Correlate the delta's positional key columns (k1..kn) with the register's named
	// key columns: a tombstone is reclaimable when NO delta row shares its key.
	conds := make([]string, len(keyCols))
	for i, k := range keyCols {
		conds[i] = fmt.Sprintf("d.k%d = g.%s", i+1, quoteIdentifier(k.Name))
	}
	sql := fmt.Sprintf(
		"DELETE FROM %s AS g WHERE g.rc_deleted = true AND NOT EXISTS (SELECT 1 FROM %s d WHERE %s)",
		reg, delta, strings.Join(conds, " AND "))
	tag, err := s.conn.Exec(ctx, sql)
	if err != nil {
		return 0, fmt.Errorf("postgres: gc tombstones %s: %w", t, err)
	}
	return tag.RowsAffected(), nil
}
