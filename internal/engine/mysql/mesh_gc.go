package mysql

import (
	"context"
	"fmt"
	"strings"

	"github.com/rudimk/replicare/internal/engine"
)

// Tombstone / version-register GC for MySQL (CLAUDE.md §5.3), mirroring
// internal/engine/postgres/mesh_gc.go. A tombstone is reclaimable once its delete's
// delta row is gone — which, because a delta is purged only when consumed by ALL
// target edges (and consumption runs after the apply commits on each peer), means
// every peer has applied the delete and can no longer produce an older write for the
// key. So GC removes a register tombstone exactly when its key has no remaining delta.
//
// The multi-table DELETE form (`DELETE g FROM reg g …`) is used so the target alias
// works on MySQL 5.7 too (single-table DELETE aliases need 8.0.16+).
func (s *Source) GCTombstones(ctx context.Context, t engine.TableRef) (int64, error) {
	if !s.cluster {
		return 0, nil
	}
	if s.db == nil {
		return 0, errNotConnected
	}
	relID, pkCols, ok, err := s.lookupRegistry(ctx, t)
	if err != nil {
		return 0, fmt.Errorf("mysql: gc tombstones %s: %w", t, err)
	}
	if !ok || len(pkCols) == 0 {
		return 0, nil
	}
	reg := captureRef(registerTableName(t))
	delta := captureRef(deltaTableName(relID))
	conds := make([]string, len(pkCols))
	for i, name := range pkCols {
		conds[i] = fmt.Sprintf("d.k%d = g.%s", i+1, bq(name))
	}
	sql := fmt.Sprintf(
		"DELETE g FROM %s g WHERE g.rc_deleted = 1 AND NOT EXISTS (SELECT 1 FROM %s d WHERE %s)",
		reg, delta, strings.Join(conds, " AND "))
	res, err := s.db.ExecContext(ctx, sql)
	if err != nil {
		return 0, fmt.Errorf("mysql: gc tombstones %s: %w", t, err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}

var _ engine.TombstoneGC = (*Source)(nil)
