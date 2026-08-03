package redis

import (
	"context"

	"github.com/rudimk/replicare/internal/engine"
)

// Source is the Redis read side: topology/version introspection (RM1/RM2), SCAN
// initial snapshot (RM4), SCAN reconciliation + DUMP re-read (RM5), and durable
// delete detection (RM6). RM0 implements only the connection lifecycle and the
// version/fork probe; the remaining methods are stubs until their milestones
// (see .sisyphus/redis-plan.md).
type Source struct {
	cfg engine.ConnConfig
	db  *conn
	// sel is the compiled sync key-selection, learned from the first Introspect
	// (the daemon introspects with the real sync selection before copy; RM4's
	// CopyChunk applies it at SCAN time). nil means match-all. First-write-wins so
	// the copy layer's later column-probe Introspect (a unit-ref selection) does
	// not clobber the real key-globs. See introspect.go / copy.go.
	sel *selection

	// recon is the streaming reconciliation SCAN state (RM5), held across
	// ReadDirtyKeys calls so a pass is bounded, not buffered. changeID is the
	// monotonic per-unit change-id (a lag/ordering hint only; §0.4).
	recon    *reconState
	changeID int64
}

var _ engine.Source = (*Source)(nil)

// Connect opens the (standalone, RM0) Redis client and pings it.
func (s *Source) Connect(ctx context.Context) error {
	if s.db != nil {
		return nil
	}
	c, err := open(ctx, s.cfg)
	if err != nil {
		return err
	}
	s.db = c
	return nil
}

// Close releases the client. Safe on an unconnected Source.
func (s *Source) Close(context.Context) error {
	if s.db == nil {
		return nil
	}
	err := s.db.Close()
	s.db = nil
	return err
}

// ServerVersion returns the numeric server version and refuses unsupported forks.
func (s *Source) ServerVersion(ctx context.Context) (int, error) {
	if s.db == nil {
		return 0, errNotConnected
	}
	return serverVersion(ctx, s.db)
}

// --- stubs until their milestones (redis-plan RM2–RM7) ---

// Introspect synthesizes the unit pseudo-schema + module capabilities (RM2).
func (s *Source) Introspect(ctx context.Context, sel engine.Selection) (*engine.Schema, error) {
	if s.db == nil {
		return nil, errNotConnected
	}
	if s.sel == nil {
		s.sel = compileSelection(sel)
	}
	return introspect(ctx, s.db, s.cfg, sel)
}
func (s *Source) InstallCapture(context.Context, []engine.TableRef) error {
	return errNotImplemented // RM7 (keyspace-notification subscription)
}
func (s *Source) RemoveCapture(context.Context, []engine.TableRef) error {
	return errNotImplemented // RM7
}

// ReadDirtyKeys, RereadCurrent, ConfirmConsumed, DeltaBacklog, Purge — see stream.go (RM5).
