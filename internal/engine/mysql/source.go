package mysql

import (
	"context"
	"database/sql"

	"github.com/rudimk/replicare/internal/engine"
)

// Source is the MySQL read side: introspection (MM1a), trigger-based capture
// (MM3), chunked initial copy (MM4), and dirty-key delta consumption (MM5).
//
// A Source holds a single-connection *sql.DB (MaxOpenConns=1) and is therefore
// NOT safe for concurrent use; parallel copy uses multiple Sources. MM0
// implements only the connection lifecycle and the version probe; the remaining
// methods are stubs until their milestones (see .sisyphus/mysql-plan.md).
type Source struct {
	cfg  engine.ConnConfig
	db   *sql.DB
	meta map[engine.TableRef]engine.Table // introspection cache (single-goroutine)
}

// Compile-time assertion that *Source satisfies the interface.
var _ engine.Source = (*Source)(nil)

// Connect opens the connection. Session-variable canonicalization (§4.2 analog)
// is added in MM1a.
func (s *Source) Connect(ctx context.Context) error {
	if s.db != nil {
		return nil
	}
	db, err := open(ctx, s.cfg)
	if err != nil {
		return err
	}
	s.db = db
	return nil
}

// Close releases the connection. Safe on an unconnected Source.
func (s *Source) Close(ctx context.Context) error {
	if s.db == nil {
		return nil
	}
	err := s.db.Close()
	s.db = nil
	return err
}

// ServerVersion returns the numeric server version (e.g. 50744 for 5.7.44) and
// refuses MariaDB (out of scope for v1).
func (s *Source) ServerVersion(ctx context.Context) (int, error) {
	if s.db == nil {
		return 0, errNotConnected
	}
	return serverVersion(ctx, s.db)
}

// --- stubs until their milestones ---

// Introspect returns the schema for the selected tables (MM1a).
func (s *Source) Introspect(ctx context.Context, sel engine.Selection) (*engine.Schema, error) {
	if s.db == nil {
		return nil, errNotConnected
	}
	return introspectDB(ctx, s.db, sel)
}

// InstallCapture/RemoveCapture are in capture.go (MM3).
// ReadDirtyKeys/ConfirmConsumed are in consume.go (MM3).
// PlanChunks is in chunk.go, CopyChunk in copy.go (MM4a).
// RereadCurrent is in reread.go (MM5a).

func (s *Source) Purge(context.Context, engine.TableRef, []engine.TargetID, engine.RetentionPolicy) (engine.PurgeStats, error) {
	return engine.PurgeStats{}, errNotImplemented // MM5c
}
func (s *Source) DeltaBacklog(context.Context, engine.TableRef, engine.TargetID) (engine.DeltaBacklog, error) {
	return engine.DeltaBacklog{}, errNotImplemented // MM5c
}
