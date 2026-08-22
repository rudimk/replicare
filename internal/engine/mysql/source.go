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

// HealthCheck pings the source (bounded by ctx). *sql.DB re-dials a dropped
// pooled connection on the next query (session vars ride the DSN), so a Ping
// failure here means the server itself is unreachable; the pipeline reconnects.
func (s *Source) HealthCheck(ctx context.Context) error {
	if s.db == nil {
		return errNotConnected
	}
	return s.db.PingContext(ctx)
}

// DatabaseSize implements engine.DBSizer: the connected schema's data+index size.
func (s *Source) DatabaseSize(ctx context.Context) (int64, error) {
	if s.db == nil {
		return 0, errNotConnected
	}
	return databaseSize(ctx, s.db)
}

// ReplicatedSize implements engine.DBSizer: the data+index size of just the given
// tables (§4.2 apples-to-apples source↔target figure).
func (s *Source) ReplicatedSize(ctx context.Context, tables []engine.TableRef) (int64, error) {
	if s.db == nil {
		return 0, errNotConnected
	}
	return replicatedSize(ctx, s.db, tables)
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
// Purge/DeltaBacklog are in purge.go (MM5c).
