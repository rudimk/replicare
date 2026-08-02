package mysql

import (
	"context"
	"database/sql"
	"errors"
	"io"

	"github.com/rudimk/replicare/internal/engine"
)

// errNotConnected is returned by a method invoked before Connect.
var errNotConnected = errors.New("mysql: not connected")

// Sink is the MySQL write side: introspection (MM1a), bulk load (MM4), and
// faithful FK-ordered apply (MM5). Like Source it holds a single-connection
// *sql.DB and is not safe for concurrent use. MM0 implements only the connection
// lifecycle and version probe.
type Sink struct {
	cfg engine.ConnConfig
	db  *sql.DB
}

// Compile-time assertion that *Sink satisfies the interface.
var _ engine.Sink = (*Sink)(nil)

// Connect opens the connection. Session canonicalization is added in MM1a.
func (s *Sink) Connect(ctx context.Context) error {
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

// Close releases the connection. Safe on an unconnected Sink.
func (s *Sink) Close(ctx context.Context) error {
	if s.db == nil {
		return nil
	}
	err := s.db.Close()
	s.db = nil
	return err
}

// ServerVersion returns the numeric server version and refuses MariaDB.
func (s *Sink) ServerVersion(ctx context.Context) (int, error) {
	if s.db == nil {
		return 0, errNotConnected
	}
	return serverVersion(ctx, s.db)
}

// --- stubs until their milestones ---

func (s *Sink) Introspect(context.Context, engine.Selection) (*engine.Schema, error) {
	return nil, errNotImplemented // MM1a
}
func (s *Sink) BulkLoad(context.Context, engine.TableRef, []string, io.Reader, engine.LoadMode) (int64, error) {
	return 0, errNotImplemented // MM4a
}
func (s *Sink) DeleteRange(context.Context, engine.TableRef, engine.KeyValues, engine.KeyValues) error {
	return errNotImplemented // MM4a
}
func (s *Sink) ApplyPass(context.Context, engine.TableRef, []string, []engine.KeyValues, io.Reader) error {
	return errNotImplemented // MM5a
}
func (s *Sink) BeginApply(context.Context) (engine.ApplyTx, error) {
	return nil, errNotImplemented // MM5b
}
