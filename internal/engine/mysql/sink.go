package mysql

import (
	"context"
	"database/sql"
	"errors"

	"github.com/rudimk/replicare/internal/engine"
)

// errNotConnected is returned by a method invoked before Connect.
var errNotConnected = errors.New("mysql: not connected")

// Sink is the MySQL write side: introspection (MM1a), bulk load (MM4), and
// faithful FK-ordered apply (MM5). Like Source it holds a single-connection
// *sql.DB and is not safe for concurrent use. MM0 implements only the connection
// lifecycle and version probe.
type Sink struct {
	cfg  engine.ConnConfig
	db   *sql.DB
	meta map[engine.TableRef]engine.Table // introspection cache (single-goroutine)
	// localInfile records whether the target server permits LOAD DATA LOCAL INFILE
	// (the copy/apply transport), probed at Connect. When false, load paths halt
	// loud rather than fail cryptically mid-copy (MM9; v1 has no INSERT fallback).
	localInfile bool
}

// Compile-time assertion that *Sink satisfies the interface.
var _ engine.Sink = (*Sink)(nil)

// Connect opens the connection (session canonicalization via the DSN) and probes
// whether the target permits LOAD DATA LOCAL INFILE, the copy/apply transport.
func (s *Sink) Connect(ctx context.Context) error {
	if s.db != nil {
		return nil
	}
	db, err := open(ctx, s.cfg)
	if err != nil {
		return err
	}
	s.db = db
	ok, err := probeLocalInfile(ctx, db)
	if err != nil {
		_ = db.Close()
		s.db = nil
		return err
	}
	s.localInfile = ok
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

// Introspect returns the (pre-existing) target schema for pre-flight (MM1a).
func (s *Sink) Introspect(ctx context.Context, sel engine.Selection) (*engine.Schema, error) {
	if s.db == nil {
		return nil, errNotConnected
	}
	return introspectDB(ctx, s.db, sel)
}

// BulkLoad + DeleteRange are in sink_load.go (MM4a). ApplyPass is in apply.go
// (MM5a). BeginApply + the component ApplyTx are in apply_tx.go (MM5b).
