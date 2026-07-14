package postgres

import (
	"context"
	"errors"
	"io"

	"github.com/jackc/pgx/v5"

	"github.com/rudimk/replicare/internal/engine"
)

// Sink is the Postgres write side: introspection for pre-flight (M1), bulk load
// (M4), and faithful FK-ordered per-component apply (M5). Only the connection
// lifecycle and introspection land in M1.
type Sink struct {
	cfg  engine.ConnConfig
	conn *pgx.Conn
}

// Compile-time assertion that *Sink satisfies the interface.
var _ engine.Sink = (*Sink)(nil)

// Connect opens the connection and applies session-GUC canonicalization (§4.2).
func (s *Sink) Connect(ctx context.Context) error {
	if s.conn != nil {
		return nil
	}
	conn, err := connect(ctx, s.cfg)
	if err != nil {
		return err
	}
	s.conn = conn
	return nil
}

// Close releases the connection. It is safe to call on an unconnected Sink.
func (s *Sink) Close(ctx context.Context) error {
	if s.conn == nil {
		return nil
	}
	err := s.conn.Close(ctx)
	s.conn = nil
	return err
}

// ServerVersion returns the numeric target server version.
func (s *Sink) ServerVersion(ctx context.Context) (int, error) {
	if s.conn == nil {
		return 0, errors.New("postgres sink: not connected (call Connect first)")
	}
	return serverVersion(ctx, s.conn)
}

// Introspect returns the (pre-existing) target schema for pre-flight (M1).
func (s *Sink) Introspect(ctx context.Context, sel engine.Selection) (*engine.Schema, error) {
	return nil, errNotYet("Introspect", "M1")
}

// BulkLoad streams a text COPY into the target table for initial copy (M4).
func (s *Sink) BulkLoad(ctx context.Context, t engine.TableRef, cols []string, r io.Reader, mode engine.LoadMode) error {
	return errNotYet("BulkLoad", "M4")
}

// BeginApply starts a transaction scoped to one FK component's drain pass (M5).
func (s *Sink) BeginApply(ctx context.Context) (engine.ApplyTx, error) {
	return nil, errNotYet("BeginApply", "M5")
}
