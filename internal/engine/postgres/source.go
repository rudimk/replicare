package postgres

import (
	"context"
	"fmt"
	"io"

	"github.com/jackc/pgx/v5"

	"github.com/rudimk/replicare/internal/engine"
)

// Source is the Postgres read side: introspection (M1), trigger-based capture
// (M3), chunked initial copy (M4), and dirty-key delta consumption (M5). Only the
// connection lifecycle and introspection land in M1; later methods are stubs that
// fail loudly until their milestone implements them.
type Source struct {
	cfg  engine.ConnConfig
	conn *pgx.Conn
}

// Compile-time assertion that *Source satisfies the interface.
var _ engine.Source = (*Source)(nil)

// Connect opens the connection and applies session-GUC canonicalization (§4.2).
func (s *Source) Connect(ctx context.Context) error {
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

// Close releases the connection. It is safe to call on an unconnected Source.
func (s *Source) Close(ctx context.Context) error {
	if s.conn == nil {
		return nil
	}
	err := s.conn.Close(ctx)
	s.conn = nil
	return err
}

// ServerVersion returns the numeric source server version (e.g. 90600 for 9.6).
func (s *Source) ServerVersion(ctx context.Context) (int, error) {
	if err := s.requireConn(); err != nil {
		return 0, err
	}
	return serverVersion(ctx, s.conn)
}

// Introspect returns the schema for the selected tables (M1). Catalog queries
// are version-tolerant so this works against very old source servers.
func (s *Source) Introspect(ctx context.Context, sel engine.Selection) (*engine.Schema, error) {
	if err := s.requireConn(); err != nil {
		return nil, err
	}
	version, err := serverVersion(ctx, s.conn)
	if err != nil {
		return nil, err
	}
	return introspectConn(ctx, s.conn, version, sel)
}

// PlanChunks computes balanced copy chunks for a table (M4).
func (s *Source) PlanChunks(ctx context.Context, t engine.TableRef, opts engine.ChunkOptions) ([]engine.Chunk, error) {
	return nil, errNotYet("PlanChunks", "M4")
}

// CopyChunk streams one chunk as text COPY into w (M4).
func (s *Source) CopyChunk(ctx context.Context, c engine.Chunk, w io.Writer) error {
	return errNotYet("CopyChunk", "M4")
}

// ReadDirtyKeys returns unconsumed delta keys for (target, table) (M5).
func (s *Source) ReadDirtyKeys(ctx context.Context, t engine.TableRef, target engine.TargetID, max int) ([]engine.DirtyKey, error) {
	return nil, errNotYet("ReadDirtyKeys", "M5")
}

// RereadCurrent streams current source values for the given keys as text COPY (M5).
func (s *Source) RereadCurrent(ctx context.Context, t engine.TableRef, keys []engine.KeyValues, w io.Writer) error {
	return errNotYet("RereadCurrent", "M5")
}

// ConfirmConsumed records delta ids as consumed for (target, table) (M5).
func (s *Source) ConfirmConsumed(ctx context.Context, t engine.TableRef, target engine.TargetID, ids []engine.DeltaID) error {
	return errNotYet("ConfirmConsumed", "M5")
}

// Purge removes deltas consumed by all targets, subject to retention (M5c).
func (s *Source) Purge(ctx context.Context, t engine.TableRef, ret engine.RetentionPolicy) (engine.PurgeStats, error) {
	return engine.PurgeStats{}, errNotYet("Purge", "M5c")
}

// requireConn guards operations that need an open connection.
func (s *Source) requireConn() error {
	if s.conn == nil {
		return errNotConnected("source")
	}
	return nil
}

// errNotConnected is returned by operations invoked before Connect.
func errNotConnected(role string) error {
	return fmt.Errorf("postgres %s: not connected (call Connect first)", role)
}

// errNotYet reports a method whose milestone has not landed yet, so accidental
// early use fails loudly rather than silently no-op'ing.
func errNotYet(method, milestone string) error {
	return fmt.Errorf("postgres: %s is not implemented until %s", method, milestone)
}
