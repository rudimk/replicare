package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/rudimk/replicare/internal/engine"
)

// Source is the Postgres read side: introspection (M1), trigger-based capture
// (M3), chunked initial copy (M4), and dirty-key delta consumption (M5).
//
// A Source wraps a single *pgx.Conn and is therefore NOT safe for concurrent
// use (like the underlying conn). Parallel copy uses multiple Sources, one per
// worker connection.
type Source struct {
	cfg  engine.ConnConfig
	conn *pgx.Conn
	// meta caches introspected table metadata (columns/keys) so per-chunk copy
	// does not re-introspect. Safe without a lock because a Source is
	// single-connection / single-goroutine.
	meta map[engine.TableRef]engine.Table
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
