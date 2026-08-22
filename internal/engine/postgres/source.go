package postgres

import (
	"context"
	"fmt"
	"strings"

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

// HealthCheck pings the source connection (bounded by ctx). A failure tells the
// pipeline to reconnect — pgx holds a single connection with no pool/auto-redial,
// so a dropped socket is only recovered by Close + Connect.
func (s *Source) HealthCheck(ctx context.Context) error {
	if s.conn == nil {
		return errNotConnected("source")
	}
	return s.conn.Ping(ctx)
}

// DatabaseSize implements engine.DBSizer: the connected database's on-disk size.
func (s *Source) DatabaseSize(ctx context.Context) (int64, error) {
	if s.conn == nil {
		return 0, errNotConnected("source")
	}
	var b int64
	if err := s.conn.QueryRow(ctx, "SELECT pg_database_size(current_database())").Scan(&b); err != nil {
		return 0, fmt.Errorf("postgres: database size: %w", err)
	}
	return b, nil
}

// ReplicatedSize implements engine.DBSizer: the on-disk size of just the given
// tables (§4.2 apples-to-apples source↔target figure).
func (s *Source) ReplicatedSize(ctx context.Context, tables []engine.TableRef) (int64, error) {
	if s.conn == nil {
		return 0, errNotConnected("source")
	}
	return replicatedSize(ctx, s.conn, tables)
}

// replicatedSize sums pg_total_relation_size (heap + indexes + TOAST) over just
// the given tables — the "replicated data only" figure that excludes replicare's
// own capture schema on the source and any unreplicated tables, so source and
// target are comparable. It matches by (schema, name) against pg_class so a table
// absent at this endpoint simply contributes nothing (no regclass cast that could
// error on a missing relation). An empty list returns 0.
func replicatedSize(ctx context.Context, conn *pgx.Conn, tables []engine.TableRef) (int64, error) {
	if len(tables) == 0 {
		return 0, nil
	}
	pairs := make([]string, len(tables))
	args := make([]any, 0, len(tables)*2)
	for i, t := range tables {
		pairs[i] = fmt.Sprintf("($%d,$%d)", 2*i+1, 2*i+2)
		args = append(args, t.Schema, t.Name)
	}
	q := fmt.Sprintf(`SELECT COALESCE(SUM(pg_total_relation_size(c.oid)), 0)
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE (n.nspname, c.relname) IN (%s)`, strings.Join(pairs, ", "))
	var b int64
	if err := conn.QueryRow(ctx, q, args...).Scan(&b); err != nil {
		return 0, fmt.Errorf("postgres: replicated size: %w", err)
	}
	return b, nil
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
