package redis

import (
	"context"
	"errors"
	"io"

	"github.com/rudimk/replicare/internal/engine"
)

// errNotConnected is returned by a method invoked before Connect.
var errNotConnected = errors.New("redis: not connected")

// Sink is the Redis write side: RESTORE-based bulk load (RM4), RESTORE/DEL apply
// (RM5), and target-keyspace enumeration for delete reconciliation (RM6). RM0
// implements only the connection lifecycle and version/fork probe.
type Sink struct {
	cfg engine.ConnConfig
	db  *conn
}

var _ engine.Sink = (*Sink)(nil)

// Connect opens the (standalone, RM0) Redis client and pings it.
func (s *Sink) Connect(ctx context.Context) error {
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

// Close releases the client. Safe on an unconnected Sink.
func (s *Sink) Close(context.Context) error {
	if s.db == nil {
		return nil
	}
	err := s.db.Close()
	s.db = nil
	return err
}

// ServerVersion returns the numeric server version and refuses unsupported forks.
func (s *Sink) ServerVersion(ctx context.Context) (int, error) {
	if s.db == nil {
		return 0, errNotConnected
	}
	return serverVersion(ctx, s.db)
}

// --- stubs until their milestones (redis-plan RM2–RM6) ---

// Introspect synthesizes the target unit pseudo-schema + module capabilities (RM2).
func (s *Sink) Introspect(ctx context.Context, sel engine.Selection) (*engine.Schema, error) {
	if s.db == nil {
		return nil, errNotConnected
	}
	return introspect(ctx, s.db, s.cfg, sel)
}
func (s *Sink) BulkLoad(context.Context, engine.TableRef, []string, io.Reader, engine.LoadMode) (int64, error) {
	return 0, errNotImplemented // RM4 (RESTORE REPLACE)
}
func (s *Sink) DeleteRange(context.Context, engine.TableRef, engine.KeyValues, engine.KeyValues) error {
	return errNotImplemented // RM4: no-op for Redis (no ordered key ranges; redis-plan §0.4)
}
func (s *Sink) ApplyPass(context.Context, engine.TableRef, []string, []engine.KeyValues, io.Reader) error {
	return errNotImplemented // RM5
}
func (s *Sink) BeginApply(context.Context, bool, []engine.TableRef) (engine.ApplyTx, error) {
	return nil, errNotImplemented // RM5 (thin ApplyTx: RESTORE REPLACE + DEL)
}
