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

// BulkLoad reads the DUMP framing and pipelines RESTORE ... REPLACE, value-faithful
// (RM4). cols/mode are neutral-contract parameters Redis ignores: there are no
// columns, and REPLACE is inherently idempotent so LoadDirect and LoadMerge behave
// identically. In cluster mode the routing client dispatches each RESTORE to the
// key's owning shard. Returns the number of keys restored.
func (s *Sink) BulkLoad(ctx context.Context, _ engine.TableRef, _ []string, r io.Reader, _ engine.LoadMode) (int64, error) {
	if s.db == nil {
		return 0, errNotConnected
	}
	return restoreStream(ctx, s.db, r)
}

// DeleteRange is a no-op for Redis: there is no ordered key space to range-delete,
// and initial-copy resume re-SCANs from cursor 0 with idempotent RESTORE REPLACE
// (redis-plan §0.5). The neutral copy layer only calls this when a keyset watermark
// is present, which a Redis unit never sets.
func (s *Sink) DeleteRange(context.Context, engine.TableRef, engine.KeyValues, engine.KeyValues) error {
	return nil
}

// ApplyPass is unused by the Redis path: streaming apply goes through the
// component ApplyTx (BeginApply, see apply.go), not the single-table ApplyPass.
func (s *Sink) ApplyPass(context.Context, engine.TableRef, []string, []engine.KeyValues, io.Reader) error {
	return errNotImplemented // Redis uses BeginApply/ApplyTx (apply.go)
}

// BeginApply — see apply.go (RM5, the thin RESTORE-REPLACE ApplyTx).
