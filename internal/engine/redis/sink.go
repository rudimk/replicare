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
	// sel is the compiled sync key-selection (learned from the first Introspect,
	// first-write-wins), so ScanTargetKeys never surfaces — and delete
	// reconciliation never DELETEs — target keys outside the selection. nil = all.
	sel *selection
	// targetRecon is the delete-sweep's bounded per-shard target-SCAN state, held
	// across ScanTargetKeys calls (RM6, §0.4).
	targetRecon *reconState
}

var _ engine.Sink = (*Sink)(nil)
var _ engine.KeyLister = (*Sink)(nil)

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
	if s.sel == nil {
		s.sel = compileSelection(sel)
	}
	return introspect(ctx, s.db, s.cfg, sel)
}

// ScanTargetKeys returns the next bounded batch of selected target keys for the
// delete sweep (RM6). It carries the real per-shard cursor internally (targetRecon)
// across ticks; the cursor argument is ignored, and next==0 signals a completed
// rolling pass. Keys outside the sync selection are never surfaced, so the sweep
// can never delete a key the user didn't select.
func (s *Sink) ScanTargetKeys(ctx context.Context, _ engine.TableRef, _ uint64, count int) ([]engine.KeyValues, uint64, error) {
	if s.db == nil {
		return nil, 0, errNotConnected
	}
	if count <= 0 {
		count = defaultScanCount
	}
	if s.targetRecon == nil {
		sc, err := s.db.shardScanners(ctx, s.cfg)
		if err != nil {
			return nil, 0, err
		}
		s.targetRecon = &reconState{scanners: sc, cursors: make([]uint64, len(sc)), done: make([]bool, len(sc))}
	}
	tun := tuningFromParams(s.cfg.Params)
	strs, passComplete, err := scanBatch(ctx, s.targetRecon, tun.scanCount, count, s.sel)
	if err != nil {
		return nil, 0, err
	}
	keys := make([]engine.KeyValues, 0, len(strs))
	for _, k := range strs {
		keys = append(keys, engine.KeyValues{k})
	}
	next := uint64(1)
	if passComplete {
		next = 0
	}
	return keys, next, nil
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
	return restoreStream(ctx, s.db, r, nil)
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
