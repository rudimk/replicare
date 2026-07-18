package postgres

import (
	"context"
	"fmt"
	"io"

	"github.com/jackc/pgx/v5"

	"github.com/rudimk/replicare/internal/engine"
)

// Sink is the Postgres write side: introspection for pre-flight (M1), bulk load
// (M4), and faithful FK-ordered per-component apply (M5).
//
// Like Source, a Sink wraps a single *pgx.Conn and is NOT safe for concurrent
// use; parallel load uses multiple Sinks.
type Sink struct {
	cfg  engine.ConnConfig
	conn *pgx.Conn
	meta map[engine.TableRef]engine.Table
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
		return 0, errNotConnected("sink")
	}
	return serverVersion(ctx, s.conn)
}

// Introspect returns the (pre-existing) target schema for pre-flight (M1). It
// reuses the same version-tolerant catalog queries as the Source.
func (s *Sink) Introspect(ctx context.Context, sel engine.Selection) (*engine.Schema, error) {
	if s.conn == nil {
		return nil, errNotConnected("sink")
	}
	version, err := serverVersion(ctx, s.conn)
	if err != nil {
		return nil, err
	}
	return introspectConn(ctx, s.conn, version, sel)
}

// BulkLoad streams a text COPY into the target table for initial copy (§4.1).
// The direct path COPYs straight into the (empty) target; the merge path (TEMP
// staging + upsert) for a non-empty target lands in a later slice. The column
// list is explicit and name-matched to the source.
func (s *Sink) BulkLoad(ctx context.Context, t engine.TableRef, cols []string, r io.Reader, mode engine.LoadMode) error {
	if s.conn == nil {
		return errNotConnected("sink")
	}
	if len(cols) == 0 {
		return fmt.Errorf("postgres: bulk load: no columns for %s", t)
	}
	switch mode {
	case engine.LoadDirect, "":
		sql := fmt.Sprintf("COPY %s (%s) FROM STDIN", qualifyTable(t), quotedColumnList(cols))
		if _, err := s.conn.PgConn().CopyFrom(ctx, r, sql); err != nil {
			return fmt.Errorf("postgres: bulk load into %s: %w", t, err)
		}
		return nil
	default:
		return fmt.Errorf("postgres: bulk load mode %q not implemented yet", mode)
	}
}

// DeleteRange deletes target rows in the half-open key range [lo, hi) so an
// incomplete chunk can be re-COPYed on resume (§4.1). The predicate uses the
// target's own key columns/types.
func (s *Sink) DeleteRange(ctx context.Context, t engine.TableRef, lo, hi engine.KeyValues) error {
	if s.conn == nil {
		return errNotConnected("sink")
	}
	table, err := s.tableMeta(ctx, t)
	if err != nil {
		return err
	}
	keyCols := captureColsFor(table)
	if len(keyCols) == 0 {
		return fmt.Errorf("postgres: delete-range: target %s has no usable key", t)
	}
	pred, err := keysetPredicate(keyCols, lo, hi)
	if err != nil {
		return err
	}
	if _, err := s.conn.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE %s", qualifyTable(t), pred)); err != nil {
		return fmt.Errorf("postgres: delete-range on %s: %w", t, err)
	}
	return nil
}

// BeginApply starts a transaction scoped to one FK component's drain pass (M5).
func (s *Sink) BeginApply(ctx context.Context) (engine.ApplyTx, error) {
	return nil, errNotYet("BeginApply", "M5")
}

// tableMeta returns cached introspected metadata for a target table.
func (s *Sink) tableMeta(ctx context.Context, ref engine.TableRef) (engine.Table, error) {
	if t, ok := s.meta[ref]; ok {
		return t, nil
	}
	version, err := serverVersion(ctx, s.conn)
	if err != nil {
		return engine.Table{}, err
	}
	schema, err := introspectConn(ctx, s.conn, version, engine.Selection{Include: []string{ref.String()}})
	if err != nil {
		return engine.Table{}, err
	}
	for _, t := range schema.Tables {
		if t.Ref == ref {
			if s.meta == nil {
				s.meta = make(map[engine.TableRef]engine.Table)
			}
			s.meta[ref] = t
			return t, nil
		}
	}
	return engine.Table{}, fmt.Errorf("postgres: target table %s not found", ref)
}
