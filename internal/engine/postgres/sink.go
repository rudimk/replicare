package postgres

import (
	"context"
	"fmt"
	"io"
	"strings"

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
// The direct path COPYs straight into an (empty) target; the merge path stages
// into a TEMP table then upserts, for a non-empty target. The column list is
// explicit and name-matched to the source.
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
	case engine.LoadMerge:
		return s.mergeLoad(ctx, t, cols, r)
	default:
		return fmt.Errorf("postgres: bulk load mode %q not implemented", mode)
	}
}

// mergeLoad implements the non-empty-target path (CLAUDE.md §4.1): COPY the chunk
// into a TEMP staging table, then INSERT ... ON CONFLICT DO UPDATE from it. This
// is privilege-light and idempotent (~2x write amplification). Requires a usable
// key for the conflict target.
func (s *Sink) mergeLoad(ctx context.Context, t engine.TableRef, cols []string, r io.Reader) error {
	table, err := s.tableMeta(ctx, t)
	if err != nil {
		return err
	}
	pk := captureColsFor(table)
	if len(pk) == 0 {
		return fmt.Errorf("postgres: merge load: target %s has no usable key for ON CONFLICT", t)
	}
	typeByName := make(map[string]string, len(table.Columns))
	identity := false
	colSet := make(map[string]bool, len(cols))
	for _, c := range cols {
		colSet[c] = true
	}
	for _, c := range table.Columns {
		typeByName[c.Name] = c.DataType
		if c.Identity && colSet[c.Name] {
			identity = true
		}
	}

	const stg = "replicare_stg"
	stgCols := make([]string, len(cols))
	for i, c := range cols {
		stgCols[i] = quoteIdentifier(c) + " " + typeByName[c]
	}

	if _, err := s.conn.Exec(ctx, "BEGIN"); err != nil {
		return fmt.Errorf("postgres: merge load: begin: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = s.conn.Exec(context.Background(), "ROLLBACK")
		}
	}()

	if _, err := s.conn.Exec(ctx, fmt.Sprintf("CREATE TEMP TABLE %s (%s) ON COMMIT DROP",
		quoteIdentifier(stg), strings.Join(stgCols, ", "))); err != nil {
		return fmt.Errorf("postgres: merge load: create staging: %w", err)
	}
	copySQL := fmt.Sprintf("COPY %s (%s) FROM STDIN", quoteIdentifier(stg), quotedColumnList(cols))
	if _, err := s.conn.PgConn().CopyFrom(ctx, r, copySQL); err != nil {
		return fmt.Errorf("postgres: merge load: copy to staging: %w", err)
	}
	if _, err := s.conn.Exec(ctx, mergeInsertSQL(t, stg, cols, pk, colSetOf(pk), identity)); err != nil {
		return fmt.Errorf("postgres: merge load: upsert %s: %w", t, err)
	}
	if _, err := s.conn.Exec(ctx, "COMMIT"); err != nil {
		return fmt.Errorf("postgres: merge load: commit: %w", err)
	}
	committed = true
	return nil
}

// mergeInsertSQL builds the upsert from the staging table into the target.
func mergeInsertSQL(t engine.TableRef, stg string, cols []string, pk []captureCol, pkSet map[string]bool, identity bool) string {
	overriding := ""
	if identity {
		overriding = " OVERRIDING SYSTEM VALUE"
	}
	pkNames := make([]string, len(pk))
	for i, c := range pk {
		pkNames[i] = quoteIdentifier(c.Name)
	}
	var setParts []string
	for _, c := range cols {
		if !pkSet[c] {
			setParts = append(setParts, fmt.Sprintf("%s = EXCLUDED.%s", quoteIdentifier(c), quoteIdentifier(c)))
		}
	}
	action := "DO NOTHING"
	if len(setParts) > 0 {
		action = "DO UPDATE SET " + strings.Join(setParts, ", ")
	}
	return fmt.Sprintf("INSERT INTO %s (%s)%s SELECT %s FROM %s ON CONFLICT (%s) %s",
		qualifyTable(t), quotedColumnList(cols), overriding, quotedColumnList(cols),
		quoteIdentifier(stg), strings.Join(pkNames, ", "), action)
}

// colSetOf returns a name set for the given key columns.
func colSetOf(cols []captureCol) map[string]bool {
	m := make(map[string]bool, len(cols))
	for _, c := range cols {
		m[c.Name] = true
	}
	return m
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

// BeginApply starts a transaction for one FK component's drain pass (M5b): it
// opens the transaction and defers FK checks (SET CONSTRAINTS ALL DEFERRED) so
// deferrable cyclic FKs commit. The returned ApplyTx uses the sink's connection,
// so the sink must not be used for other work until the tx completes.
func (s *Sink) BeginApply(ctx context.Context) (engine.ApplyTx, error) {
	if s.conn == nil {
		return nil, errNotConnected("sink")
	}
	if _, err := s.conn.Exec(ctx, "BEGIN"); err != nil {
		return nil, fmt.Errorf("postgres: begin apply: %w", err)
	}
	if _, err := s.conn.Exec(ctx, "SET CONSTRAINTS ALL DEFERRED"); err != nil {
		_, _ = s.conn.Exec(context.Background(), "ROLLBACK")
		return nil, fmt.Errorf("postgres: begin apply: defer constraints: %w", err)
	}
	return &pgApplyTx{sink: s, staging: map[engine.TableRef]stagingInfo{}}, nil
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
