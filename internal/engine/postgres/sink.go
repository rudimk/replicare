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

// *Sink also implements the optional cyclic-component capabilities.
var (
	_ engine.CyclicComponentCopier = (*Sink)(nil)
	_ engine.NullFillCyclicSink    = (*Sink)(nil)
)

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

// HealthCheck pings the target connection (bounded by ctx). A failure tells the
// pipeline to reconnect — pgx holds a single connection with no pool/auto-redial,
// so a dropped socket is only recovered by Close + Connect.
func (s *Sink) HealthCheck(ctx context.Context) error {
	if s.conn == nil {
		return errNotConnected("sink")
	}
	return s.conn.Ping(ctx)
}

// DatabaseSize implements engine.DBSizer: the connected database's on-disk size.
func (s *Sink) DatabaseSize(ctx context.Context) (int64, error) {
	if s.conn == nil {
		return 0, errNotConnected("sink")
	}
	var b int64
	if err := s.conn.QueryRow(ctx, "SELECT pg_database_size(current_database())").Scan(&b); err != nil {
		return 0, fmt.Errorf("postgres: database size: %w", err)
	}
	return b, nil
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
func (s *Sink) BulkLoad(ctx context.Context, t engine.TableRef, cols []string, r io.Reader, mode engine.LoadMode) (int64, error) {
	if s.conn == nil {
		return 0, errNotConnected("sink")
	}
	if len(cols) == 0 {
		return 0, fmt.Errorf("postgres: bulk load: no columns for %s", t)
	}
	switch mode {
	case engine.LoadDirect, "":
		sql := fmt.Sprintf("COPY %s (%s) FROM STDIN", qualifyTable(t), quotedColumnList(cols))
		tag, err := s.conn.PgConn().CopyFrom(ctx, r, sql)
		if err != nil {
			return 0, fmt.Errorf("postgres: bulk load into %s: %w", t, err)
		}
		return tag.RowsAffected(), nil
	case engine.LoadMerge:
		return s.mergeLoad(ctx, t, cols, r)
	default:
		return 0, fmt.Errorf("postgres: bulk load mode %q not implemented", mode)
	}
}

// mergeLoad implements the non-empty-target path (CLAUDE.md §4.1): COPY the chunk
// into a TEMP staging table, then INSERT ... ON CONFLICT DO UPDATE from it. This
// is privilege-light and idempotent (~2x write amplification). Requires a usable
// key for the conflict target.
func (s *Sink) mergeLoad(ctx context.Context, t engine.TableRef, cols []string, r io.Reader) (int64, error) {
	table, err := s.tableMeta(ctx, t)
	if err != nil {
		return 0, err
	}
	pk := captureColsFor(table)
	if len(pk) == 0 {
		return 0, fmt.Errorf("postgres: merge load: target %s has no usable key for ON CONFLICT", t)
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
		return 0, fmt.Errorf("postgres: merge load: begin: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = s.conn.Exec(context.Background(), "ROLLBACK")
		}
	}()

	if _, err := s.conn.Exec(ctx, fmt.Sprintf("CREATE TEMP TABLE %s (%s) ON COMMIT DROP",
		quoteIdentifier(stg), strings.Join(stgCols, ", "))); err != nil {
		return 0, fmt.Errorf("postgres: merge load: create staging: %w", err)
	}
	copySQL := fmt.Sprintf("COPY %s (%s) FROM STDIN", quoteIdentifier(stg), quotedColumnList(cols))
	tag, err := s.conn.PgConn().CopyFrom(ctx, r, copySQL)
	if err != nil {
		return 0, fmt.Errorf("postgres: merge load: copy to staging: %w", err)
	}
	if _, err := s.conn.Exec(ctx, mergeInsertSQL(t, stg, cols, pk, colSetOf(pk), identity)); err != nil {
		return 0, fmt.Errorf("postgres: merge load: upsert %s: %w", t, err)
	}
	if _, err := s.conn.Exec(ctx, "COMMIT"); err != nil {
		return 0, fmt.Errorf("postgres: merge load: commit: %w", err)
	}
	committed = true
	// The staging COPY row count is the number of source rows loaded; the upsert
	// may touch fewer (ON CONFLICT DO NOTHING) but rows-copied tracks throughput.
	return tag.RowsAffected(), nil
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
//
// Postgres defers ALL FK checks to commit regardless of `cyclic`, so both the
// cyclic flag and componentTables are ignored here — the deferred-check + abort-
// at-COMMIT behavior already gives cyclic components loud-before-corrupt safety.
// The parameters exist for MySQL, which has no deferral (CLAUDE.md §8.1,
// mysql-plan §0.2).
func (s *Sink) BeginApply(ctx context.Context, cyclic bool, componentTables []engine.TableRef) (engine.ApplyTx, error) {
	if s.conn == nil {
		return nil, errNotConnected("sink")
	}
	// For a cyclic component, resolve which FK columns close the cycle so the apply
	// can load them NULL then fill them (see pgApplyTx.cyclicCols). Done before BEGIN
	// so an introspection error doesn't leave a transaction open.
	var cyclicCols map[engine.TableRef][]string
	if cyclic {
		cc, err := s.cyclicColsFor(ctx, componentTables)
		if err != nil {
			return nil, fmt.Errorf("postgres: begin apply: resolve cyclic FK columns: %w", err)
		}
		cyclicCols = cc
	}
	if _, err := s.conn.Exec(ctx, "BEGIN"); err != nil {
		return nil, fmt.Errorf("postgres: begin apply: %w", err)
	}
	// Harmless for standard (non-DEFERRABLE) FKs and correct-and-helpful when the
	// target's cyclic FKs happen to be DEFERRABLE; the NULL-then-fill above is what
	// makes the non-deferrable case work.
	if _, err := s.conn.Exec(ctx, "SET CONSTRAINTS ALL DEFERRED"); err != nil {
		_, _ = s.conn.Exec(context.Background(), "ROLLBACK")
		return nil, fmt.Errorf("postgres: begin apply: defer constraints: %w", err)
	}
	return &pgApplyTx{sink: s, staging: map[engine.TableRef]stagingInfo{}, cyclicCols: cyclicCols}, nil
}

// CyclicCols implements engine.NullFillCyclicSink: it reports the nullable cyclic
// FK child columns per table for a component, so the neutral drain can pick the
// per-table NULL-then-fill strategy for a cyclic component (and the atomic
// SET CONSTRAINTS ALL DEFERRED path when the result is empty — an all-DEFERRABLE
// cycle). DEFERRABLE / NOT NULL cyclic columns are excluded (nullFillColsByTable).
func (s *Sink) CyclicCols(ctx context.Context, componentTables []engine.TableRef) (map[engine.TableRef][]string, error) {
	return s.cyclicColsFor(ctx, componentTables)
}

// cyclicColsFor resolves the nullable cyclic FK child columns per table for a
// component, from the cached target metadata (columns to load NULL then fill in
// the apply). Only NULL-then-fill (nullable) columns are returned: a DEFERRABLE
// cyclic FK's columns may be NOT NULL and must NOT be nulled — the deferred-check
// path handles them — so BeginApply(cyclic) on an all-DEFERRABLE cycle sets no
// cyclicCols and relies purely on SET CONSTRAINTS ALL DEFERRED.
func (s *Sink) cyclicColsFor(ctx context.Context, componentTables []engine.TableRef) (map[engine.TableRef][]string, error) {
	members := make([]engine.Table, 0, len(componentTables))
	for _, ref := range componentTables {
		m, err := s.tableMeta(ctx, ref)
		if err != nil {
			return nil, err
		}
		members = append(members, m)
	}
	return nullFillColsByTable(classifyCyclicFKs(members)), nil
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
