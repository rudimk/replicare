package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/rudimk/replicare/internal/engine"
)

// Source is the MySQL read side: introspection (MM1a), trigger-based capture
// (MM3), chunked initial copy (MM4), and dirty-key delta consumption (MM5).
//
// A Source holds a single-connection *sql.DB (MaxOpenConns=1) and is therefore
// NOT safe for concurrent use; parallel copy uses multiple Sources. MM0
// implements only the connection lifecycle and the version probe; the remaining
// methods are stubs until their milestones (see .sisyphus/mysql-plan.md).
type Source struct {
	cfg  engine.ConnConfig
	db   *sql.DB
	meta map[engine.TableRef]engine.Table // introspection cache (single-goroutine)
	// cluster marks this Source as a cluster-edge source: its re-read emits the mesh
	// version (hlc, node, deleted) alongside each row for HLC last-write-wins
	// (CLAUDE.md §5.3). Off by default → the one-way re-read is byte-identical.
	cluster bool
}

// Compile-time assertion that *Source satisfies the interface.
var _ engine.Source = (*Source)(nil)

// EnableClusterReads implements engine.ClusterReadSource: it switches this Source's
// re-read to the version-aware variant for a cluster edge. nodeID is accepted for
// interface symmetry and ignored: the MySQL version register records each row's origin
// inline, stamped by the capture trigger.
func (s *Source) EnableClusterReads(string) { s.cluster = true }

var _ engine.ClusterReadSource = (*Source)(nil)

// Connect opens the connection. Session-variable canonicalization (§4.2 analog)
// is added in MM1a.
func (s *Source) Connect(ctx context.Context) error {
	if s.db != nil {
		return nil
	}
	db, err := open(ctx, s.cfg)
	if err != nil {
		return err
	}
	s.db = db
	return nil
}

// Close releases the connection. Safe on an unconnected Source.
func (s *Source) Close(ctx context.Context) error {
	if s.db == nil {
		return nil
	}
	err := s.db.Close()
	s.db = nil
	return err
}

// HealthCheck pings the source (bounded by ctx). *sql.DB re-dials a dropped
// pooled connection on the next query (session vars ride the DSN), so a Ping
// failure here means the server itself is unreachable; the pipeline reconnects.
func (s *Source) HealthCheck(ctx context.Context) error {
	if s.db == nil {
		return errNotConnected
	}
	return s.db.PingContext(ctx)
}

// DatabaseSize implements engine.DBSizer: the connected schema's data+index size.
func (s *Source) DatabaseSize(ctx context.Context) (int64, error) {
	if s.db == nil {
		return 0, errNotConnected
	}
	return databaseSize(ctx, s.db)
}

// ReplicatedSize implements engine.DBSizer: the data+index size of just the given
// tables (§4.2 apples-to-apples source↔target figure).
func (s *Source) ReplicatedSize(ctx context.Context, tables []engine.TableRef) (int64, error) {
	if s.db == nil {
		return 0, errNotConnected
	}
	return replicatedSize(ctx, s.db, tables)
}

// EstimateRows implements engine.RowEstimator from information_schema.TABLES.TABLE_ROWS
// (InnoDB's sampled estimate; no scan). Tables reporting 0 or NULL are left out.
func (s *Source) EstimateRows(ctx context.Context, tables []engine.TableRef) (map[engine.TableRef]int64, error) {
	if s.db == nil {
		return nil, errNotConnected
	}
	out := make(map[engine.TableRef]int64, len(tables))
	// In pieces (engine.MaxKeysPerStatement): two placeholders per table.
	for _, piece := range engine.Pieces(tables, engine.MaxKeysPerStatement) {
		if err := s.tableStatPiece(ctx, "COALESCE(TABLE_ROWS, 0)", "estimate rows", piece, out); err != nil {
			return nil, err
		}
	}
	return out, nil
}

var _ engine.RowEstimator = (*Source)(nil)

// EstimateTableBytes implements engine.TableSizeEstimator from information_schema's
// DATA_LENGTH (the clustered index, i.e. the rows; secondary indexes excluded).
func (s *Source) EstimateTableBytes(ctx context.Context, tables []engine.TableRef) (map[engine.TableRef]int64, error) {
	if s.db == nil {
		return nil, errNotConnected
	}
	out := make(map[engine.TableRef]int64, len(tables))
	// In pieces (engine.MaxKeysPerStatement): two placeholders per table.
	for _, piece := range engine.Pieces(tables, engine.MaxKeysPerStatement) {
		if err := s.tableStatPiece(ctx, "COALESCE(DATA_LENGTH, 0)", "estimate table bytes", piece, out); err != nil {
			return nil, err
		}
	}
	return out, nil
}

var _ engine.TableSizeEstimator = (*Source)(nil)

// ServerVersion returns the numeric server version (e.g. 50744 for 5.7.44) and
// refuses MariaDB (out of scope for v1).
func (s *Source) ServerVersion(ctx context.Context) (int, error) {
	if s.db == nil {
		return 0, errNotConnected
	}
	return serverVersion(ctx, s.db)
}

// --- stubs until their milestones ---

// Introspect returns the schema for the selected tables (MM1a).
func (s *Source) Introspect(ctx context.Context, sel engine.Selection) (*engine.Schema, error) {
	if s.db == nil {
		return nil, errNotConnected
	}
	return introspectDB(ctx, s.db, sel)
}

// InstallCapture/RemoveCapture are in capture.go (MM3).
// ReadDirtyKeys/ConfirmConsumed are in consume.go (MM3).
// PlanChunks is in chunk.go, CopyChunk in copy.go (MM4a).
// RereadCurrent is in reread.go (MM5a).
// Purge/DeltaBacklog are in purge.go (MM5c).

// tableStatPiece reads one information_schema.TABLES column (an expression such as
// COALESCE(TABLE_ROWS, 0)) for a piece of tables into out, keeping positive values.
func (s *Source) tableStatPiece(ctx context.Context, expr, what string, tables []engine.TableRef, out map[engine.TableRef]int64) error {
	pairs := make([]string, len(tables))
	args := make([]any, 0, len(tables)*2)
	for i, t := range tables {
		pairs[i] = "(?, ?)"
		args = append(args, t.Schema, t.Name)
	}
	rows, err := s.db.QueryContext(ctx, `SELECT TABLE_SCHEMA, TABLE_NAME, `+expr+`
FROM information_schema.TABLES WHERE (TABLE_SCHEMA, TABLE_NAME) IN (`+strings.Join(pairs, ", ")+`)`, args...)
	if err != nil {
		return fmt.Errorf("mysql: %s: %w", what, err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var schema, name string
		var n int64
		if err := rows.Scan(&schema, &name, &n); err != nil {
			return fmt.Errorf("mysql: %s: %w", what, err)
		}
		if n > 0 {
			out[engine.TableRef{Schema: schema, Name: name}] = n
		}
	}
	return rows.Err()
}
