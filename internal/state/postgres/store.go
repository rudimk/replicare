package statepg

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/migrate"
	"github.com/rudimk/replicare/internal/pgmigrate"
	"github.com/rudimk/replicare/internal/state"
)

// Store is the Postgres-backed StateStore. It uses a connection pool for
// checkpoint-heavy CRUD; the single-active ownership lock (Acquire) holds a
// dedicated connection for the lock's lifetime.
type Store struct {
	cfg  engine.ConnConfig
	pool *pgxpool.Pool
}

// Compile-time assertion that *Store satisfies the interface.
var _ state.StateStore = (*Store)(nil)

// New constructs (does not yet connect) a Postgres StateStore for the endpoint.
func New(cfg engine.ConnConfig) *Store {
	return &Store{cfg: cfg}
}

// Open builds the connection pool and runs migrations to the current schema
// version (F4). It is idempotent: re-running against an up-to-date schema is a
// no-op, and it never drops existing state.
func (s *Store) Open(ctx context.Context) error {
	if s.pool != nil {
		return nil
	}
	pool, err := pgxpool.New(ctx, buildDSN(s.cfg))
	if err != nil {
		return fmt.Errorf("statepg: create pool: %w", err)
	}
	if _, err := migrate.Apply(ctx, pgmigrate.New(pool), schemaSet); err != nil {
		pool.Close()
		return fmt.Errorf("statepg: migrate: %w", err)
	}
	s.pool = pool
	return nil
}

// Close releases the pool. It is safe to call on an unopened Store.
func (s *Store) Close(_ context.Context) error {
	if s.pool != nil {
		s.pool.Close()
		s.pool = nil
	}
	return nil
}

// requirePool guards operations that need an open pool.
func (s *Store) requirePool() error {
	if s.pool == nil {
		return errors.New("statepg: not open (call Open first)")
	}
	return nil
}

// buildDSN renders a libpq keyword/value DSN from the resolved ConnConfig. The
// state store stores only our own operational data, so it does not apply the
// §4.2 transport GUCs.
func buildDSN(cfg engine.ConnConfig) string {
	port := cfg.Port
	if port == 0 {
		port = 5432
	}
	sslmode := string(cfg.TLS)
	if sslmode == "" {
		sslmode = "prefer"
	}
	kv := map[string]string{
		"host":     cfg.Host,
		"port":     fmt.Sprintf("%d", port),
		"dbname":   cfg.Database,
		"user":     cfg.User,
		"password": cfg.Password,
		"sslmode":  sslmode,
	}
	keys := make([]string, 0, len(kv))
	for k := range kv {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		if b.Len() > 0 {
			b.WriteByte(' ')
		}
		fmt.Fprintf(&b, "%s=%s", k, quoteDSNValue(kv[k]))
	}
	return b.String()
}

func quoteDSNValue(v string) string {
	if v == "" {
		return "''"
	}
	if !strings.ContainsAny(v, " '\\") {
		return v
	}
	r := strings.NewReplacer(`\`, `\\`, `'`, `\'`)
	return "'" + r.Replace(v) + "'"
}
