package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"strconv"

	"github.com/go-sql-driver/mysql"

	"github.com/rudimk/replicare/internal/engine"
)

// dsn builds a go-sql-driver DSN from a resolved engine.ConnConfig. MM0 wires
// connectivity + a minimal TLS mapping (disable vs. verify-full); the full
// six-mode mapping and session-variable canonicalization (time_zone, sql_mode,
// character_set_results=binary, ...) land in MM0.5/MM1a. Any extra Params are
// threaded through as DSN parameters.
func dsn(cc engine.ConnConfig) (string, error) {
	cfg := mysql.NewConfig()
	cfg.Net = "tcp"
	cfg.Addr = net.JoinHostPort(cc.Host, strconv.Itoa(cc.Port))
	cfg.DBName = cc.Database
	cfg.User = cc.User
	cfg.Passwd = cc.Password
	// Parse DATE/DATETIME/TIMESTAMP without the driver rejecting zero-dates: the
	// faithful-transport paths (MM1a/MM4) read values as raw bytes, so the driver
	// must not interpret or reject them. MM1a pins the full session canon.
	cfg.Params = map[string]string{}
	switch cc.TLS {
	case engine.TLSDisable, "":
		cfg.TLSConfig = "false"
	default:
		// MM0 minimal: anything stricter than disable requests TLS with full
		// verification; the precise six-mode mapping (incl. custom verify-ca) is
		// MM0.5. This never silently downgrades — an unmapped mode gets the
		// strictest driver setting.
		cfg.TLSConfig = "true"
	}
	for k, v := range cc.Params {
		if _, reserved := cfg.Params[k]; reserved {
			return "", fmt.Errorf("mysql: connection param %q is reserved", k)
		}
		cfg.Params[k] = v
	}
	return cfg.FormatDSN(), nil
}

// open opens a *sql.DB constrained to a single underlying connection, matching
// the "one connection, not concurrent-safe" contract the Source/Sink rely on
// (session variables pinned in MM1a must apply to the one connection every query
// uses; parallelism comes from multiple Source/Sink instances, not a pool).
func open(ctx context.Context, cc engine.ConnConfig) (*sql.DB, error) {
	d, err := dsn(cc)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("mysql", d)
	if err != nil {
		return nil, fmt.Errorf("mysql: open: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("mysql: connect %s:%d: %w", cc.Host, cc.Port, err)
	}
	return db, nil
}

// serverVersion queries the connected server's version, rejects MariaDB (out of
// scope for v1), and returns the comparable version number (§1.6).
func serverVersion(ctx context.Context, db *sql.DB) (int, error) {
	var v string
	if err := db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&v); err != nil {
		return 0, fmt.Errorf("mysql: read server version: %w", err)
	}
	if isMariaDB(v) {
		return 0, fmt.Errorf("mysql: server reports MariaDB (%q); MariaDB is not supported in v1 "+
			"(see .sisyphus/mysql-plan.md, Open Q4)", v)
	}
	return serverVersionNum(v)
}
