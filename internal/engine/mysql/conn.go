package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/go-sql-driver/mysql"

	"github.com/rudimk/replicare/internal/engine"
)

// paramLocalInfile is an internal ConnConfig.Params key (rc_ namespace) carrying
// the local_infile capability hint from the config block to the engine. Internal
// (rc_-prefixed) params are NEVER forwarded to the DSN — go-sql-driver would try
// to SET them as session variables and fail. The engine reads this in MM4a.
const paramLocalInfile = "rc_local_infile"

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
	tlsVal, err := tlsParam(cc.TLS)
	if err != nil {
		return "", err
	}
	cfg.TLSConfig = tlsVal
	for k, v := range cc.Params {
		// Internal rc_ hints (e.g. local_infile) are consumed by the engine, not
		// the driver — never leak them into the DSN.
		if strings.HasPrefix(k, "rc_") {
			continue
		}
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
