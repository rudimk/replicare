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

// sessionVars is the session-variable canonicalization applied on EVERY
// connection via the DSN, so the go-sql-driver applies them (as one SET) on every
// physical connection including reconnects (mysql-plan §0.1/§0.4). These are the
// MySQL analog of the Postgres session GUCs — they make transport byte-faithful
// and deterministic without any special privilege. Applied to source and target
// alike (harmless on reads); the write-relevant flags matter on the target.
//
//   - time_zone='+00:00'           deterministic TIMESTAMP render/parse (UTC).
//   - character_set_results=binary the server returns RAW column bytes with no
//     wire transcoding, so reads are byte-faithful across a latin1<->utf8mb4 gap
//     (§0.1). Values are scanned as bytes; text is valid UTF-8 for our catalogs.
//   - sql_mode= an EXPLICIT strict-safe mode: STRICT_TRANS_TABLES+STRICT_ALL_TABLES
//     keep every out-of-range/oversize value a LOUD error (§1.7), while the
//     absence of NO_ZERO_DATE/NO_ZERO_IN_DATE lets '0000-00-00' land VERBATIM
//     (§0.4/Momus B2); NO_AUTO_VALUE_ON_ZERO makes a source 0 in an
//     auto_increment column faithful (the OVERRIDING SYSTEM VALUE analog);
//     NO_ENGINE_SUBSTITUTION fails loud rather than silently swapping engines.
//     Values quoted as the driver sends `SET <name>=<value>` verbatim.
var sessionVars = []struct{ name, value string }{
	{"time_zone", "'+00:00'"},
	{"character_set_results", "binary"},
	{"sql_mode", "'STRICT_TRANS_TABLES,STRICT_ALL_TABLES,NO_AUTO_VALUE_ON_ZERO,NO_ENGINE_SUBSTITUTION'"},
}

// dsn builds a go-sql-driver DSN from a resolved engine.ConnConfig: the full
// six-mode TLS mapping (tls.go), the session-variable canonicalization above, and
// any user-supplied Params (internal rc_ hints excluded).
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
	// Session canonicalization first; user params may not override it.
	for _, sv := range sessionVars {
		cfg.Params[sv.name] = sv.value
	}
	for k, v := range cc.Params {
		// Internal rc_ hints (e.g. local_infile) are consumed by the engine, not
		// the driver — never leak them into the DSN.
		if strings.HasPrefix(k, "rc_") {
			continue
		}
		if _, reserved := cfg.Params[k]; reserved {
			return "", fmt.Errorf("mysql: connection param %q is reserved (session canonicalization)", k)
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

// probeLocalInfile reports whether the server permits LOAD DATA LOCAL INFILE by
// reading the `local_infile` system variable. This is the copy/apply transport
// (mysql-plan §0.1); when it is OFF, the load paths halt loud with an actionable
// error rather than fail cryptically (errno 1148) mid-copy. A read error is
// surfaced (not silently assumed) so a permissions/version quirk is visible.
func probeLocalInfile(ctx context.Context, db *sql.DB) (bool, error) {
	var v sql.NullString
	if err := db.QueryRowContext(ctx, "SELECT @@GLOBAL.local_infile").Scan(&v); err != nil {
		return false, fmt.Errorf("mysql: probe local_infile: %w", err)
	}
	// The variable is 0/1 (or ON/OFF on some builds); treat anything but a clear
	// enabled value as disabled.
	switch strings.ToUpper(strings.TrimSpace(v.String)) {
	case "1", "ON":
		return true, nil
	default:
		return false, nil
	}
}

// errLocalInfileRequired is the loud, actionable halt when a target refuses
// LOAD DATA LOCAL INFILE. v1 has no INSERT fallback (deferred), so the copy/apply
// transport requires it — surfaced before any reader is consumed.
var errLocalInfileRequired = fmt.Errorf("mysql: target requires local_infile=ON for the LOAD DATA " +
	"copy/apply transport, but the server has it disabled (v1 has no INSERT fallback yet). " +
	"Enable it on the target: SET GLOBAL local_infile=1, or set local-infile=ON in my.cnf. See docs/mysql.md")

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
