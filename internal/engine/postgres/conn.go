package postgres

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/rudimk/replicare/internal/engine"
)

// sessionGUCs is the session-GUC canonicalization applied on EVERY source-read
// and target-write connection (CLAUDE.md §4.2). These change *formatting only*
// and require no special privileges; they make text COPY round-trip faithfully
// across very different server versions. See the CLAUDE.md §4.2 table.
//
// lc_monetary is intentionally NOT pinned here: it is locale-dependent, only
// affects the `money` type (which we warn against), and SET lc_monetary can fail
// if the locale is unavailable on the server. It is pinned opportunistically by
// pinMoneyLocale only when a selected table actually uses `money`.
var sessionGUCs = []struct{ name, value string }{
	{"DateStyle", "ISO, YMD"},     // unambiguous date text + input order
	{"TimeZone", "UTC"},           // deterministic timestamptz render/parse
	{"extra_float_digits", "3"},   // round-trippable float4/float8 (critical <PG12)
	{"IntervalStyle", "postgres"}, // stable interval text
	{"bytea_output", "hex"},       // consistent bytea text
	{"client_encoding", "UTF8"},   // well-defined transcoding; invalid bytes error loudly
}

// sslmodeParam maps our engine.TLSMode to the libpq sslmode connection keyword.
// pgx honours the standard libpq sslmode spectrum (CLAUDE.md §11).
func sslmodeParam(m engine.TLSMode) string {
	if m == "" {
		return defaultSSLMode
	}
	return string(m)
}

// pgxConfig builds a *pgx.ConnConfig from a resolved engine.ConnConfig. It sets
// the sslmode and threads any extra Params through as connection parameters, but
// does not open a connection.
func pgxConfig(cc engine.ConnConfig) (*pgx.ConnConfig, error) {
	// Build a keyword/value DSN so pgx parses sslmode and params the libpq way.
	kv := map[string]string{
		"host":     cc.Host,
		"port":     fmt.Sprintf("%d", cc.Port),
		"dbname":   cc.Database,
		"user":     cc.User,
		"password": cc.Password,
		"sslmode":  sslmodeParam(cc.TLS),
	}
	for k, v := range cc.Params {
		// Never let a caller-supplied param override the fields we control.
		if _, reserved := kv[k]; reserved {
			return nil, fmt.Errorf("postgres: connection param %q is reserved", k)
		}
		kv[k] = v
	}

	// Deterministic DSN ordering keeps errors and logs stable across runs.
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

	cfg, err := pgx.ParseConfig(b.String())
	if err != nil {
		return nil, fmt.Errorf("postgres: parse connection config: %w", err)
	}
	return cfg, nil
}

// quoteDSNValue quotes a libpq keyword/value DSN value if it is empty or contains
// characters that would otherwise break parsing (space, quote, backslash).
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

// connect opens a pgx connection and applies session-GUC canonicalization before
// returning it. Every connection replicare opens — source or target — goes
// through here so the §4.2 canonicalization is never accidentally skipped.
func connect(ctx context.Context, cc engine.ConnConfig) (*pgx.Conn, error) {
	cfg, err := pgxConfig(cc)
	if err != nil {
		return nil, err
	}
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("postgres: connect to %s:%d/%s: %w", cc.Host, cc.Port, cc.Database, err)
	}
	if err := applySessionGUCs(ctx, conn); err != nil {
		_ = conn.Close(ctx)
		return nil, err
	}
	return conn, nil
}

// applySessionGUCs pins the canonicalization GUCs on an open connection. Each is
// a session-level SET requiring no special privilege (CLAUDE.md §4.2).
func applySessionGUCs(ctx context.Context, conn *pgx.Conn) error {
	for _, g := range sessionGUCs {
		// GUC names are from our fixed allowlist above (never user input), so
		// interpolating the name is safe; the value is a fixed literal too.
		if _, err := conn.Exec(ctx, fmt.Sprintf("SET %s = %s", g.name, quoteLiteral(g.value))); err != nil {
			return fmt.Errorf("postgres: canonicalize session (SET %s): %w", g.name, err)
		}
	}
	return nil
}

// quoteLiteral single-quotes a SQL string literal, doubling embedded quotes. Used
// only for our fixed GUC values, but kept correct regardless.
func quoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// serverVersion returns the numeric server version (e.g. 90600 for 9.6, 170000
// for 17) so version-tolerant code paths can branch (CLAUDE.md §1.6). It reads
// the server_version_num GUC, which exists on every supported server. SHOW
// yields a text value, so we scan a string and parse it.
func serverVersion(ctx context.Context, conn *pgx.Conn) (int, error) {
	var s string
	if err := conn.QueryRow(ctx, "SHOW server_version_num").Scan(&s); err != nil {
		return 0, fmt.Errorf("postgres: read server_version_num: %w", err)
	}
	num, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil {
		return 0, fmt.Errorf("postgres: parse server_version_num %q: %w", s, err)
	}
	return num, nil
}
