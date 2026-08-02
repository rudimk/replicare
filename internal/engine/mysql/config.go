package mysql

import (
	"fmt"

	"gopkg.in/yaml.v3"

	"github.com/rudimk/replicare/internal/config"
	"github.com/rudimk/replicare/internal/engine"
)

const defaultPort = 3306

// defaultTLS follows a conservative default: prefer TLS but don't fail if the
// (possibly old) server can't do it, matching the Postgres block's spirit. The
// full spectrum disable -> verify-full is supported (mysql-plan §MF1).
const defaultTLS = string(engine.TLSPrefer)

// Conn is the MySQL connection block (the `mysql:` block under an endpoint).
// `database` is the DSN default database only — table selection (neutral
// include/exclude globs on the sync) may reference other databases as
// `db.table`, since a MySQL schema IS a database (mysql-plan §0.5).
type Conn struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Database string `yaml:"database"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	TLS      string `yaml:"tls"`
	// LocalInfile hints that this (target) server permits LOAD DATA LOCAL INFILE,
	// the copy/apply transport (mysql-plan §0.1). It is only a hint: the engine
	// probes the server's actual `local_infile` at connect and, when it is
	// disabled, halts the load LOUD with an actionable error (v1 has no INSERT
	// fallback — deferred; see docs/mysql.md), rather than failing cryptically
	// mid-copy. Set it true to document the requirement in config.
	LocalInfile *bool             `yaml:"local_infile"`
	Params      map[string]string `yaml:"params"`
}

// validTLSModes is the accepted set, mapped to engine.TLSMode. The same six-mode
// libpq-style spectrum as the Postgres block, for a consistent config surface;
// the driver-level mapping (incl. custom verify-ca) lives in conn.go.
var validTLSModes = map[string]engine.TLSMode{
	string(engine.TLSDisable):    engine.TLSDisable,
	string(engine.TLSAllow):      engine.TLSAllow,
	string(engine.TLSPrefer):     engine.TLSPrefer,
	string(engine.TLSRequire):    engine.TLSRequire,
	string(engine.TLSVerifyCA):   engine.TLSVerifyCA,
	string(engine.TLSVerifyFull): engine.TLSVerifyFull,
}

// EngineName satisfies config.EngineConn.
func (c *Conn) EngineName() string { return EngineName }

// Validate checks the MySQL connection block.
func (c *Conn) Validate(role, name string) error {
	if c.Host == "" {
		return fmt.Errorf("%s %q: mysql.host is required", role, name)
	}
	if c.Database == "" {
		return fmt.Errorf("%s %q: mysql.database is required", role, name)
	}
	if c.User == "" {
		return fmt.Errorf("%s %q: mysql.user is required", role, name)
	}
	if c.Port < 0 || c.Port > 65535 {
		return fmt.Errorf("%s %q: mysql.port %d is out of range", role, name, c.Port)
	}
	if c.TLS != "" {
		if _, ok := validTLSModes[c.TLS]; !ok {
			return fmt.Errorf("%s %q: invalid mysql.tls %q (valid: disable, allow, prefer, require, verify-ca, verify-full)", role, name, c.TLS)
		}
	}
	return nil
}

// ConnConfig produces the resolved low-level descriptor used by the Source/Sink.
// The local_infile hint is threaded through Params (the engine reads it in MM4a);
// it does not belong in the DSN.
func (c *Conn) ConnConfig() engine.ConnConfig {
	port := c.Port
	if port == 0 {
		port = defaultPort
	}
	mode := c.TLS
	if mode == "" {
		mode = defaultTLS
	}
	params := map[string]string{}
	for k, v := range c.Params {
		params[k] = v
	}
	if c.LocalInfile != nil {
		if *c.LocalInfile {
			params[paramLocalInfile] = "1"
		} else {
			params[paramLocalInfile] = "0"
		}
	}
	return engine.ConnConfig{
		Host:     c.Host,
		Port:     port,
		Database: c.Database,
		User:     c.User,
		Password: c.Password,
		TLS:      validTLSModes[mode],
		Params:   params,
	}
}

// parse decodes a MySQL connection block, rejecting unknown keys.
func parse(block *yaml.Node) (config.EngineConn, error) {
	var c Conn
	if err := config.StrictDecode(block, &c); err != nil {
		return nil, fmt.Errorf("mysql block: %w", err)
	}
	return &c, nil
}

func init() {
	config.RegisterEngine(EngineName, parse)
}
