// Package postgres implements the Postgres engine for replicare. In M0.5 it
// provides the typed connection-config block and registers it with the config
// engine registry; the Source/Sink implementation (pgx) lands in M1.
package postgres

import (
	"fmt"

	"gopkg.in/yaml.v3"

	"github.com/rudimk/replicare/internal/config"
	"github.com/rudimk/replicare/internal/engine"
)

// EngineName is the engine identifier used in config and the registry.
const EngineName = "postgres"

const defaultPort = 5432

// defaultSSLMode follows libpq's default. Override per connection; the full
// spectrum disable -> verify-full is supported (CLAUDE.md §11).
const defaultSSLMode = string(engine.TLSPrefer)

// Conn is the Postgres connection block (the `postgres:` block under an endpoint).
type Conn struct {
	Host     string            `yaml:"host"`
	Port     int               `yaml:"port"`
	Database string            `yaml:"database"`
	User     string            `yaml:"user"`
	Password string            `yaml:"password"`
	SSLMode  string            `yaml:"sslmode"`
	Params   map[string]string `yaml:"params"`
}

// validSSLModes is the accepted set, mapped to engine.TLSMode.
var validSSLModes = map[string]engine.TLSMode{
	string(engine.TLSDisable):    engine.TLSDisable,
	string(engine.TLSAllow):      engine.TLSAllow,
	string(engine.TLSPrefer):     engine.TLSPrefer,
	string(engine.TLSRequire):    engine.TLSRequire,
	string(engine.TLSVerifyCA):   engine.TLSVerifyCA,
	string(engine.TLSVerifyFull): engine.TLSVerifyFull,
}

// EngineName satisfies config.EngineConn.
func (c *Conn) EngineName() string { return EngineName }

// Validate checks the Postgres connection block.
func (c *Conn) Validate(role, name string) error {
	if c.Host == "" {
		return fmt.Errorf("%s %q: postgres.host is required", role, name)
	}
	if c.Database == "" {
		return fmt.Errorf("%s %q: postgres.database is required", role, name)
	}
	if c.User == "" {
		return fmt.Errorf("%s %q: postgres.user is required", role, name)
	}
	if c.Port < 0 || c.Port > 65535 {
		return fmt.Errorf("%s %q: postgres.port %d is out of range", role, name, c.Port)
	}
	if c.SSLMode != "" {
		if _, ok := validSSLModes[c.SSLMode]; !ok {
			return fmt.Errorf("%s %q: invalid postgres.sslmode %q (valid: disable, allow, prefer, require, verify-ca, verify-full)", role, name, c.SSLMode)
		}
	}
	return nil
}

// ConnConfig produces the resolved low-level descriptor used by the Source/Sink.
func (c *Conn) ConnConfig() engine.ConnConfig {
	port := c.Port
	if port == 0 {
		port = defaultPort
	}
	mode := c.SSLMode
	if mode == "" {
		mode = defaultSSLMode
	}
	return engine.ConnConfig{
		Host:     c.Host,
		Port:     port,
		Database: c.Database,
		User:     c.User,
		Password: c.Password,
		TLS:      validSSLModes[mode],
		Params:   c.Params,
	}
}

// parse decodes a Postgres connection block, rejecting unknown keys.
func parse(block *yaml.Node) (config.EngineConn, error) {
	var c Conn
	if err := config.StrictDecode(block, &c); err != nil {
		return nil, fmt.Errorf("postgres block: %w", err)
	}
	return &c, nil
}

func init() {
	config.RegisterEngine(EngineName, parse)
}
