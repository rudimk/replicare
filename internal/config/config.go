// Package config loads and validates replicare's YAML configuration.
//
// This is the M0 skeleton: the top-level shape (endpoints + syncs + logging) with
// basic structural validation and strict unknown-field rejection. The full
// schema — tuning knobs (worker/chunk/batch/drain/retention/pool), env-var
// override of any field, secret resolution, and per-connection TLS plumbing — is
// designed and locked in M0.5 (see .sisyphus plan F1; CLAUDE.md §11).
package config

import (
	"errors"
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config is the root configuration document.
type Config struct {
	Logging Logging             `yaml:"logging"`
	Sources map[string]Endpoint `yaml:"sources"`
	Targets map[string]Endpoint `yaml:"targets"`
	Syncs   []Sync              `yaml:"syncs"`
}

// Logging configures structured logging output.
type Logging struct {
	Level  string `yaml:"level"`  // debug|info|warn|error (default info)
	Format string `yaml:"format"` // json|text (default json)
}

// Endpoint is a source or target database connection.
//
// NOTE (M0.5): Password is inline here for now; env-override and secret
// resolution, plus richer TLS settings, are added in M0.5 (CLAUDE.md §11).
type Endpoint struct {
	Engine   string `yaml:"engine"`
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Database string `yaml:"database"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	TLS      string `yaml:"tls"` // disable|allow|prefer|require|verify-ca|verify-full
}

// Sync is one replication job: a source, one or more targets, and a table
// selection (include/exclude globs; CLAUDE.md §11).
type Sync struct {
	Name    string   `yaml:"name"`
	Source  string   `yaml:"source"`  // key into Config.Sources
	Targets []string `yaml:"targets"` // keys into Config.Targets
	Include []string `yaml:"include"` // schema globs, e.g. "public.*"
	Exclude []string `yaml:"exclude"` // schema globs, e.g. "*_audit"
}

// Load reads and parses a YAML config file, applies defaults, and validates it.
// Unknown fields are rejected (strict) to catch typos early.
func Load(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("config: open %s: %w", path, err)
	}
	defer f.Close()

	dec := yaml.NewDecoder(f)
	dec.KnownFields(true)

	var c Config
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("config: parse %s: %w", path, err)
	}

	c.applyDefaults()
	if err := c.Validate(); err != nil {
		return nil, fmt.Errorf("config: invalid %s: %w", path, err)
	}
	return &c, nil
}

// applyDefaults fills in sensible defaults for omitted fields.
func (c *Config) applyDefaults() {
	if c.Logging.Level == "" {
		c.Logging.Level = "info"
	}
	if c.Logging.Format == "" {
		c.Logging.Format = "json"
	}
}

// Validate performs basic structural checks. Deep validation (tuning ranges, TLS
// modes, glob syntax) is added in M0.5.
func (c *Config) Validate() error {
	if len(c.Syncs) == 0 {
		return errors.New("no syncs defined")
	}

	seen := map[string]bool{}
	for i, s := range c.Syncs {
		if s.Name == "" {
			return fmt.Errorf("syncs[%d]: name is required", i)
		}
		if seen[s.Name] {
			return fmt.Errorf("duplicate sync name %q", s.Name)
		}
		seen[s.Name] = true

		if s.Source == "" {
			return fmt.Errorf("sync %q: source is required", s.Name)
		}
		if _, ok := c.Sources[s.Source]; !ok {
			return fmt.Errorf("sync %q: source %q is not defined in sources", s.Name, s.Source)
		}
		if len(s.Targets) == 0 {
			return fmt.Errorf("sync %q: at least one target is required", s.Name)
		}
		for _, tgt := range s.Targets {
			if _, ok := c.Targets[tgt]; !ok {
				return fmt.Errorf("sync %q: target %q is not defined in targets", s.Name, tgt)
			}
		}
	}

	for name, ep := range c.Sources {
		if err := ep.validate("source", name); err != nil {
			return err
		}
	}
	for name, ep := range c.Targets {
		if err := ep.validate("target", name); err != nil {
			return err
		}
	}
	return nil
}

func (e Endpoint) validate(kind, name string) error {
	if e.Engine == "" {
		return fmt.Errorf("%s %q: engine is required", kind, name)
	}
	if e.Host == "" {
		return fmt.Errorf("%s %q: host is required", kind, name)
	}
	if e.Database == "" {
		return fmt.Errorf("%s %q: database is required", kind, name)
	}
	return nil
}
