// Package config loads and validates replicare's YAML configuration.
//
// The schema is a thin engine-neutral envelope (logging, observability, tuning,
// sync wiring) plus a typed per-engine connection block dispatched through the
// engine registry (CLAUDE.md §11) — each engine owns and validates its own block.
// A sync is single-engine: its source and all targets must share one engine
// (never cross-engine; CLAUDE.md §6).
//
// Env/secret resolution: any string value may use ${VAR} / ${VAR:-default}
// references (see env.go). v1 ships the Postgres connection block; MySQL/Redis
// slot in by registering their own parser.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config is the root configuration document.
type Config struct {
	Logging       Logging              `yaml:"logging"`
	Observability Observability        `yaml:"observability"`
	StateStore    *Endpoint            `yaml:"state_store"`
	Sources       map[string]*Endpoint `yaml:"sources"`
	Targets       map[string]*Endpoint `yaml:"targets"`
	Syncs         []*Sync              `yaml:"syncs"`
}

// Logging configures structured logging output.
type Logging struct {
	Level  string `yaml:"level"`  // debug|info|warn|error (default info)
	Format string `yaml:"format"` // json|text (default json)
}

// Observability configures the metrics, status, and tracing endpoints (§10).
type Observability struct {
	MetricsAddr  string `yaml:"metrics_addr"`  // Prometheus /metrics listen addr, e.g. ":9090"
	StatusAddr   string `yaml:"status_addr"`   // health/status HTTP API listen addr
	OTLPEndpoint string `yaml:"otlp_endpoint"` // OTLP traces/metrics endpoint
}

// Endpoint is a source/target/state-store database, declared as an engine name
// plus a typed per-engine connection block. The block is captured raw and parsed
// by the engine's registered parser in resolve().
type Endpoint struct {
	Engine string `yaml:"engine"`
	// Blocks captures every non-"engine" key (the inline engine block, e.g.
	// `postgres:`). resolve() requires exactly the engine-named block.
	Blocks map[string]yaml.Node `yaml:",inline"`
	// Conn is the resolved, validated engine-specific connection (set by resolve).
	Conn EngineConn `yaml:"-"`
}

// Sync is one replication job. Selection (include/exclude) is neutral glob
// syntax interpreted per engine (relational: schema.table; Redis: key patterns).
type Sync struct {
	Name    string   `yaml:"name"`
	Source  string   `yaml:"source"`  // key into Config.Sources
	Targets []string `yaml:"targets"` // keys into Config.Targets
	Include []string `yaml:"include"` // selection globs, e.g. "public.*"
	Exclude []string `yaml:"exclude"` // selection globs, e.g. "*_audit"
	Tuning  Tuning   `yaml:"tuning"`
}

// Tuning holds engine-neutral tuning knobs.
type Tuning struct {
	DrainInterval Duration  `yaml:"drain_interval"`
	Retention     Retention `yaml:"retention"`
	Pool          Pool      `yaml:"pool"`
}

// Retention bounds source-side delta retention (CLAUDE.md §3.4).
type Retention struct {
	MaxAge   Duration `yaml:"max_age"`   // 0 = unbounded by age
	MaxBytes ByteSize `yaml:"max_bytes"` // 0 = unbounded by size
}

// Pool bounds connection usage so we stay a polite client (CLAUDE.md §4.1).
type Pool struct {
	MaxSourceConns int `yaml:"max_source_connections"`
	MaxTargetConns int `yaml:"max_target_connections"`
}

// Load reads, env-expands, parses, resolves engine blocks, and validates a config
// file. Unknown fields are rejected (strict) to catch typos early.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: read %s: %w", path, err)
	}
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil, fmt.Errorf("config: %s is empty", path)
	}

	// Parse into a node tree, expand env references, then re-emit so expanded
	// scalar types re-resolve correctly before strict typed decoding.
	var doc yaml.Node
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("config: parse %s: %w", path, err)
	}
	if err := expandEnv(&doc); err != nil {
		return nil, fmt.Errorf("config: %s: %w", path, err)
	}
	expanded, err := yaml.Marshal(&doc)
	if err != nil {
		return nil, fmt.Errorf("config: re-encode %s: %w", path, err)
	}

	dec := yaml.NewDecoder(bytes.NewReader(expanded))
	dec.KnownFields(true)
	var c Config
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("config: parse %s: %w", path, err)
	}

	c.applyDefaults()
	if err := c.resolveEngines(); err != nil {
		return nil, fmt.Errorf("config: %s: %w", path, err)
	}
	if err := c.Validate(); err != nil {
		return nil, fmt.Errorf("config: invalid %s: %w", path, err)
	}
	return &c, nil
}

// applyDefaults fills sensible defaults for omitted fields.
func (c *Config) applyDefaults() {
	if c.Logging.Level == "" {
		c.Logging.Level = "info"
	}
	if c.Logging.Format == "" {
		c.Logging.Format = "json"
	}
	for _, s := range c.Syncs {
		if s.Tuning.DrainInterval == 0 {
			s.Tuning.DrainInterval = Duration(defaultDrainInterval)
		}
		if s.Tuning.Retention.MaxAge == 0 {
			s.Tuning.Retention.MaxAge = Duration(defaultRetentionMaxAge)
		}
		if s.Tuning.Pool.MaxSourceConns == 0 {
			s.Tuning.Pool.MaxSourceConns = defaultMaxConns
		}
		if s.Tuning.Pool.MaxTargetConns == 0 {
			s.Tuning.Pool.MaxTargetConns = defaultMaxConns
		}
	}
}

// resolveEngines parses and validates each endpoint's engine-specific block.
func (c *Config) resolveEngines() error {
	for name, ep := range c.Sources {
		if err := ep.resolve("source", name); err != nil {
			return err
		}
	}
	for name, ep := range c.Targets {
		if err := ep.resolve("target", name); err != nil {
			return err
		}
	}
	if c.StateStore != nil {
		if err := c.StateStore.resolve("state_store", "state_store"); err != nil {
			return err
		}
	}
	return nil
}

// resolve parses and validates the endpoint's engine-specific connection block.
func (e *Endpoint) resolve(role, name string) error {
	if e.Engine == "" {
		return fmt.Errorf("%s %q: engine is required", role, name)
	}
	parser, ok := lookupEngineParser(e.Engine)
	if !ok {
		return fmt.Errorf("%s %q: unknown engine %q (registered: %v)", role, name, e.Engine, RegisteredEngines())
	}
	block, ok := e.Blocks[e.Engine]
	if !ok {
		return fmt.Errorf("%s %q: missing %q connection block", role, name, e.Engine)
	}
	for k := range e.Blocks {
		if k != e.Engine {
			return fmt.Errorf("%s %q: unexpected key %q (expected only the %q block)", role, name, k, e.Engine)
		}
	}
	conn, err := parser(&block)
	if err != nil {
		return fmt.Errorf("%s %q: %w", role, name, err)
	}
	if err := conn.Validate(role, name); err != nil {
		return err
	}
	e.Conn = conn
	return nil
}

// Validate performs structural validation: at least one sync, unique names,
// resolvable refs, the single-engine rule, and tuning sanity.
func (c *Config) Validate() error {
	if len(c.Syncs) == 0 {
		return errors.New("no syncs defined")
	}
	if c.StateStore != nil && c.StateStore.Engine != "postgres" {
		return fmt.Errorf("state_store: engine %q is not supported in v1 (only postgres)", c.StateStore.Engine)
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
		src, ok := c.Sources[s.Source]
		if !ok {
			return fmt.Errorf("sync %q: source %q is not defined in sources", s.Name, s.Source)
		}
		if len(s.Targets) == 0 {
			return fmt.Errorf("sync %q: at least one target is required", s.Name)
		}
		for _, ref := range s.Targets {
			tgt, ok := c.Targets[ref]
			if !ok {
				return fmt.Errorf("sync %q: target %q is not defined in targets", s.Name, ref)
			}
			// Single-engine rule: never cross-engine (CLAUDE.md §6).
			if tgt.Engine != src.Engine {
				return fmt.Errorf("sync %q: cross-engine replication is not supported: source %q is %q but target %q is %q",
					s.Name, s.Source, src.Engine, ref, tgt.Engine)
			}
		}

		if s.Tuning.Pool.MaxSourceConns <= 0 || s.Tuning.Pool.MaxTargetConns <= 0 {
			return fmt.Errorf("sync %q: pool connection limits must be positive", s.Name)
		}
		if s.Tuning.DrainInterval <= 0 {
			return fmt.Errorf("sync %q: drain_interval must be positive", s.Name)
		}
	}
	return nil
}
