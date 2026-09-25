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
	"strings"

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
	// Nodes are the endpoints that participate in an active-active cluster. Unlike a
	// source or target, a node is both read from (capture/snapshot) and written to
	// (apply). It is a SEPARATE map from sources/targets so the one-way surface is
	// untouched; a node is referenced by name from a cluster's members. Optional —
	// present only for multi-master configs (see docs/multi-master.md).
	Nodes map[string]*Endpoint `yaml:"nodes"`
	// Clusters are active-active (multi-master) replication groups. Optional and
	// additive: a config with no clusters behaves exactly like a one-way daemon.
	Clusters []*Cluster `yaml:"clusters"`
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
	// StallTimeout is how long a sync's streaming loop may go without completing an
	// iteration before /healthz reports unhealthy (so Kubernetes restarts a wedged
	// pod). It must exceed the slowest expected drain pass. Unset/0 = default 2m; a
	// negative value disables the staleness check.
	StallTimeout Duration `yaml:"stall_timeout"`
}

// Endpoint is a source/target/state-store database, declared as an engine name
// plus a typed per-engine connection block. The block is captured raw and parsed
// by the engine's registered parser in resolve().
type Endpoint struct {
	Engine string `yaml:"engine"`
	// NodeID is the stable replication-origin identity of this endpoint when it is a
	// member of a cluster (multi-master). It is the value stamped into every change's
	// (hlc, node_id) version and must be unique within a cluster. Optional and ignored
	// on the one-way path; for a cluster member it defaults to the node's map key.
	// Declared as an explicit field (not inline) so it is not mistaken for an engine
	// block by resolve().
	NodeID string `yaml:"node_id"`
	// Blocks captures every non-"engine"/"node_id" key (the inline engine block, e.g.
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
	// Enabled gates whether this sync runs. Unset (nil) or true → the daemon brings
	// it up and streams it (the default; a config without this field behaves exactly
	// as before). false → the sync is PAUSED: the daemon skips it at startup and does
	// not acquire its ownership lock. Existing source capture triggers are left in
	// place, so deltas keep queuing and a later unpause (enabled: true + restart)
	// drains the backlog with no data loss — at the cost of source-side delta growth
	// while paused (retention enforcement runs in the streaming loop, which is
	// skipped). Takes effect at daemon start, so pausing/resuming is a config change +
	// restart, not a live toggle.
	Enabled *bool `yaml:"enabled"`
}

// IsEnabled reports whether the sync should run. The zero/unset value is enabled, so
// the flag is a pure opt-out and every pre-existing config keeps running unchanged.
func (s *Sync) IsEnabled() bool { return s.Enabled == nil || *s.Enabled }

// Cluster is one active-active (multi-master) replication group: a set of peer
// nodes, each simultaneously a source and a target, kept converged with writes
// accepted on any node. Conflict resolution is zero-config (replicare-managed HLC
// last-write-wins; see docs/multi-master.md §5.3) — there is nothing to declare
// beyond membership. Single-engine, like a sync. Additive and opt-in: a config with
// no clusters is exactly today's one-way daemon.
type Cluster struct {
	Name    string   `yaml:"name"`
	Engine  string   `yaml:"engine"`  // all members must share this engine
	Members []string `yaml:"members"` // keys into Config.Nodes; each is source AND target
	// Topology is the edge shape. v1 supports only "mesh" (full mesh, any N >= 2);
	// "ring"/"explicit" partial topologies are deferred (docs/multi-master.md §5.2).
	// Defaults to "mesh".
	Topology string   `yaml:"topology"`
	Include  []string `yaml:"include"` // selection globs, engine-interpreted (as syncs)
	Exclude  []string `yaml:"exclude"`
	Tuning   Tuning   `yaml:"tuning"`
	// Enabled gates whether this cluster runs, with the same opt-out semantics as
	// Sync.Enabled: unset/true runs every mesh edge; false pauses the whole cluster
	// (the daemon skips all of its edges at startup). Per-edge pausing is not exposed.
	Enabled *bool `yaml:"enabled"`
}

// IsEnabled reports whether the cluster should run. Unset value is enabled.
func (c *Cluster) IsEnabled() bool { return c.Enabled == nil || *c.Enabled }

// Tuning holds engine-neutral tuning knobs.
type Tuning struct {
	DrainInterval    Duration  `yaml:"drain_interval"`
	DrainBatch       int       `yaml:"drain_batch"`
	ApplyConcurrency int       `yaml:"apply_concurrency"`
	Retention        Retention `yaml:"retention"`
	Pool             Pool      `yaml:"pool"`
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
	if c.Observability.StallTimeout == 0 {
		c.Observability.StallTimeout = Duration(defaultStallTimeout)
	}
	for _, s := range c.Syncs {
		applyTuningDefaults(&s.Tuning)
	}
	// Cluster nodes: default each node's replication origin id to its map key.
	for name, n := range c.Nodes {
		if n.NodeID == "" {
			n.NodeID = name
		}
	}
	// Clusters share the sync tuning defaults; topology defaults to a full mesh.
	for _, cl := range c.Clusters {
		if cl.Topology == "" {
			cl.Topology = topologyMesh
		}
		applyTuningDefaults(&cl.Tuning)
	}
}

// applyTuningDefaults fills omitted tuning knobs — shared by syncs and clusters so
// the two stay in lockstep.
func applyTuningDefaults(t *Tuning) {
	if t.DrainInterval == 0 {
		t.DrainInterval = Duration(defaultDrainInterval)
	}
	if t.DrainBatch == 0 {
		t.DrainBatch = defaultDrainBatch
	}
	if t.ApplyConcurrency == 0 {
		t.ApplyConcurrency = defaultApplyConcurrency
	}
	if t.Retention.MaxAge == 0 {
		t.Retention.MaxAge = Duration(defaultRetentionMaxAge)
	}
	if t.Pool.MaxSourceConns == 0 {
		t.Pool.MaxSourceConns = defaultMaxConns
	}
	if t.Pool.MaxTargetConns == 0 {
		t.Pool.MaxTargetConns = defaultMaxConns
	}
}

// topologyMesh is the only cluster topology supported in v1 (full mesh, any N >= 2).
const topologyMesh = "mesh"

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
	for name, ep := range c.Nodes {
		if err := ep.resolve("node", name); err != nil {
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
	if len(c.Syncs) == 0 && len(c.Clusters) == 0 {
		return errors.New("no syncs or clusters defined")
	}
	if c.StateStore != nil && c.StateStore.Engine != "postgres" {
		return fmt.Errorf("state_store: engine %q is not supported in v1 (only postgres)", c.StateStore.Engine)
	}

	// Sync and cluster names share one namespace (each will own a distinct advisory
	// lock / cursor namespace at runtime), so they must be collectively unique.
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
		if s.Tuning.DrainBatch <= 0 {
			return fmt.Errorf("sync %q: drain_batch must be positive", s.Name)
		}
		if s.Tuning.ApplyConcurrency < 1 {
			return fmt.Errorf("sync %q: apply_concurrency must be >= 1", s.Name)
		}
	}

	for i, cl := range c.Clusters {
		if err := c.validateCluster(i, cl, seen); err != nil {
			return err
		}
	}

	// Reject an un-declared replication cycle among one-way syncs (A->B + B->A, or a
	// ring). That topology loops changes forever and corrupts data (there is no origin
	// filtering on the one-way path); before this check it was accepted silently. A
	// genuine active-active topology must be declared as a cluster, which is exempt
	// (clusters are not syncs). See docs/multi-master.md §4.
	if err := c.detectSyncCycles(); err != nil {
		return err
	}
	return nil
}

// connIdentity is a database's identity for cycle detection: same engine + host +
// port + database ⇒ the same physical DB, regardless of the endpoint name, user, or
// TLS mode it was reached through. This is what lets us catch A->B + B->A even when
// the two directions name the same DB differently.
func connIdentity(ep *Endpoint) string {
	if ep == nil || ep.Conn == nil {
		return ""
	}
	cc := ep.Conn.ConnConfig()
	return fmt.Sprintf("%s://%s:%d/%s", ep.Engine, cc.Host, cc.Port, cc.Database)
}

// detectSyncCycles builds the directed graph of one-way syncs (each source → each of
// its targets, keyed by connection identity) and rejects any cycle, including a
// self-loop (a sync whose source and target are the same DB). Clusters are excluded
// by construction — they are the sanctioned bidirectional topology.
func (c *Config) detectSyncCycles() error {
	adj := map[string]map[string]bool{}
	addNode := func(id string) {
		if _, ok := adj[id]; !ok {
			adj[id] = map[string]bool{}
		}
	}
	for _, s := range c.Syncs {
		from := connIdentity(c.Sources[s.Source])
		addNode(from)
		for _, ref := range s.Targets {
			to := connIdentity(c.Targets[ref])
			addNode(to)
			adj[from][to] = true
		}
	}

	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := map[string]int{}
	var stack, cycle []string
	var dfs func(string) bool
	dfs = func(n string) bool {
		color[n] = gray
		stack = append(stack, n)
		for m := range adj[n] {
			switch color[m] {
			case gray:
				start := 0
				for i, x := range stack {
					if x == m {
						start = i
						break
					}
				}
				cycle = append(append([]string{}, stack[start:]...), m)
				return true
			case white:
				if dfs(m) {
					return true
				}
			}
		}
		stack = stack[:len(stack)-1]
		color[n] = black
		return false
	}
	for n := range adj {
		if color[n] == white && dfs(n) {
			return fmt.Errorf("replication cycle detected among one-way syncs (%s): this loops changes and corrupts data — declare an active-active topology as a `clusters:` entry instead (docs/multi-master.md)",
				strings.Join(cycle, " -> "))
		}
	}
	return nil
}

// validateCluster checks one active-active cluster: a unique name, an engine, at
// least two members that all resolve to nodes of that one engine, unique node ids,
// a supported topology, and sane tuning. Conflict resolution is zero-config, so
// there is no policy to validate.
func (c *Config) validateCluster(i int, cl *Cluster, seen map[string]bool) error {
	if cl.Name == "" {
		return fmt.Errorf("clusters[%d]: name is required", i)
	}
	if seen[cl.Name] {
		return fmt.Errorf("duplicate sync/cluster name %q", cl.Name)
	}
	seen[cl.Name] = true

	if cl.Engine == "" {
		return fmt.Errorf("cluster %q: engine is required", cl.Name)
	}
	if cl.Topology != topologyMesh {
		return fmt.Errorf("cluster %q: topology %q is not supported in v1 (only %q — full mesh; ring/partial are deferred, see docs/multi-master.md)",
			cl.Name, cl.Topology, topologyMesh)
	}
	if len(cl.Members) < 2 {
		return fmt.Errorf("cluster %q: at least two members are required (an active-active cluster needs >= 2 nodes)", cl.Name)
	}

	nodeIDs := map[string]bool{}
	memberNames := map[string]bool{}
	for _, ref := range cl.Members {
		if memberNames[ref] {
			return fmt.Errorf("cluster %q: member %q listed more than once", cl.Name, ref)
		}
		memberNames[ref] = true

		node, ok := c.Nodes[ref]
		if !ok {
			return fmt.Errorf("cluster %q: member %q is not defined in nodes", cl.Name, ref)
		}
		// Single-engine rule, as for syncs (CLAUDE.md §6): a cluster never crosses engines.
		if node.Engine != cl.Engine {
			return fmt.Errorf("cluster %q: cross-engine replication is not supported: cluster engine is %q but member %q is %q",
				cl.Name, cl.Engine, ref, node.Engine)
		}
		if nodeIDs[node.NodeID] {
			return fmt.Errorf("cluster %q: duplicate node_id %q (members must have distinct node ids)", cl.Name, node.NodeID)
		}
		nodeIDs[node.NodeID] = true
	}

	if cl.Tuning.Pool.MaxSourceConns <= 0 || cl.Tuning.Pool.MaxTargetConns <= 0 {
		return fmt.Errorf("cluster %q: pool connection limits must be positive", cl.Name)
	}
	if cl.Tuning.DrainInterval <= 0 {
		return fmt.Errorf("cluster %q: drain_interval must be positive", cl.Name)
	}
	if cl.Tuning.DrainBatch <= 0 {
		return fmt.Errorf("cluster %q: drain_batch must be positive", cl.Name)
	}
	if cl.Tuning.ApplyConcurrency < 1 {
		return fmt.Errorf("cluster %q: apply_concurrency must be >= 1", cl.Name)
	}
	return nil
}
