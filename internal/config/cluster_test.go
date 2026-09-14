package config

import (
	"strings"
	"sync"
	"testing"
)

// A second registered engine so cross-engine cluster validation can be exercised.
var registerFake2Once sync.Once

func registerFake2(t *testing.T) {
	t.Helper()
	registerFake2Once.Do(func() { RegisterEngine("fake2", parseFake) })
}

const clusterYAML = `
nodes:
  us:
    engine: fake
    fake: { dsn: "fake://us" }
  eu:
    engine: fake
    fake: { dsn: "fake://eu" }
  ap:
    engine: fake
    node_id: ap-1
    fake: { dsn: "fake://ap" }
clusters:
  - name: global-app
    engine: fake
    members: [us, eu, ap]
    include: ["public.*"]
    exclude: ["*_audit"]
`

// TestLoadClusterValid: a clusters-only config (no syncs) parses, defaults apply,
// members resolve, and node_id defaults to the map key (overridable).
func TestLoadClusterValid(t *testing.T) {
	registerFake(t)
	c, err := Load(writeTemp(t, clusterYAML))
	if err != nil {
		t.Fatalf("Load cluster: %v", err)
	}
	if len(c.Syncs) != 0 {
		t.Fatalf("expected no syncs, got %d", len(c.Syncs))
	}
	if len(c.Clusters) != 1 {
		t.Fatalf("expected 1 cluster, got %d", len(c.Clusters))
	}
	cl := c.Clusters[0]
	if cl.Topology != topologyMesh {
		t.Errorf("topology = %q, want default %q", cl.Topology, topologyMesh)
	}
	// Tuning defaults applied to clusters exactly like syncs.
	if cl.Tuning.DrainInterval.Duration() != defaultDrainInterval {
		t.Errorf("cluster drain_interval = %v, want default %v", cl.Tuning.DrainInterval, defaultDrainInterval)
	}
	if cl.Tuning.DrainBatch != defaultDrainBatch || cl.Tuning.ApplyConcurrency != defaultApplyConcurrency {
		t.Errorf("cluster tuning defaults not applied: %+v", cl.Tuning)
	}
	// node_id defaults to the map key; explicit node_id wins.
	if c.Nodes["us"].NodeID != "us" || c.Nodes["eu"].NodeID != "eu" {
		t.Errorf("node_id default = %q/%q, want us/eu", c.Nodes["us"].NodeID, c.Nodes["eu"].NodeID)
	}
	if c.Nodes["ap"].NodeID != "ap-1" {
		t.Errorf("explicit node_id = %q, want ap-1", c.Nodes["ap"].NodeID)
	}
	// Node connections resolved through the engine registry.
	if c.Nodes["us"].Conn == nil || c.Nodes["us"].Conn.EngineName() != "fake" {
		t.Errorf("node conn not resolved: %+v", c.Nodes["us"].Conn)
	}
}

// TestClusterAndSyncsCoexist: clusters and one-way syncs live side by side.
func TestClusterAndSyncsCoexist(t *testing.T) {
	registerFake(t)
	yml := validYAML + `
nodes:
  n1: { engine: fake, fake: { dsn: "fake://n1" } }
  n2: { engine: fake, fake: { dsn: "fake://n2" } }
clusters:
  - name: mesh1
    engine: fake
    members: [n1, n2]
`
	c, err := Load(writeTemp(t, yml))
	if err != nil {
		t.Fatalf("Load coexist: %v", err)
	}
	if len(c.Syncs) != 1 || len(c.Clusters) != 1 {
		t.Fatalf("want 1 sync + 1 cluster, got %d/%d", len(c.Syncs), len(c.Clusters))
	}
}

// TestOneWayConfigLeavesClusterFieldsNil: a config with neither nodes nor clusters
// resolves with those fields nil — the BC guarantee that the one-way surface is
// untouched.
func TestOneWayConfigLeavesClusterFieldsNil(t *testing.T) {
	registerFake(t)
	c, err := Load(writeTemp(t, validYAML))
	if err != nil {
		t.Fatalf("Load one-way: %v", err)
	}
	if c.Nodes != nil || c.Clusters != nil {
		t.Errorf("one-way config populated cluster fields: nodes=%v clusters=%v", c.Nodes, c.Clusters)
	}
}

func TestClusterValidationErrors(t *testing.T) {
	registerFake(t)
	registerFake2(t)
	cases := []struct {
		name string
		yaml string
		want string
	}{
		{
			name: "too few members",
			yaml: `
nodes:
  a: { engine: fake, fake: { dsn: "fake://a" } }
clusters:
  - name: c1
    engine: fake
    members: [a]
`,
			want: "at least two members",
		},
		{
			name: "unknown member",
			yaml: `
nodes:
  a: { engine: fake, fake: { dsn: "fake://a" } }
clusters:
  - name: c1
    engine: fake
    members: [a, ghost]
`,
			want: `member "ghost" is not defined in nodes`,
		},
		{
			name: "cross-engine member",
			yaml: `
nodes:
  a: { engine: fake, fake: { dsn: "fake://a" } }
  b: { engine: fake2, fake2: { dsn: "fake://b" } }
clusters:
  - name: c1
    engine: fake
    members: [a, b]
`,
			want: "cross-engine replication is not supported",
		},
		{
			name: "duplicate node_id",
			yaml: `
nodes:
  a: { engine: fake, node_id: dup, fake: { dsn: "fake://a" } }
  b: { engine: fake, node_id: dup, fake: { dsn: "fake://b" } }
clusters:
  - name: c1
    engine: fake
    members: [a, b]
`,
			want: `duplicate node_id "dup"`,
		},
		{
			name: "member listed twice",
			yaml: `
nodes:
  a: { engine: fake, fake: { dsn: "fake://a" } }
clusters:
  - name: c1
    engine: fake
    members: [a, a]
`,
			want: `member "a" listed more than once`,
		},
		{
			name: "unsupported topology",
			yaml: `
nodes:
  a: { engine: fake, fake: { dsn: "fake://a" } }
  b: { engine: fake, fake: { dsn: "fake://b" } }
clusters:
  - name: c1
    engine: fake
    topology: ring
    members: [a, b]
`,
			want: "topology \"ring\" is not supported",
		},
		{
			name: "missing name",
			yaml: `
nodes:
  a: { engine: fake, fake: { dsn: "fake://a" } }
  b: { engine: fake, fake: { dsn: "fake://b" } }
clusters:
  - engine: fake
    members: [a, b]
`,
			want: "name is required",
		},
		{
			name: "name collides with sync",
			yaml: validYAML + `
nodes:
  a: { engine: fake, fake: { dsn: "fake://a" } }
  b: { engine: fake, fake: { dsn: "fake://b" } }
clusters:
  - name: app-to-warehouse
    engine: fake
    members: [a, b]
`,
			want: `duplicate sync/cluster name "app-to-warehouse"`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(writeTemp(t, tc.yaml))
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %q, want containing %q", err.Error(), tc.want)
			}
		})
	}
}

// TestSyncCycleDetection: MM1 — an un-declared cycle among one-way syncs (by
// connection identity, not endpoint name) is rejected; legitimate DAGs pass.
func TestSyncCycleDetection(t *testing.T) {
	registerFake(t)
	fk := func(name, dsn string) string {
		return name + `: { engine: fake, fake: { dsn: "` + dsn + `" } }`
	}
	cases := []struct {
		name      string
		yaml      string
		wantCycle bool
	}{
		{
			name: "two-node cycle A<->B",
			yaml: `
sources:
  ` + fk("a", "db://a") + `
  ` + fk("b", "db://b") + `
targets:
  ` + fk("a", "db://a") + `
  ` + fk("b", "db://b") + `
syncs:
  - { name: a2b, source: a, targets: [b] }
  - { name: b2a, source: b, targets: [a] }
`,
			wantCycle: true,
		},
		{
			name: "self loop A->A",
			yaml: `
sources:
  ` + fk("a", "db://x") + `
targets:
  ` + fk("a", "db://x") + `
syncs:
  - { name: s, source: a, targets: [a] }
`,
			wantCycle: true,
		},
		{
			name: "three-node ring A->B->C->A",
			yaml: `
sources:
  ` + fk("a", "db://a") + `
  ` + fk("b", "db://b") + `
  ` + fk("c", "db://c") + `
targets:
  ` + fk("a", "db://a") + `
  ` + fk("b", "db://b") + `
  ` + fk("c", "db://c") + `
syncs:
  - { name: a2b, source: a, targets: [b] }
  - { name: b2c, source: b, targets: [c] }
  - { name: c2a, source: c, targets: [a] }
`,
			wantCycle: true,
		},
		{
			name: "fan-out A->{B,C} is fine",
			yaml: `
sources:
  ` + fk("a", "db://a") + `
targets:
  ` + fk("b", "db://b") + `
  ` + fk("c", "db://c") + `
syncs:
  - { name: fan, source: a, targets: [b, c] }
`,
			wantCycle: false,
		},
		{
			name: "chain A->B->C is fine",
			yaml: `
sources:
  ` + fk("a", "db://a") + `
  ` + fk("b", "db://b") + `
targets:
  ` + fk("b", "db://b") + `
  ` + fk("c", "db://c") + `
syncs:
  - { name: a2b, source: a, targets: [b] }
  - { name: b2c, source: b, targets: [c] }
`,
			wantCycle: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(writeTemp(t, tc.yaml))
			if tc.wantCycle {
				if err == nil || !strings.Contains(err.Error(), "replication cycle detected") {
					t.Fatalf("expected cycle rejection, got err=%v", err)
				}
			} else if err != nil {
				t.Fatalf("expected no cycle, got err=%v", err)
			}
		})
	}
}

// TestNodeIDNotSwallowedByInlineBlocks guards the subtlety that `node_id` is an
// explicit field, so resolve() does not mistake it for a stray engine block.
func TestNodeIDNotSwallowedByInlineBlocks(t *testing.T) {
	registerFake(t)
	yml := `
nodes:
  a:
    engine: fake
    node_id: alpha
    fake: { dsn: "fake://a" }
  b:
    engine: fake
    fake: { dsn: "fake://b" }
clusters:
  - name: c1
    engine: fake
    members: [a, b]
`
	c, err := Load(writeTemp(t, yml))
	if err != nil {
		t.Fatalf("Load: %v (node_id likely captured as an engine block)", err)
	}
	if c.Nodes["a"].NodeID != "alpha" {
		t.Errorf("node_id = %q, want alpha", c.Nodes["a"].NodeID)
	}
}
