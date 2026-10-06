package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/rudimk/replicare/internal/config"
	"github.com/rudimk/replicare/internal/engine"
)

// fakeEngineConn is a minimal config.EngineConn for the pure mesh-guard tests (no I/O).
type fakeEngineConn struct{ cc engine.ConnConfig }

func (f fakeEngineConn) EngineName() string               { return "postgres" }
func (f fakeEngineConn) Validate(role, name string) error { return nil }
func (f fakeEngineConn) ConnConfig() engine.ConnConfig    { return f.cc }

func mkEndpoint(host string, port int, db string) *config.Endpoint {
	return &config.Endpoint{Engine: "postgres", Conn: fakeEngineConn{engine.ConnConfig{Host: host, Port: port, Database: db}}}
}

func TestReseedSequencesUsageMissingFlags(t *testing.T) {
	var out, errb bytes.Buffer
	if code := run([]string{"reseed-sequences", "cfg.yml"}, &out, &errb); code != 2 {
		t.Fatalf("reseed-sequences missing --sync exit = %d, want 2", code)
	}
	if !strings.Contains(errb.String(), "usage: replicare reseed-sequences") {
		t.Fatalf("expected usage, got %q", errb.String())
	}
}

func TestReseedSequencesSyncNeedsValue(t *testing.T) {
	var out, errb bytes.Buffer
	if code := run([]string{"reseed-sequences", "cfg.yml", "--sync"}, &out, &errb); code != 2 {
		t.Fatalf("reseed-sequences --sync no-value exit = %d, want 2", code)
	}
	if !strings.Contains(errb.String(), "--sync needs a value") {
		t.Fatalf("expected --sync error, got %q", errb.String())
	}
}

func TestReseedSequencesListedInUsage(t *testing.T) {
	var out, errb bytes.Buffer
	run([]string{"help"}, &out, &errb)
	if !strings.Contains(out.String(), "reseed-sequences") {
		t.Fatalf("help should list reseed-sequences, got %q", out.String())
	}
}

// TestMeshGuard is the load-bearing safety check: reseed must refuse a target that points
// at the same database as any active-active cluster member (reseeding a mesh node is
// silent data loss, plan §5), and must allow a target that doesn't.
func TestMeshGuard(t *testing.T) {
	cfg := &config.Config{
		Nodes: map[string]*config.Endpoint{
			"a": mkEndpoint("hostA", 5432, "app"),
			"b": mkEndpoint("hostB", 5432, "app"),
		},
		Clusters: []*config.Cluster{{Name: "c", Members: []string{"a", "b"}}},
		Targets: map[string]*config.Endpoint{
			"mesh":  mkEndpoint("hostB", 5432, "app"), // identical conn to node b
			"plain": mkEndpoint("hostZ", 5432, "app"), // a genuine one-way target
		},
	}
	members := clusterMemberIdentities(cfg)
	if len(members) != 2 {
		t.Fatalf("expected 2 cluster member identities, got %d", len(members))
	}
	if id, ok := matchingClusterMember(cfg.Targets["mesh"], members); !ok {
		t.Fatalf("target 'mesh' (== node b) must be refused as a cluster member")
	} else if id.String() != "hostB:5432/app" {
		t.Fatalf("unexpected matched identity %q", id.String())
	}
	if _, ok := matchingClusterMember(cfg.Targets["plain"], members); ok {
		t.Fatalf("target 'plain' must NOT be flagged a cluster member")
	}
}

func TestMeshGuardNoClusters(t *testing.T) {
	// A config with no clusters yields no member identities, so nothing is ever refused.
	if n := len(clusterMemberIdentities(&config.Config{})); n != 0 {
		t.Fatalf("no clusters should yield 0 member identities, got %d", n)
	}
	if _, ok := matchingClusterMember(mkEndpoint("h", 1, "d"), nil); ok {
		t.Fatalf("no members should never match")
	}
}
