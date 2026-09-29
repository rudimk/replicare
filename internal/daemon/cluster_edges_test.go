package daemon

import (
	"fmt"
	"testing"

	"github.com/rudimk/replicare/internal/config"
)

// TestClusterEdgesFullMesh proves the mesh has no fixed node count: for any N >= 2 a
// cluster expands to the full set of N*(N-1) directed edges (every ordered pair of
// distinct members), with a deterministic order and unique per-edge ownership names.
// Pure (no live DB), so it runs in CI — the N>=3 correctness backstop that the live
// convergence tests (local Redis gate) complement.
func TestClusterEdgesFullMesh(t *testing.T) {
	members := func(n int) []string {
		m := make([]string, n)
		for i := range m {
			m[i] = fmt.Sprintf("n%d", i)
		}
		return m
	}

	for _, n := range []int{2, 3, 5, 7} {
		cl := &config.Cluster{Name: "mesh", Members: members(n)}
		edges := clusterEdges([]*config.Cluster{cl})

		if want := n * (n - 1); len(edges) != want {
			t.Fatalf("N=%d: got %d edges, want %d (full mesh)", n, len(edges), want)
		}

		// Every ordered pair of distinct members appears exactly once, and each edge
		// name (the ownership-lock key) is unique.
		seenPair := map[[2]string]bool{}
		seenName := map[string]bool{}
		for _, e := range edges {
			if e.srcNode == e.dstNode {
				t.Errorf("N=%d: self-edge %s->%s", n, e.srcNode, e.dstNode)
			}
			pair := [2]string{e.srcNode, e.dstNode}
			if seenPair[pair] {
				t.Errorf("N=%d: duplicate edge %s->%s", n, e.srcNode, e.dstNode)
			}
			seenPair[pair] = true
			if seenName[e.name()] {
				t.Errorf("N=%d: duplicate edge name %q", n, e.name())
			}
			seenName[e.name()] = true
		}
		for i, a := range cl.Members {
			for j, b := range cl.Members {
				if i != j && !seenPair[[2]string{a, b}] {
					t.Errorf("N=%d: missing edge %s->%s", n, a, b)
				}
			}
		}
	}
}

// TestClusterEdgesDeterministicOrder: edge order follows member declaration order, so
// ownership acquisition and logs are stable across restarts (guards the flake fix that
// keyed edges by a stable name).
func TestClusterEdgesDeterministicOrder(t *testing.T) {
	cl := &config.Cluster{Name: "c", Members: []string{"a", "b", "c"}}
	got := clusterEdges([]*config.Cluster{cl})
	want := []string{
		"c::a->b", "c::a->c",
		"c::b->a", "c::b->c",
		"c::c->a", "c::c->b",
	}
	if len(got) != len(want) {
		t.Fatalf("got %d edges, want %d", len(got), len(want))
	}
	for i, e := range got {
		if e.name() != want[i] {
			t.Errorf("edge[%d] = %q, want %q", i, e.name(), want[i])
		}
	}
}

// TestClusterEdgesMultipleClusters: distinct clusters expand independently and their
// edge names never collide (cluster name is part of the ownership key).
func TestClusterEdgesMultipleClusters(t *testing.T) {
	clusters := []*config.Cluster{
		{Name: "east", Members: []string{"a", "b", "c"}},
		{Name: "west", Members: []string{"a", "b"}},
	}
	edges := clusterEdges(clusters)
	if want := 3*2 + 2*1; len(edges) != want {
		t.Fatalf("got %d edges, want %d", len(edges), want)
	}
	names := map[string]bool{}
	for _, e := range edges {
		if names[e.name()] {
			t.Errorf("edge name collision across clusters: %q", e.name())
		}
		names[e.name()] = true
	}
}
