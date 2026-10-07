package config

import (
	"fmt"
	"strings"
	"testing"
)

// TestSyncSequencesFlag: the sync_sequences knob is a pure opt-in — unset/false is off,
// explicit true is on — so every pre-existing config is unchanged.
func TestSyncSequencesFlag(t *testing.T) {
	registerFake(t)
	base := `
sources:
  s:
    engine: fake
    fake: {dsn: "x"}
targets:
  t:
    engine: fake
    fake: {dsn: "y"}
syncs:
  - name: only
    source: s
    targets: [t]%s
`
	cases := []struct {
		name string
		line string
		want bool
	}{
		{"unset defaults to off", "", false},
		{"explicit false is off", "\n    sync_sequences: false", false},
		{"explicit true is on", "\n    sync_sequences: true", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, err := Load(writeTemp(t, fmt.Sprintf(base, tc.line)))
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if got := c.Syncs[0].SyncsSequences(); got != tc.want {
				t.Errorf("SyncsSequences() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestSyncSequencesMeshRefusal: continuous sequence syncing is passive/DR only, so a
// sync whose target points at the same database as an active-active cluster member must
// be refused at load (the mesh counter advance is silent data loss, plan §5); a target
// that is not a mesh member is allowed.
func TestSyncSequencesMeshRefusal(t *testing.T) {
	registerFake(t)
	// Target "t" shares its DSN ("shared") with cluster node "a" → same connIdentity.
	refused := `
sources:
  s:
    engine: fake
    fake: {dsn: "src"}
targets:
  t:
    engine: fake
    fake: {dsn: "shared"}
nodes:
  a: { engine: fake, fake: {dsn: "shared"} }
  b: { engine: fake, fake: {dsn: "nodeb"} }
clusters:
  - name: mesh
    engine: fake
    members: [a, b]
syncs:
  - name: one-way
    source: s
    targets: [t]
    sync_sequences: true
`
	_, err := Load(writeTemp(t, refused))
	if err == nil {
		t.Fatal("expected sync_sequences against a mesh-member target to be refused")
	}
	for _, want := range []string{"sync_sequences", "cluster member", "UUID"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error = %q, want containing %q", err.Error(), want)
		}
	}

	// The same sync with sync_sequences OFF loads fine (the overlap itself is not the
	// problem — only advancing a mesh counter is).
	off := strings.Replace(refused, "    sync_sequences: true", "    sync_sequences: false", 1)
	if _, err := Load(writeTemp(t, off)); err != nil {
		t.Fatalf("sync_sequences:false against the same target should load: %v", err)
	}

	// A target that is NOT a mesh member is allowed even with sync_sequences on.
	allowed := strings.Replace(refused, `    fake: {dsn: "shared"}`, `    fake: {dsn: "lonely"}`, 1)
	if _, err := Load(writeTemp(t, allowed)); err != nil {
		t.Fatalf("sync_sequences:true against a non-member target should load: %v", err)
	}
}
