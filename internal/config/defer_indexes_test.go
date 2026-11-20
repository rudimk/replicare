package config_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rudimk/replicare/internal/config"

	_ "github.com/rudimk/replicare/internal/engine/postgres"
)

func loadYAML(t *testing.T, body string) (*config.Config, error) {
	t.Helper()
	p := filepath.Join(t.TempDir(), "c.yml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return config.Load(p)
}

const deferBase = `
sources:
  s: { engine: postgres, postgres: { host: src, database: app, user: u } }
targets:
  t: { engine: postgres, postgres: { host: dst, database: app, user: u } }
  m: { engine: postgres, postgres: { host: nodea, database: app, user: u } }
nodes:
  a: { engine: postgres, postgres: { host: nodea, database: app, user: u }, node_id: a }
  b: { engine: postgres, postgres: { host: nodeb, database: app, user: u }, node_id: b }
clusters:
  - name: mesh
    engine: postgres
    members: [a, b]
syncs:
  - name: one
    source: s
    targets: [t, m]
`

// TestDeferIndexesDefaults: on by default for a Postgres target, off when set false,
// and always off for a target that is also an active-active cluster member.
func TestDeferIndexesDefaults(t *testing.T) {
	c, err := loadYAML(t, deferBase)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	s := c.Syncs[0]
	if !c.DefersIndexes(s, "t") {
		t.Error("unset defer_indexes: want on for a plain Postgres target")
	}
	if c.DefersIndexes(s, "m") {
		t.Error("unset defer_indexes: want off for a target that is also a mesh member")
	}

	c, err = loadYAML(t, deferBase+"    defer_indexes: { enabled: false, maintenance_work_mem: 1GB, parallel_workers: 2 }\n")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	s = c.Syncs[0]
	if c.DefersIndexes(s, "t") {
		t.Error("enabled: false: want off")
	}
	if s.DeferIndexes.MaintenanceWorkMem != "1GB" || s.DeferIndexes.ParallelWorkers != 2 {
		t.Errorf("build settings = %+v", s.DeferIndexes)
	}
}

// TestDeferIndexesRefusals: explicitly turning it on for a mesh-member target, or with
// negative workers, is refused at load.
func TestDeferIndexesRefusals(t *testing.T) {
	_, err := loadYAML(t, deferBase+"    defer_indexes: { enabled: true }\n")
	if err == nil || !strings.Contains(err.Error(), "defer_indexes") || !strings.Contains(err.Error(), "cluster member") {
		t.Fatalf("enabled: true with a mesh-member target: err = %v, want a refusal", err)
	}
	onlyPlain := strings.Replace(deferBase, "targets: [t, m]", "targets: [t]", 1)
	if _, err := loadYAML(t, onlyPlain+"    defer_indexes: { enabled: true }\n"); err != nil {
		t.Fatalf("enabled: true with a plain target: %v", err)
	}
	if _, err := loadYAML(t, onlyPlain+"    defer_indexes: { parallel_workers: -1 }\n"); err == nil {
		t.Fatal("parallel_workers: -1 was accepted")
	}
}
