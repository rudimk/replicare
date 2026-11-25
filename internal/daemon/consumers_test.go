package daemon

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/rudimk/replicare/internal/config"
	"github.com/rudimk/replicare/internal/engine"
)

// TestConsumersOf: every sync target and cluster edge reading a database is listed,
// whatever the endpoint is called (same host/port/database = same database), paused
// ones included and marked; syncs reading other databases are not.
func TestConsumersOf(t *testing.T) {
	body := `
state_store: { engine: postgres, postgres: { host: st, database: s, user: u } }
sources:
  main:  { engine: postgres, postgres: { host: db1, database: app, user: u } }
  alias: { engine: postgres, postgres: { host: db1, database: app, user: u } }
  other: { engine: postgres, postgres: { host: db9, database: app, user: u } }
targets:
  t1: { engine: postgres, postgres: { host: db2, database: app, user: u } }
  t2: { engine: postgres, postgres: { host: db3, database: app, user: u } }
  t3: { engine: postgres, postgres: { host: db4, database: app, user: u } }
nodes:
  n1: { engine: postgres, postgres: { host: db1, database: app, user: u } }
  n2: { engine: postgres, postgres: { host: db5, database: app, user: u } }
clusters:
  - { name: mesh, engine: postgres, members: [n1, n2], include: ["public.*"] }
syncs:
  - { name: fan, source: main, targets: [t1, t2], include: ["public.*"] }
  - { name: paused, source: alias, targets: [t3], include: ["public.orders"], enabled: false }
  - { name: elsewhere, source: other, targets: [t1], include: ["public.*"] }
`
	p := filepath.Join(t.TempDir(), "c.yml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(p)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	got := map[string]consumer{}
	for _, c := range consumersOf(cfg, cfg.Sources["main"]) {
		got[c.job+"/"+string(c.target)] = c
	}
	want := map[string]bool{"fan/t1": true, "fan/t2": true, "paused/t3": false, "mesh::n1->n2/n2": true}
	if len(got) != len(want) {
		t.Fatalf("consumers = %v, want %v", got, want)
	}
	for k, enabled := range want {
		c, ok := got[k]
		if !ok {
			t.Errorf("missing consumer %s", k)
			continue
		}
		if c.enabled != enabled {
			t.Errorf("%s enabled = %v, want %v", k, c.enabled, enabled)
		}
	}
	if sel := got["paused/t3"].sel; len(sel.Include) != 1 || sel.Include[0] != "public.orders" {
		t.Errorf("paused selection = %+v", sel)
	}
	if c := got["mesh::n1->n2/n2"]; c.target != engine.TargetID("n2") {
		t.Errorf("mesh edge target = %q", c.target)
	}
}
