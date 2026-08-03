package redis

import (
	"testing"

	"github.com/rudimk/replicare/internal/engine"
)

func hasFinding(r *engine.PreflightReport, sev engine.Severity, category string) bool {
	for _, f := range r.Findings {
		if f.Severity == sev && f.Category == category {
			return true
		}
	}
	return false
}

func schemaWith(modules ...string) *engine.Schema {
	return &engine.Schema{
		Tables:       []engine.Table{pseudoTable(engine.ConnConfig{Database: "0"})},
		Capabilities: engine.Capabilities{Modules: modules},
	}
}

// TestPreflightRDBVersionGate: newer source -> older target blocks; older/equal ->
// newer/equal passes.
func TestPreflightRDBVersionGate(t *testing.T) {
	// 7.4 source -> 6.2 target: BLOCK.
	r := buildPreflight("s", 70400, 60200, schemaWith(), schemaWith())
	if !hasFinding(r, engine.SevBlock, "rdb-version") || !r.Blocked() {
		t.Fatalf("newer->older should block, got %+v", r.Findings)
	}
	// 6.2 source -> 7.4 target: OK.
	r = buildPreflight("s", 60200, 70400, schemaWith(), schemaWith())
	if r.Blocked() {
		t.Fatalf("older->newer should pass, got %+v", r.Findings)
	}
	// equal: OK.
	r = buildPreflight("s", 70400, 70400, schemaWith(), schemaWith())
	if r.Blocked() {
		t.Fatalf("equal versions should pass, got %+v", r.Findings)
	}
}

// TestPreflightModuleGate: a source module absent on the target blocks.
func TestPreflightModuleGate(t *testing.T) {
	r := buildPreflight("s", 70400, 70400, schemaWith("ReJSON-RL"), schemaWith())
	if !hasFinding(r, engine.SevBlock, "missing-module") || !r.Blocked() {
		t.Fatalf("missing module should block, got %+v", r.Findings)
	}
	// Same modules on both: OK.
	r = buildPreflight("s", 70400, 70400, schemaWith("ReJSON-RL"), schemaWith("ReJSON-RL", "bf"))
	if r.Blocked() {
		t.Fatalf("target superset of modules should pass, got %+v", r.Findings)
	}
}

// TestPreflightComponents: the unit is a single-member acyclic replicable component.
func TestPreflightComponents(t *testing.T) {
	r := buildPreflight("s", 70400, 70400, schemaWith(), schemaWith())
	if len(r.Replicable) != 1 || len(r.Components) != 1 {
		t.Fatalf("want 1 replicable + 1 component, got %d / %d", len(r.Replicable), len(r.Components))
	}
	c := r.Components[0]
	if len(c.Order) != 1 || len(c.Cyclic) != 0 || c.HasCycle() {
		t.Errorf("component should be single-member acyclic: %+v", c)
	}
	if r.Replicable[0] != (engine.TableRef{Schema: "redis", Name: "db0"}) {
		t.Errorf("unit ref = %v, want redis.db0", r.Replicable[0])
	}
}

func TestUnitRef(t *testing.T) {
	if got := unitRef(engine.ConnConfig{Database: "3"}); got.Name != "db3" || got.Schema != "redis" {
		t.Errorf("unitRef(db3) = %v, want redis.db3", got)
	}
	if got := unitRef(engine.ConnConfig{}); got.Name != "db0" {
		t.Errorf("unitRef(default) = %v, want db0", got)
	}
	if !pseudoTable(engine.ConnConfig{}).HasUsableKey() {
		t.Error("pseudo-table must have a usable key")
	}
}
