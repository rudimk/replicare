package config_test

import (
	"strings"
	"testing"
)

// TestTargetTriggersDefaults: on by default for a Postgres target, off when set false,
// and always off for a target that is also an active-active cluster member.
func TestTargetTriggersDefaults(t *testing.T) {
	c, err := loadYAML(t, deferBase)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	s := c.Syncs[0]
	if !c.GuardsTargetTriggers(s, "t") {
		t.Error("unset target_triggers: want on for a plain Postgres target")
	}
	if c.GuardsTargetTriggers(s, "m") {
		t.Error("unset target_triggers: want off for a target that is also a mesh member")
	}
	c, err = loadYAML(t, deferBase+"    target_triggers: { enabled: false }\n")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.GuardsTargetTriggers(c.Syncs[0], "t") {
		t.Error("enabled: false: want off")
	}
}

// TestTargetTriggersRefusals: explicitly on for a mesh-member target, or for another
// engine, is refused at load.
func TestTargetTriggersRefusals(t *testing.T) {
	_, err := loadYAML(t, deferBase+"    target_triggers: { enabled: true }\n")
	if err == nil || !strings.Contains(err.Error(), "target_triggers") || !strings.Contains(err.Error(), "cluster member") {
		t.Fatalf("enabled: true with a mesh-member target: err = %v, want a refusal", err)
	}
	onlyPlain := strings.Replace(deferBase, "targets: [t, m]", "targets: [t]", 1)
	if _, err := loadYAML(t, onlyPlain+"    target_triggers: { enabled: true }\n"); err != nil {
		t.Fatalf("enabled: true with a plain target: %v", err)
	}

}
