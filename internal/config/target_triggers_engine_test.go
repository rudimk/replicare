package config

import (
	"strings"
	"testing"
)

// TestTargetTriggersPostgresOnly: another engine's sync has trigger handling off, and
// turning it on explicitly is refused.
func TestTargetTriggersPostgresOnly(t *testing.T) {
	registerFake(t)
	fake := `
sources:
  s: { engine: fake, fake: {dsn: "x"} }
targets:
  t: { engine: fake, fake: {dsn: "y"} }
syncs:
  - name: only
    source: s
    targets: [t]
`
	c, err := Load(writeTemp(t, fake))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.GuardsTargetTriggers(c.Syncs[0], "t") {
		t.Error("a non-Postgres sync guards target triggers")
	}
	if _, err := Load(writeTemp(t, fake+"    target_triggers: { enabled: true }\n")); err == nil || !strings.Contains(err.Error(), "only supported for postgres") {
		t.Fatalf("enabled: true on a non-Postgres sync: err = %v, want a refusal", err)
	}
}
