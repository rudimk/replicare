package config

import (
	"strings"
	"testing"
)

// TestDeferIndexesPostgresOnly: deferred target indexes exist only for Postgres. Unset,
// another engine's sync simply has them off; set true, the config is refused.
func TestDeferIndexesPostgresOnly(t *testing.T) {
	registerFake(t)
	base := `
sources:
  s: { engine: fake, fake: {dsn: "x"} }
targets:
  t: { engine: fake, fake: {dsn: "y"} }
syncs:
  - name: only
    source: s
    targets: [t]
`
	c, err := Load(writeTemp(t, base))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.DefersIndexes(c.Syncs[0], "t") {
		t.Error("a non-Postgres sync defers indexes")
	}
	_, err = Load(writeTemp(t, base+"    defer_indexes: { enabled: true }\n"))
	if err == nil || !strings.Contains(err.Error(), "only supported for postgres") {
		t.Fatalf("enabled: true on a non-Postgres sync: err = %v, want a refusal", err)
	}
}
