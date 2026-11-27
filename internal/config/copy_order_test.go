package config_test

import (
	"strings"
	"testing"

	"github.com/rudimk/replicare/internal/config"
)

const copyOrderBase = `
sources:
  s: { engine: postgres, postgres: { host: src, database: app, user: u } }
targets:
  t: { engine: postgres, postgres: { host: dst, database: app, user: u } }
syncs:
  - name: one
    source: s
    targets: [t]
`

// TestCopyOrder: unset reads as auto; auto, key and storage are accepted; anything
// else is refused.
func TestCopyOrder(t *testing.T) {
	c, err := loadYAML(t, copyOrderBase)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got := c.Syncs[0].ReadOrder(); got != config.CopyOrderAuto {
		t.Errorf("unset copy_order = %q, want auto", got)
	}
	for _, v := range []string{"auto", "key", "storage"} {
		c, err := loadYAML(t, copyOrderBase+"    copy_order: "+v+"\n")
		if err != nil {
			t.Fatalf("copy_order: %s: %v", v, err)
		}
		if got := c.Syncs[0].ReadOrder(); got != v {
			t.Errorf("copy_order: %s reads as %q", v, got)
		}
	}
	_, err = loadYAML(t, copyOrderBase+"    copy_order: physical\n")
	if err == nil || !strings.Contains(err.Error(), "copy_order must be auto, key or storage") {
		t.Fatalf("copy_order: physical: err = %v, want a refusal", err)
	}
}
