package config_test

import (
	"strings"
	"testing"
)

// TestSkipInitialCopyDefaults: off unless enabled. The check after catch-up is on
// unless verify is set false, whether or not skipping is still enabled, so tables an
// earlier run skipped are still checked.
func TestSkipInitialCopyDefaults(t *testing.T) {
	onlyPlain := strings.Replace(deferBase, "targets: [t, m]", "targets: [t]", 1)
	cases := []struct {
		name       string
		extra      string
		skip, chck bool
	}{
		{"unset", "", false, true},
		{"enabled false", "    skip_initial_copy: { enabled: false }\n", false, true},
		{"enabled false, verify false", "    skip_initial_copy: { enabled: false, verify: false }\n", false, false},
		{"enabled", "    skip_initial_copy: { enabled: true }\n", true, true},
		{"enabled, verify true", "    skip_initial_copy: { enabled: true, verify: true }\n", true, true},
		{"enabled, verify false", "    skip_initial_copy: { enabled: true, verify: false }\n", true, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, err := loadYAML(t, onlyPlain+tc.extra)
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			s := c.Syncs[0]
			if got := s.SkipsInitialCopy(); got != tc.skip {
				t.Errorf("SkipsInitialCopy() = %v, want %v", got, tc.skip)
			}
			if got := s.VerifiesSkippedCopy(); got != tc.chck {
				t.Errorf("VerifiesSkippedCopy() = %v, want %v", got, tc.chck)
			}
		})
	}
}

// TestSkipInitialCopyRefusals: refused for a target that is also a mesh member.
func TestSkipInitialCopyRefusals(t *testing.T) {
	_, err := loadYAML(t, deferBase+"    skip_initial_copy: { enabled: true }\n")
	if err == nil || !strings.Contains(err.Error(), "skip_initial_copy") || !strings.Contains(err.Error(), "cluster member") {
		t.Fatalf("enabled with a mesh-member target: err = %v, want a refusal", err)
	}
}
