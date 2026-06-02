package config

import (
	"strings"
	"testing"
)

func TestExpandString(t *testing.T) {
	t.Setenv("REPLICARE_TEST_SET", "from-env")

	cases := []struct {
		name    string
		in      string
		want    string
		wantErr bool
	}{
		{"plain", "no refs here", "no refs here", false},
		{"set var", "x=${REPLICARE_TEST_SET}", "x=from-env", false},
		{"default used when unset", "x=${REPLICARE_TEST_UNSET:-fallback}", "x=fallback", false},
		{"env over default", "x=${REPLICARE_TEST_SET:-fallback}", "x=from-env", false},
		{"escape", "literal $${NOT_A_VAR}", "literal ${NOT_A_VAR}", false},
		{"missing required", "x=${REPLICARE_TEST_UNSET}", "", true},
		{"empty name", "x=${}", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := expandString(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for %q", tc.in)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error for %q: %v", tc.in, err)
			}
			if got != tc.want {
				t.Fatalf("expandString(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestLoadEnvPrecedence proves env/secret resolution end-to-end: a secret sourced
// from an env var resolves over the inline default (CLAUDE.md §11 precedence).
func TestLoadEnvPrecedence(t *testing.T) {
	registerFake(t)
	t.Setenv("REPLICARE_DSN", "fake://from-env")

	yaml := `
sources:
  s:
    engine: fake
    fake:
      dsn: "${REPLICARE_DSN:-fake://inline-default}"
targets:
  t:
    engine: fake
    fake:
      dsn: "${MISSING_DSN:-fake://inline-default}"
syncs:
  - {name: n, source: s, targets: [t]}
`
	c, err := Load(writeTemp(t, yaml))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	src := c.Sources["s"].Conn.(*fakeConn)
	if src.DSN != "fake://from-env" {
		t.Errorf("source dsn = %q, want env value to win over default", src.DSN)
	}
	tgt := c.Targets["t"].Conn.(*fakeConn)
	if tgt.DSN != "fake://inline-default" {
		t.Errorf("target dsn = %q, want inline default when env unset", tgt.DSN)
	}
}

func TestLoadMissingRequiredEnv(t *testing.T) {
	registerFake(t)
	yaml := `
sources:
  s:
    engine: fake
    fake:
      dsn: "${DEFINITELY_UNSET_REPLICARE_VAR}"
targets:
  t:
    engine: fake
    fake: {dsn: y}
syncs:
  - {name: n, source: s, targets: [t]}
`
	_, err := Load(writeTemp(t, yaml))
	if err == nil || !strings.Contains(err.Error(), "is not set") {
		t.Fatalf("expected missing-env error, got %v", err)
	}
}
