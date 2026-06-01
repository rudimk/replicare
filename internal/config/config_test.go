package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const validYAML = `
logging:
  level: debug
sources:
  prod:
    engine: postgres
    host: src.example.com
    port: 5432
    database: app
    user: replicare
    password: secret
    tls: verify-full
targets:
  warehouse:
    engine: postgres
    host: dst.example.com
    database: app
    user: replicare
    password: secret
    tls: require
syncs:
  - name: app-to-warehouse
    source: prod
    targets: [warehouse]
    include: ["public.*"]
    exclude: ["*_audit"]
`

func writeTemp(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.yml")
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatalf("write temp config: %v", err)
	}
	return p
}

func TestLoadValid(t *testing.T) {
	c, err := Load(writeTemp(t, validYAML))
	if err != nil {
		t.Fatalf("Load valid: %v", err)
	}
	if c.Logging.Level != "debug" {
		t.Errorf("level = %q, want debug", c.Logging.Level)
	}
	if c.Logging.Format != "json" {
		t.Errorf("format default = %q, want json", c.Logging.Format)
	}
	if len(c.Syncs) != 1 || c.Syncs[0].Name != "app-to-warehouse" {
		t.Fatalf("unexpected syncs: %+v", c.Syncs)
	}
}

func TestLoadRejectsUnknownField(t *testing.T) {
	bad := validYAML + "\nbogusTopLevel: true\n"
	if _, err := Load(writeTemp(t, bad)); err == nil {
		t.Fatal("expected error on unknown field, got nil")
	}
}

func TestValidateErrors(t *testing.T) {
	cases := []struct {
		name string
		yaml string
		want string
	}{
		{
			name: "no syncs",
			yaml: "sources: {}\ntargets: {}\n",
			want: "no syncs",
		},
		{
			name: "undefined source",
			yaml: `
targets:
  t:
    engine: postgres
    host: h
    database: d
syncs:
  - name: s
    source: missing
    targets: [t]
`,
			want: `source "missing" is not defined`,
		},
		{
			name: "undefined target",
			yaml: `
sources:
  src:
    engine: postgres
    host: h
    database: d
syncs:
  - name: s
    source: src
    targets: [nope]
`,
			want: `target "nope" is not defined`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(writeTemp(t, tc.yaml))
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %q, want it to contain %q", err.Error(), tc.want)
			}
		})
	}
}
