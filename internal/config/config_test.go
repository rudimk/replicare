package config

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/rudimk/replicare/internal/engine"
)

// fakeConn is a minimal EngineConn for a test engine ("fake"), so config tests
// exercise the registry/dispatch without importing a real engine package.
type fakeConn struct {
	DSN string `yaml:"dsn"`
}

func (c *fakeConn) EngineName() string { return "fake" }
func (c *fakeConn) Validate(role, name string) error {
	if c.DSN == "" {
		return &validationError{role: role, name: name, msg: "dsn is required"}
	}
	return nil
}
func (c *fakeConn) ConnConfig() engine.ConnConfig { return engine.ConnConfig{} }

type validationError struct {
	role, name, msg string
}

func (e *validationError) Error() string {
	return e.role + " " + e.name + ": " + e.msg
}

func parseFake(block *yaml.Node) (EngineConn, error) {
	var c fakeConn
	if err := StrictDecode(block, &c); err != nil {
		return nil, err
	}
	return &c, nil
}

var registerFakeOnce sync.Once

func registerFake(t *testing.T) {
	t.Helper()
	registerFakeOnce.Do(func() { RegisterEngine("fake", parseFake) })
}

func writeTemp(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.yml")
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatalf("write temp config: %v", err)
	}
	return p
}

const validYAML = `
logging:
  level: debug
observability:
  metrics_addr: ":9090"
sources:
  prod:
    engine: fake
    fake:
      dsn: "fake://src"
targets:
  warehouse:
    engine: fake
    fake:
      dsn: "fake://dst"
syncs:
  - name: app-to-warehouse
    source: prod
    targets: [warehouse]
    include: ["public.*"]
    exclude: ["*_audit"]
    tuning:
      drain_interval: 2s
      retention:
        max_age: 12h
        max_bytes: 5GB
      pool:
        max_source_connections: 8
        max_target_connections: 6
`

func TestLoadValid(t *testing.T) {
	registerFake(t)
	c, err := Load(writeTemp(t, validYAML))
	if err != nil {
		t.Fatalf("Load valid: %v", err)
	}
	if c.Logging.Level != "debug" || c.Logging.Format != "json" {
		t.Errorf("logging = %+v, want level=debug format=json(default)", c.Logging)
	}
	if c.Observability.MetricsAddr != ":9090" {
		t.Errorf("metrics_addr = %q", c.Observability.MetricsAddr)
	}
	s := c.Syncs[0]
	if s.Tuning.DrainInterval.Duration() != 2*time.Second {
		t.Errorf("drain_interval = %v, want 2s", s.Tuning.DrainInterval)
	}
	if s.Tuning.Retention.MaxBytes.Bytes() != 5*1000*1000*1000 {
		t.Errorf("max_bytes = %d", s.Tuning.Retention.MaxBytes.Bytes())
	}
	if s.Tuning.Pool.MaxSourceConns != 8 || s.Tuning.Pool.MaxTargetConns != 6 {
		t.Errorf("pool = %+v", s.Tuning.Pool)
	}
	src := c.Sources["prod"]
	if src.Conn == nil || src.Conn.EngineName() != "fake" {
		t.Errorf("source conn not resolved: %+v", src.Conn)
	}
}

func TestDefaultsApplied(t *testing.T) {
	registerFake(t)
	minimal := `
sources:
  s:
    engine: fake
    fake: {dsn: "x"}
targets:
  t:
    engine: fake
    fake: {dsn: "y"}
syncs:
  - name: only
    source: s
    targets: [t]
`
	c, err := Load(writeTemp(t, minimal))
	if err != nil {
		t.Fatalf("Load minimal: %v", err)
	}
	tn := c.Syncs[0].Tuning
	if tn.DrainInterval.Duration() != time.Second {
		t.Errorf("default drain_interval = %v, want 1s", tn.DrainInterval)
	}
	if tn.Pool.MaxSourceConns != 4 || tn.Pool.MaxTargetConns != 4 {
		t.Errorf("default pool = %+v, want 4/4", tn.Pool)
	}
	if tn.Retention.MaxAge.Duration() != 24*time.Hour {
		t.Errorf("default max_age = %v, want 24h", tn.Retention.MaxAge)
	}
}

func TestLoadRejectsUnknownTopLevelField(t *testing.T) {
	registerFake(t)
	if _, err := Load(writeTemp(t, validYAML+"\nbogus: true\n")); err == nil {
		t.Fatal("expected error on unknown top-level field")
	}
}

func TestResolveErrors(t *testing.T) {
	registerFake(t)
	cases := []struct {
		name string
		yaml string
		want string
	}{
		{
			name: "unknown engine",
			yaml: `
sources: {s: {engine: martian, martian: {x: 1}}}
targets: {t: {engine: fake, fake: {dsn: y}}}
syncs: [{name: n, source: s, targets: [t]}]
`,
			want: `unknown engine "martian"`,
		},
		{
			name: "missing engine block",
			yaml: `
sources: {s: {engine: fake}}
targets: {t: {engine: fake, fake: {dsn: y}}}
syncs: [{name: n, source: s, targets: [t]}]
`,
			want: `missing "fake" connection block`,
		},
		{
			name: "unexpected key in endpoint",
			yaml: `
sources: {s: {engine: fake, fake: {dsn: x}, postgres: {host: h}}}
targets: {t: {engine: fake, fake: {dsn: y}}}
syncs: [{name: n, source: s, targets: [t]}]
`,
			want: `unexpected key "postgres"`,
		},
		{
			name: "engine block validation fails",
			yaml: `
sources: {s: {engine: fake, fake: {dsn: ""}}}
targets: {t: {engine: fake, fake: {dsn: y}}}
syncs: [{name: n, source: s, targets: [t]}]
`,
			want: "dsn is required",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(writeTemp(t, tc.yaml))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want it to contain %q", err, tc.want)
			}
		})
	}
}

func TestValidateErrors(t *testing.T) {
	registerFake(t)
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
targets: {t: {engine: fake, fake: {dsn: y}}}
syncs: [{name: s, source: missing, targets: [t]}]
`,
			want: `source "missing" is not defined`,
		},
		{
			name: "undefined target",
			yaml: `
sources: {src: {engine: fake, fake: {dsn: x}}}
syncs: [{name: s, source: src, targets: [nope]}]
`,
			want: `target "nope" is not defined`,
		},
		{
			name: "duplicate sync name",
			yaml: `
sources: {src: {engine: fake, fake: {dsn: x}}}
targets: {t: {engine: fake, fake: {dsn: y}}}
syncs:
  - {name: dup, source: src, targets: [t]}
  - {name: dup, source: src, targets: [t]}
`,
			want: `duplicate sync name "dup"`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(writeTemp(t, tc.yaml))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want it to contain %q", err, tc.want)
			}
		})
	}
}
