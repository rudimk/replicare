package postgres

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/rudimk/replicare/internal/engine"
)

func decodeBlock(t *testing.T, y string) *yaml.Node {
	t.Helper()
	var n yaml.Node
	if err := yaml.Unmarshal([]byte(y), &n); err != nil {
		t.Fatalf("unmarshal block: %v", err)
	}
	// n is a DocumentNode; return its mapping content.
	return n.Content[0]
}

func TestParseAndConnConfig(t *testing.T) {
	block := decodeBlock(t, `
host: db.example.com
port: 6543
database: app
user: replicare
password: secret
sslmode: verify-full
params:
  application_name: replicare
`)
	ec, err := parse(block)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if err := ec.Validate("source", "prod"); err != nil {
		t.Fatalf("validate: %v", err)
	}
	cc := ec.ConnConfig()
	if cc.Host != "db.example.com" || cc.Port != 6543 || cc.Database != "app" {
		t.Errorf("conn = %+v", cc)
	}
	if cc.TLS != engine.TLSVerifyFull {
		t.Errorf("TLS = %q, want verify-full", cc.TLS)
	}
	if cc.Params["application_name"] != "replicare" {
		t.Errorf("params not carried: %+v", cc.Params)
	}
}

func TestDefaultsPortAndSSLMode(t *testing.T) {
	ec, err := parse(decodeBlock(t, "host: h\ndatabase: d\nuser: u\n"))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	cc := ec.ConnConfig()
	if cc.Port != defaultPort {
		t.Errorf("default port = %d, want %d", cc.Port, defaultPort)
	}
	if cc.TLS != engine.TLSPrefer {
		t.Errorf("default TLS = %q, want prefer", cc.TLS)
	}
}

func TestValidateErrors(t *testing.T) {
	cases := []struct {
		name string
		yaml string
		want string
	}{
		{"missing host", "database: d\nuser: u\n", "host is required"},
		{"missing database", "host: h\nuser: u\n", "database is required"},
		{"missing user", "host: h\ndatabase: d\n", "user is required"},
		{"bad sslmode", "host: h\ndatabase: d\nuser: u\nsslmode: bogus\n", "invalid postgres.sslmode"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ec, err := parse(decodeBlock(t, tc.yaml))
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			err = ec.Validate("source", "x")
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Validate error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestParseRejectsUnknownKey(t *testing.T) {
	_, err := parse(decodeBlock(t, "host: h\ndatabase: d\nuser: u\nbogus: 1\n"))
	if err == nil || !strings.Contains(err.Error(), "bogus") {
		t.Fatalf("expected unknown-key error, got %v", err)
	}
}
