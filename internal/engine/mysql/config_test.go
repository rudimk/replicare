package mysql

import (
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/rudimk/replicare/internal/engine"
)

func parseBlock(t *testing.T, y string) (*Conn, error) {
	t.Helper()
	var doc yaml.Node
	if err := yaml.Unmarshal([]byte(y), &doc); err != nil {
		t.Fatalf("yaml: %v", err)
	}
	ec, err := parse(doc.Content[0])
	if err != nil {
		return nil, err
	}
	return ec.(*Conn), nil
}

func TestConnValidateAndDefaults(t *testing.T) {
	c, err := parseBlock(t, "{host: db, database: app, user: repl, password: pw}")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if err := c.Validate("source", "s"); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	cc := c.ConnConfig()
	if cc.Port != 3306 {
		t.Errorf("default port = %d, want 3306", cc.Port)
	}
	if cc.TLS != engine.TLSPrefer {
		t.Errorf("default TLS = %q, want prefer", cc.TLS)
	}
}

func TestConnValidateErrors(t *testing.T) {
	cases := map[string]string{
		"missing host":     "{database: app, user: u}",
		"missing database": "{host: h, user: u}",
		"missing user":     "{host: h, database: app}",
		"bad port":         "{host: h, database: app, user: u, port: 70000}",
		"bad tls":          "{host: h, database: app, user: u, tls: bogus}",
	}
	for name, y := range cases {
		t.Run(name, func(t *testing.T) {
			c, err := parseBlock(t, y)
			if err != nil {
				return // decode error is an acceptable rejection
			}
			if err := c.Validate("source", "s"); err == nil {
				t.Errorf("expected validation error for %s", name)
			}
		})
	}
}

func TestConnUnknownKeyRejected(t *testing.T) {
	if _, err := parseBlock(t, "{host: h, database: app, user: u, bogus: x}"); err == nil {
		t.Error("expected strict-decode to reject unknown key")
	}
}

func TestLocalInfileThreadedInternally(t *testing.T) {
	tt := true
	ff := false
	for _, tc := range []struct {
		li   *bool
		want string
	}{{&tt, "1"}, {&ff, "0"}} {
		c := &Conn{Host: "h", Database: "app", User: "u", LocalInfile: tc.li}
		cc := c.ConnConfig()
		if got := cc.Params[paramLocalInfile]; got != tc.want {
			t.Errorf("local_infile=%v -> param %q, want %q", *tc.li, got, tc.want)
		}
		// The internal hint must never reach the DSN.
		d, err := dsn(cc)
		if err != nil {
			t.Fatalf("dsn: %v", err)
		}
		if contains(d, "rc_local_infile") {
			t.Errorf("internal hint leaked into DSN: %s", d)
		}
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
