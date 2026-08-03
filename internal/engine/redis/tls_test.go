package redis

import (
	"testing"

	"github.com/rudimk/replicare/internal/engine"
)

func TestTLSConfigSpectrum(t *testing.T) {
	// disable/allow/prefer -> plaintext (nil).
	for _, m := range []engine.TLSMode{engine.TLSDisable, engine.TLSAllow, engine.TLSPrefer, ""} {
		if c := tlsConfig(m, "host"); c != nil {
			t.Errorf("tlsConfig(%q) = %+v, want nil (plaintext)", m, c)
		}
	}
	// require -> encrypt, no verification.
	if c := tlsConfig(engine.TLSRequire, "host"); c == nil || !c.InsecureSkipVerify {
		t.Errorf("require: want InsecureSkipVerify, got %+v", c)
	}
	// verify-ca -> chain verified (custom verifier), hostname skipped.
	if c := tlsConfig(engine.TLSVerifyCA, "host"); c == nil || c.VerifyPeerCertificate == nil || c.ServerName != "" {
		t.Errorf("verify-ca: want a chain verifier and no ServerName, got %+v", c)
	}
	// verify-full -> full verification (ServerName set, default verifier).
	if c := tlsConfig(engine.TLSVerifyFull, "host"); c == nil || c.ServerName != "host" || c.InsecureSkipVerify {
		t.Errorf("verify-full: want ServerName=host and verification on, got %+v", c)
	}
}

func TestSeedAddrs(t *testing.T) {
	// rc_nodes list wins.
	cc := engine.ConnConfig{Host: "h", Port: 6379, Params: map[string]string{paramNodes: "a:1,b:2"}}
	if got := seedAddrs(cc); len(got) != 2 || got[0] != "a:1" || got[1] != "b:2" {
		t.Errorf("seedAddrs(nodes) = %v, want [a:1 b:2]", got)
	}
	// fall back to host:port.
	cc2 := engine.ConnConfig{Host: "h", Port: 6390}
	if got := seedAddrs(cc2); len(got) != 1 || got[0] != "h:6390" {
		t.Errorf("seedAddrs(host) = %v, want [h:6390]", got)
	}
}
