package mysql

import (
	"testing"

	"github.com/rudimk/replicare/internal/engine"
)

// TestTLSParamAllModes confirms every one of the six libpq-style modes maps to a
// concrete go-sql-driver DSN value (no silent downgrade), including the custom
// verify-ca config, and that empty/unknown modes error.
func TestTLSParamAllModes(t *testing.T) {
	cases := map[engine.TLSMode]string{
		engine.TLSDisable:    "false",
		engine.TLSAllow:      "preferred",
		engine.TLSPrefer:     "preferred",
		engine.TLSRequire:    "skip-verify",
		engine.TLSVerifyCA:   tlsConfigVerifyCA,
		engine.TLSVerifyFull: "true",
	}
	for mode, want := range cases {
		got, err := tlsParam(mode)
		if err != nil {
			t.Errorf("tlsParam(%q): %v", mode, err)
			continue
		}
		if got != want {
			t.Errorf("tlsParam(%q) = %q, want %q", mode, got, want)
		}
	}
	if _, err := tlsParam(""); err == nil {
		t.Error("tlsParam(empty): expected error")
	}
	if _, err := tlsParam("bogus"); err == nil {
		t.Error("tlsParam(bogus): expected error")
	}
}

// TestVerifyCARegisters confirms the verify-ca custom TLS config registers
// without error (system roots load, driver accepts it).
func TestVerifyCARegisters(t *testing.T) {
	if err := registerVerifyCA(); err != nil {
		t.Fatalf("registerVerifyCA: %v", err)
	}
	// Idempotent (sync.Once): a second call is a no-op and still nil.
	if err := registerVerifyCA(); err != nil {
		t.Fatalf("registerVerifyCA (2nd): %v", err)
	}
}
