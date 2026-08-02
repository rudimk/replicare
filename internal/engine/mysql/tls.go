package mysql

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"sync"

	driver "github.com/go-sql-driver/mysql"

	"github.com/rudimk/replicare/internal/engine"
)

// tlsParam maps a resolved engine.TLSMode to the value go-sql-driver expects in
// the DSN `tls` parameter, mapping ALL six libpq-style modes (mysql-plan §MF1,
// Momus m6). go-sql-driver natively understands "false", "true", "skip-verify",
// and "preferred"; the sixth mode — verify-ca (verify the CA chain but NOT the
// hostname) — has no native flag, so we register a custom *tls.Config once and
// reference it by name.
//
//	disable      -> false        (no TLS)
//	allow/prefer -> preferred    (use TLS if the server offers it, else plain)
//	require      -> skip-verify  (encrypt, do not verify the certificate)
//	verify-ca    -> custom       (verify chain against system roots, not hostname)
//	verify-full  -> true         (full verification incl. hostname)
//
// An empty/unknown mode is an error rather than a silent downgrade.
func tlsParam(mode engine.TLSMode) (string, error) {
	switch mode {
	case engine.TLSDisable:
		return "false", nil
	case engine.TLSAllow, engine.TLSPrefer:
		return "preferred", nil
	case engine.TLSRequire:
		return "skip-verify", nil
	case engine.TLSVerifyCA:
		if err := registerVerifyCA(); err != nil {
			return "", err
		}
		return tlsConfigVerifyCA, nil
	case engine.TLSVerifyFull:
		return "true", nil
	case "":
		return "", fmt.Errorf("mysql: empty TLS mode")
	default:
		return "", fmt.Errorf("mysql: unsupported TLS mode %q", mode)
	}
}

const tlsConfigVerifyCA = "replicare-verify-ca"

var verifyCAOnce sync.Once
var verifyCAErr error

// registerVerifyCA registers (once) a driver TLS config that verifies the
// server certificate chain against the system root pool but skips hostname
// verification — the verify-ca semantics. It uses InsecureSkipVerify to disable
// the default (hostname-checking) verification and re-implements chain
// verification in VerifyPeerCertificate.
func registerVerifyCA() error {
	verifyCAOnce.Do(func() {
		roots, err := x509.SystemCertPool()
		if err != nil {
			verifyCAErr = fmt.Errorf("mysql: verify-ca: load system roots: %w", err)
			return
		}
		cfg := &tls.Config{
			InsecureSkipVerify: true, // we do chain (not hostname) verification below
			VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
				certs := make([]*x509.Certificate, 0, len(rawCerts))
				for _, raw := range rawCerts {
					c, err := x509.ParseCertificate(raw)
					if err != nil {
						return err
					}
					certs = append(certs, c)
				}
				if len(certs) == 0 {
					return fmt.Errorf("mysql: verify-ca: server sent no certificate")
				}
				inter := x509.NewCertPool()
				for _, c := range certs[1:] {
					inter.AddCert(c)
				}
				_, err := certs[0].Verify(x509.VerifyOptions{Roots: roots, Intermediates: inter})
				return err
			},
		}
		verifyCAErr = driver.RegisterTLSConfig(tlsConfigVerifyCA, cfg)
	})
	return verifyCAErr
}
