package redis

import (
	"crypto/tls"
	"crypto/x509"

	"github.com/rudimk/replicare/internal/engine"
)

// tlsConfig maps a TLSMode to a *tls.Config for go-redis (nil = plaintext). Redis
// has no STARTTLS negotiation, so the libpq-style "opportunistic" modes collapse:
//
//   - disable / allow / prefer -> plaintext (nil). allow/prefer do NOT force TLS;
//     use require+ to encrypt (documented in docs/redis-version-support.md).
//   - require                  -> encrypt, DO NOT verify (libpq "require":
//     confidentiality without authentication).
//   - verify-ca                -> verify the cert CHAIN against system roots but
//     NOT the hostname.
//   - verify-full              -> full verification (chain + hostname).
//
// Custom CA bundles / client certs are a later addition; verify-ca/verify-full use
// the system root pool.
func tlsConfig(mode engine.TLSMode, serverName string) *tls.Config {
	switch mode {
	case engine.TLSRequire:
		return &tls.Config{InsecureSkipVerify: true} //nolint:gosec // "require" = encrypt-without-verify by design
	case engine.TLSVerifyCA:
		// Verify the chain against system roots, but skip the hostname check. We
		// disable the default verifier (which checks hostname) and supply our own
		// chain-only verifier.
		return &tls.Config{
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: true, //nolint:gosec // hostname intentionally skipped; chain verified below
			VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
				return verifyChain(rawCerts, serverName, false)
			},
		}
	case engine.TLSVerifyFull:
		return &tls.Config{MinVersion: tls.VersionTLS12, ServerName: serverName}
	default:
		// disable / allow / prefer / unset -> plaintext.
		return nil
	}
}

// verifyChain verifies the presented certificate chain against the system root
// pool. When checkHostname is false the DNS-name check is skipped (verify-ca).
func verifyChain(rawCerts [][]byte, serverName string, checkHostname bool) error {
	certs := make([]*x509.Certificate, 0, len(rawCerts))
	for _, raw := range rawCerts {
		c, err := x509.ParseCertificate(raw)
		if err != nil {
			return err
		}
		certs = append(certs, c)
	}
	if len(certs) == 0 {
		return x509.CertificateInvalidError{Reason: x509.NotAuthorizedToSign}
	}
	roots, err := x509.SystemCertPool()
	if err != nil {
		return err
	}
	inter := x509.NewCertPool()
	for _, c := range certs[1:] {
		inter.AddCert(c)
	}
	opts := x509.VerifyOptions{Roots: roots, Intermediates: inter}
	if checkHostname {
		opts.DNSName = serverName
	}
	_, err = certs[0].Verify(opts)
	return err
}
