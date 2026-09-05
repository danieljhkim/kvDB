package testfixture

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc/credentials"
)

// PKI is a throwaway certificate authority with one server and one client
// identity, written to disk so tests exercise the same file-based
// configuration operators use.
type PKI struct {
	Dir              string
	CABundlePath     string
	ServerCertPath   string
	ServerKeyPath    string
	ClientCertPath   string
	ClientKeyPath    string
	serverCredential tls.Certificate
	pool             *x509.CertPool
}

// NewPKI issues a CA, a server certificate for 127.0.0.1/localhost, and a
// client certificate.
func NewPKI(t *testing.T) *PKI {
	t.Helper()
	dir := t.TempDir()

	caKey, caCertificate, caPEM := issueCA(t)
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		t.Fatal("test CA bundle did not parse")
	}

	serverCertPEM, serverKeyPEM := issueLeaf(t, caCertificate, caKey, "kvdb-gateway", true)
	clientCertPEM, clientKeyPEM := issueLeaf(t, caCertificate, caKey, "kvdb-client", false)

	serverCredential, err := tls.X509KeyPair(serverCertPEM, serverKeyPEM)
	if err != nil {
		t.Fatalf("cannot build server credential: %v", err)
	}

	pki := &PKI{
		Dir:              dir,
		CABundlePath:     write(t, dir, "ca.pem", caPEM),
		ServerCertPath:   write(t, dir, "server.crt", serverCertPEM),
		ServerKeyPath:    write(t, dir, "server.key", serverKeyPEM),
		ClientCertPath:   write(t, dir, "client.crt", clientCertPEM),
		ClientKeyPath:    write(t, dir, "client.key", clientKeyPEM),
		serverCredential: serverCredential,
		pool:             pool,
	}
	return pki
}

// ServerCredentials requires a client certificate signed by the test CA,
// matching the gateway listener policy.
func (p *PKI) ServerCredentials() credentials.TransportCredentials {
	return credentials.NewTLS(&tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{p.serverCredential},
		ClientCAs:    p.pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	})
}

// ServerCredentialsWithoutClientAuth serves TLS without demanding a client
// certificate, for tests that isolate server verification.
func (p *PKI) ServerCredentialsWithoutClientAuth() credentials.TransportCredentials {
	return credentials.NewTLS(&tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{p.serverCredential},
	})
}

func issueCA(t *testing.T) (*ecdsa.PrivateKey, *x509.Certificate, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("cannot generate CA key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "kvdb-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("cannot self-sign CA: %v", err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("cannot parse CA: %v", err)
	}
	return key, certificate, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func issueLeaf(
	t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey, commonName string, server bool,
) ([]byte, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("cannot generate leaf key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: commonName},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
	}
	if server {
		template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		template.DNSNames = []string{"localhost", "kvdb-gateway"}
		template.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
	} else {
		template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatalf("cannot sign leaf: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("cannot marshal leaf key: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
}

func write(t *testing.T, dir, name string, contents []byte) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatalf("cannot write %s: %v", path, err)
	}
	return path
}
