package client_test

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/danieljhkim/kv/internal/client"
	"github.com/danieljhkim/kv/internal/config"
	"github.com/danieljhkim/kv/internal/testfixture"
)

func splitHostPort(address string) (string, int) {
	host, portText, err := net.SplitHostPort(address)
	if err != nil {
		panic(err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		panic(err)
	}
	return host, port
}

func mtlsConfig(address string, pki *testfixture.PKI) *config.Config {
	cfg := &config.Config{}
	cfg.Server.Host, cfg.Server.Port = splitHostPort(address)
	cfg.Security.Mode = config.ModeMTLS
	cfg.Security.TrustBundle = pki.CABundlePath
	cfg.Security.CertChain = pki.ClientCertPath
	cfg.Security.PrivateKey = pki.ClientKeyPath
	cfg.Request.Timeout = 5 * time.Second
	return cfg
}

func TestMutualTlsAuthenticatesBothSides(t *testing.T) {
	pki := testfixture.NewPKI(t)
	server := testfixture.Start(t, testfixture.Hooks{}, pki.ServerCredentials())

	cfg := mtlsConfig(server.Address(), pki)
	kv := dial(t, cfg)

	if _, err := kv.Put(context.Background(), []byte("k"), []byte("v"), client.WriteOptions{}); err != nil {
		t.Fatalf("authenticated put failed: %v", err)
	}
	result, err := kv.Get(context.Background(), []byte("k"), client.ReadOptions{})
	if err != nil {
		t.Fatalf("authenticated get failed: %v", err)
	}
	if string(result.Value) != "v" {
		t.Fatalf("unexpected value %q", result.Value)
	}
}

func TestUntrustedServerCertificateIsRejected(t *testing.T) {
	serverPKI := testfixture.NewPKI(t)
	clientPKI := testfixture.NewPKI(t)
	server := testfixture.Start(t, testfixture.Hooks{}, serverPKI.ServerCredentialsWithoutClientAuth())

	// Trust a different CA than the one that issued the server certificate.
	cfg := mtlsConfig(server.Address(), clientPKI)
	kv := dial(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := kv.Get(ctx, []byte("k"), client.ReadOptions{})
	var transportErr *client.TransportError
	if err == nil {
		t.Fatal("connecting to an untrusted server must fail")
	}
	if !errors.As(err, &transportErr) {
		t.Fatalf("expected a transport error, got %v", err)
	}
}

func TestServerNameMismatchIsRejected(t *testing.T) {
	pki := testfixture.NewPKI(t)
	server := testfixture.Start(t, testfixture.Hooks{}, pki.ServerCredentialsWithoutClientAuth())

	cfg := mtlsConfig(server.Address(), pki)
	cfg.Security.ServerName = "not-the-gateway"
	kv := dial(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := kv.Get(ctx, []byte("k"), client.ReadOptions{}); err == nil {
		t.Fatal("a certificate that does not name the server must be rejected")
	}
}

func TestMissingClientIdentityIsRejectedByTheGateway(t *testing.T) {
	pki := testfixture.NewPKI(t)
	server := testfixture.Start(t, testfixture.Hooks{}, pki.ServerCredentials())

	cfg := plaintextConfig(server.Address())
	kv := dial(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := kv.Get(ctx, []byte("k"), client.ReadOptions{}); err == nil {
		t.Fatal("a plaintext client must not reach a TLS gateway")
	}
}

func TestInvalidCredentialFilesFailBeforeAnyRpc(t *testing.T) {
	pki := testfixture.NewPKI(t)

	cfg := mtlsConfig("127.0.0.1:1", pki)
	cfg.Security.TrustBundle = filepath.Join(pki.Dir, "client.key") // not a CA bundle
	if _, err := client.Dial(cfg); err == nil {
		t.Fatal("a trust bundle without certificates must be rejected")
	}

	cfg = mtlsConfig("127.0.0.1:1", pki)
	cfg.Security.PrivateKey = pki.ServerKeyPath // does not match the client chain
	if _, err := client.Dial(cfg); err == nil {
		t.Fatal("a key that does not match the certificate must be rejected")
	}
}
