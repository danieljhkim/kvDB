package client_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"google.golang.org/grpc"
	grpccodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/danieljhkim/kv/internal/client"
	"github.com/danieljhkim/kv/internal/config"
	gateway "github.com/danieljhkim/kv/internal/gen/kvdb/gateway"
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

// rawGatewayClient dials the gateway over TLS with the CA and server name
// trusted but no client certificate. client.Dial always loads a client
// identity, so the gRPC client is built directly.
func rawGatewayClient(t *testing.T, pki *testfixture.PKI, address string) gateway.KvGatewayClient {
	t.Helper()
	caPEM, err := os.ReadFile(pki.CABundlePath)
	if err != nil {
		t.Fatalf("cannot read CA bundle: %v", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		t.Fatal("CA bundle contains no certificates")
	}
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    pool,
		ServerName: "localhost",
	})))
	if err != nil {
		t.Fatalf("cannot create gateway client: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return gateway.NewKvGatewayClient(conn)
}

func TestMissingClientIdentityIsRejectedByTheGateway(t *testing.T) {
	pki := testfixture.NewPKI(t)
	server := testfixture.Start(t, testfixture.Hooks{}, pki.ServerCredentials())

	api := rawGatewayClient(t, pki, server.Address())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := api.Get(ctx, &gateway.GetRequest{Key: []byte("k")})
	if err == nil {
		t.Fatal("a TLS client without a certificate must not reach a gateway that requires one")
	}
	// The handshake is rejected at the transport, so the failure must be a
	// transport-class UNAVAILABLE, not an application status such as NOT_FOUND.
	if code := grpcstatus.Code(err); code != grpccodes.Unavailable {
		t.Fatalf("expected transport rejection as UNAVAILABLE, got %s: %v", code, err)
	}
}

func TestPlaintextClientIsRejectedByTheTLSGateway(t *testing.T) {
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
