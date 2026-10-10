package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeConfig(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "kvcli.yaml")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadUsesExplicitConfiguration(t *testing.T) {
	path := writeConfig(t, "server:\n  host: 192.0.2.10\n  port: 7443\n")

	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Server.Host != "192.0.2.10" || cfg.Server.Port != 7443 {
		t.Fatalf("unexpected server configuration: %+v", cfg.Server)
	}
	if cfg.Address() != "192.0.2.10:7443" {
		t.Fatalf("unexpected address %q", cfg.Address())
	}
}

func TestAddressJoinsIPv6AndPreservesOtherHosts(t *testing.T) {
	for _, test := range []struct {
		host string
		want string
	}{
		{host: "::1", want: "[::1]:7443"},
		{host: "fe80::1%lo0", want: "[fe80::1%lo0]:7443"},
		{host: "[::1]", want: "[::1]:7443"},
		{host: "192.0.2.10", want: "192.0.2.10:7443"},
		{host: "localhost", want: "localhost:7443"},
	} {
		t.Run(test.host, func(t *testing.T) {
			cfg := baseConfig()
			cfg.Server.Host = test.host
			cfg.Server.Port = 7443
			if got := cfg.Address(); got != test.want {
				t.Fatalf("Address() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestLoadFallsBackToSafeLocalDefaults(t *testing.T) {
	cfg, err := Load(filepath.Join(t.TempDir(), "missing.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Server.Host != "localhost" || cfg.Server.Port != 7000 {
		t.Fatalf("unexpected defaults: %+v", cfg.Server)
	}
	if cfg.Security.Mode != ModeMTLS {
		t.Fatalf("transport security must default to mTLS, got %q", cfg.Security.Mode)
	}
	if cfg.Request.Timeout != DefaultTimeout {
		t.Fatalf("RPCs must be bounded by default, got %s", cfg.Request.Timeout)
	}
}

// isolateDiscovery points the working directory and HOME at empty temporary
// directories so no real config.yaml or ~/.kvcli/config.yaml can affect a test.
func isolateDiscovery(t *testing.T) (workdir, home string) {
	t.Helper()
	home = t.TempDir()
	workdir = t.TempDir()
	t.Setenv("HOME", home)
	t.Chdir(workdir)
	return workdir, home
}

func TestAbsentDiscoveredConfigUsesDefaults(t *testing.T) {
	isolateDiscovery(t)

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("an absent optional config must fall back to defaults: %v", err)
	}
	if cfg.Server.Host != "localhost" || cfg.Server.Port != 7000 {
		t.Fatalf("unexpected defaults: %+v", cfg.Server)
	}
}

func TestMalformedExplicitConfigIsRejected(t *testing.T) {
	path := writeConfig(t, "server: [unterminated\n")

	if _, err := Load(path); err == nil {
		t.Fatal("a malformed explicit config must not fall back to defaults")
	}
}

func TestUnreadableExplicitConfigIsRejected(t *testing.T) {
	// A directory exists at the path but cannot be read as a config file.
	if _, err := Load(t.TempDir()); err == nil {
		t.Fatal("an unreadable explicit config must not fall back to defaults")
	}

	if os.Geteuid() == 0 {
		t.Skip("file permissions do not restrict root")
	}
	path := writeConfig(t, "server:\n  port: 7443\n")
	if err := os.Chmod(path, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("a permission-denied explicit config must not fall back to defaults")
	}
}

func TestMalformedDiscoveredConfigIsRejected(t *testing.T) {
	workdir, _ := isolateDiscovery(t)
	writeFile(t, filepath.Join(workdir, "config.yaml"), "server: [unterminated\n")

	if _, err := Load(""); err == nil {
		t.Fatal("a malformed ./config.yaml must not fall back to defaults")
	}
}

func TestMalformedHomeConfigIsRejected(t *testing.T) {
	_, home := isolateDiscovery(t)
	if err := os.Mkdir(filepath.Join(home, ".kvcli"), 0o700); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(home, ".kvcli", "config.yaml"), "security: [unterminated\n")

	if _, err := Load(""); err == nil {
		t.Fatal("a malformed ~/.kvcli/config.yaml must not fall back to defaults")
	}
}

func writeFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestSecurityIsConfiguredWithServerEnvironmentVariables(t *testing.T) {
	directory := t.TempDir()
	certificate := filepath.Join(directory, "client.crt")
	key := filepath.Join(directory, "client.key")
	bundle := filepath.Join(directory, "ca.pem")
	for _, path := range []string{certificate, key, bundle} {
		if err := os.WriteFile(path, []byte("placeholder"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	t.Setenv("KVDB_GRPC_SECURITY_MODE", "mtls")
	t.Setenv("KVDB_CLIENT_TLS_CERT_CHAIN", certificate)
	t.Setenv("KVDB_CLIENT_TLS_PRIVATE_KEY", key)
	t.Setenv("KVDB_CLIENT_TLS_TRUST_BUNDLE", bundle)
	t.Setenv("KVDB_CLIENT_TLS_SERVER_NAME", "kvdb-gateway")
	t.Setenv("KVDB_CLIENT_TENANT_ID", "tenant-7")

	cfg, err := Load(filepath.Join(t.TempDir(), "missing.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Security.CertChain != certificate || cfg.Security.PrivateKey != key ||
		cfg.Security.TrustBundle != bundle || cfg.Security.ServerName != "kvdb-gateway" {
		t.Fatalf("environment did not configure TLS: %+v", cfg.Security)
	}
	if cfg.Request.TenantID != "tenant-7" {
		t.Fatalf("environment did not configure the request context: %+v", cfg.Request)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("complete mTLS configuration was rejected: %v", err)
	}
}

func TestConfigFileConfiguresSecurity(t *testing.T) {
	path := writeConfig(t, strings.Join([]string{
		"security:",
		"  mode: development-plaintext",
		"  deployment: local",
		"request:",
		"  timeout: 250ms",
		"  tenant_id: tenant-9",
		"  principal: alice",
		"",
	}, "\n"))

	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Security.Mode != ModeDevelopmentPlaintext || cfg.Security.Deployment != "local" {
		t.Fatalf("file did not configure security: %+v", cfg.Security)
	}
	if cfg.Request.Timeout != 250*time.Millisecond || cfg.Request.TenantID != "tenant-9" || cfg.Request.Principal != "alice" {
		t.Fatalf("file did not configure requests: %+v", cfg.Request)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("local development configuration was rejected: %v", err)
	}
}

func TestPlaintextRequiresADevelopmentDeployment(t *testing.T) {
	for _, deployment := range []string{"", "prod", "production", "staging", "Dev "} {
		cfg := validPlaintextConfig()
		cfg.Security.Deployment = deployment
		if err := cfg.Validate(); err == nil {
			t.Fatalf("plaintext must be refused for KVDB_ENV=%q", deployment)
		}
	}
	for _, deployment := range []string{"dev", "development", "local", "test"} {
		cfg := validPlaintextConfig()
		cfg.Security.Deployment = deployment
		if err := cfg.Validate(); err != nil {
			t.Fatalf("plaintext must be allowed for KVDB_ENV=%q: %v", deployment, err)
		}
	}
}

func TestPlaintextRequiresACompleteDevelopmentIdentity(t *testing.T) {
	for _, request := range []Request{
		{},
		{TenantID: "tenant-a"},
		{Principal: "alice"},
		{TenantID: "tenant/a", Principal: "alice"},
		{TenantID: "tenant-a", Principal: "alice/bob"},
		{TenantID: "tenant-a", Principal: " alice"},
	} {
		cfg := validPlaintextConfig()
		cfg.Request = request
		cfg.Request.Timeout = DefaultTimeout
		if err := cfg.Validate(); err == nil {
			t.Fatalf("plaintext identity %+v must be rejected", request)
		}
	}

	cfg := validPlaintextConfig()
	cfg.Request.TenantID = "tenant-a"
	cfg.Request.Principal = "alice"
	identity, err := cfg.DevelopmentIdentity()
	if err != nil {
		t.Fatalf("complete development identity was rejected: %v", err)
	}
	if identity != "client/tenant-a/alice" {
		t.Fatalf("unexpected development identity %q", identity)
	}
}

func TestUnknownSecurityModeIsRejected(t *testing.T) {
	cfg := validPlaintextConfig()
	cfg.Security.Mode = "insecure"
	err := cfg.Validate()
	if err == nil {
		t.Fatal("an unknown security mode must be rejected")
	}
	if !strings.Contains(err.Error(), "development-plaintext") {
		t.Fatalf("error should name the supported modes: %v", err)
	}
}

func TestMtlsRequiresReadableCredentialFiles(t *testing.T) {
	directory := t.TempDir()
	existing := filepath.Join(directory, "present.pem")
	if err := os.WriteFile(existing, []byte("placeholder"), 0o600); err != nil {
		t.Fatal(err)
	}

	missingAll := baseConfig()
	missingAll.Security.Mode = ModeMTLS
	if err := missingAll.Validate(); err == nil {
		t.Fatal("mTLS without credentials must be rejected")
	}

	missingKey := baseConfig()
	missingKey.Security.Mode = ModeMTLS
	missingKey.Security.TrustBundle = existing
	missingKey.Security.CertChain = existing
	missingKey.Security.PrivateKey = filepath.Join(directory, "absent.key")
	err := missingKey.Validate()
	if err == nil {
		t.Fatal("an unreadable private key must be rejected")
	}
	if !strings.Contains(err.Error(), "KVDB_CLIENT_TLS_PRIVATE_KEY") {
		t.Fatalf("error should name the missing setting: %v", err)
	}

	directoryInsteadOfFile := baseConfig()
	directoryInsteadOfFile.Security.Mode = ModeMTLS
	directoryInsteadOfFile.Security.TrustBundle = directory
	directoryInsteadOfFile.Security.CertChain = existing
	directoryInsteadOfFile.Security.PrivateKey = existing
	if err := directoryInsteadOfFile.Validate(); err == nil {
		t.Fatal("a directory is not a credential file")
	}
}

func TestEndpointAndTimeoutAreValidated(t *testing.T) {
	noHost := validPlaintextConfig()
	noHost.Server.Host = ""
	if err := noHost.Validate(); err == nil {
		t.Fatal("an empty host must be rejected")
	}

	badPort := validPlaintextConfig()
	badPort.Server.Port = 70000
	if err := badPort.Validate(); err == nil {
		t.Fatal("an out-of-range port must be rejected")
	}

	unbounded := validPlaintextConfig()
	unbounded.Request.Timeout = 0
	if err := unbounded.Validate(); err == nil {
		t.Fatal("an unbounded timeout must be rejected")
	}
}

func baseConfig() *Config {
	cfg := &Config{}
	cfg.Server.Host = "127.0.0.1"
	cfg.Server.Port = 7000
	cfg.Request.Timeout = DefaultTimeout
	return cfg
}

func validPlaintextConfig() *Config {
	cfg := baseConfig()
	cfg.Security.Mode = ModeDevelopmentPlaintext
	cfg.Security.Deployment = "test"
	cfg.Request.TenantID = "tenant-1"
	cfg.Request.Principal = "operator-1"
	return cfg
}
