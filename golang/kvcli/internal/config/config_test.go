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
		"",
	}, "\n"))

	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Security.Mode != ModeDevelopmentPlaintext || cfg.Security.Deployment != "local" {
		t.Fatalf("file did not configure security: %+v", cfg.Security)
	}
	if cfg.Request.Timeout != 250*time.Millisecond || cfg.Request.TenantID != "tenant-9" {
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
	return cfg
}
