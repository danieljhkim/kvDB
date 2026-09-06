// Package config resolves kvcli endpoint, transport security, and request
// settings from (in order of precedence) command flags, environment
// variables, an optional YAML file, and built-in defaults.
package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// Mode mirrors the server-side KVDB_GRPC_SECURITY_MODE policy in
// kv.common GrpcSecurityConfig: mutual TLS by default, plaintext only when
// it is selected explicitly for local development.
type Mode string

const (
	ModeMTLS                 Mode = "mtls"
	ModeDevelopmentPlaintext Mode = "development-plaintext"

	// DefaultTimeout bounds every RPC so no invocation can hang forever.
	DefaultTimeout = 5 * time.Second
)

// developmentDeployments matches the server's allow-list for plaintext.
var developmentDeployments = map[string]bool{
	"dev":         true,
	"development": true,
	"local":       true,
	"test":        true,
}

// Security describes the client end of the gateway TLS boundary.
type Security struct {
	// Mode is "mtls" (default) or "development-plaintext".
	Mode Mode
	// Deployment is KVDB_ENV; plaintext is refused unless it names a
	// development deployment.
	Deployment string
	// ServerName overrides the name verified against the server certificate.
	// Empty means the connection host is verified.
	ServerName string `mapstructure:"server_name"`
	// CertChain and PrivateKey are the client identity presented to the
	// gateway; the gateway listener requires a client certificate.
	CertChain  string `mapstructure:"cert_chain"`
	PrivateKey string `mapstructure:"private_key"`
	// TrustBundle is the CA bundle used to verify the gateway certificate.
	TrustBundle string `mapstructure:"trust_bundle"`
}

// Request carries per-call settings shared by every operation.
type Request struct {
	// Timeout bounds a single RPC including connection establishment.
	Timeout time.Duration
	// TenantID and Principal populate RequestContext. In development plaintext
	// they also form the required, forgeable local client identity.
	TenantID  string `mapstructure:"tenant_id"`
	Principal string
}

// Config is the fully resolved client configuration.
type Config struct {
	Server struct {
		Host string
		Port int
	}
	Security Security
	Request  Request
}

// Address renders the gRPC target for the configured endpoint.
func (c *Config) Address() string {
	return fmt.Sprintf("%s:%d", c.Server.Host, c.Server.Port)
}

// Load reads defaults, an optional config file, and environment overrides.
// It does not validate: callers apply flag overrides first and then call
// Validate.
func Load(cfgFile string) (*Config, error) {
	v := viper.New()

	v.SetDefault("server.host", "localhost")
	v.SetDefault("server.port", 7000)
	v.SetDefault("security.mode", string(ModeMTLS))
	v.SetDefault("request.timeout", DefaultTimeout)

	if cfgFile != "" {
		v.SetConfigFile(cfgFile)
	} else {
		v.AddConfigPath(".")
		v.AddConfigPath("$HOME/.kvcli")
		v.SetConfigName("config")
		v.SetConfigType("yaml")
	}

	v.AutomaticEnv()
	v.SetEnvPrefix("KVCLI")

	// Transport security is configured with the same variable names the
	// server uses, so one exported environment configures both sides.
	for key, env := range map[string]string{
		"security.mode":         "KVDB_GRPC_SECURITY_MODE",
		"security.deployment":   "KVDB_ENV",
		"security.server_name":  "KVDB_CLIENT_TLS_SERVER_NAME",
		"security.cert_chain":   "KVDB_CLIENT_TLS_CERT_CHAIN",
		"security.private_key":  "KVDB_CLIENT_TLS_PRIVATE_KEY",
		"security.trust_bundle": "KVDB_CLIENT_TLS_TRUST_BUNDLE",
		"request.tenant_id":     "KVDB_CLIENT_TENANT_ID",
		"request.principal":     "KVDB_CLIENT_PRINCIPAL",
	} {
		if err := v.BindEnv(key, env); err != nil {
			return nil, err
		}
	}

	if err := v.ReadInConfig(); err == nil {
		// Never write to stdout: raw value bytes are the only thing that
		// may appear there.
		fmt.Fprintln(os.Stderr, "Using config file:", v.ConfigFileUsed())
	}

	var config Config
	if err := v.Unmarshal(&config); err != nil {
		return nil, err
	}
	config.Security.Mode = Mode(strings.ToLower(strings.TrimSpace(string(config.Security.Mode))))
	config.Security.Deployment = strings.ToLower(strings.TrimSpace(config.Security.Deployment))
	if config.Request.Timeout <= 0 {
		config.Request.Timeout = DefaultTimeout
	}

	return &config, nil
}

// Validate fails closed: an unknown mode, plaintext outside a development
// deployment, or unreadable credential files are configuration errors.
func (c *Config) Validate() error {
	if c.Server.Host == "" {
		return fmt.Errorf("server host is required")
	}
	if c.Server.Port <= 0 || c.Server.Port > 65535 {
		return fmt.Errorf("server port %d is out of range", c.Server.Port)
	}
	if c.Request.Timeout <= 0 {
		return fmt.Errorf("request timeout must be positive")
	}

	switch c.Security.Mode {
	case ModeDevelopmentPlaintext:
		if !developmentDeployments[c.Security.Deployment] {
			return fmt.Errorf(
				"%s requires KVDB_ENV to be dev, development, local, or test", ModeDevelopmentPlaintext)
		}
		_, err := c.DevelopmentIdentity()
		return err
	case ModeMTLS:
		for _, field := range []struct{ name, value string }{
			{"KVDB_CLIENT_TLS_TRUST_BUNDLE (--tls-ca)", c.Security.TrustBundle},
			{"KVDB_CLIENT_TLS_CERT_CHAIN (--tls-cert)", c.Security.CertChain},
			{"KVDB_CLIENT_TLS_PRIVATE_KEY (--tls-key)", c.Security.PrivateKey},
		} {
			if strings.TrimSpace(field.value) == "" {
				return fmt.Errorf("%s is required when mTLS is enabled", field.name)
			}
			if err := readableFile(field.value); err != nil {
				return fmt.Errorf("%s must name a readable file: %w", field.name, err)
			}
		}
		return nil
	default:
		return fmt.Errorf(
			"security mode %q is not supported; use %s or %s", c.Security.Mode, ModeMTLS, ModeDevelopmentPlaintext)
	}
}

// DevelopmentIdentity returns the identity required by the gateway's
// development-plaintext authentication boundary. It deliberately validates
// each segment before dialing so malformed local configuration cannot turn
// into an unauthenticated RPC.
func (c *Config) DevelopmentIdentity() (string, error) {
	tenant, err := developmentIdentityComponent("KVDB_CLIENT_TENANT_ID (--tenant)", c.Request.TenantID)
	if err != nil {
		return "", err
	}
	principal, err := developmentIdentityComponent("KVDB_CLIENT_PRINCIPAL (--principal)", c.Request.Principal)
	if err != nil {
		return "", err
	}
	return "client/" + tenant + "/" + principal, nil
}

func developmentIdentityComponent(name, value string) (string, error) {
	if value == "" || strings.TrimSpace(value) == "" {
		return "", fmt.Errorf("%s is required when development-plaintext is enabled", name)
	}
	if strings.TrimSpace(value) != value {
		return "", fmt.Errorf("%s must not have leading or trailing whitespace", name)
	}
	if strings.Contains(value, "/") {
		return "", fmt.Errorf("%s must not contain /", name)
	}
	for _, character := range value {
		if character < 0x21 || character > 0x7e {
			return "", fmt.Errorf("%s must contain printable ASCII characters only", name)
		}
	}
	return value, nil
}

func readableFile(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("%s is not a regular file", path)
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	return file.Close()
}
