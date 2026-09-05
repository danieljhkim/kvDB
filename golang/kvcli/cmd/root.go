/*
Copyright © 2025 danieljhkim
*/
package cmd

import (
	"fmt"
	"net"
	"os"
	"strconv"

	"github.com/spf13/cobra"

	"github.com/danieljhkim/kv/internal/config"
)

var cfgFile string

var rootCmd = &cobra.Command{
	Use:   "kv",
	Short: "KV CLI - a non-interactive client for the KvDB gRPC gateway",
	Long: `kv is a non-interactive client for the KvDB gateway data plane.

It speaks the KvGateway gRPC contract (Get, Put, Delete) over mutually
authenticated TLS. Keys and values are byte strings: use the file and stdin
options to move arbitrary binary data without corrupting it.

The legacy interactive line protocol is not supported.`,
	SilenceUsage:  true,
	SilenceErrors: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		if interactive, _ := cmd.Flags().GetBool("interactive"); interactive {
			return errLegacyInteractive
		}
		return cmd.Help()
	},
}

// Execute runs the command tree and returns the process exit code.
func Execute() int {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		return exitCode(err)
	}
	return ExitOK
}

func init() {
	flags := rootCmd.PersistentFlags()
	flags.StringVar(&cfgFile, "config", "", "config file (default is ./config.yaml)")
	flags.StringP("host", "H", "", "gateway hostname or IP address")
	flags.IntP("port", "p", 0, "gateway gRPC port")
	flags.String("address", "", "gateway endpoint as host:port (overrides --host and --port)")
	flags.Duration("timeout", 0, "deadline applied to each RPC")
	flags.String("security-mode", "",
		"transport security: mtls (default) or development-plaintext (requires KVDB_ENV=dev|development|local|test)")
	flags.String("tls-ca", "", "CA bundle used to verify the gateway certificate")
	flags.String("tls-cert", "", "client certificate chain presented to the gateway")
	flags.String("tls-key", "", "private key for the client certificate chain")
	flags.String("server-name", "", "name verified against the gateway certificate (defaults to the host)")
	flags.String("tenant", "", "informational RequestContext.tenant_id")
	flags.String("principal", "", "informational RequestContext.principal")

	rootCmd.Flags().BoolP("interactive", "i", false, "unsupported: the interactive line protocol was removed")
}

// resolveConfig layers flag overrides on top of file and environment values.
func resolveConfig(cmd *cobra.Command) (*config.Config, error) {
	cfg, err := config.Load(cfgFile)
	if err != nil {
		return nil, &UsageError{Err: fmt.Errorf("cannot load configuration: %w", err)}
	}

	flags := cmd.Flags()
	if flags.Changed("host") {
		cfg.Server.Host, _ = flags.GetString("host")
	}
	if flags.Changed("port") {
		cfg.Server.Port, _ = flags.GetInt("port")
	}
	if flags.Changed("address") {
		address, _ := flags.GetString("address")
		host, port, err := splitAddress(address)
		if err != nil {
			return nil, &UsageError{Err: err}
		}
		cfg.Server.Host, cfg.Server.Port = host, port
	}
	if flags.Changed("timeout") {
		timeout, _ := flags.GetDuration("timeout")
		cfg.Request.Timeout = timeout
	}
	if flags.Changed("security-mode") {
		mode, _ := flags.GetString("security-mode")
		cfg.Security.Mode = config.Mode(mode)
	}
	if flags.Changed("tls-ca") {
		cfg.Security.TrustBundle, _ = flags.GetString("tls-ca")
	}
	if flags.Changed("tls-cert") {
		cfg.Security.CertChain, _ = flags.GetString("tls-cert")
	}
	if flags.Changed("tls-key") {
		cfg.Security.PrivateKey, _ = flags.GetString("tls-key")
	}
	if flags.Changed("server-name") {
		cfg.Security.ServerName, _ = flags.GetString("server-name")
	}
	if flags.Changed("tenant") {
		cfg.Request.TenantID, _ = flags.GetString("tenant")
	}
	if flags.Changed("principal") {
		cfg.Request.Principal, _ = flags.GetString("principal")
	}

	if err := cfg.Validate(); err != nil {
		return nil, &UsageError{Err: err}
	}
	return cfg, nil
}

func splitAddress(address string) (string, int, error) {
	host, portText, err := net.SplitHostPort(address)
	if err != nil {
		return "", 0, fmt.Errorf("--address must be host:port: %w", err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		return "", 0, fmt.Errorf("--address port %q is not a number", portText)
	}
	return host, port, nil
}
