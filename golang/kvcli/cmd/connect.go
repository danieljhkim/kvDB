package cmd

import (
	"github.com/spf13/cobra"
)

// connectCmd survives only to reject the removed interactive line protocol
// with an explanation instead of an unknown-command error.
var connectCmd = &cobra.Command{
	Use:   "connect",
	Short: "Unsupported: the interactive line protocol was removed",
	Long: `kvcli no longer opens an interactive session.

The gateway exposes a gRPC data plane (Get, Put, Delete) over authenticated
TLS, so there is no line protocol to connect to. Configure the endpoint and
credentials once, then run individual commands:

  kv put greeting hello
  kv get greeting
  kv del greeting`,
	SilenceUsage:  true,
	SilenceErrors: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		return errLegacyInteractive
	},
}

func init() {
	rootCmd.AddCommand(connectCmd)
}
