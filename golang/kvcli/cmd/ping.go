/*
Copyright © 2025 danieljhkim
*/
package cmd

import (
	"github.com/spf13/cobra"

	"github.com/danieljhkim/kv/internal/client"
	gateway "github.com/danieljhkim/kv/internal/gen/kvdb/gateway"
)

// probeKey is read head-only to prove reachability without storing anything.
const probeKey = "__kvcli_probe__"

var pingCmd = &cobra.Command{
	Use:   "ping [key]",
	Short: "Check that the gateway is reachable and accepts this identity",
	Long: `Issue one bounded, head-only Get and report whether the gateway answered.

A missing probe key counts as reachable: the check proves connectivity,
transport security, and authorization, not the presence of data.`,
	Args:          cobra.MaximumNArgs(1),
	SilenceUsage:  true,
	SilenceErrors: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		keyFile, _ := cmd.Flags().GetString("key-file")

		key := []byte(probeKey)
		if len(args) > 0 || keyFile != "" {
			source := &bytesSource{}
			resolved, err := source.operand("key", positional(args, 0), keyFile)
			if err != nil {
				return err
			}
			key = resolved
		}

		op, err := start(cmd)
		if err != nil {
			return err
		}
		defer op.close()

		_, err = op.client.Get(op.ctx, key, client.ReadOptions{HeadOnly: true})
		if err != nil && !client.IsStatus(err, gateway.Status_NOT_FOUND) {
			return err
		}

		writeMetadata(cmd.OutOrStdout(),
			"status", gateway.Status_OK.String(),
			"endpoint", op.cfg.Address(),
			"security_mode", string(op.cfg.Security.Mode))
		return nil
	},
}

func init() {
	rootCmd.AddCommand(pingCmd)
	pingCmd.Flags().String("key-file", "", `probe with the key bytes in a file, or "-" for standard input`)
}
