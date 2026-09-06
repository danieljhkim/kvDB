package cmd

import (
	"strconv"

	"github.com/spf13/cobra"

	gateway "github.com/danieljhkim/kv/internal/gen/kvdb/gateway"
)

var delCmd = &cobra.Command{
	Use:     "del [key]",
	Aliases: []string{"delete"},
	Short:   "Delete a key",
	Long: `Delete one key.

Like put, the delete is attempted exactly once and an unknown outcome exits 5
without an automatic retry.`,
	Example: `  kv del greeting
  kv del --key-file ./binary.key`,
	Args:          cobra.MaximumNArgs(1),
	SilenceUsage:  true,
	SilenceErrors: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		flags := cmd.Flags()
		keyFile, _ := flags.GetString("key-file")
		writeOptions, err := writeOptionsFromFlags(cmd)
		if err != nil {
			return err
		}

		source := &bytesSource{}
		key, err := source.operand("key", positional(args, 0), keyFile)
		if err != nil {
			return err
		}

		op, err := start(cmd)
		if err != nil {
			return err
		}
		defer op.close()

		result, err := op.client.Delete(op.ctx, key, writeOptions)
		if err != nil {
			return err
		}

		writeMetadata(cmd.OutOrStdout(),
			"status", gateway.Status_OK.String(),
			"version", strconv.FormatUint(result.Version, 10),
			"request_id", result.RequestID)
		return nil
	},
}

func init() {
	rootCmd.AddCommand(delCmd)
	addWriteFlags(delCmd)
}
