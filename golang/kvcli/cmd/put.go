package cmd

import (
	"strconv"

	"github.com/spf13/cobra"

	"github.com/danieljhkim/kv/internal/client"
	gateway "github.com/danieljhkim/kv/internal/gen/kvdb/gateway"
)

var putCmd = &cobra.Command{
	Use:     "put [key] [value]",
	Aliases: []string{"set"},
	Short:   "Write a value for a key",
	Long: `Write one key.

The write is attempted exactly once. If the outcome is unknown the command
exits 5 and does not retry; rerun it with the same --request-id so the cluster
can de-duplicate the operation.`,
	Example: `  kv put greeting hello
  kv put --key-file ./binary.key --value-file ./binary.value
  cat payload.bin | kv put greeting --value-file -
  kv put greeting hello --request-id 5b1f6b1e-6d0e-4a54-9c94-1f9a8f4c2f10`,
	Args:          cobra.MaximumNArgs(2),
	SilenceUsage:  true,
	SilenceErrors: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		flags := cmd.Flags()
		keyFile, _ := flags.GetString("key-file")
		valueFile, _ := flags.GetString("value-file")
		requestID, _ := flags.GetString("request-id")
		allowReplay, _ := flags.GetBool("allow-server-replay")

		source := &bytesSource{}
		key, err := source.operand("key", positional(args, 0), keyFile)
		if err != nil {
			return err
		}
		value, err := source.operand("value", positional(args, 1), valueFile)
		if err != nil {
			return err
		}

		op, err := start(cmd)
		if err != nil {
			return err
		}
		defer op.close()

		result, err := op.client.Put(op.ctx, key, value, client.WriteOptions{
			RequestID:         requestID,
			AllowServerReplay: allowReplay,
		})
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
	rootCmd.AddCommand(putCmd)
	addWriteFlags(putCmd)
	putCmd.Flags().String("value-file", "", `read the value bytes from a file, or "-" for standard input`)
}

// addWriteFlags registers the options shared by put and del.
func addWriteFlags(cmd *cobra.Command) {
	flags := cmd.Flags()
	flags.String("key-file", "", `read the key bytes from a file, or "-" for standard input`)
	flags.String("request-id", "",
		"reuse a specific RequestContext.request_id (default: a fresh identifier per attempt)")
	flags.Bool("allow-server-replay", false,
		"set require_idempotency so the gateway may replay this write under the same request id")
}
