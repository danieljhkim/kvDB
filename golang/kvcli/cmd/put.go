package cmd

import (
	"fmt"
	"strconv"
	"time"

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
  kv put greeting hello --if-not-exists
  kv put greeting updated --if-version 7
  kv put cache value --ttl 10m
  kv put greeting hello --request-id 5b1f6b1e-6d0e-4a54-9c94-1f9a8f4c2f10`,
	Args:          cobra.MaximumNArgs(2),
	SilenceUsage:  true,
	SilenceErrors: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		flags := cmd.Flags()
		keyFile, _ := flags.GetString("key-file")
		valueFile, _ := flags.GetString("value-file")
		writeOptions, err := writeOptionsFromFlags(cmd)
		if err != nil {
			return err
		}

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

		result, err := op.client.Put(op.ctx, key, value, writeOptions)
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
	putCmd.Flags().Bool("if-not-exists", false, "write only when the key does not exist")
	putCmd.Flags().Duration("ttl", 0, "value lifetime using Go duration syntax (for example 10m; 0 means no expiry)")
}

// addWriteFlags registers the options shared by put and del.
func addWriteFlags(cmd *cobra.Command) {
	flags := cmd.Flags()
	flags.String("key-file", "", `read the key bytes from a file, or "-" for standard input`)
	flags.String("request-id", "",
		"reuse a specific RequestContext.request_id (default: a fresh identifier per attempt)")
	flags.Bool("allow-server-replay", false,
		"set require_idempotency so the gateway may replay this write under the same request id")
	flags.Uint64("if-version", 0, "write only when the current version equals this value")
}

func writeOptionsFromFlags(cmd *cobra.Command) (client.WriteOptions, error) {
	flags := cmd.Flags()
	requestID, _ := flags.GetString("request-id")
	allowReplay, _ := flags.GetBool("allow-server-replay")
	options := client.WriteOptions{RequestID: requestID, AllowServerReplay: allowReplay}
	if flags.Changed("if-version") {
		version, _ := flags.GetUint64("if-version")
		options.IfVersionEquals = &version
	}
	if flag := flags.Lookup("if-not-exists"); flag != nil {
		options.IfNotExists, _ = flags.GetBool("if-not-exists")
	}
	if flags.Lookup("ttl") != nil {
		ttl, _ := flags.GetDuration("ttl")
		if ttl < 0 {
			return client.WriteOptions{}, &UsageError{Err: fmt.Errorf("--ttl must not be negative")}
		}
		if ttl%time.Millisecond != 0 {
			return client.WriteOptions{}, &UsageError{Err: fmt.Errorf("--ttl must be an exact number of milliseconds")}
		}
		options.TTL = ttl
	}
	return options, nil
}
