/*
Copyright © danieljhkim
*/
package cmd

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/danieljhkim/kv/internal/client"
	gateway "github.com/danieljhkim/kv/internal/gen/kvdb/gateway"
)

var getCmd = &cobra.Command{
	Use:   "get [key]",
	Short: "Read the value of a key",
	Long: `Read one key from the gateway.

The value is written to standard output and the outcome is reported on
standard error, so redirecting standard output captures exactly the stored
bytes. A missing key exits 4; a key that holds an empty value exits 0 with no
output.`,
	Example: `  kv get greeting
  kv get --key-file ./binary.key --raw > value.bin
  printf 'k' | kv get --key-file - --output-file value.bin`,
	Args:          cobra.MaximumNArgs(1),
	SilenceUsage:  true,
	SilenceErrors: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		flags := cmd.Flags()
		keyFile, _ := flags.GetString("key-file")
		raw, _ := flags.GetBool("raw")
		headOnly, _ := flags.GetBool("head")
		outputFile, _ := flags.GetString("output-file")
		consistencyName, _ := flags.GetString("consistency")

		source := &bytesSource{}
		key, err := source.operand("key", positional(args, 0), keyFile)
		if err != nil {
			return err
		}
		consistency, err := parseConsistency(consistencyName)
		if err != nil {
			return err
		}

		op, err := start(cmd)
		if err != nil {
			return err
		}
		defer op.close()

		result, err := op.client.Get(op.ctx, key, client.ReadOptions{
			Consistency: consistency,
			HeadOnly:    headOnly,
		})
		if err != nil {
			return err
		}

		writeMetadata(cmd.ErrOrStderr(),
			"status", gateway.Status_OK.String(),
			"version", strconv.FormatUint(result.Version, 10),
			"applied_version", strconv.FormatUint(result.AppliedVersion, 10),
			"value_bytes", valueBytesLabel(result),
			"request_id", result.RequestID)

		if headOnly {
			return nil
		}
		if outputFile != "" {
			if err := os.WriteFile(outputFile, result.Value, 0o600); err != nil {
				return &UsageError{Err: fmt.Errorf("cannot write %s: %w", outputFile, err)}
			}
			return nil
		}

		out := cmd.OutOrStdout()
		if _, err := out.Write(result.Value); err != nil {
			return &UsageError{Err: fmt.Errorf("cannot write value: %w", err)}
		}
		if !raw {
			if _, err := fmt.Fprintln(out); err != nil {
				return &UsageError{Err: fmt.Errorf("cannot write value: %w", err)}
			}
		}
		return nil
	},
}

// valueBytesLabel keeps head-only reads from claiming an empty value.
func valueBytesLabel(result *client.ReadResult) string {
	if result.HeadOnly {
		return "omitted"
	}
	return strconv.Itoa(len(result.Value))
}

func parseConsistency(name string) (gateway.Consistency, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "", "default":
		return gateway.Consistency_CONSISTENCY_UNSPECIFIED, nil
	case "strong":
		return gateway.Consistency_STRONG, nil
	case "eventual":
		return gateway.Consistency_EVENTUAL, nil
	default:
		return gateway.Consistency_CONSISTENCY_UNSPECIFIED,
			&UsageError{Err: fmt.Errorf("--consistency must be strong or eventual, got %q", name)}
	}
}

func init() {
	rootCmd.AddCommand(getCmd)

	flags := getCmd.Flags()
	flags.String("key-file", "", `read the key bytes from a file, or "-" for standard input`)
	flags.Bool("raw", false, "write the value with no trailing newline")
	flags.Bool("head", false, "request metadata only, without value bytes")
	flags.String("output-file", "", "write the value to a file instead of standard output")
	flags.String("consistency", "", "read consistency: strong or eventual (default: server policy)")
}
