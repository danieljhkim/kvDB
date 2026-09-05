package cmd

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"

	"github.com/danieljhkim/kv/internal/client"
	gateway "github.com/danieljhkim/kv/internal/gen/kvdb/gateway"
)

const (
	maxBatchGetInputBytes = 1 << 20
	maxBatchGetKeys       = 1024
)

type batchGetDocument struct {
	Version   int                `json:"version"`
	RequestID string             `json:"request_id"`
	Status    string             `json:"status"`
	Items     []batchGetJSONItem `json:"items"`
}

type batchGetJSONItem struct {
	RequestIndex   int     `json:"request_index"`
	KeyBase64      string  `json:"key_base64"`
	Status         string  `json:"status"`
	Message        string  `json:"message,omitempty"`
	Outcome        string  `json:"outcome"`
	ValueBase64    *string `json:"value_base64,omitempty"`
	Version        uint64  `json:"version,omitempty"`
	AppliedVersion uint64  `json:"applied_version,omitempty"`
	CreateTimeMs   uint64  `json:"create_time_ms,omitempty"`
	UpdateTimeMs   uint64  `json:"update_time_ms,omitempty"`
	ExpireTimeMs   uint64  `json:"expire_time_ms,omitempty"`
}

var batchGetCmd = &cobra.Command{
	Use:   "batch-get --input <path|->",
	Short: "Read ordered binary keys with one BatchGet RPC",
	Long: `Read an ordered JSON array of standard-base64 keys with exactly one BatchGet RPC.

The result is one versioned JSON document on stdout. Duplicate keys preserve
their input positions. A stored empty value has value_base64 set to "", while
a missing key has status NOT_FOUND and no value_base64 field. Mixed per-item
failures still emit the complete document and exit 2; transport failures emit
no document and exit 3. The command never retries automatically.`,
	Example: `  kv batch-get --input keys.json
  printf '["YQ==","AAH/"]' | kv batch-get --input - --consistency strong`,
	Args:          cobra.NoArgs,
	SilenceUsage:  true,
	SilenceErrors: true,
	RunE: func(cmd *cobra.Command, _ []string) error {
		input, _ := cmd.Flags().GetString("input")
		consistencyName, _ := cmd.Flags().GetString("consistency")
		keys, err := readBatchGetKeys(input)
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
		result, err := op.client.BatchGet(op.ctx, keys, client.ReadOptions{Consistency: consistency})
		if err != nil {
			return err
		}

		document, partial := batchGetOutput(keys, result)
		encoder := json.NewEncoder(cmd.OutOrStdout())
		if err := encoder.Encode(document); err != nil {
			return &UsageError{Err: fmt.Errorf("cannot write BatchGet result: %w", err)}
		}
		if partial {
			return &BatchPartialError{}
		}
		return nil
	},
}

func readBatchGetKeys(path string) ([][]byte, error) {
	if path == "" {
		return nil, &UsageError{Err: fmt.Errorf("--input is required (a JSON file or - for standard input)")}
	}
	var input io.Reader
	if path == stdinSelector {
		input = os.Stdin
	} else {
		file, err := os.Open(path)
		if err != nil {
			return nil, &UsageError{Err: fmt.Errorf("cannot read %s: %w", path, err)}
		}
		defer file.Close()
		input = file
	}
	data, err := io.ReadAll(io.LimitReader(input, maxBatchGetInputBytes+1))
	if err != nil {
		return nil, &UsageError{Err: fmt.Errorf("cannot read BatchGet input: %w", err)}
	}
	if len(data) > maxBatchGetInputBytes {
		return nil, &UsageError{Err: fmt.Errorf("BatchGet input exceeds %d bytes", maxBatchGetInputBytes)}
	}
	var encoded []string
	if err := json.Unmarshal(data, &encoded); err != nil {
		return nil, &UsageError{Err: fmt.Errorf("BatchGet input must be a JSON array of standard-base64 strings: %w", err)}
	}
	if len(encoded) == 0 {
		return nil, &UsageError{Err: fmt.Errorf("BatchGet input must contain at least one key")}
	}
	if len(encoded) > maxBatchGetKeys {
		return nil, &UsageError{Err: fmt.Errorf("BatchGet input exceeds %d keys", maxBatchGetKeys)}
	}
	keys := make([][]byte, len(encoded))
	for index, text := range encoded {
		key, err := base64.StdEncoding.DecodeString(text)
		if err != nil {
			return nil, &UsageError{Err: fmt.Errorf("BatchGet key %d is not standard base64: %w", index, err)}
		}
		keys[index] = key
	}
	return keys, nil
}

func batchGetOutput(keys [][]byte, result *client.BatchReadResult) (batchGetDocument, bool) {
	document := batchGetDocument{Version: 1, RequestID: result.RequestID, Status: gateway.Status_OK.String(), Items: make([]batchGetJSONItem, len(result.Results))}
	partial := false
	for index, item := range result.Results {
		jsonItem := batchGetJSONItem{
			RequestIndex: index,
			KeyBase64:    base64.StdEncoding.EncodeToString(keys[index]),
			Status:       item.Status.StatusName(),
			Message:      item.Status.Message,
			Outcome:      item.Outcome.String(),
		}
		if item.Found {
			value := base64.StdEncoding.EncodeToString(item.Value)
			jsonItem.ValueBase64 = &value
			jsonItem.Version = item.Version
			jsonItem.AppliedVersion = item.AppliedVersion
			jsonItem.CreateTimeMs = item.CreateTimeMs
			jsonItem.UpdateTimeMs = item.UpdateTimeMs
			jsonItem.ExpireTimeMs = item.ExpireTimeMs
		}
		if item.Status.Code != gateway.Status_OK || item.Outcome != gateway.BatchGetOutcome_COMPLETED {
			partial = true
		}
		document.Items[index] = jsonItem
	}
	return document, partial
}

func init() {
	rootCmd.AddCommand(batchGetCmd)
	flags := batchGetCmd.Flags()
	flags.String("input", "", "JSON array of standard-base64 keys, or - for standard input")
	flags.String("consistency", "", "read consistency: strong or eventual (default: server policy)")
}
