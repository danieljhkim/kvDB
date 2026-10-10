package cmd

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	gateway "github.com/danieljhkim/kv/internal/gen/kvdb/gateway"
	"github.com/danieljhkim/kv/internal/testfixture"
)

var binaryValue = []byte{0x00, 0xc3, 0x28, '\n', 'v', 0xff}

// run executes the CLI exactly as main does and reports what a shell would
// observe: standard output, standard error, and the process exit code.
func run(t *testing.T, args ...string) (stdout, stderr string, code int) {
	t.Helper()
	resetFlags(rootCmd)

	outBuffer := &bytes.Buffer{}
	errBuffer := &bytes.Buffer{}
	rootCmd.SetOut(outBuffer)
	rootCmd.SetErr(errBuffer)
	rootCmd.SetArgs(args)
	rootCmd.SetContext(context.Background())
	t.Cleanup(func() {
		rootCmd.SetOut(nil)
		rootCmd.SetErr(nil)
		rootCmd.SetArgs(nil)
	})

	err := rootCmd.Execute()
	if err != nil {
		errBuffer.WriteString(err.Error())
		return outBuffer.String(), errBuffer.String(), exitCode(err)
	}
	return outBuffer.String(), errBuffer.String(), ExitOK
}

// resetFlags clears state left behind by a previous invocation so each test
// starts from documented defaults.
func resetFlags(cmd *cobra.Command) {
	clear := func(flag *pflag.Flag) {
		flag.Changed = false
		_ = flag.Value.Set(flag.DefValue)
	}
	cmd.Flags().VisitAll(clear)
	cmd.PersistentFlags().VisitAll(clear)
	for _, child := range cmd.Commands() {
		resetFlags(child)
	}
}

// localGateway starts a fixture and returns the flags that address it over
// explicit development plaintext.
func localGateway(t *testing.T, hooks testfixture.Hooks) (*testfixture.Server, []string) {
	t.Helper()
	t.Setenv("KVDB_ENV", "test")
	server := testfixture.Start(t, hooks, nil)
	configPath := filepath.Join(t.TempDir(), "kvcli.yaml")
	if err := os.WriteFile(configPath, []byte("server:\n  host: 127.0.0.1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return server, []string{
		"--config", configPath,
		"--address", server.Address(),
		"--security-mode", "development-plaintext",
		"--tenant", "test-tenant",
		"--principal", "test-principal",
		"--timeout", "10s",
	}
}

func localIPv6Gateway(t *testing.T, hooks testfixture.Hooks) (*testfixture.Server, []string) {
	t.Helper()
	probe, err := net.Listen("tcp", "[::1]:0")
	if err != nil {
		t.Skipf("IPv6 loopback is unavailable: %v", err)
	}
	if err := probe.Close(); err != nil {
		t.Fatalf("cannot release IPv6 loopback probe listener: %v", err)
	}

	t.Setenv("KVDB_ENV", "test")
	server := testfixture.StartOn(t, "[::1]:0", hooks, nil)
	return server, []string{
		"--config", filepath.Join(t.TempDir(), "missing.yaml"),
		"--security-mode", "development-plaintext",
		"--tenant", "test-tenant",
		"--principal", "test-principal",
		"--timeout", "10s",
	}
}

func withArgs(base []string, extra ...string) []string {
	return append(append([]string{}, extra...), base...)
}

func TestRawGetWritesOnlyTheValueBytes(t *testing.T) {
	server, connection := localGateway(t, testfixture.Hooks{})
	server.Seed([]byte("binary"), binaryValue)

	stdout, stderr, code := run(t, withArgs(connection, "get", "binary", "--raw")...)
	if code != ExitOK {
		t.Fatalf("get failed with %d: %s", code, stderr)
	}
	if stdout != string(binaryValue) {
		t.Fatalf("raw stdout must contain only the value bytes, got %q", stdout)
	}
	if !strings.Contains(stderr, "status=OK") || !strings.Contains(stderr, "request_id=") {
		t.Fatalf("outcome must be documented on stderr, got %q", stderr)
	}
}

func TestDefaultGetTerminatesTheValueWithANewline(t *testing.T) {
	server, connection := localGateway(t, testfixture.Hooks{})
	server.Seed([]byte("greeting"), []byte("hello"))

	stdout, _, code := run(t, withArgs(connection, "get", "greeting")...)
	if code != ExitOK {
		t.Fatalf("unexpected exit code %d", code)
	}
	if stdout != "hello\n" {
		t.Fatalf("unexpected stdout %q", stdout)
	}
}

func TestBinaryKeysAndValuesRoundTripThroughFiles(t *testing.T) {
	_, connection := localGateway(t, testfixture.Hooks{})
	directory := t.TempDir()
	keyPath := filepath.Join(directory, "key.bin")
	valuePath := filepath.Join(directory, "value.bin")
	outputPath := filepath.Join(directory, "roundtrip.bin")
	key := []byte{0x00, 0xfe, '\n', 0x80}
	if err := os.WriteFile(keyPath, key, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(valuePath, binaryValue, 0o600); err != nil {
		t.Fatal(err)
	}

	if _, stderr, code := run(t, withArgs(connection,
		"put", "--key-file", keyPath, "--value-file", valuePath)...); code != ExitOK {
		t.Fatalf("put failed with %d: %s", code, stderr)
	}
	if _, stderr, code := run(t, withArgs(connection,
		"get", "--key-file", keyPath, "--output-file", outputPath)...); code != ExitOK {
		t.Fatalf("get failed with %d: %s", code, stderr)
	}

	roundTripped, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(roundTripped, binaryValue) {
		t.Fatalf("value changed: got %v want %v", roundTripped, binaryValue)
	}
}

func TestValueCanBeReadFromStandardInput(t *testing.T) {
	server, connection := localGateway(t, testfixture.Hooks{})
	withStdin(t, binaryValue)

	if _, stderr, code := run(t, withArgs(connection, "put", "stdin-key", "--value-file", "-")...); code != ExitOK {
		t.Fatalf("put failed with %d: %s", code, stderr)
	}

	calls := server.Calls()
	if len(calls) != 1 || !bytes.Equal(calls[0].Value, binaryValue) {
		t.Fatalf("stdin bytes were altered: %+v", calls)
	}
}

func TestMissingKeyAndEmptyValueAreDistinct(t *testing.T) {
	server, connection := localGateway(t, testfixture.Hooks{})
	server.Seed([]byte("empty"), []byte{})

	stdout, _, code := run(t, withArgs(connection, "get", "empty", "--raw")...)
	if code != ExitOK || stdout != "" {
		t.Fatalf("an empty value must succeed with empty output, got %d %q", code, stdout)
	}

	stdout, _, code = run(t, withArgs(connection, "get", "empty")...)
	if code != ExitOK || stdout != "" {
		t.Fatalf("a non-raw read of an empty value must also write nothing, got %d %q", code, stdout)
	}

	stdout, stderr, code := run(t, withArgs(connection, "get", "absent", "--raw")...)
	if code != ExitNotFound {
		t.Fatalf("a missing key must exit %d, got %d", ExitNotFound, code)
	}
	if stdout != "" {
		t.Fatalf("a missing key must not write to stdout, got %q", stdout)
	}
	if !strings.Contains(stderr, "NOT_FOUND") {
		t.Fatalf("stderr should name the status, got %q", stderr)
	}
}

func TestBatchGetWritesOneOrderedJSONDocumentForMixedResults(t *testing.T) {
	server, connection := localGateway(t, testfixture.Hooks{
		BatchGet: func(_ context.Context, request *gateway.BatchGetRequest) (*gateway.BatchGetResponse, error) {
			return &gateway.BatchGetResponse{Status: &gateway.Status{Code: gateway.Status_OK}, Results: []*gateway.BatchGetResult{
				{Key: request.Keys[0], Status: &gateway.Status{Code: gateway.Status_OK}, Kv: &gateway.KeyValue{Key: request.Keys[0], Value: []byte("value"), Version: 7}, Outcome: gateway.BatchGetOutcome_COMPLETED},
				{Key: request.Keys[1], Status: &gateway.Status{Code: gateway.Status_OK}, Kv: &gateway.KeyValue{Key: request.Keys[1], Value: []byte{}, Version: 8}, Outcome: gateway.BatchGetOutcome_COMPLETED},
				{Key: request.Keys[2], Status: &gateway.Status{Code: gateway.Status_NOT_FOUND, Message: "missing"}, Outcome: gateway.BatchGetOutcome_COMPLETED},
				{Key: request.Keys[3], Status: &gateway.Status{Code: gateway.Status_TIMEOUT, Message: "deadline"}, Outcome: gateway.BatchGetOutcome_DEADLINE_EXCEEDED},
			}}, nil
		},
	})
	keys := [][]byte{[]byte("duplicate"), {0, 0xff}, []byte("missing"), []byte("duplicate")}
	encoded := make([]string, len(keys))
	for index, key := range keys {
		encoded[index] = base64.StdEncoding.EncodeToString(key)
	}
	input, err := json.Marshal(encoded)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "keys.json")
	if err := os.WriteFile(path, input, 0o600); err != nil {
		t.Fatal(err)
	}

	stdout, stderr, code := run(t, withArgs(connection, "batch-get", "--input", path, "--consistency", "strong")...)
	if code != ExitApplication {
		t.Fatalf("mixed results must exit %d, got %d: %s", ExitApplication, code, stderr)
	}
	var document batchGetDocument
	if err := json.Unmarshal([]byte(stdout), &document); err != nil {
		t.Fatalf("stdout must be one JSON document: %v; %q", err, stdout)
	}
	if document.Version != 1 || len(document.Items) != len(keys) {
		t.Fatalf("unexpected document: %+v", document)
	}
	for index, item := range document.Items {
		if item.RequestIndex != index || item.KeyBase64 != encoded[index] {
			t.Fatalf("result order or key changed at %d: %+v", index, item)
		}
	}
	if document.Items[1].ValueBase64 == nil || *document.Items[1].ValueBase64 != "" {
		t.Fatalf("stored empty value must be present as empty base64: %+v", document.Items[1])
	}
	if document.Items[2].Status != gateway.Status_NOT_FOUND.String() || document.Items[2].ValueBase64 != nil {
		t.Fatalf("missing key must not be an empty value: %+v", document.Items[2])
	}
	if document.Items[3].Outcome != gateway.BatchGetOutcome_DEADLINE_EXCEEDED.String() {
		t.Fatalf("terminal outcome lost: %+v", document.Items[3])
	}
	calls := server.Calls()
	if len(calls) != 1 || calls[0].Method != "BatchGet" || len(calls[0].Keys) != len(keys) {
		t.Fatalf("BatchGet must issue exactly one request: %+v", calls)
	}
	if calls[0].Consistency != gateway.Consistency_STRONG {
		t.Fatalf("read options not propagated: %+v", calls[0])
	}
}

func TestBatchGetRejectsMalformedAndOversizedInputBeforeRPC(t *testing.T) {
	server, connection := localGateway(t, testfixture.Hooks{})
	badPath := filepath.Join(t.TempDir(), "bad.json")
	if err := os.WriteFile(badPath, []byte("[\"not base64!\"]"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, stderr, code := run(t, withArgs(connection, "batch-get", "--input", badPath)...)
	if code != ExitUsage || !strings.Contains(stderr, "standard base64") {
		t.Fatalf("malformed input must fail as usage, got %d %q", code, stderr)
	}
	overPath := filepath.Join(t.TempDir(), "over.json")
	if err := os.WriteFile(overPath, bytes.Repeat([]byte("x"), maxBatchGetInputBytes+1), 0o600); err != nil {
		t.Fatal(err)
	}
	_, stderr, code = run(t, withArgs(connection, "batch-get", "--input", overPath)...)
	if code != ExitUsage || !strings.Contains(stderr, "exceeds") {
		t.Fatalf("oversized input must fail as usage, got %d %q", code, stderr)
	}
	if len(server.Calls()) != 0 {
		t.Fatalf("invalid input must not issue an RPC: %+v", server.Calls())
	}
}

func TestBatchGetTransportFailureEmitsNoJSONAndUsesTransportExit(t *testing.T) {
	_, connection := localGateway(t, testfixture.Hooks{BatchGet: func(context.Context, *gateway.BatchGetRequest) (*gateway.BatchGetResponse, error) {
		return nil, grpcstatus.Error(grpccodes.DeadlineExceeded, "slow gateway")
	}})
	path := filepath.Join(t.TempDir(), "keys.json")
	if err := os.WriteFile(path, []byte("[\"aw==\"]"), 0o600); err != nil {
		t.Fatal(err)
	}
	stdout, stderr, code := run(t, withArgs(connection, "batch-get", "--input", path)...)
	if code != ExitTransport || stdout != "" || !strings.Contains(stderr, "DeadlineExceeded") {
		t.Fatalf("transport failure must produce no JSON and exit %d, got %d stdout=%q stderr=%q", ExitTransport, code, stdout, stderr)
	}
}

func TestApplicationStatusFailuresExitNonZeroWithStableNames(t *testing.T) {
	_, connection := localGateway(t, testfixture.Hooks{
		Put: func(context.Context, *gateway.PutRequest) (*gateway.PutResponse, error) {
			return &gateway.PutResponse{Status: &gateway.Status{
				Code: gateway.Status_PAYLOAD_TOO_LARGE, Message: "value exceeds the shard limit",
			}}, nil
		},
	})

	stdout, stderr, code := run(t, withArgs(connection, "put", "k", "v")...)
	if code != ExitApplication {
		t.Fatalf("expected exit %d, got %d", ExitApplication, code)
	}
	if stdout != "" {
		t.Fatalf("a failed write must not report success on stdout, got %q", stdout)
	}
	if !strings.Contains(stderr, "PAYLOAD_TOO_LARGE") {
		t.Fatalf("stderr should name the status, got %q", stderr)
	}
}

func TestTransportFailuresExitDistinctlyFromApplicationFailures(t *testing.T) {
	_, connection := localGateway(t, testfixture.Hooks{
		Get: func(context.Context, *gateway.GetRequest) (*gateway.GetResponse, error) {
			return nil, grpcstatus.Error(grpccodes.Unauthenticated, "client identity is required")
		},
	})

	_, stderr, code := run(t, withArgs(connection, "get", "k")...)
	if code != ExitTransport {
		t.Fatalf("expected exit %d, got %d (%s)", ExitTransport, code, stderr)
	}
	if !strings.Contains(stderr, "Unauthenticated") {
		t.Fatalf("stderr should name the transport status, got %q", stderr)
	}
}

func TestDefiniteWriteRejectionUsesApplicationExitCode(t *testing.T) {
	server, connection := localGateway(t, testfixture.Hooks{
		Delete: func(context.Context, *gateway.DeleteRequest) (*gateway.DeleteResponse, error) {
			return &gateway.DeleteResponse{Status: &gateway.Status{
				Code: gateway.Status_UNAVAILABLE, Message: "rejected before mutation",
			}}, nil
		},
	})
	_, stderr, code := run(t, withArgs(connection, "del", "k")...)
	if code != ExitApplication || !strings.Contains(stderr, "UNAVAILABLE") {
		t.Fatalf("expected UNAVAILABLE exit %d, got %d (%s)", ExitApplication, code, stderr)
	}
	if len(server.Calls()) != 1 {
		t.Fatalf("the CLI must not retry a rejected write, saw %d attempts", len(server.Calls()))
	}
}

func TestUnknownWriteOutcomeHasItsOwnExitCode(t *testing.T) {
	server, connection := localGateway(t, testfixture.Hooks{
		Delete: func(context.Context, *gateway.DeleteRequest) (*gateway.DeleteResponse, error) {
			return &gateway.DeleteResponse{Status: &gateway.Status{
				Code: gateway.Status_WRITE_OUTCOME_UNKNOWN, Message: "no definitive node response",
			}}, nil
		},
	})

	_, stderr, code := run(t, withArgs(connection, "del", "k")...)
	if code != ExitWriteOutcomeUnknown {
		t.Fatalf("expected exit %d, got %d", ExitWriteOutcomeUnknown, code)
	}
	if !strings.Contains(stderr, "WRITE_OUTCOME_UNKNOWN") {
		t.Fatalf("stderr should name the status, got %q", stderr)
	}
	if len(server.Calls()) != 1 {
		t.Fatalf("an ambiguous write must not be retried, saw %d attempts", len(server.Calls()))
	}
}

func TestRequestIdIsGeneratedAndCanBeReusedDeliberately(t *testing.T) {
	server, connection := localGateway(t, testfixture.Hooks{})

	stdout, stderr, code := run(t, withArgs(connection, "put", "k", "v1")...)
	if code != ExitOK {
		t.Fatalf("put failed with %d: %s", code, stderr)
	}
	if !strings.Contains(stdout, "version=") || !strings.Contains(stdout, "request_id=") {
		t.Fatalf("write output must document version and request id, got %q", stdout)
	}

	reused := "5b1f6b1e-6d0e-4a54-9c94-1f9a8f4c2f10"
	stdout, stderr, code = run(t, withArgs(connection,
		"put", "k", "v2", "--request-id", reused, "--allow-server-replay")...)
	if code != ExitOK {
		t.Fatalf("replay failed with %d: %s", code, stderr)
	}
	if !strings.Contains(stdout, "request_id="+reused) {
		t.Fatalf("explicit request id must be reported, got %q", stdout)
	}

	calls := server.Calls()
	if calls[0].RequestID == "" || calls[0].RequestID == reused {
		t.Fatalf("the first write should carry a generated id, got %q", calls[0].RequestID)
	}
	if calls[0].RequireIdempotency {
		t.Fatal("writes must not opt into server replay by default")
	}
	if calls[1].RequestID != reused || !calls[1].RequireIdempotency {
		t.Fatalf("deliberate reuse was not sent: %+v", calls[1])
	}
}

func TestConditionalWriteFlagsPreservePresenceAndSupportBinaryOperands(t *testing.T) {
	server, connection := localGateway(t, testfixture.Hooks{})
	keyPath := filepath.Join(t.TempDir(), "key.bin")
	valuePath := filepath.Join(t.TempDir(), "value.bin")
	if err := os.WriteFile(keyPath, binaryValue, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(valuePath, binaryValue, 0o600); err != nil {
		t.Fatal(err)
	}

	if _, stderr, code := run(t, withArgs(connection,
		"put", "--key-file", keyPath, "--value-file", valuePath, "--if-version", "0", "--if-not-exists", "--ttl", "10ms")...); code != ExitOK {
		t.Fatalf("conditional binary put failed with %d: %s", code, stderr)
	}
	if _, stderr, code := run(t, withArgs(connection, "del", "--key-file", keyPath, "--if-version", "1")...); code != ExitOK {
		t.Fatalf("conditional binary delete failed with %d: %s", code, stderr)
	}

	calls := server.Calls()
	if len(calls) != 2 || calls[0].IfVersionEquals == nil || *calls[0].IfVersionEquals != 0 || !calls[0].IfNotExists || calls[0].TtlMs != 10 || !bytes.Equal(calls[0].Key, binaryValue) || !bytes.Equal(calls[0].Value, binaryValue) {
		t.Fatalf("put flags were not carried byte-for-byte with presence: %+v", calls)
	}
	if calls[1].IfVersionEquals == nil || *calls[1].IfVersionEquals != 1 || calls[1].IfNotExists || calls[1].TtlMs != 0 {
		t.Fatalf("delete should carry only its version condition: %+v", calls[1])
	}
}

func TestConditionalConflictsLeaveStoredValuesUnchanged(t *testing.T) {
	_, connection := localGateway(t, testfixture.Hooks{})
	if _, stderr, code := run(t, withArgs(connection, "put", "key", "initial", "--if-not-exists")...); code != ExitOK {
		t.Fatalf("create-only put failed with %d: %s", code, stderr)
	}
	_, stderr, code := run(t, withArgs(connection, "put", "key", "replacement", "--if-not-exists")...)
	if code != ExitApplication || !strings.Contains(stderr, "ALREADY_EXISTS") {
		t.Fatalf("create-only conflict must keep the stable application outcome, got %d %q", code, stderr)
	}
	_, stderr, code = run(t, withArgs(connection, "put", "key", "replacement", "--if-version", "999")...)
	if code != ExitApplication || !strings.Contains(stderr, "VERSION_MISMATCH") {
		t.Fatalf("stale update must keep the stable application outcome, got %d %q", code, stderr)
	}
	stdout, stderr, code := run(t, withArgs(connection, "get", "key", "--raw")...)
	if code != ExitOK || stdout != "initial" {
		t.Fatalf("conflicts must not alter the stored value, got %d %q (%s)", code, stdout, stderr)
	}

	_, stderr, code = run(t, withArgs(connection, "del", "key", "--if-version", "999")...)
	if code != ExitApplication || !strings.Contains(stderr, "VERSION_MISMATCH") {
		t.Fatalf("stale delete must keep the stable application outcome, got %d %q", code, stderr)
	}
	if _, stderr, code = run(t, withArgs(connection, "get", "key", "--raw")...); code != ExitOK {
		t.Fatalf("stale delete changed the stored value: %d %s", code, stderr)
	}

	if _, stderr, code = run(t, withArgs(connection, "put", "key", "replacement", "--if-version", "1")...); code != ExitOK {
		t.Fatalf("current-version update failed with %d: %s", code, stderr)
	}
	if _, stderr, code = run(t, withArgs(connection, "del", "key", "--if-version", "2")...); code != ExitOK {
		t.Fatalf("current-version delete failed with %d: %s", code, stderr)
	}
}

func TestTTLAndInvalidWriteFlagsFailBeforeRPC(t *testing.T) {
	server, connection := localGateway(t, testfixture.Hooks{})
	if _, stderr, code := run(t, withArgs(connection, "put", "ephemeral", "value", "--ttl", "20ms")...); code != ExitOK {
		t.Fatalf("TTL put failed with %d: %s", code, stderr)
	}
	time.Sleep(40 * time.Millisecond)
	if _, _, code := run(t, withArgs(connection, "get", "ephemeral", "--raw")...); code != ExitNotFound {
		t.Fatalf("expired value should be missing, got exit %d", code)
	}

	for _, args := range [][]string{
		{"put", "k", "v", "--ttl=-1s"},
		{"put", "k", "v", "--ttl=1ns"},
		{"put", "k", "v", "--ttl=999999999999999999999999h"},
		{"put", "k", "v", "--if-version=-1"},
		{"put", "k", "v", "--if-version=18446744073709551616"},
	} {
		_, stderr, code := run(t, withArgs(connection, args...)...)
		if code != ExitUsage {
			t.Fatalf("invalid flags %v must fail as usage, got %d %q", args, code, stderr)
		}
	}
	if len(server.Calls()) != 2 {
		t.Fatalf("invalid flags must not issue RPCs: %+v", server.Calls())
	}
}

func TestSetAndDeleteAliasesStillWork(t *testing.T) {
	server, connection := localGateway(t, testfixture.Hooks{})

	if _, stderr, code := run(t, withArgs(connection, "set", "alias", "value")...); code != ExitOK {
		t.Fatalf("set alias failed with %d: %s", code, stderr)
	}
	if _, stderr, code := run(t, withArgs(connection, "delete", "alias")...); code != ExitOK {
		t.Fatalf("delete alias failed with %d: %s", code, stderr)
	}
	if len(server.Calls()) != 2 || server.Calls()[1].Method != "Delete" {
		t.Fatalf("aliases did not reach the gateway: %+v", server.Calls())
	}
}

func TestPingReportsReachabilityWithoutStoringAnything(t *testing.T) {
	server, connection := localGateway(t, testfixture.Hooks{})

	stdout, stderr, code := run(t, withArgs(connection, "ping")...)
	if code != ExitOK {
		t.Fatalf("ping failed with %d: %s", code, stderr)
	}
	if !strings.Contains(stdout, "status=OK") || !strings.Contains(stdout, "security_mode=development-plaintext") {
		t.Fatalf("ping should report the endpoint and mode, got %q", stdout)
	}
	calls := server.Calls()
	if len(calls) != 1 || calls[0].Method != "Get" || !calls[0].HeadOnly {
		t.Fatalf("ping must probe with a head-only read: %+v", calls)
	}
}

func TestPingReachesIPv6LoopbackByAddressAndHostPort(t *testing.T) {
	server, connection := localIPv6Gateway(t, testfixture.Hooks{})
	wantEndpoint := net.JoinHostPort(server.Host(), strconv.Itoa(server.Port()))
	for index, test := range []struct {
		name  string
		flags []string
	}{
		{name: "address", flags: []string{"--address", server.Address()}},
		{name: "host and port", flags: []string{"--host", server.Host(), "--port", strconv.Itoa(server.Port())}},
	} {
		t.Run(test.name, func(t *testing.T) {
			args := append([]string{"ping"}, test.flags...)
			stdout, stderr, code := run(t, withArgs(connection, args...)...)
			if code != ExitOK {
				t.Fatalf("IPv6 ping failed with %d: %s", code, stderr)
			}
			if !strings.Contains(stdout, "status=OK") || !strings.Contains(stdout, "endpoint="+wantEndpoint) {
				t.Fatalf("ping should report the bracketed IPv6 endpoint, got %q", stdout)
			}
			calls := server.Calls()
			if len(calls) != index+1 || calls[index].Method != "Get" || !calls[index].HeadOnly {
				t.Fatalf("ping must reach the IPv6 gateway with a head-only read: %+v", calls)
			}
		})
	}
}

func TestLegacyInteractiveBehaviorIsRejectedExplicitly(t *testing.T) {
	_, stderr, code := run(t, "connect", "--host", "127.0.0.1", "--port", "7000")
	if code != ExitUsage {
		t.Fatalf("connect must fail with %d, got %d", ExitUsage, code)
	}
	if !strings.Contains(stderr, "gRPC") {
		t.Fatalf("the rejection should explain the supported API, got %q", stderr)
	}

	_, stderr, code = run(t, "--interactive")
	if code != ExitUsage {
		t.Fatalf("interactive mode must fail with %d, got %d", ExitUsage, code)
	}
	if !strings.Contains(stderr, "interactive mode was removed") {
		t.Fatalf("the rejection should be explicit, got %q", stderr)
	}
}

func TestInvalidConfigurationIsRejectedBeforeAnyRpc(t *testing.T) {
	server, connection := localGateway(t, testfixture.Hooks{})

	_, stderr, code := run(t, append(withArgs(connection, "get", "k"), "--security-mode", "insecure")...)
	if code != ExitUsage {
		t.Fatalf("an unknown security mode must exit %d, got %d", ExitUsage, code)
	}
	if !strings.Contains(stderr, "not supported") {
		t.Fatalf("unexpected error %q", stderr)
	}

	_, stderr, code = run(t, append(withArgs(connection, "get", "k"), "--security-mode", "mtls")...)
	if code != ExitUsage {
		t.Fatalf("mTLS without credentials must exit %d, got %d", ExitUsage, code)
	}
	if !strings.Contains(stderr, "KVDB_CLIENT_TLS_TRUST_BUNDLE") {
		t.Fatalf("unexpected error %q", stderr)
	}

	if len(server.Calls()) != 0 {
		t.Fatalf("rejected configuration must not reach the gateway: %+v", server.Calls())
	}
}

func TestMalformedConfigurationIsRejectedBeforeAnyRpc(t *testing.T) {
	server, connection := localGateway(t, testfixture.Hooks{})
	malformed := filepath.Join(t.TempDir(), "kvcli.yaml")
	if err := os.WriteFile(malformed, []byte("server: [unterminated\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	// connection begins with "--config <path>"; point it at the malformed file
	// and keep the valid gateway flags, which would otherwise succeed.
	flags := append([]string{"--config", malformed}, connection[2:]...)

	for _, args := range [][]string{{"ping"}, {"get", "k"}} {
		_, stderr, code := run(t, withArgs(flags, args...)...)
		if code != ExitUsage || !strings.Contains(stderr, "cannot load configuration") {
			t.Fatalf("%v: malformed config must exit %d as a configuration error, got %d %q", args, ExitUsage, code, stderr)
		}
	}
	if len(server.Calls()) != 0 {
		t.Fatalf("malformed configuration must not reach the gateway: %+v", server.Calls())
	}
}

func TestPlaintextOutsideDevelopmentIsRefused(t *testing.T) {
	_, connection := localGateway(t, testfixture.Hooks{})
	t.Setenv("KVDB_ENV", "production")

	_, stderr, code := run(t, withArgs(connection, "get", "k")...)
	if code != ExitUsage {
		t.Fatalf("plaintext outside development must exit %d, got %d", ExitUsage, code)
	}
	if !strings.Contains(stderr, "KVDB_ENV") {
		t.Fatalf("the error should name the deployment requirement, got %q", stderr)
	}
}

func TestConflictingKeySourcesAreRejected(t *testing.T) {
	_, connection := localGateway(t, testfixture.Hooks{})
	keyPath := filepath.Join(t.TempDir(), "key.bin")
	if err := os.WriteFile(keyPath, []byte("k"), 0o600); err != nil {
		t.Fatal(err)
	}

	_, stderr, code := run(t, withArgs(connection, "get", "k", "--key-file", keyPath)...)
	if code != ExitUsage {
		t.Fatalf("two key sources must exit %d, got %d", ExitUsage, code)
	}
	if !strings.Contains(stderr, "not both") {
		t.Fatalf("unexpected error %q", stderr)
	}

	_, stderr, code = run(t, withArgs(connection, "get")...)
	if code != ExitUsage {
		t.Fatalf("a missing key must exit %d, got %d", ExitUsage, code)
	}
	if !strings.Contains(stderr, "a key is required") {
		t.Fatalf("unexpected error %q", stderr)
	}
}

func TestStandardInputIsOnlyConsumedOnce(t *testing.T) {
	_, connection := localGateway(t, testfixture.Hooks{})
	withStdin(t, []byte("payload"))

	_, stderr, code := run(t, withArgs(connection, "put", "--key-file", "-", "--value-file", "-")...)
	if code != ExitUsage {
		t.Fatalf("two stdin consumers must exit %d, got %d", ExitUsage, code)
	}
	if !strings.Contains(stderr, "standard input") {
		t.Fatalf("unexpected error %q", stderr)
	}
}

// withStdin replaces the process standard input for one test.
func withStdin(t *testing.T, contents []byte) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "stdin")
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	original := os.Stdin
	os.Stdin = file
	t.Cleanup(func() {
		os.Stdin = original
		_ = file.Close()
	})
}

func TestAmbiguousWritesReportTheRequestIdSentWithoutSuccessMetadata(t *testing.T) {
	unknown := &gateway.Status{Code: gateway.Status_WRITE_OUTCOME_UNKNOWN, Message: "ambiguous"}
	server, connection := localGateway(t, testfixture.Hooks{
		Put: func(context.Context, *gateway.PutRequest) (*gateway.PutResponse, error) {
			return &gateway.PutResponse{Status: unknown}, nil
		},
		Delete: func(context.Context, *gateway.DeleteRequest) (*gateway.DeleteResponse, error) {
			return &gateway.DeleteResponse{Status: unknown}, nil
		},
	})
	const explicit = "5b1f6b1e-6d0e-4a54-9c94-1f9a8f4c2f10"
	invocations := [][]string{
		{"put", "k", "v"},
		{"del", "k"},
		{"put", "k", "v", "--request-id", explicit},
		{"del", "k", "--request-id", explicit},
	}
	for index, args := range invocations {
		stdout, stderr, code := run(t, withArgs(connection, args...)...)
		if code != ExitWriteOutcomeUnknown {
			t.Fatalf("%v: expected exit %d, got %d", args, ExitWriteOutcomeUnknown, code)
		}
		if stdout != "" || strings.Contains(stderr, "status=OK") {
			t.Fatalf("%v: a failed write must not report success, stdout=%q stderr=%q", args, stdout, stderr)
		}
		calls := server.Calls()
		if len(calls) != index+1 {
			t.Fatalf("%v: ambiguous write was retried, saw %d calls", args, len(calls))
		}
		sent := calls[index].RequestID
		if sent == "" || !strings.Contains(stderr, "request_id="+sent) {
			t.Fatalf("%v: stderr must report the sent id %q, got %q", args, sent, stderr)
		}
		if index >= 2 && sent != explicit {
			t.Fatalf("%v: explicit id was not sent, got %q", args, sent)
		}
	}
}

func TestApplicationRejectionsReportTheRequestIdSent(t *testing.T) {
	server, connection := localGateway(t, testfixture.Hooks{
		Put: func(context.Context, *gateway.PutRequest) (*gateway.PutResponse, error) {
			return &gateway.PutResponse{Status: &gateway.Status{Code: gateway.Status_PAYLOAD_TOO_LARGE}}, nil
		},
	})
	stdout, stderr, code := run(t, withArgs(connection, "put", "k", "v")...)
	calls := server.Calls()
	if code != ExitApplication || stdout != "" || len(calls) != 1 {
		t.Fatalf("got code=%d stdout=%q calls=%d", code, stdout, len(calls))
	}
	if !strings.Contains(stderr, "request_id="+calls[0].RequestID) {
		t.Fatalf("stderr must report the sent id %q, got %q", calls[0].RequestID, stderr)
	}
}

func TestWriteTransportFailuresReportTheRequestIdAndKeepTheirExitCode(t *testing.T) {
	server, connection := localGateway(t, testfixture.Hooks{
		Put: func(context.Context, *gateway.PutRequest) (*gateway.PutResponse, error) {
			return nil, grpcstatus.Error(grpccodes.Unavailable, "connection lost")
		},
		Delete: func(context.Context, *gateway.DeleteRequest) (*gateway.DeleteResponse, error) {
			return nil, grpcstatus.Error(grpccodes.Unavailable, "connection lost")
		},
	})
	for index, args := range [][]string{{"put", "k", "v"}, {"del", "k", "--request-id", "fixed-id"}} {
		stdout, stderr, code := run(t, withArgs(connection, args...)...)
		if code != ExitTransport || stdout != "" {
			t.Fatalf("%v: got code=%d stdout=%q", args, code, stdout)
		}
		calls := server.Calls()
		if len(calls) != index+1 {
			t.Fatalf("%v: transport failure was retried, saw %d calls", args, len(calls))
		}
		if !strings.Contains(stderr, "Unavailable") || !strings.Contains(stderr, "request_id="+calls[index].RequestID) {
			t.Fatalf("%v: stderr must name the transport status and sent id, got %q", args, stderr)
		}
	}
}

func TestMalformedGetResponseExitsNonZeroWithoutSuccessOutput(t *testing.T) {
	ok := &gateway.Status{Code: gateway.Status_OK}
	cases := map[string]*gateway.GetResponse{
		"ok without key-value":    {Status: ok},
		"key-value for wrong key": {Status: ok, Kv: &gateway.KeyValue{Key: []byte("other"), Value: []byte("wrong-value"), Version: 9}},
	}
	for name, response := range cases {
		for _, extra := range [][]string{{"get", "k"}, {"get", "k", "--raw"}} {
			t.Run(name+"/"+strings.Join(extra, " "), func(t *testing.T) {
				_, connection := localGateway(t, testfixture.Hooks{
					Get: func(context.Context, *gateway.GetRequest) (*gateway.GetResponse, error) { return response, nil },
				})
				stdout, stderr, code := run(t, withArgs(connection, extra...)...)
				if code != ExitApplication || stdout != "" {
					t.Fatalf("malformed Get must exit %d with empty stdout, got %d stdout=%q stderr=%q", ExitApplication, code, stdout, stderr)
				}
				if !strings.Contains(stderr, "malformed Get response") || strings.Contains(stderr, "version=") || strings.Contains(stderr, "wrong-value") {
					t.Fatalf("stderr must explain the malformed response without a value, got %q", stderr)
				}
			})
		}
	}
}

func TestMalformedBatchGetResponseExitsNonZeroWithoutJSON(t *testing.T) {
	ok := &gateway.Status{Code: gateway.Status_OK}
	cases := map[string][]*gateway.BatchGetResult{
		"wrong key attributed to request": {{Key: []byte("wrong-key"), Status: ok, Kv: &gateway.KeyValue{Key: []byte("wrong-key"), Value: []byte("wrong-value"), Version: 1}, Outcome: gateway.BatchGetOutcome_COMPLETED}},
		"kv key differs":                  {{Key: []byte("k"), Status: ok, Kv: &gateway.KeyValue{Key: []byte("wrong-key"), Value: []byte("wrong-value"), Version: 1}, Outcome: gateway.BatchGetOutcome_COMPLETED}},
		"completed ok without kv":         {{Key: []byte("k"), Status: ok, Outcome: gateway.BatchGetOutcome_COMPLETED}},
	}
	for name, results := range cases {
		t.Run(name, func(t *testing.T) {
			_, connection := localGateway(t, testfixture.Hooks{
				BatchGet: func(context.Context, *gateway.BatchGetRequest) (*gateway.BatchGetResponse, error) {
					return &gateway.BatchGetResponse{Status: ok, Results: results}, nil
				},
			})
			path := filepath.Join(t.TempDir(), "keys.json")
			if err := os.WriteFile(path, []byte("[\"aw==\"]"), 0o600); err != nil {
				t.Fatal(err)
			}
			stdout, stderr, code := run(t, withArgs(connection, "batch-get", "--input", path)...)
			if code != ExitApplication || stdout != "" {
				t.Fatalf("malformed BatchGet must exit %d with no JSON, got %d stdout=%q stderr=%q", ExitApplication, code, stdout, stderr)
			}
			if !strings.Contains(stderr, "malformed BatchGet response") || strings.Contains(stderr, "d3JvbmctdmFsdWU=") {
				t.Fatalf("unexpected stderr %q", stderr)
			}
		})
	}
}
