package cmd

import (
	"errors"
	"fmt"
	"strings"

	"github.com/danieljhkim/kv/internal/client"
	gateway "github.com/danieljhkim/kv/internal/gen/kvdb/gateway"
)

// Stable process exit codes. Only ExitOK means the operation succeeded.
const (
	// ExitOK means the operation completed with an OK application status.
	ExitOK = 0
	// ExitUsage covers invalid arguments and rejected configuration.
	ExitUsage = 1
	// ExitApplication covers a non-OK application status other than the ones
	// with a dedicated code below.
	ExitApplication = 2
	// ExitTransport covers a non-OK gRPC status, including DEADLINE_EXCEEDED
	// and CANCELLED.
	ExitTransport = 3
	// ExitNotFound means the key does not exist. It is distinct from a
	// successful read of an empty value, which exits ExitOK.
	ExitNotFound = 4
	// ExitWriteOutcomeUnknown means the write may or may not have been
	// applied. The CLI never retries it automatically.
	ExitWriteOutcomeUnknown = 5
	// ExitOutput means the RPC completed with a known outcome, but that
	// outcome could not be written. It is distinct from
	// ExitWriteOutcomeUnknown because the RPC returned OK. The CLI does
	// not retry the RPC: a write may already have changed stored state.
	ExitOutput = 6
)

// UsageError marks argument and configuration failures.
type UsageError struct {
	Err error
}

// BatchPartialError follows a successful BatchGet envelope whose individual
// results include non-OK statuses or fanout termination. The JSON document was
// written before this error is returned so scripts can inspect every position.
type BatchPartialError struct{}

func (*BatchPartialError) Error() string { return "BatchGet completed with partial failures" }

func (e *UsageError) Error() string { return e.Err.Error() }

func (e *UsageError) Unwrap() error { return e.Err }

// OutputError means an RPC finished with a known outcome and the CLI could
// not write that outcome. Writes are not repeated. Version and RequestID
// carry the applied result when the RPC produced them, so a lost stdout
// line is not the only copy of a write's identity.
type OutputError struct {
	Err     error
	Op      string
	Mutated bool
	Version *uint64
	// RequestID is the id the RPC used. It is empty when the outcome line
	// does not include one, as with ping.
	RequestID string
	// ValueWithheld is set when a read's value bytes were not written
	// because the metadata write failed first.
	ValueWithheld bool
}

func (e *OutputError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "OUTPUT: %s could not write its outcome", e.Op)
	if e.Mutated {
		b.WriteString(" after the RPC already changed stored state; the operation will not be repeated")
	} else {
		b.WriteString(" after the RPC completed without changing stored state")
	}
	if e.ValueWithheld {
		b.WriteString("; the value was not written")
	}
	if e.Version != nil || e.RequestID != "" {
		b.WriteString(" (")
		if e.Version != nil {
			fmt.Fprintf(&b, "version=%d", *e.Version)
		}
		if e.RequestID != "" {
			if e.Version != nil {
				b.WriteByte(' ')
			}
			b.WriteString("request_id=")
			b.WriteString(e.RequestID)
		}
		b.WriteByte(')')
	}
	if e.Err != nil {
		b.WriteString(": ")
		b.WriteString(e.Err.Error())
	}
	return b.String()
}

func (e *OutputError) Unwrap() error { return e.Err }

// errLegacyInteractive rejects the removed line protocol explicitly instead
// of silently maintaining a second network protocol.
var errLegacyInteractive = &UsageError{Err: errors.New(
	"interactive mode was removed: kvcli speaks the KvGateway gRPC API only; " +
		"use `kv get`, `kv put`, and `kv del` instead")}

func exitCode(err error) int {
	var partial *BatchPartialError
	if errors.As(err, &partial) {
		return ExitApplication
	}
	var statusErr *client.StatusError
	if errors.As(err, &statusErr) {
		switch statusErr.Code {
		case gateway.Status_NOT_FOUND:
			return ExitNotFound
		case gateway.Status_WRITE_OUTCOME_UNKNOWN:
			return ExitWriteOutcomeUnknown
		default:
			return ExitApplication
		}
	}
	var transportErr *client.TransportError
	if errors.As(err, &transportErr) {
		return ExitTransport
	}
	var outputErr *OutputError
	if errors.As(err, &outputErr) {
		return ExitOutput
	}
	return ExitUsage
}

// statusName renders the stable protocol name for an error, for operators
// and scripts that key on names rather than message text.
func statusName(err error) string {
	var partial *BatchPartialError
	if errors.As(err, &partial) {
		return "PARTIAL_FAILURE"
	}
	var statusErr *client.StatusError
	if errors.As(err, &statusErr) {
		return statusErr.StatusName()
	}
	var transportErr *client.TransportError
	if errors.As(err, &transportErr) {
		return transportErr.StatusName()
	}
	var outputErr *OutputError
	if errors.As(err, &outputErr) {
		return "OUTPUT"
	}
	return "USAGE"
}

func describeError(err error) string {
	return fmt.Sprintf("status=%s exit=%d", statusName(err), exitCode(err))
}
