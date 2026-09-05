package cmd

import (
	"errors"
	"fmt"

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
)

// UsageError marks argument and configuration failures.
type UsageError struct {
	Err error
}

func (e *UsageError) Error() string { return e.Err.Error() }

func (e *UsageError) Unwrap() error { return e.Err }

// errLegacyInteractive rejects the removed line protocol explicitly instead
// of silently maintaining a second network protocol.
var errLegacyInteractive = &UsageError{Err: errors.New(
	"interactive mode was removed: kvcli speaks the KvGateway gRPC API only; " +
		"use `kv get`, `kv put`, and `kv del` instead")}

func exitCode(err error) int {
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
	return ExitUsage
}

// statusName renders the stable protocol name for an error, for operators
// and scripts that key on names rather than message text.
func statusName(err error) string {
	var statusErr *client.StatusError
	if errors.As(err, &statusErr) {
		return statusErr.StatusName()
	}
	var transportErr *client.TransportError
	if errors.As(err, &transportErr) {
		return transportErr.StatusName()
	}
	return "USAGE"
}

func describeError(err error) string {
	return fmt.Sprintf("status=%s exit=%d", statusName(err), exitCode(err))
}
