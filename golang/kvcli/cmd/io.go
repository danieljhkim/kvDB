package cmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/danieljhkim/kv/internal/client"
	"github.com/danieljhkim/kv/internal/config"
)

// stdinSelector selects standard input in the file options.
const stdinSelector = "-"

// operation holds the per-invocation client and its bounded context.
type operation struct {
	client *client.Client
	cfg    *config.Config
	ctx    context.Context
	stop   func()
}

// start resolves configuration, dials the gateway, and derives a context that
// is bounded by the configured timeout and cancelled on SIGINT or SIGTERM.
func start(cmd *cobra.Command) (*operation, error) {
	cfg, err := resolveConfig(cmd)
	if err != nil {
		return nil, err
	}
	kv, err := client.Dial(cfg)
	if err != nil {
		return nil, &UsageError{Err: err}
	}

	parent := cmd.Context()
	if parent == nil {
		parent = context.Background()
	}
	signalCtx, stopSignals := signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
	ctx, cancelTimeout := context.WithTimeout(signalCtx, cfg.Request.Timeout)

	return &operation{
		client: kv,
		cfg:    cfg,
		ctx:    ctx,
		stop: func() {
			cancelTimeout()
			stopSignals()
			_ = kv.Close()
		},
	}, nil
}

// close releases the connection, the deadline, and the signal handler.
func (o *operation) close() { o.stop() }

// bytesSource collects binary input, allowing at most one stdin consumer per
// invocation so key and value can never be interleaved.
type bytesSource struct {
	stdinUsed bool
}

// literal returns the bytes of a positional text argument.
func (s *bytesSource) literal(value string) []byte { return []byte(value) }

// file reads the named file, or standard input when path is "-".
func (s *bytesSource) file(path string) ([]byte, error) {
	if path == stdinSelector {
		if s.stdinUsed {
			return nil, &UsageError{Err: fmt.Errorf("standard input can only be read once per invocation")}
		}
		s.stdinUsed = true
		data, err := io.ReadAll(os.Stdin)
		if err != nil {
			return nil, &UsageError{Err: fmt.Errorf("cannot read standard input: %w", err)}
		}
		return data, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, &UsageError{Err: fmt.Errorf("cannot read %s: %w", path, err)}
	}
	return data, nil
}

// operand resolves exactly one of a positional text argument or a file
// option. Binary data that is not valid UTF-8 must use the file option.
func (s *bytesSource) operand(name string, arg *string, path string) ([]byte, error) {
	switch {
	case arg != nil && path != "":
		return nil, &UsageError{Err: fmt.Errorf("pass the %s as an argument or with --%s-file, not both", name, name)}
	case arg != nil:
		return s.literal(*arg), nil
	case path != "":
		return s.file(path)
	default:
		return nil, &UsageError{Err: fmt.Errorf("a %s is required: pass it as an argument or with --%s-file", name, name)}
	}
}

// positional returns args[index] or nil when it was not supplied.
func positional(args []string, index int) *string {
	if index >= len(args) {
		return nil
	}
	return &args[index]
}

// writeMetadata reports operation outcome on the given stream as stable
// key=value pairs. Value bytes never travel on this path.
func writeMetadata(out io.Writer, pairs ...string) {
	for index := 0; index+1 < len(pairs); index += 2 {
		if index > 0 {
			fmt.Fprint(out, " ")
		}
		fmt.Fprintf(out, "%s=%s", pairs[index], pairs[index+1])
	}
	fmt.Fprintln(out)
}
