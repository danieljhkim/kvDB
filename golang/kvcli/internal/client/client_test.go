package client_test

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/danieljhkim/kv/internal/client"
	"github.com/danieljhkim/kv/internal/config"
	gateway "github.com/danieljhkim/kv/internal/gen/kvdb/gateway"
	"github.com/danieljhkim/kv/internal/testfixture"
)

// binaryKey and binaryValue contain a zero byte, a newline, and a byte
// sequence that is not valid UTF-8.
var (
	binaryKey   = []byte{0x00, 'k', '\n', 0xff, 0xfe, 0x7f}
	binaryValue = []byte{0xc3, 0x28, 0x00, '\n', 'v', 0x80}
)

func plaintextConfig(address string) *config.Config {
	cfg := &config.Config{}
	cfg.Server.Host, cfg.Server.Port = splitHostPort(address)
	cfg.Security.Mode = config.ModeDevelopmentPlaintext
	cfg.Security.Deployment = "test"
	cfg.Request.Timeout = 5 * time.Second
	cfg.Request.TenantID = "tenant-1"
	cfg.Request.Principal = "operator-1"
	return cfg
}

func dial(t *testing.T, cfg *config.Config) *client.Client {
	t.Helper()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("configuration rejected: %v", err)
	}
	kv, err := client.Dial(cfg)
	if err != nil {
		t.Fatalf("cannot dial gateway: %v", err)
	}
	t.Cleanup(func() { _ = kv.Close() })
	return kv
}

func TestBinaryRoundTripPreservesEveryByte(t *testing.T) {
	server := testfixture.Start(t, testfixture.Hooks{}, nil)
	kv := dial(t, plaintextConfig(server.Address()))

	if _, err := kv.Put(context.Background(), binaryKey, binaryValue, client.WriteOptions{}); err != nil {
		t.Fatalf("put failed: %v", err)
	}
	result, err := kv.Get(context.Background(), binaryKey, client.ReadOptions{})
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if !bytes.Equal(result.Value, binaryValue) {
		t.Fatalf("value changed on the wire: got %v want %v", result.Value, binaryValue)
	}

	calls := server.Calls()
	if len(calls) != 2 {
		t.Fatalf("expected one put and one get, got %d calls", len(calls))
	}
	if !bytes.Equal(calls[0].Key, binaryKey) || !bytes.Equal(calls[0].Value, binaryValue) {
		t.Fatalf("server received altered bytes: %+v", calls[0])
	}
	if calls[0].TenantID != "tenant-1" || calls[0].Principal != "operator-1" {
		t.Fatalf("request context not propagated: %+v", calls[0])
	}

	if _, err := kv.Delete(context.Background(), binaryKey, client.WriteOptions{}); err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	if _, err := kv.Get(context.Background(), binaryKey, client.ReadOptions{}); !client.IsStatus(err, gateway.Status_NOT_FOUND) {
		t.Fatalf("expected NOT_FOUND after delete, got %v", err)
	}
}

func TestMissingKeyDiffersFromStoredEmptyValue(t *testing.T) {
	server := testfixture.Start(t, testfixture.Hooks{}, nil)
	kv := dial(t, plaintextConfig(server.Address()))

	if _, err := kv.Put(context.Background(), []byte("empty"), []byte{}, client.WriteOptions{}); err != nil {
		t.Fatalf("put failed: %v", err)
	}
	result, err := kv.Get(context.Background(), []byte("empty"), client.ReadOptions{})
	if err != nil {
		t.Fatalf("reading a stored empty value must succeed, got %v", err)
	}
	if len(result.Value) != 0 {
		t.Fatalf("expected an empty value, got %v", result.Value)
	}

	_, err = kv.Get(context.Background(), []byte("absent"), client.ReadOptions{})
	var statusErr *client.StatusError
	if !errors.As(err, &statusErr) {
		t.Fatalf("expected an application status error, got %v", err)
	}
	if statusErr.StatusName() != "NOT_FOUND" {
		t.Fatalf("expected NOT_FOUND, got %s", statusErr.StatusName())
	}
}

func TestHeadOnlyReadOmitsValueAndCarriesConsistency(t *testing.T) {
	server := testfixture.Start(t, testfixture.Hooks{}, nil)
	server.Seed([]byte("greeting"), []byte("hello"))
	kv := dial(t, plaintextConfig(server.Address()))

	result, err := kv.Get(context.Background(), []byte("greeting"), client.ReadOptions{
		HeadOnly:    true,
		Consistency: gateway.Consistency_STRONG,
	})
	if err != nil {
		t.Fatalf("head-only get failed: %v", err)
	}
	if len(result.Value) != 0 || !result.HeadOnly {
		t.Fatalf("head-only read returned value bytes: %+v", result)
	}
	if result.Version == 0 {
		t.Fatal("head-only read must still report the version")
	}
	if got := server.Calls()[0]; !got.HeadOnly || got.Consistency != gateway.Consistency_STRONG {
		t.Fatalf("read options not propagated: %+v", got)
	}
}

func TestNonOkApplicationStatusExposesStableName(t *testing.T) {
	server := testfixture.Start(t, testfixture.Hooks{
		Put: func(context.Context, *gateway.PutRequest) (*gateway.PutResponse, error) {
			return &gateway.PutResponse{Status: &gateway.Status{
				Code:         gateway.Status_RATE_LIMITED,
				Message:      "shard is throttling",
				ShardId:      "shard-17",
				RetryAfterMs: 250,
			}}, nil
		},
	}, nil)
	kv := dial(t, plaintextConfig(server.Address()))

	_, err := kv.Put(context.Background(), []byte("k"), []byte("v"), client.WriteOptions{})
	var statusErr *client.StatusError
	if !errors.As(err, &statusErr) {
		t.Fatalf("expected an application status error, got %v", err)
	}
	if statusErr.StatusName() != "RATE_LIMITED" || statusErr.ShardID != "shard-17" || statusErr.RetryAfterMs != 250 {
		t.Fatalf("status details lost: %+v", statusErr)
	}
}

func TestMissingStatusIsNotTreatedAsSuccess(t *testing.T) {
	server := testfixture.Start(t, testfixture.Hooks{
		Get: func(context.Context, *gateway.GetRequest) (*gateway.GetResponse, error) {
			return &gateway.GetResponse{}, nil
		},
	}, nil)
	kv := dial(t, plaintextConfig(server.Address()))

	if _, err := kv.Get(context.Background(), []byte("k"), client.ReadOptions{}); err == nil {
		t.Fatal("a response without a status must not be reported as success")
	}
}

func TestTransportFailureIsReportedWithGrpcCodeName(t *testing.T) {
	server := testfixture.Start(t, testfixture.Hooks{
		Get: func(context.Context, *gateway.GetRequest) (*gateway.GetResponse, error) {
			return nil, grpcstatus.Error(grpccodes.PermissionDenied, "client certificate is not authorized")
		},
	}, nil)
	kv := dial(t, plaintextConfig(server.Address()))

	_, err := kv.Get(context.Background(), []byte("k"), client.ReadOptions{})
	var transportErr *client.TransportError
	if !errors.As(err, &transportErr) {
		t.Fatalf("expected a transport error, got %v", err)
	}
	if transportErr.StatusName() != "PermissionDenied" {
		t.Fatalf("unexpected transport status %s", transportErr.StatusName())
	}
}

func TestDeadlineIsBoundedAndCancelsTheCall(t *testing.T) {
	released := make(chan struct{})
	server := testfixture.Start(t, testfixture.Hooks{
		Get: func(ctx context.Context, _ *gateway.GetRequest) (*gateway.GetResponse, error) {
			<-ctx.Done()
			close(released)
			return nil, ctx.Err()
		},
	}, nil)

	cfg := plaintextConfig(server.Address())
	cfg.Request.Timeout = 200 * time.Millisecond
	kv := dial(t, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Request.Timeout)
	defer cancel()

	start := time.Now()
	_, err := kv.Get(ctx, []byte("slow"), client.ReadOptions{})
	elapsed := time.Since(start)

	var transportErr *client.TransportError
	if !errors.As(err, &transportErr) || transportErr.StatusName() != "DeadlineExceeded" {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("deadline was not enforced, call took %s", elapsed)
	}
	select {
	case <-released:
	case <-time.After(3 * time.Second):
		t.Fatal("server side was not cancelled")
	}
}

func TestCancelledContextStopsTheCall(t *testing.T) {
	server := testfixture.Start(t, testfixture.Hooks{
		Get: func(ctx context.Context, _ *gateway.GetRequest) (*gateway.GetResponse, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}, nil)
	kv := dial(t, plaintextConfig(server.Address()))

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	_, err := kv.Get(ctx, []byte("slow"), client.ReadOptions{})
	var transportErr *client.TransportError
	if !errors.As(err, &transportErr) || transportErr.StatusName() != "Canceled" {
		t.Fatalf("expected Canceled, got %v", err)
	}
}

func TestWritesGenerateFreshRequestIdsAndAcceptDeliberateReuse(t *testing.T) {
	server := testfixture.Start(t, testfixture.Hooks{}, nil)
	kv := dial(t, plaintextConfig(server.Address()))

	first, err := kv.Put(context.Background(), []byte("k"), []byte("v1"), client.WriteOptions{})
	if err != nil {
		t.Fatalf("put failed: %v", err)
	}
	second, err := kv.Put(context.Background(), []byte("k"), []byte("v2"), client.WriteOptions{})
	if err != nil {
		t.Fatalf("put failed: %v", err)
	}
	if first.RequestID == "" || first.RequestID == second.RequestID {
		t.Fatalf("each attempt needs its own request id, got %q and %q", first.RequestID, second.RequestID)
	}

	reused := first.RequestID
	replay, err := kv.Put(context.Background(), []byte("k"), []byte("v3"), client.WriteOptions{
		RequestID:         reused,
		AllowServerReplay: true,
	})
	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}
	if replay.RequestID != reused {
		t.Fatalf("explicit request id was not reused: %q", replay.RequestID)
	}

	calls := server.Calls()
	if calls[0].RequestID != first.RequestID || calls[2].RequestID != reused {
		t.Fatalf("request ids not sent verbatim: %+v", calls)
	}
	if calls[0].RequireIdempotency {
		t.Fatal("require_idempotency must be off unless replay is requested")
	}
	if !calls[2].RequireIdempotency {
		t.Fatal("--allow-server-replay must set require_idempotency")
	}
}

func TestUnknownWriteOutcomeIsSurfacedWithoutRetrying(t *testing.T) {
	var attempts atomic.Int64
	server := testfixture.Start(t, testfixture.Hooks{
		Put: func(context.Context, *gateway.PutRequest) (*gateway.PutResponse, error) {
			attempts.Add(1)
			return &gateway.PutResponse{Status: &gateway.Status{
				Code:    gateway.Status_WRITE_OUTCOME_UNKNOWN,
				Message: "gateway lost the node response",
			}}, nil
		},
	}, nil)
	kv := dial(t, plaintextConfig(server.Address()))

	_, err := kv.Put(context.Background(), []byte("k"), []byte("v"), client.WriteOptions{})
	if !client.IsStatus(err, gateway.Status_WRITE_OUTCOME_UNKNOWN) {
		t.Fatalf("expected WRITE_OUTCOME_UNKNOWN, got %v", err)
	}
	if attempts.Load() != 1 {
		t.Fatalf("ambiguous write was retried %d times", attempts.Load())
	}
	if len(server.Calls()) != 1 {
		t.Fatalf("ambiguous write produced %d requests", len(server.Calls()))
	}
}
