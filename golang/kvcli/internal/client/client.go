// Package client speaks the KvGateway gRPC contract defined in
// kv.proto/src/main/proto/kvgateway.proto. It is byte-safe: keys and values
// are carried as raw bytes and never parsed as text.
package client

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/danieljhkim/kv/internal/config"
	gateway "github.com/danieljhkim/kv/internal/gen/kvdb/gateway"
)

// Client is a bounded, non-interactive KvGateway client.
type Client struct {
	conn *grpc.ClientConn
	api  gateway.KvGatewayClient
	cfg  *config.Config
}

// ReadResult reports the outcome of a Get.
type ReadResult struct {
	Value          []byte
	Version        uint64
	AppliedVersion uint64
	CreateTimeMs   uint64
	UpdateTimeMs   uint64
	ExpireTimeMs   uint64
	RequestID      string
	// HeadOnly records that value bytes were not requested, so an empty
	// Value does not mean the stored value is empty.
	HeadOnly bool
}

// WriteResult reports the outcome of a Put or Delete.
type WriteResult struct {
	Version   uint64
	RequestID string
}

// ReadOptions selects the consistency of a Get.
type ReadOptions struct {
	Consistency gateway.Consistency
	HeadOnly    bool
}

// BatchReadResult reports one ordered BatchGet response. Results always align
// with the supplied keys; a response that cannot preserve that contract is
// rejected instead of being interpreted as successful zero values.
type BatchReadResult struct {
	RequestID string
	Results   []BatchReadItem
}

// BatchReadItem is an individual BatchGet terminal result. Found distinguishes
// a stored empty value from a missing key; Value is present whenever Found is
// true unless HeadOnly was requested.
type BatchReadItem struct {
	Status         *StatusError
	Outcome        gateway.BatchGetOutcome
	Found          bool
	Value          []byte
	Version        uint64
	AppliedVersion uint64
	CreateTimeMs   uint64
	UpdateTimeMs   uint64
	ExpireTimeMs   uint64
}

// WriteOptions carries the request identity of a Put or Delete.
type WriteOptions struct {
	// RequestID is reused verbatim when set; otherwise a fresh identifier is
	// generated for this attempt.
	RequestID string
	// AllowServerReplay sets require_idempotency. When false the gateway
	// reports WRITE_OUTCOME_UNKNOWN instead of replaying an ambiguous write;
	// this client never retries a write on its own.
	AllowServerReplay bool
}

// StatusError is a non-OK application status carried inside a response body.
type StatusError struct {
	Code         gateway.Status_Code
	Message      string
	LeaderHint   string
	ShardID      string
	RetryAfterMs uint64
}

func (e *StatusError) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("gateway status %s", e.Code)
	}
	return fmt.Sprintf("gateway status %s: %s", e.Code, e.Message)
}

// StatusName returns the stable protocol name of an application status.
func (e *StatusError) StatusName() string { return e.Code.String() }

// IsStatus reports whether err is an application status with the given code.
func IsStatus(err error, code gateway.Status_Code) bool {
	var statusErr *StatusError
	return errors.As(err, &statusErr) && statusErr.Code == code
}

// TransportError is a non-OK gRPC status, i.e. the call itself failed.
type TransportError struct {
	Err error
}

func (e *TransportError) Error() string {
	return fmt.Sprintf("gateway transport %s: %s", e.StatusName(), grpcstatus.Convert(e.Err).Message())
}

func (e *TransportError) Unwrap() error { return e.Err }

// StatusName returns the stable gRPC code name, e.g. DEADLINE_EXCEEDED.
func (e *TransportError) StatusName() string { return grpcstatus.Code(e.Err).String() }

// Dial creates a client for the configured endpoint. The configuration must
// already be validated; transport security is applied fail-closed.
func Dial(cfg *config.Config) (*Client, error) {
	creds, err := transportCredentials(cfg)
	if err != nil {
		return nil, err
	}
	conn, err := grpc.NewClient(cfg.Address(), grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, fmt.Errorf("cannot create gateway client for %s: %w", cfg.Address(), err)
	}
	return &Client{conn: conn, api: gateway.NewKvGatewayClient(conn), cfg: cfg}, nil
}

// Close releases the underlying connection.
func (c *Client) Close() error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

func transportCredentials(cfg *config.Config) (credentials.TransportCredentials, error) {
	if cfg.Security.Mode == config.ModeDevelopmentPlaintext {
		return insecure.NewCredentials(), nil
	}
	certificate, err := tls.LoadX509KeyPair(cfg.Security.CertChain, cfg.Security.PrivateKey)
	if err != nil {
		return nil, fmt.Errorf("cannot load client identity: %w", err)
	}
	bundle, err := os.ReadFile(cfg.Security.TrustBundle)
	if err != nil {
		return nil, fmt.Errorf("cannot read trust bundle: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(bundle) {
		return nil, fmt.Errorf("trust bundle %s contains no certificates", cfg.Security.TrustBundle)
	}
	return credentials.NewTLS(&tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{certificate},
		RootCAs:      pool,
		ServerName:   cfg.Security.ServerName,
	}), nil
}

// Get reads one key. A missing key returns a *StatusError with code
// NOT_FOUND, which is distinct from a successful read of an empty value.
func (c *Client) Get(ctx context.Context, key []byte, options ReadOptions) (*ReadResult, error) {
	requestID := newRequestID()
	request := &gateway.GetRequest{
		Ctx:      c.requestContext(requestID),
		Key:      key,
		HeadOnly: options.HeadOnly,
	}
	if options.Consistency != gateway.Consistency_CONSISTENCY_UNSPECIFIED {
		request.Options = &gateway.ReadOptions{Consistency: options.Consistency}
	}

	response, err := c.api.Get(ctx, request)
	if err != nil {
		return nil, &TransportError{Err: err}
	}
	if err := applicationError(response.GetStatus()); err != nil {
		return nil, err
	}

	kv := response.GetKv()
	return &ReadResult{
		Value:          kv.GetValue(),
		Version:        kv.GetVersion(),
		AppliedVersion: response.GetAppliedVersion(),
		CreateTimeMs:   kv.GetCreateTimeMs(),
		UpdateTimeMs:   kv.GetUpdateTimeMs(),
		ExpireTimeMs:   kv.GetExpireTimeMs(),
		RequestID:      requestID,
		HeadOnly:       options.HeadOnly,
	}, nil
}

// BatchGet reads keys in one RPC. It preserves duplicate keys and input order;
// per-key application statuses are returned as items so callers can report
// partial outcomes without pretending that a missing value is success.
func (c *Client) BatchGet(ctx context.Context, keys [][]byte, options ReadOptions) (*BatchReadResult, error) {
	requestID := newRequestID()
	request := &gateway.BatchGetRequest{
		Ctx:      c.requestContext(requestID),
		Keys:     keys,
		HeadOnly: options.HeadOnly,
	}
	if options.Consistency != gateway.Consistency_CONSISTENCY_UNSPECIFIED {
		request.Options = &gateway.ReadOptions{Consistency: options.Consistency}
	}

	response, err := c.api.BatchGet(ctx, request)
	if err != nil {
		return nil, &TransportError{Err: err}
	}
	if err := applicationError(response.GetStatus()); err != nil {
		return nil, err
	}
	if len(response.GetResults()) != len(keys) {
		return nil, malformedBatchResponse("result count does not match request key count")
	}

	result := &BatchReadResult{RequestID: requestID, Results: make([]BatchReadItem, len(keys))}
	for index, item := range response.GetResults() {
		if item == nil {
			return nil, malformedBatchResponse(fmt.Sprintf("result %d is missing", index))
		}
		if item.GetStatus() == nil {
			return nil, malformedBatchResponse(fmt.Sprintf("result %d has no status", index))
		}
		if item.GetOutcome() == gateway.BatchGetOutcome_BATCH_GET_OUTCOME_UNSPECIFIED {
			return nil, malformedBatchResponse(fmt.Sprintf("result %d has no terminal outcome", index))
		}
		status := statusError(item.GetStatus())
		kv := item.GetKv()
		result.Results[index] = BatchReadItem{
			Status:         status,
			Outcome:        item.GetOutcome(),
			Found:          status.Code == gateway.Status_OK && kv != nil,
			AppliedVersion: item.GetAppliedVersion(),
		}
		if kv != nil {
			result.Results[index].Value = kv.GetValue()
			result.Results[index].Version = kv.GetVersion()
			result.Results[index].CreateTimeMs = kv.GetCreateTimeMs()
			result.Results[index].UpdateTimeMs = kv.GetUpdateTimeMs()
			result.Results[index].ExpireTimeMs = kv.GetExpireTimeMs()
		}
	}
	return result, nil
}

func malformedBatchResponse(message string) error {
	return &StatusError{Code: gateway.Status_INTERNAL, Message: "malformed BatchGet response: " + message}
}

// Put writes one key. The write is attempted exactly once: an ambiguous
// outcome is reported, never retried here.
func (c *Client) Put(ctx context.Context, key, value []byte, options WriteOptions) (*WriteResult, error) {
	requestID := options.requestID()
	response, err := c.api.Put(ctx, &gateway.PutRequest{
		Ctx:     c.requestContext(requestID),
		Key:     key,
		Value:   value,
		Options: &gateway.WriteOptions{RequireIdempotency: options.AllowServerReplay},
	})
	if err != nil {
		return nil, &TransportError{Err: err}
	}
	if err := applicationError(response.GetStatus()); err != nil {
		return nil, err
	}
	return &WriteResult{Version: response.GetVersion(), RequestID: requestID}, nil
}

// Delete removes one key with the same single-attempt guarantee as Put.
func (c *Client) Delete(ctx context.Context, key []byte, options WriteOptions) (*WriteResult, error) {
	requestID := options.requestID()
	response, err := c.api.Delete(ctx, &gateway.DeleteRequest{
		Ctx:     c.requestContext(requestID),
		Key:     key,
		Options: &gateway.WriteOptions{RequireIdempotency: options.AllowServerReplay},
	})
	if err != nil {
		return nil, &TransportError{Err: err}
	}
	if err := applicationError(response.GetStatus()); err != nil {
		return nil, err
	}
	return &WriteResult{Version: response.GetVersion(), RequestID: requestID}, nil
}

func (c *Client) requestContext(requestID string) *gateway.RequestContext {
	return &gateway.RequestContext{
		RequestId: requestID,
		TenantId:  c.cfg.Request.TenantID,
		Principal: c.cfg.Request.Principal,
	}
}

func (o WriteOptions) requestID() string {
	if o.RequestID != "" {
		return o.RequestID
	}
	return newRequestID()
}

// applicationError converts a response Status into an error for every code
// other than OK. A response without a status is treated as INTERNAL rather
// than as success.
func applicationError(status *gateway.Status) error {
	if status == nil {
		return &StatusError{Code: gateway.Status_INTERNAL, Message: "gateway returned no status"}
	}
	if status.GetCode() == gateway.Status_OK {
		return nil
	}
	return statusError(status)
}

func statusError(status *gateway.Status) *StatusError {
	return &StatusError{
		Code:         status.GetCode(),
		Message:      status.GetMessage(),
		LeaderHint:   status.GetLeaderHint(),
		ShardID:      status.GetShardId(),
		RetryAfterMs: status.GetRetryAfterMs(),
	}
}

// newRequestID returns an RFC 4122 version 4 identifier. Callers that need a
// retry to be de-duplicated pass the previous identifier back in explicitly.
func newRequestID() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		panic(fmt.Sprintf("cannot read random bytes for request id: %v", err))
	}
	buf[6] = (buf[6] & 0x0f) | 0x40
	buf[8] = (buf[8] & 0x3f) | 0x80
	encoded := hex.EncodeToString(buf[:])
	return encoded[0:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:32]
}
