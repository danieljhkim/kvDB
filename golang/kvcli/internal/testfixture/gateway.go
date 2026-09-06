// Package testfixture starts a real in-process KvGateway gRPC server so tests
// exercise the generated bindings and the transport instead of a stub.
package testfixture

import (
	"context"
	"net"
	"strconv"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	gateway "github.com/danieljhkim/kv/internal/gen/kvdb/gateway"
)

// Call records one received request for assertions about wire content.
type Call struct {
	Method             string
	RequestID          string
	TenantID           string
	Principal          string
	Key                []byte
	Keys               [][]byte
	Value              []byte
	HeadOnly           bool
	Consistency        gateway.Consistency
	RequireIdempotency bool
}

// Hooks replaces the default in-memory behavior for a single method.
type Hooks struct {
	Get      func(context.Context, *gateway.GetRequest) (*gateway.GetResponse, error)
	BatchGet func(context.Context, *gateway.BatchGetRequest) (*gateway.BatchGetResponse, error)
	Put      func(context.Context, *gateway.PutRequest) (*gateway.PutResponse, error)
	Delete   func(context.Context, *gateway.DeleteRequest) (*gateway.DeleteResponse, error)
}

type entry struct {
	value   []byte
	version uint64
}

// Server is a KvGateway implementation backed by an in-memory store.
type Server struct {
	gateway.UnimplementedKvGatewayServer

	mu      sync.Mutex
	data    map[string]entry
	calls   []Call
	version uint64

	hooks   Hooks
	address string
}

// Start listens on an ephemeral loopback port and serves until the test ends.
// Passing nil credentials serves plaintext.
func Start(t *testing.T, hooks Hooks, creds credentials.TransportCredentials, options ...grpc.ServerOption) *Server {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("cannot listen: %v", err)
	}

	server := &Server{data: map[string]entry{}, hooks: hooks, address: listener.Addr().String()}

	serverOptions := options
	if creds != nil {
		serverOptions = append(serverOptions, grpc.Creds(creds))
	}
	grpcServer := grpc.NewServer(serverOptions...)
	gateway.RegisterKvGatewayServer(grpcServer, server)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = grpcServer.Serve(listener)
	}()
	t.Cleanup(func() {
		grpcServer.Stop()
		<-done
	})

	return server
}

// Address returns the host:port the fixture is listening on.
func (s *Server) Address() string { return s.address }

// Host and Port split Address for clients configured by field.
func (s *Server) Host() string {
	host, _, _ := net.SplitHostPort(s.address)
	return host
}

func (s *Server) Port() int {
	_, port, _ := net.SplitHostPort(s.address)
	number, _ := strconv.Atoi(port)
	return number
}

// Calls returns a copy of every request the fixture received.
func (s *Server) Calls() []Call {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Call(nil), s.calls...)
}

// Seed stores a value directly, without going through the RPC surface.
func (s *Server) Seed(key, value []byte) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.version++
	s.data[string(key)] = entry{value: value, version: s.version}
	return s.version
}

func (s *Server) record(call Call) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, call)
}

func (s *Server) Get(ctx context.Context, request *gateway.GetRequest) (*gateway.GetResponse, error) {
	s.record(Call{
		Method:      "Get",
		RequestID:   request.GetCtx().GetRequestId(),
		TenantID:    request.GetCtx().GetTenantId(),
		Principal:   request.GetCtx().GetPrincipal(),
		Key:         request.GetKey(),
		HeadOnly:    request.GetHeadOnly(),
		Consistency: request.GetOptions().GetConsistency(),
	})
	if s.hooks.Get != nil {
		return s.hooks.Get(ctx, request)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	stored, found := s.data[string(request.GetKey())]
	if !found {
		return &gateway.GetResponse{
			Status:         &gateway.Status{Code: gateway.Status_NOT_FOUND, Message: "key not found"},
			AppliedVersion: s.version,
		}, nil
	}
	kv := &gateway.KeyValue{Key: request.GetKey(), Version: stored.version}
	if !request.GetHeadOnly() {
		kv.Value = stored.value
	}
	return &gateway.GetResponse{
		Status:         &gateway.Status{Code: gateway.Status_OK},
		Kv:             kv,
		AppliedVersion: s.version,
	}, nil
}

func (s *Server) BatchGet(ctx context.Context, request *gateway.BatchGetRequest) (*gateway.BatchGetResponse, error) {
	s.record(Call{
		Method:      "BatchGet",
		RequestID:   request.GetCtx().GetRequestId(),
		TenantID:    request.GetCtx().GetTenantId(),
		Principal:   request.GetCtx().GetPrincipal(),
		HeadOnly:    request.GetHeadOnly(),
		Consistency: request.GetOptions().GetConsistency(),
		Keys:        append([][]byte(nil), request.GetKeys()...),
	})
	if s.hooks.BatchGet != nil {
		return s.hooks.BatchGet(ctx, request)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	response := &gateway.BatchGetResponse{Status: &gateway.Status{Code: gateway.Status_OK}}
	for _, key := range request.GetKeys() {
		item := &gateway.BatchGetResult{
			Key:     key,
			Outcome: gateway.BatchGetOutcome_COMPLETED,
		}
		stored, found := s.data[string(key)]
		if !found {
			item.Status = &gateway.Status{Code: gateway.Status_NOT_FOUND, Message: "key not found"}
		} else {
			item.Status = &gateway.Status{Code: gateway.Status_OK}
			item.AppliedVersion = s.version
			item.Kv = &gateway.KeyValue{Key: key, Version: stored.version}
			if !request.GetHeadOnly() {
				item.Kv.Value = stored.value
			}
		}
		response.Results = append(response.Results, item)
	}
	return response, nil
}

func (s *Server) Put(ctx context.Context, request *gateway.PutRequest) (*gateway.PutResponse, error) {
	s.record(Call{
		Method:             "Put",
		RequestID:          request.GetCtx().GetRequestId(),
		TenantID:           request.GetCtx().GetTenantId(),
		Principal:          request.GetCtx().GetPrincipal(),
		Key:                request.GetKey(),
		Value:              request.GetValue(),
		RequireIdempotency: request.GetOptions().GetRequireIdempotency(),
	})
	if s.hooks.Put != nil {
		return s.hooks.Put(ctx, request)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.version++
	s.data[string(request.GetKey())] = entry{value: request.GetValue(), version: s.version}
	return &gateway.PutResponse{Status: &gateway.Status{Code: gateway.Status_OK}, Version: s.version}, nil
}

func (s *Server) Delete(ctx context.Context, request *gateway.DeleteRequest) (*gateway.DeleteResponse, error) {
	s.record(Call{
		Method:             "Delete",
		RequestID:          request.GetCtx().GetRequestId(),
		TenantID:           request.GetCtx().GetTenantId(),
		Principal:          request.GetCtx().GetPrincipal(),
		Key:                request.GetKey(),
		RequireIdempotency: request.GetOptions().GetRequireIdempotency(),
	})
	if s.hooks.Delete != nil {
		return s.hooks.Delete(ctx, request)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, found := s.data[string(request.GetKey())]; !found {
		return &gateway.DeleteResponse{
			Status: &gateway.Status{Code: gateway.Status_NOT_FOUND, Message: "key not found"},
		}, nil
	}
	delete(s.data, string(request.GetKey()))
	s.version++
	return &gateway.DeleteResponse{Status: &gateway.Status{Code: gateway.Status_OK}, Version: s.version}, nil
}
