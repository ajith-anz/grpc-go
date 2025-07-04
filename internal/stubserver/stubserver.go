/*
 *
 * Copyright 2020 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

// Package stubserver is a stubbable implementation of
// github.com/ajith-anz/grpc-go/interop/grpc_testing for testing purposes.
package stubserver

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"github.com/ajith-anz/grpc-go"
	"github.com/ajith-anz/grpc-go/connectivity"
	"github.com/ajith-anz/grpc-go/credentials/insecure"
	"github.com/ajith-anz/grpc-go/resolver"
	"github.com/ajith-anz/grpc-go/resolver/manual"
	"github.com/ajith-anz/grpc-go/serviceconfig"

	testgrpc "github.com/ajith-anz/grpc-go/interop/grpc_testing"
	testpb "github.com/ajith-anz/grpc-go/interop/grpc_testing"
)

// GRPCServer is an interface that groups methods implemented by a grpc.Server
// or an xds.GRPCServer that are used by the StubServer.
type GRPCServer interface {
	grpc.ServiceRegistrar
	Stop()
	GracefulStop()
	Serve(net.Listener) error
}

// StubServer is a server that is easy to customize within individual test
// cases.
type StubServer struct {
	// Guarantees we satisfy this interface; panics if unimplemented methods are called.
	testgrpc.TestServiceServer

	// Customizable implementations of server handlers.
	EmptyCallF           func(ctx context.Context, in *testpb.Empty) (*testpb.Empty, error)
	UnaryCallF           func(ctx context.Context, in *testpb.SimpleRequest) (*testpb.SimpleResponse, error)
	FullDuplexCallF      func(stream testgrpc.TestService_FullDuplexCallServer) error
	StreamingInputCallF  func(stream testgrpc.TestService_StreamingInputCallServer) error
	StreamingOutputCallF func(req *testpb.StreamingOutputCallRequest, stream testgrpc.TestService_StreamingOutputCallServer) error

	// A client connected to this service the test may use.  Created in Start().
	Client testgrpc.TestServiceClient
	CC     *grpc.ClientConn

	// Server to serve this service from.
	//
	// If nil, a new grpc.Server is created, listening on the provided Network
	// and Address fields, or listening using the provided Listener.
	S GRPCServer

	// Parameters for Listen and Dial. Defaults will be used if these are empty
	// before Start.
	Network string
	Address string
	Target  string

	// Custom listener to use for serving. If unspecified, a new listener is
	// created on a local port.
	Listener net.Listener

	cleanups []func() // Lambdas executed in Stop(); populated by Start().

	// Set automatically if Target == ""
	R *manual.Resolver
}

// EmptyCall is the handler for testpb.EmptyCall
func (ss *StubServer) EmptyCall(ctx context.Context, in *testpb.Empty) (*testpb.Empty, error) {
	return ss.EmptyCallF(ctx, in)
}

// UnaryCall is the handler for testpb.UnaryCall
func (ss *StubServer) UnaryCall(ctx context.Context, in *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
	return ss.UnaryCallF(ctx, in)
}

// FullDuplexCall is the handler for testpb.FullDuplexCall
func (ss *StubServer) FullDuplexCall(stream testgrpc.TestService_FullDuplexCallServer) error {
	return ss.FullDuplexCallF(stream)
}

// StreamingInputCall is the handler for testpb.StreamingInputCall
func (ss *StubServer) StreamingInputCall(stream testgrpc.TestService_StreamingInputCallServer) error {
	return ss.StreamingInputCallF(stream)
}

// StreamingOutputCall is the handler for testpb.StreamingOutputCall
func (ss *StubServer) StreamingOutputCall(req *testpb.StreamingOutputCallRequest, stream testgrpc.TestService_StreamingOutputCallServer) error {
	return ss.StreamingOutputCallF(req, stream)
}

// Start starts the server and creates a client connected to it.
func (ss *StubServer) Start(sopts []grpc.ServerOption, dopts ...grpc.DialOption) error {
	if err := ss.StartServer(sopts...); err != nil {
		return err
	}
	if err := ss.StartClient(dopts...); err != nil {
		ss.Stop()
		return err
	}
	return nil
}

type registerServiceServerOption struct {
	grpc.EmptyServerOption
	f func(grpc.ServiceRegistrar)
}

// RegisterServiceServerOption returns a ServerOption that will run f() in
// Start or StartServer with the grpc.Server created before serving.  This
// allows other services to be registered on the test server (e.g. ORCA,
// health, or reflection).
func RegisterServiceServerOption(f func(grpc.ServiceRegistrar)) grpc.ServerOption {
	return &registerServiceServerOption{f: f}
}

func (ss *StubServer) setupServer(sopts ...grpc.ServerOption) (net.Listener, error) {
	if ss.Network == "" {
		ss.Network = "tcp"
	}
	if ss.Address == "" {
		ss.Address = "localhost:0"
	}
	if ss.Target == "" {
		ss.R = manual.NewBuilderWithScheme("whatever")
	}

	lis := ss.Listener
	if lis == nil {
		var err error
		lis, err = net.Listen(ss.Network, ss.Address)
		if err != nil {
			return nil, fmt.Errorf("net.Listen(%q, %q) = %v", ss.Network, ss.Address, err)
		}
	}
	ss.Address = lis.Addr().String()

	if ss.S == nil {
		ss.S = grpc.NewServer(sopts...)
	}
	for _, so := range sopts {
		if x, ok := so.(*registerServiceServerOption); ok {
			x.f(ss.S)
		}
	}

	testgrpc.RegisterTestServiceServer(ss.S, ss)
	ss.cleanups = append(ss.cleanups, ss.S.Stop)
	return lis, nil
}

// StartHandlerServer only starts an HTTP server with a gRPC server as the
// handler. It does not create a client to it.  Cannot be used in a StubServer
// that also used StartServer.
func (ss *StubServer) StartHandlerServer(sopts ...grpc.ServerOption) error {
	lis, err := ss.setupServer(sopts...)
	if err != nil {
		return err
	}

	handler, ok := ss.S.(interface{ http.Handler })
	if !ok {
		panic(fmt.Sprintf("server of type %T does not implement http.Handler", ss.S))
	}

	go func() {
		hs := &http2.Server{}
		opts := &http2.ServeConnOpts{Handler: handler}
		for {
			conn, err := lis.Accept()
			if err != nil {
				return
			}
			hs.ServeConn(conn, opts)
		}
	}()
	ss.cleanups = append(ss.cleanups, func() { lis.Close() })

	return nil
}

// StartServer only starts the server. It does not create a client to it.
// Cannot be used in a StubServer that also used StartHandlerServer.
func (ss *StubServer) StartServer(sopts ...grpc.ServerOption) error {
	lis, err := ss.setupServer(sopts...)
	if err != nil {
		return err
	}

	go ss.S.Serve(lis)

	return nil
}

// StartClient creates a client connected to this service that the test may use.
// The newly created client will be available in the Client field of StubServer.
func (ss *StubServer) StartClient(dopts ...grpc.DialOption) error {
	opts := append([]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, dopts...)
	if ss.R != nil {
		ss.Target = ss.R.Scheme() + ":///" + ss.Address
		opts = append(opts, grpc.WithResolvers(ss.R))
	}

	cc, err := grpc.NewClient(ss.Target, opts...)
	if err != nil {
		return fmt.Errorf("grpc.NewClient(%q) = %v", ss.Target, err)
	}
	cc.Connect()
	ss.CC = cc
	if ss.R != nil {
		ss.R.UpdateState(resolver.State{Addresses: []resolver.Address{{Addr: ss.Address}}})
	}
	if err := waitForReady(cc); err != nil {
		cc.Close()
		return err
	}

	ss.cleanups = append(ss.cleanups, func() { cc.Close() })

	ss.Client = testgrpc.NewTestServiceClient(cc)
	return nil
}

// NewServiceConfig applies sc to ss.Client using the resolver (if present).
func (ss *StubServer) NewServiceConfig(sc string) {
	if ss.R != nil {
		ss.R.UpdateState(resolver.State{Addresses: []resolver.Address{{Addr: ss.Address}}, ServiceConfig: parseCfg(ss.R, sc)})
	}
}

func waitForReady(cc *grpc.ClientConn) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for {
		s := cc.GetState()
		if s == connectivity.Ready {
			return nil
		}
		if !cc.WaitForStateChange(ctx, s) {
			// ctx got timeout or canceled.
			return ctx.Err()
		}
	}
}

// Stop stops ss and cleans up all resources it consumed.
func (ss *StubServer) Stop() {
	for i := len(ss.cleanups) - 1; i >= 0; i-- {
		ss.cleanups[i]()
	}
	ss.cleanups = nil
}

func parseCfg(r *manual.Resolver, s string) *serviceconfig.ParseResult {
	g := r.CC().ParseServiceConfig(s)
	if g.Err != nil {
		panic(fmt.Sprintf("Error parsing config %q: %v", s, g.Err))
	}
	return g
}

// StartTestService spins up a stub server exposing the TestService on a local
// port. If the passed in server is nil, a stub server that implements only the
// EmptyCall and UnaryCall RPCs is started.
func StartTestService(t *testing.T, server *StubServer, sopts ...grpc.ServerOption) *StubServer {
	if server == nil {
		server = &StubServer{
			EmptyCallF: func(context.Context, *testpb.Empty) (*testpb.Empty, error) { return &testpb.Empty{}, nil },
			UnaryCallF: func(context.Context, *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
				return &testpb.SimpleResponse{}, nil
			},
		}
	}
	server.StartServer(sopts...)

	t.Logf("Started test service backend at %q", server.Address)
	return server
}
