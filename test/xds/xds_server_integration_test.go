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

package xds_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ajith-anz/grpc-go"
	"github.com/ajith-anz/grpc-go/codes"
	"github.com/ajith-anz/grpc-go/credentials"
	"github.com/ajith-anz/grpc-go/credentials/insecure"
	xdscreds "github.com/ajith-anz/grpc-go/credentials/xds"
	"github.com/ajith-anz/grpc-go/internal"
	"github.com/ajith-anz/grpc-go/internal/grpcsync"
	"github.com/ajith-anz/grpc-go/internal/stubserver"
	"github.com/ajith-anz/grpc-go/internal/testutils"
	"github.com/ajith-anz/grpc-go/internal/testutils/xds/e2e"
	"github.com/ajith-anz/grpc-go/internal/testutils/xds/e2e/setup"
	"github.com/ajith-anz/grpc-go/peer"
	"github.com/ajith-anz/grpc-go/resolver"
	"github.com/ajith-anz/grpc-go/status"
	"github.com/ajith-anz/grpc-go/xds"

	testgrpc "github.com/ajith-anz/grpc-go/interop/grpc_testing"
	testpb "github.com/ajith-anz/grpc-go/interop/grpc_testing"
)

func testModeChangeServerOption(t *testing.T) grpc.ServerOption {
	// Create a server option to get notified about serving mode changes. We don't
	// do anything other than throwing a log entry here. But this is required,
	// since the server code emits a log entry at the default level (which is
	// ERROR) if no callback is registered for serving mode changes. Our
	// testLogger fails the test if there is any log entry at ERROR level. It does
	// provide an ExpectError()  method, but that takes a string and it would be
	// painful to construct the exact error message expected here. Instead this
	// works just fine.
	return xds.ServingModeCallback(func(addr net.Addr, args xds.ServingModeChangeArgs) {
		t.Logf("Serving mode for listener %q changed to %q, err: %v", addr.String(), args.Mode, args.Err)
	})
}

// acceptNotifyingListener wraps a listener and notifies users when a server
// calls the Listener.Accept() method. This can be used to ensure that the
// server is ready before requests are sent to it.
type acceptNotifyingListener struct {
	net.Listener
	serverReady grpcsync.Event
}

func (l *acceptNotifyingListener) Accept() (net.Conn, error) {
	l.serverReady.Fire()
	return l.Listener.Accept()
}

// setupGRPCServer performs the following:
//   - spin up an xDS-enabled gRPC server, configure it with xdsCredentials and
//     register the test service on it
//   - create a local TCP listener and start serving on it
//
// Returns the following:
// - local listener on which the xDS-enabled gRPC server is serving on
// - cleanup function to be invoked by the tests when done
func setupGRPCServer(t *testing.T, bootstrapContents []byte, opts ...grpc.ServerOption) (net.Listener, func()) {
	t.Helper()

	// Configure xDS credentials to be used on the server-side.
	creds, err := xdscreds.NewServerCredentials(xdscreds.ServerOptions{
		FallbackCreds: insecure.NewCredentials(),
	})
	if err != nil {
		t.Fatal(err)
	}

	// Initialize a test gRPC server, assign it to the stub server, and start
	// the test service.
	stub := &stubserver.StubServer{
		EmptyCallF: func(context.Context, *testpb.Empty) (*testpb.Empty, error) {
			return &testpb.Empty{}, nil
		},
		UnaryCallF: func(context.Context, *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
			return &testpb.SimpleResponse{}, nil
		},
		FullDuplexCallF: func(stream testgrpc.TestService_FullDuplexCallServer) error {
			for {
				_, err := stream.Recv() // hangs here forever if stream doesn't shut down...doesn't receive EOF without any errors
				if err == io.EOF {
					return nil
				}
			}
		},
	}

	opts = append([]grpc.ServerOption{
		grpc.Creds(creds),
		testModeChangeServerOption(t),
		xds.BootstrapContentsForTesting(bootstrapContents),
	}, opts...)
	if stub.S, err = xds.NewGRPCServer(opts...); err != nil {
		t.Fatalf("Failed to create an xDS enabled gRPC server: %v", err)
	}

	// Create a local listener and pass it to Serve().
	lis, err := testutils.LocalTCPListener()
	if err != nil {
		t.Fatalf("testutils.LocalTCPListener() failed: %v", err)
	}

	readyLis := &acceptNotifyingListener{
		Listener:    lis,
		serverReady: *grpcsync.NewEvent(),
	}

	stub.Listener = readyLis
	stubserver.StartTestService(t, stub)

	// Wait for the server to start running.
	select {
	case <-readyLis.serverReady.Done():
	case <-time.After(defaultTestTimeout):
		t.Fatalf("Timed out while waiting for the backend server to start serving")
	}

	return lis, func() {
		stub.S.Stop()
	}
}

func hostPortFromListener(lis net.Listener) (string, uint32, error) {
	host, p, err := net.SplitHostPort(lis.Addr().String())
	if err != nil {
		return "", 0, fmt.Errorf("net.SplitHostPort(%s) failed: %v", lis.Addr().String(), err)
	}
	port, err := strconv.ParseInt(p, 10, 32)
	if err != nil {
		return "", 0, fmt.Errorf("strconv.ParseInt(%s, 10, 32) failed: %v", p, err)
	}
	return host, uint32(port), nil
}

// TestServerSideXDS_Fallback is an e2e test which verifies xDS credentials
// fallback functionality.
//
// The following sequence of events happen as part of this test:
//   - An xDS-enabled gRPC server is created and xDS credentials are configured.
//   - xDS is enabled on the client by the use of the xds:/// scheme, and xDS
//     credentials are configured.
//   - Control plane is configured to not send any security configuration to both
//     the client and the server. This results in both of them using the
//     configured fallback credentials (which is insecure creds in this case).
func (s) TestServerSideXDS_Fallback(t *testing.T) {
	managementServer, nodeID, bootstrapContents, xdsResolver := setup.ManagementServerAndResolver(t)

	lis, cleanup2 := setupGRPCServer(t, bootstrapContents)
	defer cleanup2()

	// Grab the host and port of the server and create client side xDS resources
	// corresponding to it. This contains default resources with no security
	// configuration in the Cluster resources.
	host, port, err := hostPortFromListener(lis)
	if err != nil {
		t.Fatalf("failed to retrieve host and port of server: %v", err)
	}
	const serviceName = "my-service-fallback"
	resources := e2e.DefaultClientResources(e2e.ResourceParams{
		DialTarget: serviceName,
		NodeID:     nodeID,
		Host:       host,
		Port:       port,
		SecLevel:   e2e.SecurityLevelNone,
	})

	// Create an inbound xDS listener resource for the server side that does not
	// contain any security configuration. This should force the server-side
	// xdsCredentials to use fallback.
	inboundLis := e2e.DefaultServerListener(host, port, e2e.SecurityLevelNone, "routeName")
	resources.Listeners = append(resources.Listeners, inboundLis)

	// Setup the management server with client and server-side resources.
	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	if err := managementServer.Update(ctx, resources); err != nil {
		t.Fatal(err)
	}

	// Create client-side xDS credentials with an insecure fallback.
	creds, err := xdscreds.NewClientCredentials(xdscreds.ClientOptions{
		FallbackCreds: insecure.NewCredentials(),
	})
	if err != nil {
		t.Fatal(err)
	}

	// Create a ClientConn with the xds scheme and make a successful RPC.
	cc, err := grpc.NewClient(fmt.Sprintf("xds:///%s", serviceName), grpc.WithTransportCredentials(creds), grpc.WithResolvers(xdsResolver))
	if err != nil {
		t.Fatalf("failed to create a client for server: %v", err)
	}
	defer cc.Close()

	client := testgrpc.NewTestServiceClient(cc)
	if _, err := client.EmptyCall(ctx, &testpb.Empty{}, grpc.WaitForReady(true)); err != nil {
		t.Errorf("rpc EmptyCall() failed: %v", err)
	}
}

// TestServerSideXDS_FileWatcherCerts is an e2e test which verifies xDS
// credentials with file watcher certificate provider.
//
// The following sequence of events happen as part of this test:
//   - An xDS-enabled gRPC server is created and xDS credentials are configured.
//   - xDS is enabled on the client by the use of the xds:/// scheme, and xDS
//     credentials are configured.
//   - Control plane is configured to send security configuration to both the
//     client and the server, pointing to the file watcher certificate provider.
//     We verify both TLS and mTLS scenarios.
func (s) TestServerSideXDS_FileWatcherCerts(t *testing.T) {
	tests := []struct {
		name     string
		secLevel e2e.SecurityLevel
	}{
		{
			name:     "tls",
			secLevel: e2e.SecurityLevelTLS,
		},
		{
			name:     "mtls",
			secLevel: e2e.SecurityLevelMTLS,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			managementServer, nodeID, bootstrapContents, xdsResolver := setup.ManagementServerAndResolver(t)
			lis, cleanup2 := setupGRPCServer(t, bootstrapContents)
			defer cleanup2()

			// Grab the host and port of the server and create client side xDS
			// resources corresponding to it.
			host, port, err := hostPortFromListener(lis)
			if err != nil {
				t.Fatalf("failed to retrieve host and port of server: %v", err)
			}

			// Create xDS resources to be consumed on the client side. This
			// includes the listener, route configuration, cluster (with
			// security configuration) and endpoint resources.
			serviceName := "my-service-file-watcher-certs-" + test.name
			resources := e2e.DefaultClientResources(e2e.ResourceParams{
				DialTarget: serviceName,
				NodeID:     nodeID,
				Host:       host,
				Port:       port,
				SecLevel:   test.secLevel,
			})

			// Create an inbound xDS listener resource for the server side that
			// contains security configuration pointing to the file watcher
			// plugin.
			inboundLis := e2e.DefaultServerListener(host, port, test.secLevel, "routeName")
			resources.Listeners = append(resources.Listeners, inboundLis)

			// Setup the management server with client and server resources.
			ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
			defer cancel()
			if err := managementServer.Update(ctx, resources); err != nil {
				t.Fatal(err)
			}

			// Create client-side xDS credentials with an insecure fallback.
			creds, err := xdscreds.NewClientCredentials(xdscreds.ClientOptions{
				FallbackCreds: insecure.NewCredentials(),
			})
			if err != nil {
				t.Fatal(err)
			}

			// Create a ClientConn with the xds scheme and make an RPC.
			cc, err := grpc.NewClient(fmt.Sprintf("xds:///%s", serviceName), grpc.WithTransportCredentials(creds), grpc.WithResolvers(xdsResolver))
			if err != nil {
				t.Fatalf("failed to create a client for server: %v", err)
			}
			defer cc.Close()

			client := testgrpc.NewTestServiceClient(cc)
			if _, err := client.EmptyCall(ctx, &testpb.Empty{}, grpc.WaitForReady(true)); err != nil {
				t.Fatalf("rpc EmptyCall() failed: %v", err)
			}
		})
	}
}

// TestServerSideXDS_SecurityConfigChange is an e2e test where xDS is enabled on
// the server-side and xdsCredentials are configured for security. The control
// plane initially does not any security configuration. This forces the
// xdsCredentials to use fallback creds, which is this case is insecure creds.
// We verify that a client connecting with TLS creds is not able to successfully
// make an RPC. The control plane then sends a listener resource with security
// configuration pointing to the use of the file_watcher plugin and we verify
// that the same client is now able to successfully make an RPC.
func (s) TestServerSideXDS_SecurityConfigChange(t *testing.T) {
	managementServer := e2e.StartManagementServer(t, e2e.ManagementServerOptions{AllowResourceSubset: true})

	// Create bootstrap configuration pointing to the above management server.
	nodeID := uuid.New().String()
	bootstrapContents := e2e.DefaultBootstrapContents(t, nodeID, managementServer.Address)

	// Create an xDS resolver with the above bootstrap configuration.
	if internal.NewXDSResolverWithConfigForTesting == nil {
		t.Fatalf("internal.NewXDSResolverWithConfigForTesting is nil")
	}
	xdsResolver, err := internal.NewXDSResolverWithConfigForTesting.(func([]byte) (resolver.Builder, error))(bootstrapContents)
	if err != nil {
		t.Fatalf("Failed to create xDS resolver for testing: %v", err)
	}

	lis, cleanup2 := setupGRPCServer(t, bootstrapContents)
	defer cleanup2()

	// Grab the host and port of the server and create client side xDS resources
	// corresponding to it. This contains default resources with no security
	// configuration in the Cluster resource. This should force the xDS
	// credentials on the client to use its fallback.
	host, port, err := hostPortFromListener(lis)
	if err != nil {
		t.Fatalf("failed to retrieve host and port of server: %v", err)
	}
	const serviceName = "my-service-security-config-change"
	resources := e2e.DefaultClientResources(e2e.ResourceParams{
		DialTarget: serviceName,
		NodeID:     nodeID,
		Host:       host,
		Port:       port,
		SecLevel:   e2e.SecurityLevelNone,
	})

	// Create an inbound xDS listener resource for the server side that does not
	// contain any security configuration. This should force the xDS credentials
	// on server to use its fallback.
	inboundLis := e2e.DefaultServerListener(host, port, e2e.SecurityLevelNone, "routeName")
	resources.Listeners = append(resources.Listeners, inboundLis)

	// Setup the management server with client and server-side resources.
	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	if err := managementServer.Update(ctx, resources); err != nil {
		t.Fatal(err)
	}

	// Create client-side xDS credentials with an insecure fallback.
	xdsCreds, err := xdscreds.NewClientCredentials(xdscreds.ClientOptions{
		FallbackCreds: insecure.NewCredentials(),
	})
	if err != nil {
		t.Fatal(err)
	}

	// Create a ClientConn with the xds scheme and make a successful RPC.
	xdsCC, err := grpc.NewClient(fmt.Sprintf("xds:///%s", serviceName), grpc.WithTransportCredentials(xdsCreds), grpc.WithResolvers(xdsResolver))
	if err != nil {
		t.Fatalf("failed to create a client for server: %v", err)
	}
	defer xdsCC.Close()

	client := testgrpc.NewTestServiceClient(xdsCC)
	if _, err := client.EmptyCall(ctx, &testpb.Empty{}, grpc.WaitForReady(true)); err != nil {
		t.Fatalf("rpc EmptyCall() failed: %v", err)
	}

	// Create a ClientConn with TLS creds. This should fail since the server is
	// using fallback credentials which in this case in insecure creds.
	tlsCreds := testutils.CreateClientTLSCredentials(t)
	tlsCC, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(tlsCreds))
	if err != nil {
		t.Fatalf("failed to create a client for server: %v", err)
	}
	defer tlsCC.Close()

	// We don't set 'waitForReady` here since we want this call to failfast.
	client = testgrpc.NewTestServiceClient(tlsCC)
	if _, err := client.EmptyCall(ctx, &testpb.Empty{}); status.Code(err) != codes.Unavailable {
		t.Fatal("rpc EmptyCall() succeeded when expected to fail")
	}

	// Switch server and client side resources with ones that contain required
	// security configuration for mTLS with a file watcher certificate provider.
	resources = e2e.DefaultClientResources(e2e.ResourceParams{
		DialTarget: serviceName,
		NodeID:     nodeID,
		Host:       host,
		Port:       port,
		SecLevel:   e2e.SecurityLevelMTLS,
	})
	inboundLis = e2e.DefaultServerListener(host, port, e2e.SecurityLevelMTLS, "routeName")
	resources.Listeners = append(resources.Listeners, inboundLis)
	if err := managementServer.Update(ctx, resources); err != nil {
		t.Fatal(err)
	}

	// Make another RPC with `waitForReady` set and expect this to succeed.
	if _, err := client.EmptyCall(ctx, &testpb.Empty{}, grpc.WaitForReady(true)); err != nil {
		t.Fatalf("rpc EmptyCall() failed: %v", err)
	}
}

// TestServerSideXDS_FileWatcherCertsSPIFFE is an e2e test which verifies xDS
// credentials with file watcher certificate provider that is configured with a
// SPIFFE Bundle Map for it's roots.
//
// The following sequence of events happen as part of this test:
//   - An xDS-enabled gRPC server is created and xDS credentials are configured.
//   - xDS is enabled on the client by the use of the xds:/// scheme, and xDS
//     credentials are configured.
//   - Control plane is configured to send security configuration to both the
//     client and the server, pointing to the file watcher certificate provider.
//     We verify both TLS and mTLS scenarios.
func (s) TestServerSideXDS_FileWatcherCertsSPIFFE(t *testing.T) {
	tests := []struct {
		name     string
		secLevel e2e.SecurityLevel
	}{
		{
			name:     "tls",
			secLevel: e2e.SecurityLevelTLS,
		},
		{
			name:     "mtls",
			secLevel: e2e.SecurityLevelMTLS,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			managementServer, nodeID, bootstrapContents, xdsResolver := setup.ManagementServerAndResolverWithSPIFFE(t)
			lis, cleanup2 := setupGRPCServer(t, bootstrapContents)
			defer cleanup2()

			// Grab the host and port of the server and create client side xDS
			// resources corresponding to it.
			host, port, err := hostPortFromListener(lis)
			if err != nil {
				t.Fatalf("failed to retrieve host and port of server: %v", err)
			}

			// Create xDS resources to be consumed on the client side. This
			// includes the listener, route configuration, cluster (with
			// security configuration) and endpoint resources.
			serviceName := "my-service-file-watcher-certs-" + test.name
			resources := e2e.DefaultClientResources(e2e.ResourceParams{
				DialTarget: serviceName,
				NodeID:     nodeID,
				Host:       host,
				Port:       port,
				SecLevel:   test.secLevel,
			})

			// Create an inbound xDS listener resource for the server side that
			// contains security configuration pointing to the file watcher
			// plugin.
			inboundLis := e2e.DefaultServerListener(host, port, test.secLevel, "routeName")
			resources.Listeners = append(resources.Listeners, inboundLis)

			// Setup the management server with client and server resources.
			ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
			defer cancel()
			if err := managementServer.Update(ctx, resources); err != nil {
				t.Fatal(err)
			}

			// Create client-side xDS credentials with an insecure fallback.
			creds, err := xdscreds.NewClientCredentials(xdscreds.ClientOptions{
				FallbackCreds: insecure.NewCredentials(),
			})
			if err != nil {
				t.Fatal(err)
			}

			// Create a ClientConn with the xds scheme and make an RPC.
			cc, err := grpc.NewClient(fmt.Sprintf("xds:///%s", serviceName), grpc.WithTransportCredentials(creds), grpc.WithResolvers(xdsResolver))
			if err != nil {
				t.Fatalf("failed to create a client for server: %v", err)
			}
			defer cc.Close()

			peer := &peer.Peer{}
			client := testgrpc.NewTestServiceClient(cc)
			if _, err := client.EmptyCall(ctx, &testpb.Empty{}, grpc.WaitForReady(true), grpc.Peer(peer)); err != nil {
				t.Fatalf("rpc EmptyCall() failed: %v", err)
			}
			verifySecurityInformationFromPeerSPIFFE(t, peer, test.secLevel, 1)
		})
	}
}

// Checks the AuthInfo available in the peer if it matches the expected security
// level of the connection.
func verifySecurityInformationFromPeerSPIFFE(t *testing.T, pr *peer.Peer, wantSecLevel e2e.SecurityLevel, wantPeerChainLen int) {
	// This is not a true helper in the Go sense, because it does not perform
	// setup or cleanup tasks. Marking it a helper is to ensure that when the
	// test fails, the line information of the caller is outputted instead of
	// from here.
	//
	// And this function directly calls t.Fatalf() instead of returning an error
	// and letting the caller decide what to do with it. This is also OK since
	// all callers will simply end up calling t.Fatalf() with the returned
	// error, and can't add any contextual information of value to the error
	// message.
	t.Helper()

	authType := pr.AuthInfo.AuthType()
	switch wantSecLevel {
	case e2e.SecurityLevelNone:
		if authType != "insecure" {
			t.Fatalf("AuthType() is %s, want insecure", authType)
		}
	case e2e.SecurityLevelMTLS:
		if authType != "tls" {
			t.Fatalf("AuthType() is %s, want tls", authType)
		}
		ai, ok := pr.AuthInfo.(credentials.TLSInfo)
		if !ok {
			t.Fatalf("AuthInfo type is %T, want %T", pr.AuthInfo, credentials.TLSInfo{})
		}
		if len(ai.State.PeerCertificates) != wantPeerChainLen {
			t.Fatalf("Number of peer certificates is %d, want %d", len(ai.State.PeerCertificates), wantPeerChainLen)
		}
	}
}
