//go:build linux
// +build linux

/*
 *
 * Copyright 2020 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package test

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/ajith-anz/grpc-go"
	"github.com/ajith-anz/grpc-go/codes"
	"github.com/ajith-anz/grpc-go/credentials/insecure"
	"github.com/ajith-anz/grpc-go/internal/stubserver"
	"github.com/ajith-anz/grpc-go/metadata"
	"github.com/ajith-anz/grpc-go/resolver"
	"github.com/ajith-anz/grpc-go/resolver/manual"
	"github.com/ajith-anz/grpc-go/status"

	testgrpc "github.com/ajith-anz/grpc-go/interop/grpc_testing"
	testpb "github.com/ajith-anz/grpc-go/interop/grpc_testing"
)

func authorityChecker(ctx context.Context, expectedAuthority string) (*testpb.Empty, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "failed to parse metadata")
	}
	auths, ok := md[":authority"]
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "no authority header")
	}
	if len(auths) != 1 {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("no authority header, auths = %v", auths))
	}
	if auths[0] != expectedAuthority {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("invalid authority header %v, expected %v", auths[0], expectedAuthority))
	}
	return &testpb.Empty{}, nil
}

func runUnixTest(t *testing.T, address, target, expectedAuthority string, dialer func(context.Context, string) (net.Conn, error)) {
	if !strings.HasPrefix(target, "unix-abstract:") {
		if err := os.RemoveAll(address); err != nil {
			t.Fatalf("Error removing socket file %v: %v\n", address, err)
		}
	}
	ss := &stubserver.StubServer{
		EmptyCallF: func(ctx context.Context, _ *testpb.Empty) (*testpb.Empty, error) {
			return authorityChecker(ctx, expectedAuthority)
		},
		Network: "unix",
		Address: address,
		Target:  target,
	}
	opts := []grpc.DialOption{}
	if dialer != nil {
		opts = append(opts, grpc.WithContextDialer(dialer))
	}
	if err := ss.Start(nil, opts...); err != nil {
		t.Fatalf("Error starting endpoint server: %v", err)
	}
	defer ss.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	_, err := ss.Client.EmptyCall(ctx, &testpb.Empty{})
	if err != nil {
		t.Errorf("us.client.EmptyCall(_, _) = _, %v; want _, nil", err)
	}
}

type authorityTest struct {
	name           string
	address        string
	target         string
	authority      string
	dialTargetWant string
}

var authorityTests = []authorityTest{
	{
		name:           "UnixRelative",
		address:        "sock.sock",
		target:         "unix:sock.sock",
		authority:      "localhost",
		dialTargetWant: "unix:sock.sock",
	},
	{
		name:           "UnixAbsolute",
		address:        "/tmp/sock.sock",
		target:         "unix:/tmp/sock.sock",
		authority:      "localhost",
		dialTargetWant: "unix:///tmp/sock.sock",
	},
	{
		name:           "UnixAbsoluteAlternate",
		address:        "/tmp/sock.sock",
		target:         "unix:///tmp/sock.sock",
		authority:      "localhost",
		dialTargetWant: "unix:///tmp/sock.sock",
	},
	{
		name:           "UnixPassthrough",
		address:        "/tmp/sock.sock",
		target:         "passthrough:///unix:///tmp/sock.sock",
		authority:      "unix:%2F%2F%2Ftmp%2Fsock.sock",
		dialTargetWant: "unix:///tmp/sock.sock",
	},
	{
		name:           "UnixAbstract",
		address:        "@abc efg",
		target:         "unix-abstract:abc efg",
		authority:      "localhost",
		dialTargetWant: "unix:@abc efg",
	},
}

// TestUnix does end to end tests with the various supported unix target
// formats, ensuring that the authority is set as expected.
func (s) TestUnix(t *testing.T) {
	for _, test := range authorityTests {
		t.Run(test.name, func(t *testing.T) {
			runUnixTest(t, test.address, test.target, test.authority, nil)
		})
	}
}

// TestUnixCustomDialer does end to end tests with various supported unix target
// formats, ensuring that the target sent to the dialer does NOT have the
// "unix:" prefix stripped.
func (s) TestUnixCustomDialer(t *testing.T) {
	for _, test := range authorityTests {
		t.Run(test.name+"WithDialer", func(t *testing.T) {
			dialer := func(ctx context.Context, address string) (net.Conn, error) {
				if address != test.dialTargetWant {
					return nil, fmt.Errorf("expected target %v in custom dialer, instead got %v", test.dialTargetWant, address)
				}
				address = address[len("unix:"):]
				return (&net.Dialer{}).DialContext(ctx, "unix", address)
			}
			runUnixTest(t, test.address, test.target, test.authority, dialer)
		})
	}
}

// TestColonPortAuthority does an end to end test with the target for grpc.NewClient
// being ":[port]". Ensures authority is "localhost:[port]".
func (s) TestColonPortAuthority(t *testing.T) {
	expectedAuthority := ""
	var authorityMu sync.Mutex
	ss := &stubserver.StubServer{
		EmptyCallF: func(ctx context.Context, _ *testpb.Empty) (*testpb.Empty, error) {
			authorityMu.Lock()
			defer authorityMu.Unlock()
			return authorityChecker(ctx, expectedAuthority)
		},
		Network: "tcp",
	}
	if err := ss.Start(nil); err != nil {
		t.Fatalf("Error starting endpoint server: %v", err)
	}
	defer ss.Stop()
	_, port, err := net.SplitHostPort(ss.Address)
	if err != nil {
		t.Fatalf("Failed splitting host from post: %v", err)
	}
	authorityMu.Lock()
	expectedAuthority = "localhost:" + port
	authorityMu.Unlock()
	// ss.Start dials the server, but we explicitly test with ":[port]"
	// as the target.
	cc, err := grpc.NewClient(":"+port, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient(%q) = %v", ss.Target, err)
	}
	defer cc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	_, err = testgrpc.NewTestServiceClient(cc).EmptyCall(ctx, &testpb.Empty{})
	if err != nil {
		t.Errorf("us.client.EmptyCall(_, _) = _, %v; want _, nil", err)
	}
}

// TestAuthorityReplacedWithResolverAddress tests the scenario where the resolver
// returned address contains a ServerName override. The test verifies that the
// :authority header value sent to the server as part of the http/2 HEADERS frame
// is set to the value specified in the resolver returned address.
func (s) TestAuthorityReplacedWithResolverAddress(t *testing.T) {
	const expectedAuthority = "test.server.name"

	ss := &stubserver.StubServer{
		EmptyCallF: func(ctx context.Context, _ *testpb.Empty) (*testpb.Empty, error) {
			return authorityChecker(ctx, expectedAuthority)
		},
	}
	if err := ss.Start(nil); err != nil {
		t.Fatalf("Error starting endpoint server: %v", err)
	}
	defer ss.Stop()

	r := manual.NewBuilderWithScheme("whatever")
	r.InitialState(resolver.State{Addresses: []resolver.Address{{Addr: ss.Address, ServerName: expectedAuthority}}})
	cc, err := grpc.NewClient(r.Scheme()+":///whatever", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithResolvers(r))
	if err != nil {
		t.Fatalf("grpc.NewClient(%q) = %v", ss.Address, err)
	}
	defer cc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	if _, err = testgrpc.NewTestServiceClient(cc).EmptyCall(ctx, &testpb.Empty{}); err != nil {
		t.Fatalf("EmptyCall() rpc failed: %v", err)
	}
}
