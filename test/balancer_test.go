/*
 *
 * Copyright 2018 gRPC authors.
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

package test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/ajith-anz/grpc-go"
	"github.com/ajith-anz/grpc-go/attributes"
	"github.com/ajith-anz/grpc-go/balancer"
	"github.com/ajith-anz/grpc-go/balancer/pickfirst"
	"github.com/ajith-anz/grpc-go/codes"
	"github.com/ajith-anz/grpc-go/connectivity"
	"github.com/ajith-anz/grpc-go/credentials"
	"github.com/ajith-anz/grpc-go/credentials/insecure"
	"github.com/ajith-anz/grpc-go/internal"
	"github.com/ajith-anz/grpc-go/internal/balancer/stub"
	"github.com/ajith-anz/grpc-go/internal/balancerload"
	"github.com/ajith-anz/grpc-go/internal/grpcsync"
	"github.com/ajith-anz/grpc-go/internal/grpcutil"
	imetadata "github.com/ajith-anz/grpc-go/internal/metadata"
	"github.com/ajith-anz/grpc-go/internal/stubserver"
	"github.com/ajith-anz/grpc-go/internal/testutils"
	"github.com/ajith-anz/grpc-go/metadata"
	"github.com/ajith-anz/grpc-go/resolver"
	"github.com/ajith-anz/grpc-go/resolver/manual"
	"github.com/ajith-anz/grpc-go/status"
	"github.com/ajith-anz/grpc-go/testdata"

	testgrpc "github.com/ajith-anz/grpc-go/interop/grpc_testing"
	testpb "github.com/ajith-anz/grpc-go/interop/grpc_testing"
)

const testBalancerName = "testbalancer"

// testBalancer creates one subconn with the first address from resolved
// addresses.
//
// It's used to test whether options for NewSubConn are applied correctly.
type testBalancer struct {
	cc balancer.ClientConn
	sc balancer.SubConn

	newSubConnOptions balancer.NewSubConnOptions
	pickInfos         []balancer.PickInfo
	pickExtraMDs      []metadata.MD
	doneInfo          []balancer.DoneInfo
}

func (b *testBalancer) Build(cc balancer.ClientConn, _ balancer.BuildOptions) balancer.Balancer {
	b.cc = cc
	return b
}

func (*testBalancer) Name() string {
	return testBalancerName
}

func (*testBalancer) ResolverError(error) {
	panic("not implemented")
}

func (b *testBalancer) UpdateClientConnState(state balancer.ClientConnState) error {
	// Only create a subconn at the first time.
	if b.sc == nil {
		var err error
		b.newSubConnOptions.StateListener = b.updateSubConnState
		b.sc, err = b.cc.NewSubConn(state.ResolverState.Addresses, b.newSubConnOptions)
		if err != nil {
			logger.Errorf("testBalancer: failed to NewSubConn: %v", err)
			return nil
		}
		b.cc.UpdateState(balancer.State{ConnectivityState: connectivity.Connecting, Picker: &picker{err: balancer.ErrNoSubConnAvailable, bal: b}})
		b.sc.Connect()
	}
	return nil
}

func (b *testBalancer) UpdateSubConnState(sc balancer.SubConn, s balancer.SubConnState) {
	panic(fmt.Sprintf("UpdateSubConnState(%v, %+v) called unexpectedly", sc, s))
}

func (b *testBalancer) updateSubConnState(s balancer.SubConnState) {
	logger.Infof("testBalancer: updateSubConnState: %v", s)

	switch s.ConnectivityState {
	case connectivity.Ready:
		b.cc.UpdateState(balancer.State{ConnectivityState: s.ConnectivityState, Picker: &picker{bal: b}})
	case connectivity.Idle:
		b.cc.UpdateState(balancer.State{ConnectivityState: s.ConnectivityState, Picker: &picker{bal: b, idle: true}})
	case connectivity.Connecting:
		b.cc.UpdateState(balancer.State{ConnectivityState: s.ConnectivityState, Picker: &picker{err: balancer.ErrNoSubConnAvailable, bal: b}})
	case connectivity.TransientFailure:
		b.cc.UpdateState(balancer.State{ConnectivityState: s.ConnectivityState, Picker: &picker{err: balancer.ErrTransientFailure, bal: b}})
	}
}

func (b *testBalancer) Close() {}

func (b *testBalancer) ExitIdle() {}

type picker struct {
	err  error
	bal  *testBalancer
	idle bool
}

func (p *picker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	if p.err != nil {
		return balancer.PickResult{}, p.err
	}
	if p.idle {
		p.bal.sc.Connect()
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}
	extraMD, _ := grpcutil.ExtraMetadata(info.Ctx)
	info.Ctx = nil // Do not validate context.
	p.bal.pickInfos = append(p.bal.pickInfos, info)
	p.bal.pickExtraMDs = append(p.bal.pickExtraMDs, extraMD)
	return balancer.PickResult{SubConn: p.bal.sc, Done: func(d balancer.DoneInfo) { p.bal.doneInfo = append(p.bal.doneInfo, d) }}, nil
}

func (s) TestCredsBundleFromBalancer(t *testing.T) {
	balancer.Register(&testBalancer{
		newSubConnOptions: balancer.NewSubConnOptions{
			CredsBundle: &testCredsBundle{},
		},
	})
	te := newTest(t, env{name: "creds-bundle", network: "tcp", balancer: ""})
	te.tapHandle = authHandle
	te.customDialOptions = []grpc.DialOption{
		grpc.WithDefaultServiceConfig(fmt.Sprintf(`{"loadBalancingConfig": [{"%s":{}}]}`, testBalancerName)),
	}
	creds, err := credentials.NewServerTLSFromFile(testdata.Path("x509/server1_cert.pem"), testdata.Path("x509/server1_key.pem"))
	if err != nil {
		t.Fatalf("Failed to generate credentials %v", err)
	}
	te.customServerOptions = []grpc.ServerOption{
		grpc.Creds(creds),
	}
	te.startServer(&testServer{})
	defer te.tearDown()

	cc := te.clientConn()
	tc := testgrpc.NewTestServiceClient(cc)
	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	if _, err := tc.EmptyCall(ctx, &testpb.Empty{}); err != nil {
		t.Fatalf("Test failed. Reason: %v", err)
	}
}

func (s) TestPickExtraMetadata(t *testing.T) {
	for _, e := range listTestEnv() {
		testPickExtraMetadata(t, e)
	}
}

func testPickExtraMetadata(t *testing.T, e env) {
	te := newTest(t, e)
	b := &testBalancer{}
	balancer.Register(b)
	const (
		testUserAgent      = "test-user-agent"
		testSubContentType = "proto"
	)

	te.customDialOptions = []grpc.DialOption{
		grpc.WithDefaultServiceConfig(fmt.Sprintf(`{"loadBalancingConfig": [{"%s":{}}]}`, testBalancerName)),
		grpc.WithUserAgent(testUserAgent),
	}
	te.startServer(&testServer{security: e.security})
	defer te.tearDown()

	// Trigger the extra-metadata-adding code path.
	defer func(old string) { internal.GRPCResolverSchemeExtraMetadata = old }(internal.GRPCResolverSchemeExtraMetadata)
	internal.GRPCResolverSchemeExtraMetadata = "passthrough"

	cc := te.clientConn()
	tc := testgrpc.NewTestServiceClient(cc)

	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	if _, err := tc.EmptyCall(ctx, &testpb.Empty{}, grpc.WaitForReady(true)); err != nil {
		t.Fatalf("TestService/EmptyCall(_, _) = _, %v, want _, %v", err, nil)
	}
	if _, err := tc.EmptyCall(ctx, &testpb.Empty{}, grpc.CallContentSubtype(testSubContentType)); err != nil {
		t.Fatalf("TestService/EmptyCall(_, _) = _, %v, want _, %v", err, nil)
	}

	want := []metadata.MD{
		// First RPC doesn't have sub-content-type.
		{"content-type": []string{"application/grpc"}},
		// Second RPC has sub-content-type "proto".
		{"content-type": []string{"application/grpc+proto"}},
	}
	if diff := cmp.Diff(want, b.pickExtraMDs); diff != "" {
		t.Fatalf("unexpected diff in metadata (-want, +got): %s", diff)
	}
}

func (s) TestDoneInfo(t *testing.T) {
	for _, e := range listTestEnv() {
		testDoneInfo(t, e)
	}
}

func testDoneInfo(t *testing.T, e env) {
	te := newTest(t, e)
	b := &testBalancer{}
	balancer.Register(b)
	te.customDialOptions = []grpc.DialOption{
		grpc.WithDefaultServiceConfig(fmt.Sprintf(`{"loadBalancingConfig": [{"%s":{}}]}`, testBalancerName)),
	}
	te.userAgent = failAppUA
	te.startServer(&testServer{security: e.security})
	defer te.tearDown()

	cc := te.clientConn()
	tc := testgrpc.NewTestServiceClient(cc)

	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	wantErr := detailedError
	if _, err := tc.EmptyCall(ctx, &testpb.Empty{}); !testutils.StatusErrEqual(err, wantErr) {
		t.Fatalf("TestService/EmptyCall(_, _) = _, %v, want _, %v", status.Convert(err).Proto(), status.Convert(wantErr).Proto())
	}
	if _, err := tc.UnaryCall(ctx, &testpb.SimpleRequest{}); err != nil {
		t.Fatalf("TestService.UnaryCall(%v, _, _, _) = _, %v; want _, <nil>", ctx, err)
	}

	if len(b.doneInfo) < 1 || !testutils.StatusErrEqual(b.doneInfo[0].Err, wantErr) {
		t.Fatalf("b.doneInfo = %v; want b.doneInfo[0].Err = %v", b.doneInfo, wantErr)
	}
	if len(b.doneInfo) < 2 || !reflect.DeepEqual(b.doneInfo[1].Trailer, testTrailerMetadata) {
		t.Fatalf("b.doneInfo = %v; want b.doneInfo[1].Trailer = %v", b.doneInfo, testTrailerMetadata)
	}
	if len(b.pickInfos) != len(b.doneInfo) {
		t.Fatalf("Got %d picks, but %d doneInfo, want equal amount", len(b.pickInfos), len(b.doneInfo))
	}
	// To test done() is always called, even if it's returned with a non-Ready
	// SubConn.
	//
	// Stop server and at the same time send RPCs. There are chances that picker
	// is not updated in time, causing a non-Ready SubConn to be returned.
	finished := make(chan struct{})
	go func() {
		for i := 0; i < 20; i++ {
			tc.UnaryCall(ctx, &testpb.SimpleRequest{})
		}
		close(finished)
	}()
	te.srv.Stop()
	<-finished
	if len(b.pickInfos) != len(b.doneInfo) {
		t.Fatalf("Got %d picks, %d doneInfo, want equal amount", len(b.pickInfos), len(b.doneInfo))
	}
}

const loadMDKey = "X-Endpoint-Load-Metrics-Bin"

type testLoadParser struct{}

func (*testLoadParser) Parse(md metadata.MD) any {
	vs := md.Get(loadMDKey)
	if len(vs) == 0 {
		return nil
	}
	return vs[0]
}

func init() {
	balancerload.SetParser(&testLoadParser{})
}

func (s) TestDoneLoads(t *testing.T) {
	testDoneLoads(t)
}

func testDoneLoads(t *testing.T) {
	b := &testBalancer{}
	balancer.Register(b)

	const testLoad = "test-load-,-should-be-orca"

	ss := &stubserver.StubServer{
		EmptyCallF: func(ctx context.Context, _ *testpb.Empty) (*testpb.Empty, error) {
			grpc.SetTrailer(ctx, metadata.Pairs(loadMDKey, testLoad))
			return &testpb.Empty{}, nil
		},
	}
	if err := ss.Start(nil, grpc.WithDefaultServiceConfig(fmt.Sprintf(`{"loadBalancingConfig": [{"%s":{}}]}`, testBalancerName))); err != nil {
		t.Fatalf("error starting testing server: %v", err)
	}
	defer ss.Stop()

	tc := testgrpc.NewTestServiceClient(ss.CC)

	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	if _, err := tc.EmptyCall(ctx, &testpb.Empty{}); err != nil {
		t.Fatalf("TestService/EmptyCall(_, _) = _, %v, want _, %v", err, nil)
	}

	piWant := []balancer.PickInfo{
		{FullMethodName: "/grpc.testing.TestService/EmptyCall"},
	}
	if !reflect.DeepEqual(b.pickInfos, piWant) {
		t.Fatalf("b.pickInfos = %v; want %v", b.pickInfos, piWant)
	}

	if len(b.doneInfo) < 1 {
		t.Fatalf("b.doneInfo = %v, want length 1", b.doneInfo)
	}
	gotLoad, _ := b.doneInfo[0].ServerLoad.(string)
	if gotLoad != testLoad {
		t.Fatalf("b.doneInfo[0].ServerLoad = %v; want = %v", b.doneInfo[0].ServerLoad, testLoad)
	}
}

type aiPicker struct {
	result balancer.PickResult
	err    error
}

func (aip *aiPicker) Pick(_ balancer.PickInfo) (balancer.PickResult, error) {
	return aip.result, aip.err
}

// attrTransportCreds is a transport credential implementation which stores
// Attributes from the ClientHandshakeInfo struct passed in the context locally
// for the test to inspect.
type attrTransportCreds struct {
	credentials.TransportCredentials
	attr *attributes.Attributes
}

func (ac *attrTransportCreds) ClientHandshake(ctx context.Context, _ string, rawConn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	ai := credentials.ClientHandshakeInfoFromContext(ctx)
	ac.attr = ai.Attributes
	return rawConn, nil, nil
}
func (ac *attrTransportCreds) Info() credentials.ProtocolInfo {
	return credentials.ProtocolInfo{}
}
func (ac *attrTransportCreds) Clone() credentials.TransportCredentials {
	return nil
}

// TestAddressAttributesInNewSubConn verifies that the Attributes passed from a
// balancer in the resolver.Address that is passes to NewSubConn reaches all the
// way to the ClientHandshake method of the credentials configured on the parent
// channel.
func (s) TestAddressAttributesInNewSubConn(t *testing.T) {
	const (
		testAttrKey      = "foo"
		testAttrVal      = "bar"
		attrBalancerName = "attribute-balancer"
	)

	// Register a stub balancer which adds attributes to the first address that
	// it receives and then calls NewSubConn on it.
	bf := stub.BalancerFuncs{
		UpdateClientConnState: func(bd *stub.BalancerData, ccs balancer.ClientConnState) error {
			addrs := ccs.ResolverState.Addresses
			if len(addrs) == 0 {
				return nil
			}

			// Only use the first address.
			attr := attributes.New(testAttrKey, testAttrVal)
			addrs[0].Attributes = attr
			var sc balancer.SubConn
			sc, err := bd.ClientConn.NewSubConn([]resolver.Address{addrs[0]}, balancer.NewSubConnOptions{
				StateListener: func(state balancer.SubConnState) {
					bd.ClientConn.UpdateState(balancer.State{ConnectivityState: state.ConnectivityState, Picker: &aiPicker{result: balancer.PickResult{SubConn: sc}, err: state.ConnectionError}})
				},
			})
			if err != nil {
				return err
			}
			sc.Connect()
			return nil
		},
	}
	stub.Register(attrBalancerName, bf)
	t.Logf("Registered balancer %s...", attrBalancerName)

	r := manual.NewBuilderWithScheme("whatever")
	t.Logf("Registered manual resolver with scheme %s...", r.Scheme())

	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	stub := &stubserver.StubServer{
		Listener: lis,
		EmptyCallF: func(_ context.Context, _ *testpb.Empty) (*testpb.Empty, error) {
			return &testpb.Empty{}, nil
		},
		S: grpc.NewServer(),
	}
	stubserver.StartTestService(t, stub)
	defer stub.S.Stop()
	t.Logf("Started gRPC server at %s...", lis.Addr().String())

	creds := &attrTransportCreds{}
	dopts := []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
		grpc.WithResolvers(r),
		grpc.WithDefaultServiceConfig(fmt.Sprintf(`{ "loadBalancingConfig": [{"%v": {}}] }`, attrBalancerName)),
	}
	cc, err := grpc.NewClient(r.Scheme()+":///test.server", dopts...)
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	tc := testgrpc.NewTestServiceClient(cc)
	t.Log("Created a ClientConn...")

	// The first RPC should fail because there's no address.
	ctx, cancel := context.WithTimeout(context.Background(), defaultTestShortTimeout)
	defer cancel()
	if _, err := tc.EmptyCall(ctx, &testpb.Empty{}); err == nil || status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("EmptyCall() = _, %v, want _, DeadlineExceeded", err)
	}
	t.Log("Made an RPC which was expected to fail...")

	state := resolver.State{Addresses: []resolver.Address{{Addr: lis.Addr().String()}}}
	r.UpdateState(state)
	t.Logf("Pushing resolver state update: %v through the manual resolver", state)

	// The second RPC should succeed.
	ctx, cancel = context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	if _, err := tc.EmptyCall(ctx, &testpb.Empty{}); err != nil {
		t.Fatalf("EmptyCall() = _, %v, want _, <nil>", err)
	}
	t.Log("Made an RPC which succeeded...")

	wantAttr := attributes.New(testAttrKey, testAttrVal)
	if gotAttr := creds.attr; !cmp.Equal(gotAttr, wantAttr, cmp.AllowUnexported(attributes.Attributes{})) {
		t.Fatalf("received attributes %v in creds, want %v", gotAttr, wantAttr)
	}
}

// TestMetadataInAddressAttributes verifies that the metadata added to
// address.Attributes will be sent with the RPCs.
func (s) TestMetadataInAddressAttributes(t *testing.T) {
	const (
		testMDKey      = "test-md"
		testMDValue    = "test-md-value"
		mdBalancerName = "metadata-balancer"
	)

	// Register a stub balancer which adds metadata to the first address that it
	// receives and then calls NewSubConn on it.
	bf := stub.BalancerFuncs{
		UpdateClientConnState: func(bd *stub.BalancerData, ccs balancer.ClientConnState) error {
			addrs := ccs.ResolverState.Addresses
			if len(addrs) == 0 {
				return nil
			}
			// Only use the first address.
			var sc balancer.SubConn
			sc, err := bd.ClientConn.NewSubConn([]resolver.Address{
				imetadata.Set(addrs[0], metadata.Pairs(testMDKey, testMDValue)),
			}, balancer.NewSubConnOptions{
				StateListener: func(state balancer.SubConnState) {
					bd.ClientConn.UpdateState(balancer.State{ConnectivityState: state.ConnectivityState, Picker: &aiPicker{result: balancer.PickResult{SubConn: sc}, err: state.ConnectionError}})
				},
			})
			if err != nil {
				return err
			}
			sc.Connect()
			return nil
		},
	}
	stub.Register(mdBalancerName, bf)
	t.Logf("Registered balancer %s...", mdBalancerName)

	testMDChan := make(chan []string, 1)
	ss := &stubserver.StubServer{
		EmptyCallF: func(ctx context.Context, _ *testpb.Empty) (*testpb.Empty, error) {
			md, ok := metadata.FromIncomingContext(ctx)
			if ok {
				select {
				case testMDChan <- md[testMDKey]:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			return &testpb.Empty{}, nil
		},
	}
	if err := ss.Start(nil, grpc.WithDefaultServiceConfig(
		fmt.Sprintf(`{ "loadBalancingConfig": [{"%v": {}}] }`, mdBalancerName),
	)); err != nil {
		t.Fatalf("Error starting endpoint server: %v", err)
	}
	defer ss.Stop()

	// The RPC should succeed with the expected md.
	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	if _, err := ss.Client.EmptyCall(ctx, &testpb.Empty{}); err != nil {
		t.Fatalf("EmptyCall() = _, %v, want _, <nil>", err)
	}
	t.Log("Made an RPC which succeeded...")

	// The server should receive the test metadata.
	md1 := <-testMDChan
	if len(md1) == 0 || md1[0] != testMDValue {
		t.Fatalf("got md: %v, want %v", md1, []string{testMDValue})
	}
}

// TestServersSwap creates two servers and verifies the client switches between
// them when the name resolver reports the first and then the second.
func (s) TestServersSwap(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()

	// Initialize servers
	reg := func(username string) (addr string, cleanup func()) {
		lis, err := net.Listen("tcp", "localhost:0")
		if err != nil {
			t.Fatalf("Error while listening. Err: %v", err)
		}

		stub := &stubserver.StubServer{
			Listener: lis,
			UnaryCallF: func(_ context.Context, _ *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
				return &testpb.SimpleResponse{Username: username}, nil
			},
			S: grpc.NewServer(),
		}
		stubserver.StartTestService(t, stub)
		return lis.Addr().String(), stub.S.Stop
	}
	const one = "1"
	addr1, cleanup := reg(one)
	defer cleanup()
	const two = "2"
	addr2, cleanup := reg(two)
	defer cleanup()

	// Initialize client
	r := manual.NewBuilderWithScheme("whatever")
	r.InitialState(resolver.State{Addresses: []resolver.Address{{Addr: addr1}}})
	cc, err := grpc.NewClient(r.Scheme()+":///", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithResolvers(r))
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer cc.Close()
	client := testgrpc.NewTestServiceClient(cc)

	// Confirm we are connected to the first server
	if res, err := client.UnaryCall(ctx, &testpb.SimpleRequest{}); err != nil || res.Username != one {
		t.Fatalf("UnaryCall(_) = %v, %v; want {Username: %q}, nil", res, err, one)
	}

	// Update resolver to report only the second server
	r.UpdateState(resolver.State{Addresses: []resolver.Address{{Addr: addr2}}})

	// Loop until new RPCs talk to server two.
	for i := 0; i < 2000; i++ {
		if res, err := client.UnaryCall(ctx, &testpb.SimpleRequest{}); err != nil {
			t.Fatalf("UnaryCall(_) = _, %v; want _, nil", err)
		} else if res.Username == two {
			break // pass
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (s) TestWaitForReady(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()

	// Initialize server
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("Error while listening. Err: %v", err)
	}
	const one = "1"
	stub := &stubserver.StubServer{
		Listener: lis,
		UnaryCallF: func(_ context.Context, _ *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
			return &testpb.SimpleResponse{Username: one}, nil
		},
		S: grpc.NewServer(),
	}
	stubserver.StartTestService(t, stub)
	defer stub.S.Stop()

	// Initialize client
	r := manual.NewBuilderWithScheme("whatever")

	cc, err := grpc.NewClient(r.Scheme()+":///", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithResolvers(r))
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer cc.Close()
	cc.Connect()
	client := testgrpc.NewTestServiceClient(cc)

	// Report an error so non-WFR RPCs will give up early.
	r.CC().ReportError(errors.New("fake resolver error"))

	// Ensure the client is not connected to anything and fails non-WFR RPCs.
	if res, err := client.UnaryCall(ctx, &testpb.SimpleRequest{}); status.Code(err) != codes.Unavailable {
		t.Fatalf("UnaryCall(_) = %v, %v; want _, Code()=%v", res, err, codes.Unavailable)
	}

	errChan := make(chan error, 1)
	go func() {
		if res, err := client.UnaryCall(ctx, &testpb.SimpleRequest{}, grpc.WaitForReady(true)); err != nil || res.Username != one {
			errChan <- fmt.Errorf("UnaryCall(_) = %v, %v; want {Username: %q}, nil", res, err, one)
		}
		close(errChan)
	}()

	select {
	case err := <-errChan:
		t.Errorf("unexpected receive from errChan before addresses provided")
		t.Fatal(err.Error())
	case <-time.After(5 * time.Millisecond):
	}

	// Resolve the server.  The WFR RPC should unblock and use it.
	r.UpdateState(resolver.State{Addresses: []resolver.Address{{Addr: lis.Addr().String()}}})

	if err := <-errChan; err != nil {
		t.Fatal(err.Error())
	}
}

// authorityOverrideTransportCreds returns the configured authority value in its
// Info() method.
type authorityOverrideTransportCreds struct {
	credentials.TransportCredentials
	authorityOverride string
}

func (ao *authorityOverrideTransportCreds) ClientHandshake(_ context.Context, _ string, rawConn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return rawConn, nil, nil
}
func (ao *authorityOverrideTransportCreds) Info() credentials.ProtocolInfo {
	return credentials.ProtocolInfo{ServerName: ao.authorityOverride}
}
func (ao *authorityOverrideTransportCreds) Clone() credentials.TransportCredentials {
	return &authorityOverrideTransportCreds{authorityOverride: ao.authorityOverride}
}

// TestAuthorityInBuildOptions tests that the Authority field in
// balancer.BuildOptions is setup correctly from gRPC.
func (s) TestAuthorityInBuildOptions(t *testing.T) {
	const dialTarget = "test.server"

	tests := []struct {
		name          string
		dopts         []grpc.DialOption
		wantAuthority string
	}{
		{
			name:          "authority from dial target",
			dopts:         []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
			wantAuthority: dialTarget,
		},
		{
			name: "authority from dial option",
			dopts: []grpc.DialOption{
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithAuthority("authority-override"),
			},
			wantAuthority: "authority-override",
		},
		{
			name:          "authority from transport creds",
			dopts:         []grpc.DialOption{grpc.WithTransportCredentials(&authorityOverrideTransportCreds{authorityOverride: "authority-override-from-transport-creds"})},
			wantAuthority: "authority-override-from-transport-creds",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			authorityCh := make(chan string, 1)
			bf := stub.BalancerFuncs{
				UpdateClientConnState: func(bd *stub.BalancerData, ccs balancer.ClientConnState) error {
					select {
					case authorityCh <- bd.BuildOptions.Authority:
					default:
					}

					addrs := ccs.ResolverState.Addresses
					if len(addrs) == 0 {
						return nil
					}

					// Only use the first address.
					var sc balancer.SubConn
					sc, err := bd.ClientConn.NewSubConn([]resolver.Address{addrs[0]}, balancer.NewSubConnOptions{
						StateListener: func(state balancer.SubConnState) {
							bd.ClientConn.UpdateState(balancer.State{ConnectivityState: state.ConnectivityState, Picker: &aiPicker{result: balancer.PickResult{SubConn: sc}, err: state.ConnectionError}})
						},
					})
					if err != nil {
						return err
					}
					sc.Connect()
					return nil
				},
			}
			balancerName := "stub-balancer-" + test.name
			stub.Register(balancerName, bf)
			t.Logf("Registered balancer %s...", balancerName)

			lis, err := testutils.LocalTCPListener()
			if err != nil {
				t.Fatal(err)
			}

			stub := &stubserver.StubServer{
				Listener: lis,
				EmptyCallF: func(_ context.Context, _ *testpb.Empty) (*testpb.Empty, error) {
					return &testpb.Empty{}, nil
				},
				S: grpc.NewServer(),
			}
			stubserver.StartTestService(t, stub)
			defer stub.S.Stop()
			t.Logf("Started gRPC server at %s...", lis.Addr().String())

			r := manual.NewBuilderWithScheme("whatever")
			t.Logf("Registered manual resolver with scheme %s...", r.Scheme())
			r.InitialState(resolver.State{Addresses: []resolver.Address{{Addr: lis.Addr().String()}}})

			dopts := append([]grpc.DialOption{
				grpc.WithResolvers(r),
				grpc.WithDefaultServiceConfig(fmt.Sprintf(`{ "loadBalancingConfig": [{"%v": {}}] }`, balancerName)),
			}, test.dopts...)
			cc, err := grpc.NewClient(r.Scheme()+":///"+dialTarget, dopts...)
			if err != nil {
				t.Fatal(err)
			}
			defer cc.Close()
			tc := testgrpc.NewTestServiceClient(cc)
			t.Log("Created a ClientConn...")

			ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
			defer cancel()
			if _, err := tc.EmptyCall(ctx, &testpb.Empty{}); err != nil {
				t.Fatalf("EmptyCall() = _, %v, want _, <nil>", err)
			}
			t.Log("Made an RPC which succeeded...")

			select {
			case <-ctx.Done():
				t.Fatal("timeout when waiting for Authority in balancer.BuildOptions")
			case gotAuthority := <-authorityCh:
				if gotAuthority != test.wantAuthority {
					t.Fatalf("Authority in balancer.BuildOptions is %s, want %s", gotAuthority, test.wantAuthority)
				}
			}
		})
	}
}

// testCCWrapper wraps a balancer.ClientConn and intercepts UpdateState and
// returns a custom picker which injects arbitrary metadata on a per-call basis.
type testCCWrapper struct {
	balancer.ClientConn
}

func (t *testCCWrapper) UpdateState(state balancer.State) {
	state.Picker = &wrappedPicker{p: state.Picker}
	t.ClientConn.UpdateState(state)
}

const (
	metadataHeaderInjectedByBalancer    = "metadata-header-injected-by-balancer"
	metadataHeaderInjectedByApplication = "metadata-header-injected-by-application"
	metadataValueInjectedByBalancer     = "metadata-value-injected-by-balancer"
	metadataValueInjectedByApplication  = "metadata-value-injected-by-application"
)

// wrappedPicker wraps the picker returned by the pick_first
type wrappedPicker struct {
	p balancer.Picker
}

func (wp *wrappedPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	res, err := wp.p.Pick(info)
	if err != nil {
		return balancer.PickResult{}, err
	}

	if res.Metadata == nil {
		res.Metadata = metadata.Pairs(metadataHeaderInjectedByBalancer, metadataValueInjectedByBalancer)
	} else {
		res.Metadata.Append(metadataHeaderInjectedByBalancer, metadataValueInjectedByBalancer)
	}
	return res, nil
}

// TestMetadataInPickResult tests the scenario where an LB policy inject
// arbitrary metadata on a per-call basis and verifies that the injected
// metadata makes it all the way to the server RPC handler.
func (s) TestMetadataInPickResult(t *testing.T) {
	t.Log("Starting test backend...")
	mdChan := make(chan metadata.MD, 1)
	ss := &stubserver.StubServer{
		EmptyCallF: func(ctx context.Context, _ *testpb.Empty) (*testpb.Empty, error) {
			md, _ := metadata.FromIncomingContext(ctx)
			select {
			case mdChan <- md:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			return &testpb.Empty{}, nil
		},
	}
	if err := ss.StartServer(); err != nil {
		t.Fatalf("Starting test backend: %v", err)
	}
	defer ss.Stop()
	t.Logf("Started test backend at %q", ss.Address)

	// Register a test balancer that contains a pick_first balancer and forwards
	// all calls from the ClientConn to it. For state updates from the
	// pick_first balancer, it creates a custom picker which injects arbitrary
	// metadata on a per-call basis.
	stub.Register(t.Name(), stub.BalancerFuncs{
		Init: func(bd *stub.BalancerData) {
			cc := &testCCWrapper{ClientConn: bd.ClientConn}
			bd.Data = balancer.Get(pickfirst.Name).Build(cc, bd.BuildOptions)
		},
		Close: func(bd *stub.BalancerData) {
			bd.Data.(balancer.Balancer).Close()
		},
		UpdateClientConnState: func(bd *stub.BalancerData, ccs balancer.ClientConnState) error {
			bal := bd.Data.(balancer.Balancer)
			return bal.UpdateClientConnState(ccs)
		},
	})

	t.Log("Creating ClientConn to test backend...")
	r := manual.NewBuilderWithScheme("whatever")
	r.InitialState(resolver.State{Addresses: []resolver.Address{{Addr: ss.Address}}})
	dopts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithResolvers(r),
		grpc.WithDefaultServiceConfig(fmt.Sprintf(`{"loadBalancingConfig": [{"%s":{}}]}`, t.Name())),
	}
	cc, err := grpc.NewClient(r.Scheme()+":///test.server", dopts...)
	if err != nil {
		t.Fatalf("grpc.NewClient(): %v", err)
	}
	defer cc.Close()
	tc := testgrpc.NewTestServiceClient(cc)

	t.Log("Making EmptyCall() RPC with custom metadata...")
	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	md := metadata.Pairs(metadataHeaderInjectedByApplication, metadataValueInjectedByApplication)
	ctx = metadata.NewOutgoingContext(ctx, md)
	if _, err := tc.EmptyCall(ctx, &testpb.Empty{}); err != nil {
		t.Fatalf("EmptyCall() RPC: %v", err)
	}
	t.Log("EmptyCall() RPC succeeded")

	t.Log("Waiting for custom metadata to be received at the test backend...")
	var gotMD metadata.MD
	select {
	case gotMD = <-mdChan:
	case <-ctx.Done():
		t.Fatalf("Timed out waiting for custom metadata to be received at the test backend")
	}

	t.Log("Verifying custom metadata added by the client application is received at the test backend...")
	wantMDVal := []string{metadataValueInjectedByApplication}
	gotMDVal := gotMD.Get(metadataHeaderInjectedByApplication)
	if !cmp.Equal(gotMDVal, wantMDVal) {
		t.Fatalf("Mismatch in custom metadata received at test backend, got: %v, want %v", gotMDVal, wantMDVal)
	}

	t.Log("Verifying custom metadata added by the LB policy is received at the test backend...")
	wantMDVal = []string{metadataValueInjectedByBalancer}
	gotMDVal = gotMD.Get(metadataHeaderInjectedByBalancer)
	if !cmp.Equal(gotMDVal, wantMDVal) {
		t.Fatalf("Mismatch in custom metadata received at test backend, got: %v, want %v", gotMDVal, wantMDVal)
	}
}

// TestSubConnShutdown confirms that the Shutdown method on subconns and
// RemoveSubConn method on ClientConn properly initiates subconn shutdown.
func (s) TestSubConnShutdown(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()

	testCases := []struct {
		name     string
		shutdown func(cc balancer.ClientConn, sc balancer.SubConn)
	}{{
		name: "ClientConn.RemoveSubConn",
		shutdown: func(cc balancer.ClientConn, sc balancer.SubConn) {
			cc.RemoveSubConn(sc)
		},
	}, {
		name: "SubConn.Shutdown",
		shutdown: func(_ balancer.ClientConn, sc balancer.SubConn) {
			sc.Shutdown()
		},
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gotShutdown := grpcsync.NewEvent()

			bf := stub.BalancerFuncs{
				UpdateClientConnState: func(bd *stub.BalancerData, ccs balancer.ClientConnState) error {
					var sc balancer.SubConn
					opts := balancer.NewSubConnOptions{
						StateListener: func(scs balancer.SubConnState) {
							switch scs.ConnectivityState {
							case connectivity.Connecting:
								// Ignored.
							case connectivity.Ready:
								tc.shutdown(bd.ClientConn, sc)
							case connectivity.Shutdown:
								gotShutdown.Fire()
							default:
								t.Errorf("got unexpected state %q in listener", scs.ConnectivityState)
							}
						},
					}
					sc, err := bd.ClientConn.NewSubConn(ccs.ResolverState.Addresses, opts)
					if err != nil {
						return err
					}
					sc.Connect()
					// Report the state as READY to unblock ss.Start(), which waits for ready.
					bd.ClientConn.UpdateState(balancer.State{ConnectivityState: connectivity.Ready})
					return nil
				},
			}

			testBalName := "shutdown-test-balancer-" + tc.name
			stub.Register(testBalName, bf)
			t.Logf("Registered balancer %s...", testBalName)

			ss := &stubserver.StubServer{}
			if err := ss.Start(nil, grpc.WithDefaultServiceConfig(
				fmt.Sprintf(`{ "loadBalancingConfig": [{"%v": {}}] }`, testBalName),
			)); err != nil {
				t.Fatalf("Error starting endpoint server: %v", err)
			}
			defer ss.Stop()

			select {
			case <-gotShutdown.Done():
				// Success
			case <-ctx.Done():
				t.Fatalf("Timed out waiting for gotShutdown to be fired.")
			}
		})
	}
}

type subConnStoringCCWrapper struct {
	balancer.ClientConn
	stateListener func(balancer.SubConnState)
	scChan        chan balancer.SubConn
}

func (ccw *subConnStoringCCWrapper) NewSubConn(addrs []resolver.Address, opts balancer.NewSubConnOptions) (balancer.SubConn, error) {
	if ccw.stateListener != nil {
		origListener := opts.StateListener
		opts.StateListener = func(scs balancer.SubConnState) {
			ccw.stateListener(scs)
			origListener(scs)
		}
	}
	sc, err := ccw.ClientConn.NewSubConn(addrs, opts)
	ccw.scChan <- sc
	return sc, err
}

// Test calls RegisterHealthListener on a SubConn to verify that expected health
// updates are sent only to the most recently registered listener.
func (s) TestSubConn_RegisterHealthListener(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	scChan := make(chan balancer.SubConn, 1)
	bf := stub.BalancerFuncs{
		Init: func(bd *stub.BalancerData) {
			cc := bd.ClientConn
			ccw := &subConnStoringCCWrapper{
				ClientConn: cc,
				scChan:     scChan,
			}
			bd.Data = balancer.Get(pickfirst.Name).Build(ccw, bd.BuildOptions)
		},
		Close: func(bd *stub.BalancerData) {
			bd.Data.(balancer.Balancer).Close()
		},
		UpdateClientConnState: func(bd *stub.BalancerData, ccs balancer.ClientConnState) error {
			return bd.Data.(balancer.Balancer).UpdateClientConnState(ccs)
		},
		ExitIdle: func(bd *stub.BalancerData) {
			bd.Data.(balancer.Balancer).ExitIdle()
		},
	}

	stub.Register(t.Name(), bf)
	svcCfg := fmt.Sprintf(`{ "loadBalancingConfig": [{%q: {}}] }`, t.Name())
	backend := stubserver.StartTestService(t, nil)
	defer backend.Stop()
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(svcCfg),
	}
	cc, err := grpc.NewClient(backend.Address, opts...)
	if err != nil {
		t.Fatalf("grpc.NewClient(%q) failed: %v", backend.Address, err)

	}
	defer cc.Close()

	cc.Connect()

	var sc balancer.SubConn
	select {
	case sc = <-scChan:
	case <-ctx.Done():
		t.Fatal("Context timed out waiting for SubConn creation")
	}
	healthUpdateChan := make(chan balancer.SubConnState, 1)

	// Register listener while Ready and verify it gets a health update.
	testutils.AwaitState(ctx, t, cc, connectivity.Ready)
	for i := 0; i < 2; i++ {
		sc.RegisterHealthListener(func(scs balancer.SubConnState) {
			healthUpdateChan <- scs
		})
		select {
		case scs := <-healthUpdateChan:
			if scs.ConnectivityState != connectivity.Ready {
				t.Fatalf("Received health update = %v, want = %v", scs.ConnectivityState, connectivity.Ready)
			}
		case <-ctx.Done():
			t.Fatalf("Context timed out waiting for health update")
		}

		// No further updates are expected.
		select {
		case scs := <-healthUpdateChan:
			t.Fatalf("Received unexpected health update while channel is in state %v: %v", cc.GetState(), scs)
		case <-time.After(defaultTestShortTimeout):
		}
	}

	// Make the SubConn enter IDLE and verify that health updates are recevied
	// on registering a new listener.
	backend.S.Stop()
	backend.S = nil
	testutils.AwaitState(ctx, t, cc, connectivity.Idle)
	if err := backend.StartServer(); err != nil {
		t.Fatalf("Error while restarting the backend server: %v", err)
	}
	cc.Connect()
	testutils.AwaitState(ctx, t, cc, connectivity.Ready)
	sc.RegisterHealthListener(func(scs balancer.SubConnState) {
		healthUpdateChan <- scs
	})
	select {
	case scs := <-healthUpdateChan:
		if scs.ConnectivityState != connectivity.Ready {
			t.Fatalf("Received health update = %v, want = %v", scs.ConnectivityState, connectivity.Ready)
		}
	case <-ctx.Done():
		t.Fatalf("Context timed out waiting for health update")
	}
}

// Test calls RegisterHealthListener on a SubConn twice while handling the
// connectivity update. The test verifies that only the latest listener
// receives the health update.
func (s) TestSubConn_RegisterHealthListener_RegisterTwice(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	scChan := make(chan balancer.SubConn, 1)
	readyUpdateResumeCh := make(chan struct{})
	readyUpdateReceivedCh := make(chan struct{})
	bf := stub.BalancerFuncs{
		Init: func(bd *stub.BalancerData) {
			cc := bd.ClientConn
			ccw := &subConnStoringCCWrapper{
				ClientConn: cc,
				scChan:     scChan,
				stateListener: func(scs balancer.SubConnState) {
					if scs.ConnectivityState != connectivity.Ready {
						return
					}
					close(readyUpdateReceivedCh)
					select {
					case <-readyUpdateResumeCh:
					case <-ctx.Done():
						t.Error("Context timed out waiting for update on ready channel")
					}
				},
			}
			bd.Data = balancer.Get(pickfirst.Name).Build(ccw, bd.BuildOptions)
		},
		Close: func(bd *stub.BalancerData) {
			bd.Data.(balancer.Balancer).Close()
		},
		UpdateClientConnState: func(bd *stub.BalancerData, ccs balancer.ClientConnState) error {
			return bd.Data.(balancer.Balancer).UpdateClientConnState(ccs)
		},
	}

	stub.Register(t.Name(), bf)
	svcCfg := fmt.Sprintf(`{ "loadBalancingConfig": [{%q: {}}] }`, t.Name())
	backend := stubserver.StartTestService(t, nil)
	defer backend.Stop()
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(svcCfg),
	}
	cc, err := grpc.NewClient(backend.Address, opts...)
	if err != nil {
		t.Fatalf("grpc.NewClient(%q) failed: %v", backend.Address, err)

	}
	defer cc.Close()

	cc.Connect()

	var sc balancer.SubConn
	select {
	case sc = <-scChan:
	case <-ctx.Done():
		t.Fatal("Context timed out waiting for SubConn creation")
	}

	// Wait for the SubConn to enter READY.
	select {
	case <-readyUpdateReceivedCh:
	case <-ctx.Done():
		t.Fatalf("Context timed out waiting for SubConn to enter READY")
	}

	healthChan1 := make(chan balancer.SubConnState, 1)
	healthChan2 := make(chan balancer.SubConnState, 1)

	sc.RegisterHealthListener(func(scs balancer.SubConnState) {
		healthChan1 <- scs
	})
	sc.RegisterHealthListener(func(scs balancer.SubConnState) {
		healthChan2 <- scs
	})
	close(readyUpdateResumeCh)

	select {
	case scs := <-healthChan2:
		if scs.ConnectivityState != connectivity.Ready {
			t.Fatalf("Received health update = %v, want = %v", scs.ConnectivityState, connectivity.Ready)
		}
	case <-ctx.Done():
		t.Fatalf("Context timed out waiting for health update")
	}

	// No updates should be received on the first listener.
	select {
	case scs := <-healthChan1:
		t.Fatalf("Received unexpected health update on first listener: %v", scs)
	case <-time.After(defaultTestShortTimeout):
	}
}

// Test calls RegisterHealthListener on a SubConn with a nil listener and
// verifies that the listener registered before the nil listener doesn't receive
// any further updates.
func (s) TestSubConn_RegisterHealthListener_NilListener(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	scChan := make(chan balancer.SubConn, 1)
	readyUpdateResumeCh := make(chan struct{})
	readyUpdateReceivedCh := make(chan struct{})
	bf := stub.BalancerFuncs{
		Init: func(bd *stub.BalancerData) {
			cc := bd.ClientConn
			ccw := &subConnStoringCCWrapper{
				ClientConn: cc,
				scChan:     scChan,
				stateListener: func(scs balancer.SubConnState) {
					if scs.ConnectivityState != connectivity.Ready {
						return
					}
					close(readyUpdateReceivedCh)
					select {
					case <-readyUpdateResumeCh:
					case <-ctx.Done():
						t.Error("Context timed out waiting for update on ready channel")
					}
				},
			}
			bd.Data = balancer.Get(pickfirst.Name).Build(ccw, bd.BuildOptions)
		},
		Close: func(bd *stub.BalancerData) {
			bd.Data.(balancer.Balancer).Close()
		},
		UpdateClientConnState: func(bd *stub.BalancerData, ccs balancer.ClientConnState) error {
			return bd.Data.(balancer.Balancer).UpdateClientConnState(ccs)
		},
	}

	stub.Register(t.Name(), bf)
	svcCfg := fmt.Sprintf(`{ "loadBalancingConfig": [{%q: {}}] }`, t.Name())
	backend := stubserver.StartTestService(t, nil)
	defer backend.Stop()
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(svcCfg),
	}
	cc, err := grpc.NewClient(backend.Address, opts...)
	if err != nil {
		t.Fatalf("grpc.NewClient(%q) failed: %v", backend.Address, err)

	}
	defer cc.Close()

	cc.Connect()

	var sc balancer.SubConn
	select {
	case sc = <-scChan:
	case <-ctx.Done():
		t.Fatal("Context timed out waiting for SubConn creation")
	}

	// Wait for the SubConn to enter READY.
	select {
	case <-readyUpdateReceivedCh:
	case <-ctx.Done():
		t.Fatalf("Context timed out waiting for SubConn to enter READY")
	}

	healthChan := make(chan balancer.SubConnState, 1)

	sc.RegisterHealthListener(func(scs balancer.SubConnState) {
		healthChan <- scs
	})

	// Registering a nil listener should invalidate the previously registered
	// listener.
	sc.RegisterHealthListener(nil)
	close(readyUpdateResumeCh)

	// No updates should be received on the listener.
	select {
	case scs := <-healthChan:
		t.Fatalf("Received unexpected health update on the listener: %v", scs)
	case <-time.After(defaultTestShortTimeout):
	}
}
