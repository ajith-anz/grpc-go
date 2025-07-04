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

package clusterimpl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/ajith-anz/grpc-go/balancer"
	"github.com/ajith-anz/grpc-go/balancer/base"
	"github.com/ajith-anz/grpc-go/balancer/roundrobin"
	"github.com/ajith-anz/grpc-go/connectivity"
	"github.com/ajith-anz/grpc-go/internal"
	"github.com/ajith-anz/grpc-go/internal/balancer/stub"
	"github.com/ajith-anz/grpc-go/internal/grpctest"
	internalserviceconfig "github.com/ajith-anz/grpc-go/internal/serviceconfig"
	"github.com/ajith-anz/grpc-go/internal/testutils"
	"github.com/ajith-anz/grpc-go/internal/xds"
	"github.com/ajith-anz/grpc-go/internal/xds/bootstrap"
	"github.com/ajith-anz/grpc-go/resolver"
	"github.com/ajith-anz/grpc-go/serviceconfig"
	xdsinternal "github.com/ajith-anz/grpc-go/xds/internal"
	"github.com/ajith-anz/grpc-go/xds/internal/testutils/fakeclient"
	"github.com/ajith-anz/grpc-go/xds/internal/xdsclient"
	"github.com/ajith-anz/grpc-go/xds/internal/xdsclient/load"

	v3orcapb "github.com/cncf/xds/go/xds/data/orca/v3"
)

const (
	defaultTestTimeout      = 5 * time.Second
	defaultShortTestTimeout = 100 * time.Microsecond

	testClusterName = "test-cluster"
	testServiceName = "test-eds-service"

	testNamedMetricsKey1 = "test-named1"
	testNamedMetricsKey2 = "test-named2"
)

var (
	testBackendEndpoints = []resolver.Endpoint{{Addresses: []resolver.Address{{Addr: "1.1.1.1:1"}}}}
	cmpOpts              = cmp.Options{
		cmpopts.EquateEmpty(),
		cmpopts.IgnoreFields(load.Data{}, "ReportInterval"),
	}
	toleranceCmpOpt = cmpopts.EquateApprox(0, 1e-5)
)

type s struct {
	grpctest.Tester
}

func Test(t *testing.T) {
	grpctest.RunSubTests(t, s{})
}

func init() {
	NewRandomWRR = testutils.NewTestWRR
}

// TestDropByCategory verifies that the balancer correctly drops the picks, and
// that the drops are reported.
func (s) TestDropByCategory(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()

	defer xdsclient.ClearCounterForTesting(testClusterName, testServiceName)
	xdsC := fakeclient.NewClient()

	builder := balancer.Get(Name)
	cc := testutils.NewBalancerClientConn(t)
	b := builder.Build(cc, balancer.BuildOptions{})
	defer b.Close()

	const (
		dropReason      = "test-dropping-category"
		dropNumerator   = 1
		dropDenominator = 2
	)
	testLRSServerConfig, err := bootstrap.ServerConfigForTesting(bootstrap.ServerConfigTestingOptions{
		URI:          "trafficdirector.googleapis.com:443",
		ChannelCreds: []bootstrap.ChannelCreds{{Type: "google_default"}},
	})
	if err != nil {
		t.Fatalf("Failed to create LRS server config for testing: %v", err)
	}
	if err := b.UpdateClientConnState(balancer.ClientConnState{
		ResolverState: xdsclient.SetClient(resolver.State{Endpoints: testBackendEndpoints}, xdsC),
		BalancerConfig: &LBConfig{
			Cluster:             testClusterName,
			EDSServiceName:      testServiceName,
			LoadReportingServer: testLRSServerConfig,
			DropCategories: []DropConfig{{
				Category:           dropReason,
				RequestsPerMillion: million * dropNumerator / dropDenominator,
			}},
			ChildPolicy: &internalserviceconfig.BalancerConfig{
				Name: roundrobin.Name,
			},
		},
	}); err != nil {
		t.Fatalf("unexpected error from UpdateClientConnState: %v", err)
	}

	got, err := xdsC.WaitForReportLoad(ctx)
	if err != nil {
		t.Fatalf("xdsClient.ReportLoad failed with error: %v", err)
	}
	if got.Server != testLRSServerConfig {
		t.Fatalf("xdsClient.ReportLoad called with {%q}: want {%q}", got.Server, testLRSServerConfig)
	}

	sc1 := <-cc.NewSubConnCh
	sc1.UpdateState(balancer.SubConnState{ConnectivityState: connectivity.Connecting})
	// This should get the connecting picker.
	if err := cc.WaitForPickerWithErr(ctx, balancer.ErrNoSubConnAvailable); err != nil {
		t.Fatal(err.Error())
	}

	sc1.UpdateState(balancer.SubConnState{ConnectivityState: connectivity.Ready})
	// Test pick with one backend.

	const rpcCount = 24
	if err := cc.WaitForPicker(ctx, func(p balancer.Picker) error {
		for i := 0; i < rpcCount; i++ {
			gotSCSt, err := p.Pick(balancer.PickInfo{})
			// Even RPCs are dropped.
			if i%2 == 0 {
				if err == nil || !strings.Contains(err.Error(), "dropped") {
					return fmt.Errorf("pick.Pick, got %v, %v, want error RPC dropped", gotSCSt, err)
				}
				continue
			}
			if err != nil || gotSCSt.SubConn != sc1 {
				return fmt.Errorf("picker.Pick, got %v, %v, want SubConn=%v", gotSCSt, err, sc1)
			}
			if gotSCSt.Done == nil {
				continue
			}
			// Fail 1/4th of the requests that are not dropped.
			if i%8 == 1 {
				gotSCSt.Done(balancer.DoneInfo{Err: fmt.Errorf("test error")})
			} else {
				gotSCSt.Done(balancer.DoneInfo{})
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err.Error())
	}

	// Dump load data from the store and compare with expected counts.
	loadStore := xdsC.LoadStore()
	if loadStore == nil {
		t.Fatal("loadStore is nil in xdsClient")
	}
	const dropCount = rpcCount * dropNumerator / dropDenominator
	wantStatsData0 := []*load.Data{{
		Cluster:    testClusterName,
		Service:    testServiceName,
		TotalDrops: dropCount,
		Drops:      map[string]uint64{dropReason: dropCount},
		LocalityStats: map[string]load.LocalityData{
			xdsinternal.LocalityID{}.ToString(): {RequestStats: load.RequestData{
				Succeeded: (rpcCount - dropCount) * 3 / 4,
				Errored:   (rpcCount - dropCount) / 4,
				Issued:    rpcCount - dropCount,
			}},
		},
	}}

	gotStatsData0 := loadStore.Stats([]string{testClusterName})
	if diff := cmp.Diff(gotStatsData0, wantStatsData0, cmpOpts); diff != "" {
		t.Fatalf("got unexpected reports, diff (-got, +want): %v", diff)
	}

	// Send an update with new drop configs.
	const (
		dropReason2      = "test-dropping-category-2"
		dropNumerator2   = 1
		dropDenominator2 = 4
	)
	if err := b.UpdateClientConnState(balancer.ClientConnState{
		ResolverState: xdsclient.SetClient(resolver.State{Endpoints: testBackendEndpoints}, xdsC),
		BalancerConfig: &LBConfig{
			Cluster:             testClusterName,
			EDSServiceName:      testServiceName,
			LoadReportingServer: testLRSServerConfig,
			DropCategories: []DropConfig{{
				Category:           dropReason2,
				RequestsPerMillion: million * dropNumerator2 / dropDenominator2,
			}},
			ChildPolicy: &internalserviceconfig.BalancerConfig{
				Name: roundrobin.Name,
			},
		},
	}); err != nil {
		t.Fatalf("unexpected error from UpdateClientConnState: %v", err)
	}

	if err := cc.WaitForPicker(ctx, func(p balancer.Picker) error {
		for i := 0; i < rpcCount; i++ {
			gotSCSt, err := p.Pick(balancer.PickInfo{})
			// Even RPCs are dropped.
			if i%4 == 0 {
				if err == nil || !strings.Contains(err.Error(), "dropped") {
					return fmt.Errorf("pick.Pick, got %v, %v, want error RPC dropped", gotSCSt, err)
				}
				continue
			}
			if err != nil || gotSCSt.SubConn != sc1 {
				return fmt.Errorf("picker.Pick, got %v, %v, want SubConn=%v", gotSCSt, err, sc1)
			}
			if gotSCSt.Done != nil {
				gotSCSt.Done(balancer.DoneInfo{})
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err.Error())
	}

	const dropCount2 = rpcCount * dropNumerator2 / dropDenominator2
	wantStatsData1 := []*load.Data{{
		Cluster:    testClusterName,
		Service:    testServiceName,
		TotalDrops: dropCount2,
		Drops:      map[string]uint64{dropReason2: dropCount2},
		LocalityStats: map[string]load.LocalityData{
			xdsinternal.LocalityID{}.ToString(): {RequestStats: load.RequestData{
				Succeeded: rpcCount - dropCount2,
				Issued:    rpcCount - dropCount2,
			}},
		},
	}}

	gotStatsData1 := loadStore.Stats([]string{testClusterName})
	if diff := cmp.Diff(gotStatsData1, wantStatsData1, cmpOpts); diff != "" {
		t.Fatalf("got unexpected reports, diff (-got, +want): %v", diff)
	}
}

// TestDropCircuitBreaking verifies that the balancer correctly drops the picks
// due to circuit breaking, and that the drops are reported.
func (s) TestDropCircuitBreaking(t *testing.T) {
	defer xdsclient.ClearCounterForTesting(testClusterName, testServiceName)
	xdsC := fakeclient.NewClient()

	builder := balancer.Get(Name)
	cc := testutils.NewBalancerClientConn(t)
	b := builder.Build(cc, balancer.BuildOptions{})
	defer b.Close()

	var maxRequest uint32 = 50
	testLRSServerConfig, err := bootstrap.ServerConfigForTesting(bootstrap.ServerConfigTestingOptions{
		URI:          "trafficdirector.googleapis.com:443",
		ChannelCreds: []bootstrap.ChannelCreds{{Type: "google_default"}},
	})
	if err != nil {
		t.Fatalf("Failed to create LRS server config for testing: %v", err)
	}
	if err := b.UpdateClientConnState(balancer.ClientConnState{
		ResolverState: xdsclient.SetClient(resolver.State{Endpoints: testBackendEndpoints}, xdsC),
		BalancerConfig: &LBConfig{
			Cluster:               testClusterName,
			EDSServiceName:        testServiceName,
			LoadReportingServer:   testLRSServerConfig,
			MaxConcurrentRequests: &maxRequest,
			ChildPolicy: &internalserviceconfig.BalancerConfig{
				Name: roundrobin.Name,
			},
		},
	}); err != nil {
		t.Fatalf("unexpected error from UpdateClientConnState: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()

	got, err := xdsC.WaitForReportLoad(ctx)
	if err != nil {
		t.Fatalf("xdsClient.ReportLoad failed with error: %v", err)
	}
	if got.Server != testLRSServerConfig {
		t.Fatalf("xdsClient.ReportLoad called with {%q}: want {%q}", got.Server, testLRSServerConfig)
	}

	sc1 := <-cc.NewSubConnCh
	sc1.UpdateState(balancer.SubConnState{ConnectivityState: connectivity.Connecting})
	// This should get the connecting picker.
	if err := cc.WaitForPickerWithErr(ctx, balancer.ErrNoSubConnAvailable); err != nil {
		t.Fatal(err.Error())
	}

	sc1.UpdateState(balancer.SubConnState{ConnectivityState: connectivity.Ready})
	// Test pick with one backend.
	const rpcCount = 100
	if err := cc.WaitForPicker(ctx, func(p balancer.Picker) error {
		dones := []func(){}
		for i := 0; i < rpcCount; i++ {
			gotSCSt, err := p.Pick(balancer.PickInfo{})
			if i < 50 && err != nil {
				return fmt.Errorf("The first 50%% picks should be non-drops, got error %v", err)
			} else if i > 50 && err == nil {
				return fmt.Errorf("The second 50%% picks should be drops, got error <nil>")
			}
			dones = append(dones, func() {
				if gotSCSt.Done != nil {
					gotSCSt.Done(balancer.DoneInfo{})
				}
			})
		}
		for _, done := range dones {
			done()
		}

		dones = []func(){}
		// Pick without drops.
		for i := 0; i < 50; i++ {
			gotSCSt, err := p.Pick(balancer.PickInfo{})
			if err != nil {
				t.Errorf("The third 50%% picks should be non-drops, got error %v", err)
			}
			dones = append(dones, func() {
				if gotSCSt.Done != nil {
					// Fail these requests to test error counts in the load
					// report.
					gotSCSt.Done(balancer.DoneInfo{Err: fmt.Errorf("test error")})
				}
			})
		}
		for _, done := range dones {
			done()
		}

		return nil
	}); err != nil {
		t.Fatal(err.Error())
	}

	// Dump load data from the store and compare with expected counts.
	loadStore := xdsC.LoadStore()
	if loadStore == nil {
		t.Fatal("loadStore is nil in xdsClient")
	}

	wantStatsData0 := []*load.Data{{
		Cluster:    testClusterName,
		Service:    testServiceName,
		TotalDrops: uint64(maxRequest),
		LocalityStats: map[string]load.LocalityData{
			xdsinternal.LocalityID{}.ToString(): {RequestStats: load.RequestData{
				Succeeded: uint64(rpcCount - maxRequest),
				Errored:   50,
				Issued:    uint64(rpcCount - maxRequest + 50),
			}},
		},
	}}

	gotStatsData0 := loadStore.Stats([]string{testClusterName})
	if diff := cmp.Diff(gotStatsData0, wantStatsData0, cmpOpts); diff != "" {
		t.Fatalf("got unexpected drop reports, diff (-got, +want): %v", diff)
	}
}

// TestPickerUpdateAfterClose covers the case where a child policy sends a
// picker update after the cluster_impl policy is closed. Because picker updates
// are handled in the run() goroutine, which exits before Close() returns, we
// expect the above picker update to be dropped.
func (s) TestPickerUpdateAfterClose(t *testing.T) {
	defer xdsclient.ClearCounterForTesting(testClusterName, testServiceName)
	xdsC := fakeclient.NewClient()

	builder := balancer.Get(Name)
	cc := testutils.NewBalancerClientConn(t)
	b := builder.Build(cc, balancer.BuildOptions{})

	// Create a stub balancer which waits for the cluster_impl policy to be
	// closed before sending a picker update (upon receipt of a subConn state
	// change).
	closeCh := make(chan struct{})
	const childPolicyName = "stubBalancer-TestPickerUpdateAfterClose"
	stub.Register(childPolicyName, stub.BalancerFuncs{
		UpdateClientConnState: func(bd *stub.BalancerData, ccs balancer.ClientConnState) error {
			// Create a subConn which will be used later on to test the race
			// between StateListener() and Close().
			sc, err := bd.ClientConn.NewSubConn(ccs.ResolverState.Addresses, balancer.NewSubConnOptions{
				StateListener: func(balancer.SubConnState) {
					go func() {
						// Wait for Close() to be called on the parent policy before
						// sending the picker update.
						<-closeCh
						bd.ClientConn.UpdateState(balancer.State{
							Picker: base.NewErrPicker(errors.New("dummy error picker")),
						})
					}()
				},
			})
			if err != nil {
				return err
			}
			sc.Connect()
			return nil
		},
	})

	var maxRequest uint32 = 50
	if err := b.UpdateClientConnState(balancer.ClientConnState{
		ResolverState: xdsclient.SetClient(resolver.State{Endpoints: testBackendEndpoints}, xdsC),
		BalancerConfig: &LBConfig{
			Cluster:               testClusterName,
			EDSServiceName:        testServiceName,
			MaxConcurrentRequests: &maxRequest,
			ChildPolicy: &internalserviceconfig.BalancerConfig{
				Name: childPolicyName,
			},
		},
	}); err != nil {
		b.Close()
		t.Fatalf("unexpected error from UpdateClientConnState: %v", err)
	}

	// Send a subConn state change to trigger a picker update. The stub balancer
	// that we use as the child policy will not send a picker update until the
	// parent policy is closed.
	sc1 := <-cc.NewSubConnCh
	sc1.UpdateState(balancer.SubConnState{ConnectivityState: connectivity.Connecting})
	b.Close()
	close(closeCh)

	select {
	case <-cc.NewPickerCh:
		t.Fatalf("unexpected picker update after balancer is closed")
	case <-time.After(defaultShortTestTimeout):
	}
}

// TestClusterNameInAddressAttributes covers the case that cluster name is
// attached to the subconn address attributes.
func (s) TestClusterNameInAddressAttributes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()

	defer xdsclient.ClearCounterForTesting(testClusterName, testServiceName)
	xdsC := fakeclient.NewClient()

	builder := balancer.Get(Name)
	cc := testutils.NewBalancerClientConn(t)
	b := builder.Build(cc, balancer.BuildOptions{})
	defer b.Close()

	if err := b.UpdateClientConnState(balancer.ClientConnState{
		ResolverState: xdsclient.SetClient(resolver.State{Endpoints: testBackendEndpoints}, xdsC),
		BalancerConfig: &LBConfig{
			Cluster:        testClusterName,
			EDSServiceName: testServiceName,
			ChildPolicy: &internalserviceconfig.BalancerConfig{
				Name: roundrobin.Name,
			},
		},
	}); err != nil {
		t.Fatalf("unexpected error from UpdateClientConnState: %v", err)
	}

	sc1 := <-cc.NewSubConnCh
	sc1.UpdateState(balancer.SubConnState{ConnectivityState: connectivity.Connecting})
	// This should get the connecting picker.
	if err := cc.WaitForPickerWithErr(ctx, balancer.ErrNoSubConnAvailable); err != nil {
		t.Fatal(err.Error())
	}

	addrs1 := <-cc.NewSubConnAddrsCh
	if got, want := addrs1[0].Addr, testBackendEndpoints[0].Addresses[0].Addr; got != want {
		t.Fatalf("sc is created with addr %v, want %v", got, want)
	}
	cn, ok := xds.GetXDSHandshakeClusterName(addrs1[0].Attributes)
	if !ok || cn != testClusterName {
		t.Fatalf("sc is created with addr with cluster name %v, %v, want cluster name %v", cn, ok, testClusterName)
	}

	sc1.UpdateState(balancer.SubConnState{ConnectivityState: connectivity.Ready})
	// Test pick with one backend.
	if err := cc.WaitForRoundRobinPicker(ctx, sc1); err != nil {
		t.Fatal(err.Error())
	}

	const testClusterName2 = "test-cluster-2"
	var addr2 = resolver.Address{Addr: "2.2.2.2"}
	if err := b.UpdateClientConnState(balancer.ClientConnState{
		ResolverState: xdsclient.SetClient(resolver.State{Endpoints: []resolver.Endpoint{{Addresses: []resolver.Address{addr2}}}}, xdsC),
		BalancerConfig: &LBConfig{
			Cluster:        testClusterName2,
			EDSServiceName: testServiceName,
			ChildPolicy: &internalserviceconfig.BalancerConfig{
				Name: roundrobin.Name,
			},
		},
	}); err != nil {
		t.Fatalf("unexpected error from UpdateClientConnState: %v", err)
	}

	addrs2 := <-cc.NewSubConnAddrsCh
	if got, want := addrs2[0].Addr, addr2.Addr; got != want {
		t.Fatalf("sc is created with addr %v, want %v", got, want)
	}
	// New addresses should have the new cluster name.
	cn2, ok := xds.GetXDSHandshakeClusterName(addrs2[0].Attributes)
	if !ok || cn2 != testClusterName2 {
		t.Fatalf("sc is created with addr with cluster name %v, %v, want cluster name %v", cn2, ok, testClusterName2)
	}
}

// TestReResolution verifies that when a SubConn turns transient failure,
// re-resolution is triggered.
func (s) TestReResolution(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()

	defer xdsclient.ClearCounterForTesting(testClusterName, testServiceName)
	xdsC := fakeclient.NewClient()

	builder := balancer.Get(Name)
	cc := testutils.NewBalancerClientConn(t)
	b := builder.Build(cc, balancer.BuildOptions{})
	defer b.Close()

	if err := b.UpdateClientConnState(balancer.ClientConnState{
		ResolverState: xdsclient.SetClient(resolver.State{Endpoints: testBackendEndpoints}, xdsC),
		BalancerConfig: &LBConfig{
			Cluster:        testClusterName,
			EDSServiceName: testServiceName,
			ChildPolicy: &internalserviceconfig.BalancerConfig{
				Name: roundrobin.Name,
			},
		},
	}); err != nil {
		t.Fatalf("unexpected error from UpdateClientConnState: %v", err)
	}

	sc1 := <-cc.NewSubConnCh
	sc1.UpdateState(balancer.SubConnState{ConnectivityState: connectivity.Connecting})
	// This should get the connecting picker.
	if err := cc.WaitForPickerWithErr(ctx, balancer.ErrNoSubConnAvailable); err != nil {
		t.Fatal(err.Error())
	}

	sc1.UpdateState(balancer.SubConnState{
		ConnectivityState: connectivity.TransientFailure,
		ConnectionError:   errors.New("test error"),
	})
	// This should get the transient failure picker.
	if err := cc.WaitForErrPicker(ctx); err != nil {
		t.Fatal(err.Error())
	}

	// The transient failure should trigger a re-resolution.
	select {
	case <-cc.ResolveNowCh:
	case <-time.After(defaultTestTimeout):
		t.Fatalf("timeout waiting for ResolveNow()")
	}

	sc1.UpdateState(balancer.SubConnState{ConnectivityState: connectivity.Ready})
	// Test pick with one backend.
	if err := cc.WaitForRoundRobinPicker(ctx, sc1); err != nil {
		t.Fatal(err.Error())
	}

	sc1.UpdateState(balancer.SubConnState{ConnectivityState: connectivity.TransientFailure})
	// This should get the transient failure picker.
	if err := cc.WaitForErrPicker(ctx); err != nil {
		t.Fatal(err.Error())
	}

	// The transient failure should trigger a re-resolution.
	select {
	case <-cc.ResolveNowCh:
	case <-time.After(defaultTestTimeout):
		t.Fatalf("timeout waiting for ResolveNow()")
	}
}

func (s) TestLoadReporting(t *testing.T) {
	var testLocality = xdsinternal.LocalityID{
		Region:  "test-region",
		Zone:    "test-zone",
		SubZone: "test-sub-zone",
	}

	xdsC := fakeclient.NewClient()

	builder := balancer.Get(Name)
	cc := testutils.NewBalancerClientConn(t)
	b := builder.Build(cc, balancer.BuildOptions{})
	defer b.Close()

	endpoints := make([]resolver.Endpoint, len(testBackendEndpoints))
	for i, e := range testBackendEndpoints {
		endpoints[i] = xdsinternal.SetLocalityIDInEndpoint(e, testLocality)
		for j, a := range e.Addresses {
			endpoints[i].Addresses[j] = xdsinternal.SetLocalityID(a, testLocality)
		}
	}
	testLRSServerConfig, err := bootstrap.ServerConfigForTesting(bootstrap.ServerConfigTestingOptions{
		URI:          "trafficdirector.googleapis.com:443",
		ChannelCreds: []bootstrap.ChannelCreds{{Type: "google_default"}},
	})
	if err != nil {
		t.Fatalf("Failed to create LRS server config for testing: %v", err)
	}
	if err := b.UpdateClientConnState(balancer.ClientConnState{
		ResolverState: xdsclient.SetClient(resolver.State{Endpoints: endpoints}, xdsC),
		BalancerConfig: &LBConfig{
			Cluster:             testClusterName,
			EDSServiceName:      testServiceName,
			LoadReportingServer: testLRSServerConfig,
			// Locality:                testLocality,
			ChildPolicy: &internalserviceconfig.BalancerConfig{
				Name: roundrobin.Name,
			},
		},
	}); err != nil {
		t.Fatalf("unexpected error from UpdateClientConnState: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()

	got, err := xdsC.WaitForReportLoad(ctx)
	if err != nil {
		t.Fatalf("xdsClient.ReportLoad failed with error: %v", err)
	}
	if got.Server != testLRSServerConfig {
		t.Fatalf("xdsClient.ReportLoad called with {%q}: want {%q}", got.Server, testLRSServerConfig)
	}

	sc1 := <-cc.NewSubConnCh
	sc1.UpdateState(balancer.SubConnState{ConnectivityState: connectivity.Connecting})
	// This should get the connecting picker.
	if err := cc.WaitForPickerWithErr(ctx, balancer.ErrNoSubConnAvailable); err != nil {
		t.Fatal(err.Error())
	}

	scs := balancer.SubConnState{ConnectivityState: connectivity.Ready}
	sca := internal.SetConnectedAddress.(func(*balancer.SubConnState, resolver.Address))
	sca(&scs, endpoints[0].Addresses[0])
	sc1.UpdateState(scs)
	// Test pick with one backend.
	const successCount = 5
	const errorCount = 5
	if err := cc.WaitForPicker(ctx, func(p balancer.Picker) error {
		for i := 0; i < successCount; i++ {
			gotSCSt, err := p.Pick(balancer.PickInfo{})
			if gotSCSt.SubConn != sc1 {
				return fmt.Errorf("picker.Pick, got %v, %v, want SubConn=%v", gotSCSt, err, sc1)
			}
			lr := &v3orcapb.OrcaLoadReport{
				NamedMetrics: map[string]float64{testNamedMetricsKey1: 3.14, testNamedMetricsKey2: 2.718},
			}
			gotSCSt.Done(balancer.DoneInfo{ServerLoad: lr})
		}
		for i := 0; i < errorCount; i++ {
			gotSCSt, err := p.Pick(balancer.PickInfo{})
			if gotSCSt.SubConn != sc1 {
				return fmt.Errorf("picker.Pick, got %v, %v, want SubConn=%v", gotSCSt, err, sc1)
			}
			gotSCSt.Done(balancer.DoneInfo{Err: fmt.Errorf("error")})
		}
		return nil
	}); err != nil {
		t.Fatal(err.Error())
	}

	// Dump load data from the store and compare with expected counts.
	loadStore := xdsC.LoadStore()
	if loadStore == nil {
		t.Fatal("loadStore is nil in xdsClient")
	}
	sds := loadStore.Stats([]string{testClusterName})
	if len(sds) == 0 {
		t.Fatalf("loads for cluster %v not found in store", testClusterName)
	}
	sd := sds[0]
	if sd.Cluster != testClusterName || sd.Service != testServiceName {
		t.Fatalf("got unexpected load for %q, %q, want %q, %q", sd.Cluster, sd.Service, testClusterName, testServiceName)
	}
	testLocalityStr := testLocality.ToString()
	localityData, ok := sd.LocalityStats[testLocalityStr]
	if !ok {
		t.Fatalf("loads for %v not found in store", testLocality)
	}
	reqStats := localityData.RequestStats
	if reqStats.Succeeded != successCount {
		t.Errorf("got succeeded %v, want %v", reqStats.Succeeded, successCount)
	}
	if reqStats.Errored != errorCount {
		t.Errorf("got errord %v, want %v", reqStats.Errored, errorCount)
	}
	if reqStats.InProgress != 0 {
		t.Errorf("got inProgress %v, want %v", reqStats.InProgress, 0)
	}
	wantLoadStats := map[string]load.ServerLoadData{
		testNamedMetricsKey1: {Count: 5, Sum: 15.7},  // aggregation of 5 * 3.14 = 15.7
		testNamedMetricsKey2: {Count: 5, Sum: 13.59}, // aggregation of 5 * 2.718 = 13.59
	}
	if diff := cmp.Diff(wantLoadStats, localityData.LoadStats, toleranceCmpOpt); diff != "" {
		t.Errorf("localityData.LoadStats returned unexpected diff (-want +got):\n%s", diff)
	}
	b.Close()
	if err := xdsC.WaitForCancelReportLoad(ctx); err != nil {
		t.Fatalf("unexpected error waiting form load report to be canceled: %v", err)
	}
}

// TestUpdateLRSServer covers the cases
// - the init config specifies "" as the LRS server
// - config modifies LRS server to a different string
// - config sets LRS server to nil to stop load reporting
func (s) TestUpdateLRSServer(t *testing.T) {
	var testLocality = xdsinternal.LocalityID{
		Region:  "test-region",
		Zone:    "test-zone",
		SubZone: "test-sub-zone",
	}

	xdsC := fakeclient.NewClient()

	builder := balancer.Get(Name)
	cc := testutils.NewBalancerClientConn(t)
	b := builder.Build(cc, balancer.BuildOptions{})
	defer b.Close()

	endpoints := make([]resolver.Endpoint, len(testBackendEndpoints))
	for i, e := range testBackendEndpoints {
		endpoints[i] = xdsinternal.SetLocalityIDInEndpoint(e, testLocality)
	}
	testLRSServerConfig, err := bootstrap.ServerConfigForTesting(bootstrap.ServerConfigTestingOptions{
		URI:          "trafficdirector.googleapis.com:443",
		ChannelCreds: []bootstrap.ChannelCreds{{Type: "google_default"}},
	})
	if err != nil {
		t.Fatalf("Failed to create LRS server config for testing: %v", err)
	}
	if err := b.UpdateClientConnState(balancer.ClientConnState{
		ResolverState: xdsclient.SetClient(resolver.State{Endpoints: endpoints}, xdsC),
		BalancerConfig: &LBConfig{
			Cluster:             testClusterName,
			EDSServiceName:      testServiceName,
			LoadReportingServer: testLRSServerConfig,
			ChildPolicy: &internalserviceconfig.BalancerConfig{
				Name: roundrobin.Name,
			},
		},
	}); err != nil {
		t.Fatalf("unexpected error from UpdateClientConnState: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()

	got, err := xdsC.WaitForReportLoad(ctx)
	if err != nil {
		t.Fatalf("xdsClient.ReportLoad failed with error: %v", err)
	}
	if got.Server != testLRSServerConfig {
		t.Fatalf("xdsClient.ReportLoad called with {%q}: want {%q}", got.Server, testLRSServerConfig)
	}

	testLRSServerConfig2, err := bootstrap.ServerConfigForTesting(bootstrap.ServerConfigTestingOptions{
		URI:          "trafficdirector-another.googleapis.com:443",
		ChannelCreds: []bootstrap.ChannelCreds{{Type: "google_default"}},
	})
	if err != nil {
		t.Fatalf("Failed to create LRS server config for testing: %v", err)
	}

	// Update LRS server to a different name.
	if err := b.UpdateClientConnState(balancer.ClientConnState{
		ResolverState: xdsclient.SetClient(resolver.State{Endpoints: endpoints}, xdsC),
		BalancerConfig: &LBConfig{
			Cluster:             testClusterName,
			EDSServiceName:      testServiceName,
			LoadReportingServer: testLRSServerConfig2,
			ChildPolicy: &internalserviceconfig.BalancerConfig{
				Name: roundrobin.Name,
			},
		},
	}); err != nil {
		t.Fatalf("unexpected error from UpdateClientConnState: %v", err)
	}
	if err := xdsC.WaitForCancelReportLoad(ctx); err != nil {
		t.Fatalf("unexpected error waiting form load report to be canceled: %v", err)
	}
	got2, err2 := xdsC.WaitForReportLoad(ctx)
	if err2 != nil {
		t.Fatalf("xdsClient.ReportLoad failed with error: %v", err2)
	}
	if got2.Server != testLRSServerConfig2 {
		t.Fatalf("xdsClient.ReportLoad called with {%q}: want {%q}", got2.Server, testLRSServerConfig2)
	}

	// Update LRS server to nil, to disable LRS.
	if err := b.UpdateClientConnState(balancer.ClientConnState{
		ResolverState: xdsclient.SetClient(resolver.State{Endpoints: endpoints}, xdsC),
		BalancerConfig: &LBConfig{
			Cluster:        testClusterName,
			EDSServiceName: testServiceName,
			ChildPolicy: &internalserviceconfig.BalancerConfig{
				Name: roundrobin.Name,
			},
		},
	}); err != nil {
		t.Fatalf("unexpected error from UpdateClientConnState: %v", err)
	}
	if err := xdsC.WaitForCancelReportLoad(ctx); err != nil {
		t.Fatalf("unexpected error waiting form load report to be canceled: %v", err)
	}

	shortCtx, shortCancel := context.WithTimeout(context.Background(), defaultShortTestTimeout)
	defer shortCancel()
	if s, err := xdsC.WaitForReportLoad(shortCtx); err != context.DeadlineExceeded {
		t.Fatalf("unexpected load report to server: %q", s)
	}
}

// Test verifies that child policies was updated on receipt of
// configuration update.
func (s) TestChildPolicyUpdatedOnConfigUpdate(t *testing.T) {
	xdsC := fakeclient.NewClient()

	builder := balancer.Get(Name)
	cc := testutils.NewBalancerClientConn(t)
	b := builder.Build(cc, balancer.BuildOptions{})
	defer b.Close()

	// Keep track of which child policy was updated
	updatedChildPolicy := ""

	// Create stub balancers to track config updates
	const (
		childPolicyName1 = "stubBalancer1"
		childPolicyName2 = "stubBalancer2"
	)

	stub.Register(childPolicyName1, stub.BalancerFuncs{
		UpdateClientConnState: func(_ *stub.BalancerData, _ balancer.ClientConnState) error {
			updatedChildPolicy = childPolicyName1
			return nil
		},
	})

	stub.Register(childPolicyName2, stub.BalancerFuncs{
		UpdateClientConnState: func(_ *stub.BalancerData, _ balancer.ClientConnState) error {
			updatedChildPolicy = childPolicyName2
			return nil
		},
	})

	// Initial config update with childPolicyName1
	if err := b.UpdateClientConnState(balancer.ClientConnState{
		ResolverState: xdsclient.SetClient(resolver.State{Endpoints: testBackendEndpoints}, xdsC),
		BalancerConfig: &LBConfig{
			Cluster: testClusterName,
			ChildPolicy: &internalserviceconfig.BalancerConfig{
				Name: childPolicyName1,
			},
		},
	}); err != nil {
		t.Fatalf("Error updating the config: %v", err)
	}

	if updatedChildPolicy != childPolicyName1 {
		t.Fatal("Child policy 1 was not updated on initial configuration update.")
	}

	// Second config update with childPolicyName2
	if err := b.UpdateClientConnState(balancer.ClientConnState{
		ResolverState: xdsclient.SetClient(resolver.State{Endpoints: testBackendEndpoints}, xdsC),
		BalancerConfig: &LBConfig{
			Cluster: testClusterName,
			ChildPolicy: &internalserviceconfig.BalancerConfig{
				Name: childPolicyName2,
			},
		},
	}); err != nil {
		t.Fatalf("Error updating the config: %v", err)
	}

	if updatedChildPolicy != childPolicyName2 {
		t.Fatal("Child policy 2 was not updated after child policy name change.")
	}
}

// Test verifies that config update fails if child policy config
// failed to parse.
func (s) TestFailedToParseChildPolicyConfig(t *testing.T) {
	xdsC := fakeclient.NewClient()

	builder := balancer.Get(Name)
	cc := testutils.NewBalancerClientConn(t)
	b := builder.Build(cc, balancer.BuildOptions{})
	defer b.Close()

	// Create a stub balancer which fails to ParseConfig.
	const parseConfigError = "failed to parse config"
	const childPolicyName = "stubBalancer-FailedToParseChildPolicyConfig"
	stub.Register(childPolicyName, stub.BalancerFuncs{
		ParseConfig: func(_ json.RawMessage) (serviceconfig.LoadBalancingConfig, error) {
			return nil, errors.New(parseConfigError)
		},
	})

	err := b.UpdateClientConnState(balancer.ClientConnState{
		ResolverState: xdsclient.SetClient(resolver.State{Endpoints: testBackendEndpoints}, xdsC),
		BalancerConfig: &LBConfig{
			Cluster: testClusterName,
			ChildPolicy: &internalserviceconfig.BalancerConfig{
				Name: childPolicyName,
			},
		},
	})

	if err == nil || !strings.Contains(err.Error(), parseConfigError) {
		t.Fatalf("Got error: %v, want error: %s", err, parseConfigError)
	}
}

// Test verify that the case picker is updated synchronously on receipt of
// configuration update.
func (s) TestPickerUpdatedSynchronouslyOnConfigUpdate(t *testing.T) {
	// Override the pickerUpdateHook to be notified that picker was updated.
	pickerUpdated := make(chan struct{}, 1)
	origNewPickerUpdated := pickerUpdateHook
	pickerUpdateHook = func() {
		pickerUpdated <- struct{}{}
	}
	defer func() { pickerUpdateHook = origNewPickerUpdated }()

	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	// Override the clientConnUpdateHook to ensure client conn was updated.
	clientConnUpdateDone := make(chan struct{}, 1)
	origClientConnUpdateHook := clientConnUpdateHook
	clientConnUpdateHook = func() {
		// Verify that picker was updated before the completion of
		// client conn update.
		select {
		case <-pickerUpdated:
		case <-ctx.Done():
			t.Fatal("Client conn update completed before picker update.")
		}
		clientConnUpdateDone <- struct{}{}
	}
	defer func() { clientConnUpdateHook = origClientConnUpdateHook }()

	defer xdsclient.ClearCounterForTesting(testClusterName, testServiceName)
	xdsC := fakeclient.NewClient()

	builder := balancer.Get(Name)
	cc := testutils.NewBalancerClientConn(t)
	b := builder.Build(cc, balancer.BuildOptions{})
	defer b.Close()

	// Create a stub balancer which waits for the cluster_impl policy to be
	// closed before sending a picker update (upon receipt of a resolver
	// update).
	stub.Register(t.Name(), stub.BalancerFuncs{
		UpdateClientConnState: func(bd *stub.BalancerData, _ balancer.ClientConnState) error {
			bd.ClientConn.UpdateState(balancer.State{
				Picker: base.NewErrPicker(errors.New("dummy error picker")),
			})
			return nil
		},
	})

	if err := b.UpdateClientConnState(balancer.ClientConnState{
		ResolverState: xdsclient.SetClient(resolver.State{Endpoints: testBackendEndpoints}, xdsC),
		BalancerConfig: &LBConfig{
			Cluster:        testClusterName,
			EDSServiceName: testServiceName,
			ChildPolicy: &internalserviceconfig.BalancerConfig{
				Name: t.Name(),
			},
		},
	}); err != nil {
		t.Fatalf("Unexpected error from UpdateClientConnState: %v", err)
	}

	select {
	case <-clientConnUpdateDone:
	case <-ctx.Done():
		t.Fatal("Timed out waiting for client conn update to be completed.")
	}
}
