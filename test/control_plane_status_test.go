/*
 *
 * Copyright 2022 gRPC authors.
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
	"strings"
	"testing"
	"time"

	"github.com/ajith-anz/grpc-go"
	"github.com/ajith-anz/grpc-go/balancer"
	"github.com/ajith-anz/grpc-go/balancer/base"
	"github.com/ajith-anz/grpc-go/codes"
	"github.com/ajith-anz/grpc-go/connectivity"
	"github.com/ajith-anz/grpc-go/internal/balancer/stub"
	iresolver "github.com/ajith-anz/grpc-go/internal/resolver"
	"github.com/ajith-anz/grpc-go/internal/stubserver"
	testpb "github.com/ajith-anz/grpc-go/interop/grpc_testing"
	"github.com/ajith-anz/grpc-go/resolver"
	"github.com/ajith-anz/grpc-go/resolver/manual"
	"github.com/ajith-anz/grpc-go/status"
)

func (s) TestConfigSelectorStatusCodes(t *testing.T) {
	testCases := []struct {
		name  string
		csErr error
		want  error
	}{{
		name:  "legal status code",
		csErr: status.Errorf(codes.Unavailable, "this error is fine"),
		want:  status.Errorf(codes.Unavailable, "this error is fine"),
	}, {
		name:  "illegal status code",
		csErr: status.Errorf(codes.NotFound, "this error is bad"),
		want:  status.Errorf(codes.Internal, "this error is bad"),
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ss := &stubserver.StubServer{
				EmptyCallF: func(context.Context, *testpb.Empty) (*testpb.Empty, error) {
					return &testpb.Empty{}, nil
				},
			}
			ss.R = manual.NewBuilderWithScheme("confSel")

			if err := ss.Start(nil); err != nil {
				t.Fatalf("Error starting endpoint server: %v", err)
			}
			defer ss.Stop()

			state := iresolver.SetConfigSelector(resolver.State{
				Addresses:     []resolver.Address{{Addr: ss.Address}},
				ServiceConfig: parseServiceConfig(t, ss.R, "{}"),
			}, funcConfigSelector{
				f: func(iresolver.RPCInfo) (*iresolver.RPCConfig, error) {
					return nil, tc.csErr
				},
			})
			ss.R.UpdateState(state) // Blocks until config selector is applied

			ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
			defer cancel()
			if _, err := ss.Client.EmptyCall(ctx, &testpb.Empty{}); status.Code(err) != status.Code(tc.want) || !strings.Contains(err.Error(), status.Convert(tc.want).Message()) {
				t.Fatalf("client.EmptyCall(_, _) = _, %v; want _, %v", err, tc.want)
			}
		})
	}
}

func (s) TestPickerStatusCodes(t *testing.T) {
	testCases := []struct {
		name      string
		pickerErr error
		want      error
	}{{
		name:      "legal status code",
		pickerErr: status.Errorf(codes.Unavailable, "this error is fine"),
		want:      status.Errorf(codes.Unavailable, "this error is fine"),
	}, {
		name:      "illegal status code",
		pickerErr: status.Errorf(codes.NotFound, "this error is bad"),
		want:      status.Errorf(codes.Internal, "this error is bad"),
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ss := &stubserver.StubServer{
				EmptyCallF: func(context.Context, *testpb.Empty) (*testpb.Empty, error) {
					return &testpb.Empty{}, nil
				},
			}

			if err := ss.Start(nil); err != nil {
				t.Fatalf("Error starting endpoint server: %v", err)
			}
			defer ss.Stop()

			// Create a stub balancer that creates a picker that always returns
			// an error.
			sbf := stub.BalancerFuncs{
				UpdateClientConnState: func(d *stub.BalancerData, _ balancer.ClientConnState) error {
					d.ClientConn.UpdateState(balancer.State{
						ConnectivityState: connectivity.TransientFailure,
						Picker:            base.NewErrPicker(tc.pickerErr),
					})
					return nil
				},
			}
			stub.Register("testPickerStatusCodesBalancer", sbf)

			ss.NewServiceConfig(`{"loadBalancingConfig": [{"testPickerStatusCodesBalancer":{}}] }`)

			// Make calls until pickerErr is received.
			ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
			defer cancel()

			var lastErr error
			for ctx.Err() == nil {
				if _, lastErr = ss.Client.EmptyCall(ctx, &testpb.Empty{}); status.Code(lastErr) == status.Code(tc.want) && strings.Contains(lastErr.Error(), status.Convert(tc.want).Message()) {
					// Success!
					return
				}
				time.Sleep(time.Millisecond)
			}

			t.Fatalf("client.EmptyCall(_, _) = _, %v; want _, %v", lastErr, tc.want)
		})
	}
}

func (s) TestCallCredsFromDialOptionsStatusCodes(t *testing.T) {
	testCases := []struct {
		name     string
		credsErr error
		want     error
	}{{
		name:     "legal status code",
		credsErr: status.Errorf(codes.Unavailable, "this error is fine"),
		want:     status.Errorf(codes.Unavailable, "this error is fine"),
	}, {
		name:     "illegal status code",
		credsErr: status.Errorf(codes.NotFound, "this error is bad"),
		want:     status.Errorf(codes.Internal, "this error is bad"),
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ss := &stubserver.StubServer{
				EmptyCallF: func(context.Context, *testpb.Empty) (*testpb.Empty, error) {
					return &testpb.Empty{}, nil
				},
			}

			errChan := make(chan error, 1)
			creds := &testPerRPCCredentials{errChan: errChan}

			if err := ss.Start(nil, grpc.WithPerRPCCredentials(creds)); err != nil {
				t.Fatalf("Error starting endpoint server: %v", err)
			}
			defer ss.Stop()

			ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
			defer cancel()

			errChan <- tc.credsErr

			if _, err := ss.Client.EmptyCall(ctx, &testpb.Empty{}); status.Code(err) != status.Code(tc.want) || !strings.Contains(err.Error(), status.Convert(tc.want).Message()) {
				t.Fatalf("client.EmptyCall(_, _) = _, %v; want _, %v", err, tc.want)
			}
		})
	}
}

func (s) TestCallCredsFromCallOptionsStatusCodes(t *testing.T) {
	testCases := []struct {
		name     string
		credsErr error
		want     error
	}{{
		name:     "legal status code",
		credsErr: status.Errorf(codes.Unavailable, "this error is fine"),
		want:     status.Errorf(codes.Unavailable, "this error is fine"),
	}, {
		name:     "illegal status code",
		credsErr: status.Errorf(codes.NotFound, "this error is bad"),
		want:     status.Errorf(codes.Internal, "this error is bad"),
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ss := &stubserver.StubServer{
				EmptyCallF: func(context.Context, *testpb.Empty) (*testpb.Empty, error) {
					return &testpb.Empty{}, nil
				},
			}

			errChan := make(chan error, 1)
			creds := &testPerRPCCredentials{errChan: errChan}

			if err := ss.Start(nil); err != nil {
				t.Fatalf("Error starting endpoint server: %v", err)
			}
			defer ss.Stop()

			ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
			defer cancel()

			errChan <- tc.credsErr

			if _, err := ss.Client.EmptyCall(ctx, &testpb.Empty{}, grpc.PerRPCCredentials(creds)); status.Code(err) != status.Code(tc.want) || !strings.Contains(err.Error(), status.Convert(tc.want).Message()) {
				t.Fatalf("client.EmptyCall(_, _) = _, %v; want _, %v", err, tc.want)
			}
		})
	}
}
