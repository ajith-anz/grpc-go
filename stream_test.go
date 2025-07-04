/*
 *
 * Copyright 2023 gRPC authors.
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

package grpc_test

import (
	"context"
	"testing"
	"time"

	"github.com/ajith-anz/grpc-go/codes"
	"github.com/ajith-anz/grpc-go/internal/grpctest"
	"github.com/ajith-anz/grpc-go/internal/stubserver"
	"github.com/ajith-anz/grpc-go/status"

	testgrpc "github.com/ajith-anz/grpc-go/interop/grpc_testing"
)

const defaultTestTimeout = 10 * time.Second

type s struct {
	grpctest.Tester
}

func Test(t *testing.T) {
	grpctest.RunSubTests(t, s{})
}

func (s) TestStream_Header_TrailersOnly(t *testing.T) {
	ss := stubserver.StubServer{
		FullDuplexCallF: func(testgrpc.TestService_FullDuplexCallServer) error {
			return status.Errorf(codes.NotFound, "a test error")
		},
	}
	if err := ss.Start(nil); err != nil {
		t.Fatal("Error starting server:", err)
	}
	defer ss.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()

	s, err := ss.Client.FullDuplexCall(ctx)
	if err != nil {
		t.Fatal("Error staring call", err)
	}
	if md, err := s.Header(); md != nil || err != nil {
		t.Fatalf("s.Header() = %v, %v; want nil, nil", md, err)
	}
	if _, err := s.Recv(); status.Code(err) != codes.NotFound {
		t.Fatalf("s.Recv() = _, %v; want _, err.Code()=codes.NotFound", err)
	}
}
