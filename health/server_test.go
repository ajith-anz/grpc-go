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

package health_test

import (
	"testing"

	"github.com/ajith-anz/grpc-go"
	"github.com/ajith-anz/grpc-go/health"
	healthgrpc "github.com/ajith-anz/grpc-go/health/grpc_health_v1"
	"github.com/ajith-anz/grpc-go/internal/grpctest"
)

type s struct {
	grpctest.Tester
}

func Test(t *testing.T) {
	grpctest.RunSubTests(t, s{})
}

// Make sure the service implementation complies with the proto definition.
func (s) TestRegister(*testing.T) {
	s := grpc.NewServer()
	healthgrpc.RegisterHealthServer(s, health.NewServer())
	s.Stop()
}
