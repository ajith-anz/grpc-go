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

// Binary server demonstrated gRPC's support for xDS APIs on the server-side. It
// exposes the Greeter service that will response with the hostname.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	rand "math/rand/v2"
	"net"
	"os"

	"github.com/ajith-anz/grpc-go"
	"github.com/ajith-anz/grpc-go/credentials/insecure"
	xdscreds "github.com/ajith-anz/grpc-go/credentials/xds"
	pb "github.com/ajith-anz/grpc-go/examples/helloworld/helloworld"
	"github.com/ajith-anz/grpc-go/health"
	healthgrpc "github.com/ajith-anz/grpc-go/health/grpc_health_v1"
	healthpb "github.com/ajith-anz/grpc-go/health/grpc_health_v1"
	"github.com/ajith-anz/grpc-go/xds"
)

var (
	port     = flag.Int("port", 50051, "the port to serve Greeter service requests on. Health service will be served on `port+1`")
	xdsCreds = flag.Bool("xds_creds", false, "whether the server should use xDS APIs to receive security configuration")
)

// server implements helloworld.GreeterServer interface.
type server struct {
	pb.UnimplementedGreeterServer
	serverName string
}

// SayHello implements helloworld.GreeterServer interface.
func (s *server) SayHello(_ context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
	log.Printf("Received: %v", in.GetName())
	return &pb.HelloReply{Message: "Hello " + in.GetName() + ", from " + s.serverName}, nil
}

func determineHostname() string {
	hostname, err := os.Hostname()
	if err != nil {
		log.Printf("Failed to get hostname: %v, will generate one", err)
		return fmt.Sprintf("generated-%03d", rand.Int()%100)
	}
	return hostname
}

func main() {
	flag.Parse()

	greeterPort := fmt.Sprintf(":%d", *port)
	greeterLis, err := net.Listen("tcp4", greeterPort)
	if err != nil {
		log.Fatalf("net.Listen(tcp4, %q) failed: %v", greeterPort, err)
	}

	creds := insecure.NewCredentials()
	if *xdsCreds {
		log.Println("Using xDS credentials...")
		var err error
		if creds, err = xdscreds.NewServerCredentials(xdscreds.ServerOptions{FallbackCreds: insecure.NewCredentials()}); err != nil {
			log.Fatalf("failed to create server-side xDS credentials: %v", err)
		}
	}

	greeterServer, err := xds.NewGRPCServer(grpc.Creds(creds))
	if err != nil {
		log.Fatalf("Failed to create an xDS enabled gRPC server: %v", err)
	}
	pb.RegisterGreeterServer(greeterServer, &server{serverName: determineHostname()})

	healthPort := fmt.Sprintf(":%d", *port+1)
	healthLis, err := net.Listen("tcp4", healthPort)
	if err != nil {
		log.Fatalf("net.Listen(tcp4, %q) failed: %v", healthPort, err)
	}
	grpcServer := grpc.NewServer()
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthgrpc.RegisterHealthServer(grpcServer, healthServer)

	log.Printf("Serving GreeterService on %s and HealthService on %s", greeterLis.Addr().String(), healthLis.Addr().String())
	go func() {
		greeterServer.Serve(greeterLis)
	}()
	grpcServer.Serve(healthLis)
}
