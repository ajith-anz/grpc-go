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

// Binary server demonstrates how to instrument RPCs for logging, metrics,
// and tracing.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ajith-anz/grpc-go"
	pb "github.com/ajith-anz/grpc-go/examples/helloworld/helloworld"
	"github.com/ajith-anz/grpc-go/gcp/observability"
)

var (
	port = flag.Int("port", 50051, "The server port")
)

// server is used to implement helloworld.GreeterServer.
type server struct {
	pb.UnimplementedGreeterServer
}

// SayHello implements helloworld.GreeterServer
func (s *server) SayHello(_ context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
	log.Printf("Received: %v", in.GetName())
	return &pb.HelloReply{Message: "Hello " + in.GetName()}, nil
}

func main() {
	// Turn on global telemetry for the whole binary. If a configuration is
	// specified, any created gRPC Client Conn's or Servers will emit telemetry
	// data according the configuration.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	err := observability.Start(ctx)
	if err != nil {
		log.Fatalf("observability.Start() failed: %v", err)
	}
	defer observability.End()

	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterGreeterServer(s, &server{})
	log.Printf("server listening at %v", lis.Addr())

	// This server can potentially be terminated by an external signal from the
	// Operating System. The following catches those signals and calls s.Stop().
	// This causes the s.Serve() call to return and run main()'s defers,
	// including the observability.End() call that ensures any pending
	// observability data is sent to Cloud Operations.
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		s.Stop()
	}()

	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
