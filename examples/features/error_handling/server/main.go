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

// Binary server demonstrates how to return specific error codes in gRPC
// responses.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"

	"github.com/ajith-anz/grpc-go"
	"github.com/ajith-anz/grpc-go/codes"
	"github.com/ajith-anz/grpc-go/status"

	pb "github.com/ajith-anz/grpc-go/examples/helloworld/helloworld"
)

var port = flag.Int("port", 50052, "port number")

// server is used to implement helloworld.GreeterServer.
type server struct {
	pb.UnimplementedGreeterServer
}

// SayHello implements helloworld.GreeterServer.
func (s *server) SayHello(_ context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
	if in.Name == "" {
		return nil, status.Errorf(codes.InvalidArgument, "request missing required field: Name")
	}
	return &pb.HelloReply{Message: "Hello " + in.Name}, nil
}

func main() {
	flag.Parse()

	address := fmt.Sprintf(":%v", *port)
	lis, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterGreeterServer(s, &server{})
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
