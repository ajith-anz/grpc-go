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

// Binary client demonstrates how to instrument RPCs with logging, metrics,
// and tracing.
package main

import (
	"context"
	"flag"
	"log"
	"time"

	"github.com/ajith-anz/grpc-go"
	"github.com/ajith-anz/grpc-go/credentials/insecure"
	pb "github.com/ajith-anz/grpc-go/examples/helloworld/helloworld"
	"github.com/ajith-anz/grpc-go/gcp/observability"
)

const (
	defaultName = "world"
)

var (
	addr = flag.String("addr", "localhost:50051", "the address to connect to")
	name = flag.String("name", defaultName, "Name to greet")
)

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
	// Set up a connection to the server.
	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewGreeterClient(conn)

	// Contact the server and print out its response.
	r, err := c.SayHello(ctx, &pb.HelloRequest{Name: *name})
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	log.Printf("Greeting: %s", r.GetMessage())
}
