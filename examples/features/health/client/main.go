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

// Binary client demonstrates how to check and observe gRPC server health using
// the health library.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/ajith-anz/grpc-go"
	"github.com/ajith-anz/grpc-go/credentials/insecure"
	pb "github.com/ajith-anz/grpc-go/examples/features/proto/echo"
	_ "github.com/ajith-anz/grpc-go/health"
	"github.com/ajith-anz/grpc-go/resolver"
	"github.com/ajith-anz/grpc-go/resolver/manual"
)

var serviceConfig = `{
	"loadBalancingPolicy": "round_robin",
	"healthCheckConfig": {
		"serviceName": ""
	}
}`

func callUnaryEcho(c pb.EchoClient) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := c.UnaryEcho(ctx, &pb.EchoRequest{})
	if err != nil {
		fmt.Println("UnaryEcho: _, ", err)
	} else {
		fmt.Println("UnaryEcho: ", r.GetMessage())
	}
}

func main() {
	flag.Parse()

	r := manual.NewBuilderWithScheme("whatever")
	r.InitialState(resolver.State{
		Addresses: []resolver.Address{
			{Addr: "localhost:50051"},
			{Addr: "localhost:50052"},
		},
	})

	address := fmt.Sprintf("%s:///unused", r.Scheme())

	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithResolvers(r),
		grpc.WithDefaultServiceConfig(serviceConfig),
	}

	conn, err := grpc.NewClient(address, options...)
	if err != nil {
		log.Fatalf("grpc.NewClient(%q): %v", address, err)
	}
	defer conn.Close()

	echoClient := pb.NewEchoClient(conn)

	for {
		callUnaryEcho(echoClient)
		time.Sleep(time.Second)
	}
}
