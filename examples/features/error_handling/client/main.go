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

// Binary client demonstrates how to handle errors returned by a gRPC server.
package main

import (
	"context"
	"flag"
	"log"
	"os/user"
	"time"

	"github.com/ajith-anz/grpc-go"
	"github.com/ajith-anz/grpc-go/codes"
	"github.com/ajith-anz/grpc-go/credentials/insecure"
	pb "github.com/ajith-anz/grpc-go/examples/helloworld/helloworld"
	"github.com/ajith-anz/grpc-go/status"
)

var addr = flag.String("addr", "localhost:50052", "the address to connect to")

func main() {
	flag.Parse()

	name := "unknown"
	if u, err := user.Current(); err == nil && u.Username != "" {
		name = u.Username
	}

	// Set up a connection to the server.
	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewGreeterClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	for _, reqName := range []string{"", name} {
		log.Printf("Calling SayHello with Name:%q", reqName)
		r, err := c.SayHello(ctx, &pb.HelloRequest{Name: reqName})
		if err != nil {
			if status.Code(err) != codes.InvalidArgument {
				log.Printf("Received unexpected error: %v", err)
				continue
			}
			log.Printf("Received error: %v", err)
			continue
		}
		log.Printf("Received response: %s", r.Message)
	}
}
