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

// Binary client demonstrates how to use TLS credentials for secure
// gRPC communication.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/ajith-anz/grpc-go"
	"github.com/ajith-anz/grpc-go/credentials"
	"github.com/ajith-anz/grpc-go/examples/data"
	ecpb "github.com/ajith-anz/grpc-go/examples/features/proto/echo"
)

var addr = flag.String("addr", "localhost:50051", "the address to connect to")

func callUnaryEcho(client ecpb.EchoClient, message string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := client.UnaryEcho(ctx, &ecpb.EchoRequest{Message: message})
	if err != nil {
		log.Fatalf("client.UnaryEcho(_) = _, %v: ", err)
	}
	fmt.Println("UnaryEcho: ", resp.Message)
}

func main() {
	flag.Parse()

	// Create tls based credential.
	creds, err := credentials.NewClientTLSFromFile(data.Path("x509/ca_cert.pem"), "x.test.example.com")
	if err != nil {
		log.Fatalf("failed to load credentials: %v", err)
	}

	// Set up a connection to the server.
	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	// Make an echo client and send an RPC.
	rgc := ecpb.NewEchoClient(conn)
	callUnaryEcho(rgc, "hello world")
}
