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

// Binary server demonstrates how to validate authorization credential metadata
// for incoming RPCs.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"time"

	"github.com/ajith-anz/grpc-go"
	"github.com/ajith-anz/grpc-go/authz"
	"github.com/ajith-anz/grpc-go/codes"
	"github.com/ajith-anz/grpc-go/credentials"
	"github.com/ajith-anz/grpc-go/examples/data"
	"github.com/ajith-anz/grpc-go/metadata"
	"github.com/ajith-anz/grpc-go/status"

	"github.com/ajith-anz/grpc-go/examples/features/authz/token"
	pb "github.com/ajith-anz/grpc-go/examples/features/proto/echo"
)

const (
	unaryEchoWriterRole      = "UNARY_ECHO:W"
	streamEchoReadWriterRole = "STREAM_ECHO:RW"
	authzPolicy              = `
	{
		"name": "authz",
		"allow_rules": [
			{
				"name": "allow_UnaryEcho",
				"request": {
					"paths": ["/grpc.examples.echo.Echo/UnaryEcho"],
					"headers": [
						{
							"key": "UNARY_ECHO:W",
							"values": ["true"]
						}
					]
				}
			},
			{
				"name": "allow_BidirectionalStreamingEcho",
				"request": {
					"paths": ["/grpc.examples.echo.Echo/BidirectionalStreamingEcho"],
					"headers": [
						{
							"key": "STREAM_ECHO:RW",
							"values": ["true"]
						}
					]
				}
			}
		],
		"deny_rules": []
	}
	`
	authzOptStatic      = "static"
	authzOptFileWatcher = "filewatcher"
)

var (
	port     = flag.Int("port", 50051, "the port to serve on")
	authzOpt = flag.String("authz-option", authzOptStatic, "the authz option (static or filewatcher)")

	errMissingMetadata = status.Errorf(codes.InvalidArgument, "missing metadata")
)

func newContextWithRoles(ctx context.Context, username string) context.Context {
	md := metadata.MD{}
	if username == "super-user" {
		md.Set(unaryEchoWriterRole, "true")
		md.Set(streamEchoReadWriterRole, "true")
	}
	return metadata.NewIncomingContext(ctx, md)
}

type server struct {
	pb.UnimplementedEchoServer
}

func (s *server) UnaryEcho(_ context.Context, in *pb.EchoRequest) (*pb.EchoResponse, error) {
	fmt.Printf("unary echoing message %q\n", in.Message)
	return &pb.EchoResponse{Message: in.Message}, nil
}

func (s *server) BidirectionalStreamingEcho(stream pb.Echo_BidirectionalStreamingEchoServer) error {
	for {
		in, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			fmt.Printf("Receiving message from stream: %v\n", err)
			return err
		}
		fmt.Printf("bidi echoing message %q\n", in.Message)
		stream.Send(&pb.EchoResponse{Message: in.Message})
	}
}

// isAuthenticated validates the authorization.
func isAuthenticated(authorization []string) (username string, err error) {
	if len(authorization) < 1 {
		return "", errors.New("received empty authorization token from client")
	}
	tokenBase64 := strings.TrimPrefix(authorization[0], "Bearer ")
	// Perform the token validation here. For the sake of this example, the code
	// here forgoes any of the usual OAuth2 token validation and instead checks
	// for a token matching an arbitrary string.
	var token token.Token
	err = token.Decode(tokenBase64)
	if err != nil {
		return "", fmt.Errorf("base64 decoding of received token %q: %v", tokenBase64, err)
	}
	if token.Secret != "super-secret" {
		return "", fmt.Errorf("received token %q does not match expected %q", token.Secret, "super-secret")
	}
	return token.Username, nil
}

// authUnaryInterceptor looks up the authorization header from the incoming RPC context,
// retrieves the username from it and creates a new context with the username before invoking
// the provided handler.
func authUnaryInterceptor(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, errMissingMetadata
	}
	username, err := isAuthenticated(md["authorization"])
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}
	return handler(newContextWithRoles(ctx, username), req)
}

// wrappedStream wraps a grpc.ServerStream associated with an incoming RPC, and
// a custom context containing the username derived from the authorization header
// specified in the incoming RPC metadata
type wrappedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedStream) Context() context.Context {
	return w.ctx
}

func newWrappedStream(ctx context.Context, s grpc.ServerStream) grpc.ServerStream {
	return &wrappedStream{s, ctx}
}

// authStreamInterceptor looks up the authorization header from the incoming RPC context,
// retrieves the username from it and creates a new context with the username before invoking
// the provided handler.
func authStreamInterceptor(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	md, ok := metadata.FromIncomingContext(ss.Context())
	if !ok {
		return errMissingMetadata
	}
	username, err := isAuthenticated(md["authorization"])
	if err != nil {
		return status.Error(codes.Unauthenticated, err.Error())
	}
	return handler(srv, newWrappedStream(newContextWithRoles(ss.Context(), username), ss))
}

func main() {
	flag.Parse()

	if *authzOpt != authzOptStatic && *authzOpt != authzOptFileWatcher {
		log.Fatalf("Invalid authz option: %s", *authzOpt)
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("Listening on local port %q: %v", *port, err)
	}

	// Create tls based credential.
	creds, err := credentials.NewServerTLSFromFile(data.Path("x509/server_cert.pem"), data.Path("x509/server_key.pem"))
	if err != nil {
		log.Fatalf("Loading credentials: %v", err)
	}

	// Create authorization interceptors according to the authz-option command-line flag.
	var unaryAuthzInterceptor grpc.UnaryServerInterceptor
	var streamAuthzInterceptor grpc.StreamServerInterceptor
	if *authzOpt == authzOptStatic {
		// Create an authorization interceptor using a static policy.
		staticInterceptor, err := authz.NewStatic(authzPolicy)
		if err != nil {
			log.Fatalf("Creating a static authz interceptor: %v", err)
		}
		unaryAuthzInterceptor, streamAuthzInterceptor = staticInterceptor.UnaryInterceptor, staticInterceptor.StreamInterceptor
	} else if *authzOpt == authzOptFileWatcher {
		// Create an authorization interceptor by watching a policy file.
		fileWatcherInterceptor, err := authz.NewFileWatcher(data.Path("rbac/policy.json"), 100*time.Millisecond)
		if err != nil {
			log.Fatalf("Creating a file watcher authz interceptor: %v", err)
		}
		unaryAuthzInterceptor, streamAuthzInterceptor = fileWatcherInterceptor.UnaryInterceptor, fileWatcherInterceptor.StreamInterceptor
	}

	unaryInterceptors := grpc.ChainUnaryInterceptor(authUnaryInterceptor, unaryAuthzInterceptor)
	streamInterceptors := grpc.ChainStreamInterceptor(authStreamInterceptor, streamAuthzInterceptor)
	s := grpc.NewServer(grpc.Creds(creds), unaryInterceptors, streamInterceptors)

	// Register EchoServer on the server.
	pb.RegisterEchoServer(s, &server{})

	if err := s.Serve(lis); err != nil {
		log.Fatalf("Serving Echo service on local port: %v", err)
	}
}
