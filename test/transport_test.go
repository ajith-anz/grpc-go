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
package test

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"testing"

	"golang.org/x/net/http2"
	"github.com/ajith-anz/grpc-go"
	"github.com/ajith-anz/grpc-go/codes"
	"github.com/ajith-anz/grpc-go/credentials"
	"github.com/ajith-anz/grpc-go/credentials/insecure"
	"github.com/ajith-anz/grpc-go/internal/grpcsync"
	"github.com/ajith-anz/grpc-go/internal/stubserver"
	"github.com/ajith-anz/grpc-go/internal/testutils"
	"github.com/ajith-anz/grpc-go/internal/transport"
	"github.com/ajith-anz/grpc-go/status"

	testgrpc "github.com/ajith-anz/grpc-go/interop/grpc_testing"
	testpb "github.com/ajith-anz/grpc-go/interop/grpc_testing"
)

// connWrapperWithCloseCh wraps a net.Conn and fires an event when closed.
type connWrapperWithCloseCh struct {
	net.Conn
	close *grpcsync.Event
}

// Close closes the connection and sends a value on the close channel.
func (cw *connWrapperWithCloseCh) Close() error {
	cw.close.Fire()
	return cw.Conn.Close()
}

// These custom creds are used for storing the connections made by the client.
// The closeCh in conn can be used to detect when conn is closed.
type transportRestartCheckCreds struct {
	mu          sync.Mutex
	connections []*connWrapperWithCloseCh
}

func (c *transportRestartCheckCreds) ServerHandshake(rawConn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return rawConn, nil, nil
}
func (c *transportRestartCheckCreds) ClientHandshake(_ context.Context, _ string, rawConn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	conn := &connWrapperWithCloseCh{Conn: rawConn, close: grpcsync.NewEvent()}
	c.connections = append(c.connections, conn)
	return conn, nil, nil
}
func (c *transportRestartCheckCreds) Info() credentials.ProtocolInfo {
	return credentials.ProtocolInfo{}
}
func (c *transportRestartCheckCreds) Clone() credentials.TransportCredentials {
	return c
}
func (c *transportRestartCheckCreds) OverrideServerName(string) error {
	return nil
}

// Tests that the client transport drains and restarts when next stream ID exceeds
// MaxStreamID. This test also verifies that subsequent RPCs use a new client
// transport and the old transport is closed.
func (s) TestClientTransportRestartsAfterStreamIDExhausted(t *testing.T) {
	// Set the transport's MaxStreamID to 4 to cause connection to drain after 2 RPCs.
	originalMaxStreamID := transport.MaxStreamID
	transport.MaxStreamID = 4
	defer func() {
		transport.MaxStreamID = originalMaxStreamID
	}()

	ss := &stubserver.StubServer{
		FullDuplexCallF: func(stream testgrpc.TestService_FullDuplexCallServer) error {
			if _, err := stream.Recv(); err != nil {
				return status.Errorf(codes.Internal, "unexpected error receiving: %v", err)
			}
			if err := stream.Send(&testpb.StreamingOutputCallResponse{}); err != nil {
				return status.Errorf(codes.Internal, "unexpected error sending: %v", err)
			}
			if recv, err := stream.Recv(); err != io.EOF {
				return status.Errorf(codes.Internal, "Recv = %v, %v; want _, io.EOF", recv, err)
			}
			return nil
		},
	}

	creds := &transportRestartCheckCreds{}
	if err := ss.Start(nil, grpc.WithTransportCredentials(creds)); err != nil {
		t.Fatalf("Starting stubServer: %v", err)
	}
	defer ss.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()

	var streams []testgrpc.TestService_FullDuplexCallClient

	const numStreams = 3
	// expected number of conns when each stream is created i.e., 3rd stream is created
	// on a new connection.
	expectedNumConns := [numStreams]int{1, 1, 2}

	// Set up 3 streams.
	for i := 0; i < numStreams; i++ {
		s, err := ss.Client.FullDuplexCall(ctx)
		if err != nil {
			t.Fatalf("Creating FullDuplex stream: %v", err)
		}
		streams = append(streams, s)
		// Verify expected num of conns after each stream is created.
		if len(creds.connections) != expectedNumConns[i] {
			t.Fatalf("Got number of connections created: %v, want: %v", len(creds.connections), expectedNumConns[i])
		}
	}

	// Verify all streams still work.
	for i, stream := range streams {
		if err := stream.Send(&testpb.StreamingOutputCallRequest{}); err != nil {
			t.Fatalf("Sending on stream %d: %v", i, err)
		}
		if _, err := stream.Recv(); err != nil {
			t.Fatalf("Receiving on stream %d: %v", i, err)
		}
	}

	for i, stream := range streams {
		if err := stream.CloseSend(); err != nil {
			t.Fatalf("CloseSend() on stream %d: %v", i, err)
		}
	}

	// Verifying first connection was closed.
	select {
	case <-creds.connections[0].close.Done():
	case <-ctx.Done():
		t.Fatal("Timeout expired when waiting for first client transport to close")
	}
}

// Tests that an RST_STREAM frame that causes an io.ErrUnexpectedEOF while
// reading a gRPC message is correctly converted to a gRPC status with code
// CANCELLED. The test sends a data frame with a partial gRPC message, followed
// by an RST_STREAM frame with HTTP/2 code CANCELLED. The test asserts the
// client receives the correct status.
func (s) TestRSTDuringMessageRead(t *testing.T) {
	lis, err := testutils.LocalTCPListener()
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()

	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	cc, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient(%s) = %v", lis.Addr().String(), err)
	}
	defer cc.Close()

	go func() {
		conn, err := lis.Accept()
		if err != nil {
			t.Errorf("lis.Accept() = %v", err)
			return
		}
		defer conn.Close()
		framer := http2.NewFramer(conn, conn)

		if _, err := io.ReadFull(conn, make([]byte, len(clientPreface))); err != nil {
			t.Errorf("Error while reading client preface: %v", err)
			return
		}
		if err := framer.WriteSettings(); err != nil {
			t.Errorf("Error while writing settings: %v", err)
			return
		}
		if err := framer.WriteSettingsAck(); err != nil {
			t.Errorf("Error while writing settings: %v", err)
			return
		}
		for ctx.Err() == nil {
			frame, err := framer.ReadFrame()
			if err != nil {
				return
			}
			switch frame := frame.(type) {
			case *http2.HeadersFrame:
				// When the client creates a stream, write a partial gRPC
				// message followed by an RST_STREAM.
				const messageLen = 2048
				buf := make([]byte, messageLen/2)
				// Write the gRPC message length header.
				binary.BigEndian.PutUint32(buf[1:5], uint32(messageLen))
				if err := framer.WriteData(1, false, buf); err != nil {
					return
				}
				framer.WriteRSTStream(1, http2.ErrCodeCancel)
			default:
				t.Logf("Server received frame: %v", frame)
			}
		}
	}()

	// The server will send a partial gRPC message before cancelling the stream.
	// The client should get a gRPC status with code CANCELLED.
	client := testgrpc.NewTestServiceClient(cc)
	if _, err := client.EmptyCall(ctx, &testpb.Empty{}); status.Code(err) != codes.Canceled {
		t.Fatalf("client.EmptyCall() returned %v; want status with code %v", err, codes.Canceled)
	}
}
