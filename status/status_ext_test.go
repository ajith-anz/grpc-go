/*
 *
 * Copyright 2019 gRPC authors.
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

package status_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/ajith-anz/grpc-go"
	"github.com/ajith-anz/grpc-go/codes"
	"github.com/ajith-anz/grpc-go/internal/grpctest"
	"github.com/ajith-anz/grpc-go/internal/stubserver"
	"github.com/ajith-anz/grpc-go/internal/testutils"
	"github.com/ajith-anz/grpc-go/metadata"
	"github.com/ajith-anz/grpc-go/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/protoadapt"
	"google.golang.org/protobuf/testing/protocmp"

	testpb "github.com/ajith-anz/grpc-go/interop/grpc_testing"
	tpb "github.com/ajith-anz/grpc-go/testdata/grpc_testing_not_regenerated"
)

const defaultTestTimeout = 10 * time.Second

type s struct {
	grpctest.Tester
}

func Test(t *testing.T) {
	grpctest.RunSubTests(t, s{})
}

func errWithDetails(t *testing.T, s *status.Status, details ...protoadapt.MessageV1) error {
	t.Helper()
	res, err := s.WithDetails(details...)
	if err != nil {
		t.Fatalf("(%v).WithDetails(%v) = %v, %v; want _, <nil>", s, details, res, err)
	}
	return res.Err()
}

func (s) TestErrorIs(t *testing.T) {
	// Test errors.
	testErr := status.Error(codes.Internal, "internal server error")
	testErrWithDetails := errWithDetails(t, status.New(codes.Internal, "internal server error"), &testpb.Empty{})

	// Test cases.
	testCases := []struct {
		err1, err2 error
		want       bool
	}{
		{err1: testErr, err2: nil, want: false},
		{err1: testErr, err2: status.Error(codes.Internal, "internal server error"), want: true},
		{err1: testErr, err2: status.Error(codes.Internal, "internal error"), want: false},
		{err1: testErr, err2: status.Error(codes.Unknown, "internal server error"), want: false},
		{err1: testErr, err2: errors.New("non-grpc error"), want: false},
		{err1: testErrWithDetails, err2: status.Error(codes.Internal, "internal server error"), want: false},
		{err1: testErrWithDetails, err2: errWithDetails(t, status.New(codes.Internal, "internal server error"), &testpb.Empty{}), want: true},
		{err1: testErrWithDetails, err2: errWithDetails(t, status.New(codes.Internal, "internal server error"), &testpb.Empty{}, &testpb.Empty{}), want: false},
	}

	for _, tc := range testCases {
		isError, ok := tc.err1.(interface{ Is(target error) bool })
		if !ok {
			t.Errorf("(%v) does not implement is", tc.err1)
			continue
		}

		is := isError.Is(tc.err2)
		if is != tc.want {
			t.Errorf("(%v).Is(%v) = %t; want %t", tc.err1, tc.err2, is, tc.want)
		}
	}
}

// TestStatusDetails tests how gRPC handles grpc-status-details-bin, especially
// in cases where it doesn't match the grpc-status trailer or contains arbitrary
// data.
func (s) TestStatusDetails(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()

	for _, serverType := range []struct {
		name            string
		startServerFunc func(*stubserver.StubServer) error
	}{{
		name: "normal server",
		startServerFunc: func(ss *stubserver.StubServer) error {
			return ss.StartServer()
		},
	}, {
		name: "handler server",
		startServerFunc: func(ss *stubserver.StubServer) error {
			return ss.StartHandlerServer()
		},
	}} {
		t.Run(serverType.name, func(t *testing.T) {
			// Convenience function for making a status including details.
			detailErr := func(c codes.Code, m string) error {
				s, err := status.New(c, m).WithDetails(&testpb.SimpleRequest{
					Payload: &testpb.Payload{Body: []byte("detail msg")},
				})
				if err != nil {
					t.Fatalf("Error adding details: %v", err)
				}
				return s.Err()
			}

			serialize := func(err error) string {
				buf, _ := proto.Marshal(status.Convert(err).Proto())
				return string(buf)
			}

			testCases := []struct {
				name        string
				trailerSent metadata.MD
				errSent     error
				trailerWant []string
				errWant     error
				errContains error
			}{{
				name:        "basic without details",
				trailerSent: metadata.MD{},
				errSent:     status.Error(codes.Aborted, "test msg"),
				errWant:     status.Error(codes.Aborted, "test msg"),
			}, {
				name:        "basic without details passes through trailers",
				trailerSent: metadata.MD{"grpc-status-details-bin": []string{"random text"}},
				errSent:     status.Error(codes.Aborted, "test msg"),
				trailerWant: []string{"random text"},
				errWant:     status.Error(codes.Aborted, "test msg"),
			}, {
				name:        "basic without details conflicts with manual details",
				trailerSent: metadata.MD{"grpc-status-details-bin": []string{serialize(status.Error(codes.Canceled, "test msg"))}},
				errSent:     status.Error(codes.Aborted, "test msg"),
				trailerWant: []string{serialize(status.Error(codes.Canceled, "test msg"))},
				errContains: status.Error(codes.Internal, "mismatch"),
			}, {
				name:        "basic with details",
				trailerSent: metadata.MD{},
				errSent:     detailErr(codes.Aborted, "test msg"),
				trailerWant: []string{serialize(detailErr(codes.Aborted, "test msg"))},
				errWant:     detailErr(codes.Aborted, "test msg"),
			}, {
				name:        "basic with details discards user's trailers",
				trailerSent: metadata.MD{"grpc-status-details-bin": []string{"will be ignored"}},
				errSent:     detailErr(codes.Aborted, "test msg"),
				trailerWant: []string{serialize(detailErr(codes.Aborted, "test msg"))},
				errWant:     detailErr(codes.Aborted, "test msg"),
			}}

			for _, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					// Start a simple server that returns the trailer and error it receives from
					// channels.
					ss := &stubserver.StubServer{
						UnaryCallF: func(ctx context.Context, _ *testpb.SimpleRequest) (*testpb.SimpleResponse, error) {
							grpc.SetTrailer(ctx, tc.trailerSent)
							return nil, tc.errSent
						},
					}
					if err := serverType.startServerFunc(ss); err != nil {
						t.Fatalf("Error starting endpoint server: %v", err)
					}
					if err := ss.StartClient(); err != nil {
						t.Fatalf("Error starting endpoint client: %v", err)
					}
					defer ss.Stop()

					trailerGot := metadata.MD{}
					_, errGot := ss.Client.UnaryCall(ctx, &testpb.SimpleRequest{}, grpc.Trailer(&trailerGot))
					gsdb := trailerGot["grpc-status-details-bin"]
					if !cmp.Equal(gsdb, tc.trailerWant) {
						t.Errorf("Trailer got: %v; want: %v", gsdb, tc.trailerWant)
					}
					if tc.errWant != nil && !testutils.StatusErrEqual(errGot, tc.errWant) {
						t.Errorf("Err got: %v; want: %v", errGot, tc.errWant)
					}
					if tc.errContains != nil && (status.Code(errGot) != status.Code(tc.errContains) || !strings.Contains(status.Convert(errGot).Message(), status.Convert(tc.errContains).Message())) {
						t.Errorf("Err got: %v; want: (Contains: %v)", errGot, tc.errWant)
					}
				})
			}
		})
	}
}

// TestStatus_ErrorDetailsMessageV1 verifies backward compatibility of the
// status.Details() method when using protobuf code generated with only the
// MessageV1 API implementation.
func (s) TestStatus_ErrorDetailsMessageV1(t *testing.T) {
	details := []protoadapt.MessageV1{
		&tpb.SimpleMessage{Data: "abc"},
	}
	s, err := status.New(codes.Aborted, "").WithDetails(details...)
	if err != nil {
		t.Fatalf("(%v).WithDetails(%+v) failed: %v", s, details, err)
	}
	gotDetails := s.Details()
	for i, msg := range gotDetails {
		if got, want := reflect.TypeOf(msg), reflect.TypeOf(details[i]); got != want {
			t.Errorf("reflect.Typeof(%v) = %v, want = %v", msg, got, want)
		}
		if _, ok := msg.(protoadapt.MessageV1); !ok {
			t.Errorf("(%v).Details() returned message that doesn't implement protoadapt.MessageV1: %v", s, msg)
		}
		if diff := cmp.Diff(msg, details[i], protocmp.Transform()); diff != "" {
			t.Errorf("(%v).Details got unexpected output, diff (-got +want):\n%s", s, diff)
		}
	}
}

// TestStatus_ErrorDetailsMessageV1AndV2 verifies that status.Details() method
// returns the same message types when using protobuf code generated with both the
// MessageV1 and MessageV2 API implementations.
func (s) TestStatus_ErrorDetailsMessageV1AndV2(t *testing.T) {
	details := []protoadapt.MessageV1{
		&testpb.Empty{},
	}
	s, err := status.New(codes.Aborted, "").WithDetails(details...)
	if err != nil {
		t.Fatalf("(%v).WithDetails(%+v) failed: %v", s, details, err)
	}
	gotDetails := s.Details()
	for i, msg := range gotDetails {
		if got, want := reflect.TypeOf(msg), reflect.TypeOf(details[i]); got != want {
			t.Errorf("reflect.Typeof(%v) = %v, want = %v", msg, got, want)
		}
		if _, ok := msg.(protoadapt.MessageV1); !ok {
			t.Errorf("(%v).Details() returned message that doesn't implement protoadapt.MessageV1: %v", s, msg)
		}
		if _, ok := msg.(protoadapt.MessageV2); !ok {
			t.Errorf("(%v).Details() returned message that doesn't implement protoadapt.MessageV2: %v", s, msg)
		}
		if diff := cmp.Diff(msg, details[i], protocmp.Transform()); diff != "" {
			t.Errorf("(%v).Details got unexpected output, diff (-got +want):\n%s", s, diff)
		}
	}
}
