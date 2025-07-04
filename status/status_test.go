/*
 *
 * Copyright 2017 gRPC authors.
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

package status

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	cpb "google.golang.org/genproto/googleapis/rpc/code"
	epb "google.golang.org/genproto/googleapis/rpc/errdetails"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/protoadapt"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/ajith-anz/grpc-go/codes"
	"github.com/ajith-anz/grpc-go/internal/grpctest"
	"github.com/ajith-anz/grpc-go/internal/status"
)

type s struct {
	grpctest.Tester
}

func Test(t *testing.T) {
	grpctest.RunSubTests(t, s{})
}

// errEqual is essentially a copy of testutils.StatusErrEqual(), to avoid a
// cyclic dependency.
func errEqual(err1, err2 error) bool {
	status1, ok := FromError(err1)
	if !ok {
		return false
	}
	status2, ok := FromError(err2)
	if !ok {
		return false
	}
	return proto.Equal(status1.Proto(), status2.Proto())
}

func (s) TestErrorsWithSameParameters(t *testing.T) {
	const description = "some description"
	e1 := Errorf(codes.AlreadyExists, description)
	e2 := Errorf(codes.AlreadyExists, description)
	if e1 == e2 || !errEqual(e1, e2) {
		t.Fatalf("Errors should be equivalent but unique - e1: %v, %v  e2: %p, %v", e1.(*status.Error), e1, e2.(*status.Error), e2)
	}
}

func (s) TestFromToProto(t *testing.T) {
	s := &spb.Status{
		Code:    int32(codes.Internal),
		Message: "test test test",
		Details: []*anypb.Any{{TypeUrl: "foo", Value: []byte{3, 2, 1}}},
	}

	err := FromProto(s)
	if got := err.Proto(); !proto.Equal(s, got) {
		t.Fatalf("Expected errors to be identical - s: %v  got: %v", s, got)
	}
}

func (s) TestFromNilProto(t *testing.T) {
	tests := []*Status{nil, FromProto(nil)}
	for _, s := range tests {
		if c := s.Code(); c != codes.OK {
			t.Errorf("s: %v - Expected s.Code() = OK; got %v", s, c)
		}
		if m := s.Message(); m != "" {
			t.Errorf("s: %v - Expected s.Message() = \"\"; got %q", s, m)
		}
		if p := s.Proto(); p != nil {
			t.Errorf("s: %v - Expected s.Proto() = nil; got %q", s, p)
		}
		if e := s.Err(); e != nil {
			t.Errorf("s: %v - Expected s.Err() = nil; got %v", s, e)
		}
	}
}

func (s) TestError(t *testing.T) {
	err := Error(codes.Internal, "test description")
	if got, want := err.Error(), "rpc error: code = Internal desc = test description"; got != want {
		t.Fatalf("err.Error() = %q; want %q", got, want)
	}
	s, _ := FromError(err)
	if got, want := s.Code(), codes.Internal; got != want {
		t.Fatalf("err.Code() = %s; want %s", got, want)
	}
	if got, want := s.Message(), "test description"; got != want {
		t.Fatalf("err.Message() = %s; want %s", got, want)
	}
}

func (s) TestErrorOK(t *testing.T) {
	err := Error(codes.OK, "foo")
	if err != nil {
		t.Fatalf("Error(codes.OK, _) = %p; want nil", err.(*status.Error))
	}
}

func (s) TestErrorProtoOK(t *testing.T) {
	s := &spb.Status{Code: int32(codes.OK)}
	if got := ErrorProto(s); got != nil {
		t.Fatalf("ErrorProto(%v) = %v; want nil", s, got)
	}
}

func (s) TestFromError(t *testing.T) {
	code, message := codes.Internal, "test description"
	err := Error(code, message)
	s, ok := FromError(err)
	if !ok || s.Code() != code || s.Message() != message || s.Err() == nil {
		t.Fatalf("FromError(%v) = %v, %v; want <Code()=%s, Message()=%q, Err()!=nil>, true", err, s, ok, code, message)
	}
}

func (s) TestFromErrorOK(t *testing.T) {
	code, message := codes.OK, ""
	s, ok := FromError(nil)
	if !ok || s.Code() != code || s.Message() != message || s.Err() != nil {
		t.Fatalf("FromError(nil) = %v, %v; want <Code()=%s, Message()=%q, Err=nil>, true", s, ok, code, message)
	}
}

type customError struct {
	Code    codes.Code
	Message string
	Details []*anypb.Any
}

func (c customError) Error() string {
	return fmt.Sprintf("rpc error: code = %s desc = %s", c.Code, c.Message)
}

func (c customError) GRPCStatus() *Status {
	return status.FromProto(&spb.Status{
		Code:    int32(c.Code),
		Message: c.Message,
		Details: c.Details,
	})
}

func (s) TestFromErrorImplementsInterface(t *testing.T) {
	code, message := codes.Internal, "test description"
	details := []*anypb.Any{{
		TypeUrl: "testUrl",
		Value:   []byte("testValue"),
	}}
	err := customError{
		Code:    code,
		Message: message,
		Details: details,
	}
	s, ok := FromError(err)
	if !ok || s.Code() != code || s.Message() != message || s.Err() == nil {
		t.Fatalf("FromError(%v) = %v, %v; want <Code()=%s, Message()=%q, Err()!=nil>, true", err, s, ok, code, message)
	}
	pd := s.Proto().GetDetails()
	if len(pd) != 1 || !proto.Equal(pd[0], details[0]) {
		t.Fatalf("s.Proto.GetDetails() = %v; want <Details()=%s>", pd, details)
	}
}

func (s) TestFromErrorUnknownError(t *testing.T) {
	code, message := codes.Unknown, "unknown error"
	err := errors.New("unknown error")
	s, ok := FromError(err)
	if ok || s.Code() != code || s.Message() != message {
		t.Fatalf("FromError(%v) = %v, %v; want <Code()=%s, Message()=%q>, false", err, s, ok, code, message)
	}
}

func (s) TestFromErrorWrapped(t *testing.T) {
	const code, message = codes.Internal, "test description"
	err := fmt.Errorf("wrapped error: %w", Error(code, message))
	s, ok := FromError(err)
	if !ok || s.Code() != code || s.Message() != err.Error() || s.Err() == nil {
		t.Fatalf("FromError(%v) = %v, %v; want <Code()=%s, Message()=%q, Err()!=nil>, true", err, s, ok, code, message)
	}
}

type customErrorNilStatus struct {
}

func (c customErrorNilStatus) Error() string {
	return "test"
}

func (c customErrorNilStatus) GRPCStatus() *Status {
	return nil
}

func (s) TestFromErrorImplementsInterfaceReturnsOKStatus(t *testing.T) {
	err := customErrorNilStatus{}
	s, ok := FromError(err)
	if ok || s.Code() != codes.Unknown || s.Message() != err.Error() {
		t.Fatalf("FromError(%v) = %v, %v; want <Code()=%s, Message()=%q, Err()!=nil>, true", err, s, ok, codes.Unknown, err.Error())
	}
}

func (s) TestFromErrorImplementsInterfaceReturnsOKStatusWrapped(t *testing.T) {
	err := fmt.Errorf("wrapping: %w", customErrorNilStatus{})
	s, ok := FromError(err)
	if ok || s.Code() != codes.Unknown || s.Message() != err.Error() {
		t.Fatalf("FromError(%v) = %v, %v; want <Code()=%s, Message()=%q, Err()!=nil>, true", err, s, ok, codes.Unknown, err.Error())
	}
}

func (s) TestFromErrorImplementsInterfaceWrapped(t *testing.T) {
	const code, message = codes.Internal, "test description"
	err := fmt.Errorf("wrapped error: %w", customError{Code: code, Message: message})
	s, ok := FromError(err)
	if !ok || s.Code() != code || s.Message() != err.Error() || s.Err() == nil {
		t.Fatalf("FromError(%v) = %v, %v; want <Code()=%s, Message()=%q, Err()!=nil>, true", err, s, ok, code, message)
	}
}

func (s) TestCode(t *testing.T) {
	const code = codes.Internal
	err := Error(code, "test description")
	if s := Code(err); s != code {
		t.Fatalf("Code(%v) = %v; want <Code()=%s>", err, s, code)
	}
}

func (s) TestCodeOK(t *testing.T) {
	if s, code := Code(nil), codes.OK; s != code {
		t.Fatalf("Code(%v) = %v; want <Code()=%s>", nil, s, code)
	}
}

func (s) TestCodeImplementsInterface(t *testing.T) {
	const code = codes.Internal
	err := customError{Code: code, Message: "test description"}
	if s := Code(err); s != code {
		t.Fatalf("Code(%v) = %v; want <Code()=%s>", err, s, code)
	}
}

func (s) TestCodeUnknownError(t *testing.T) {
	const code = codes.Unknown
	err := errors.New("unknown error")
	if s := Code(err); s != code {
		t.Fatalf("Code(%v) = %v; want <Code()=%s>", err, s, code)
	}
}

func (s) TestCodeWrapped(t *testing.T) {
	const code = codes.Internal
	err := fmt.Errorf("wrapped: %w", Error(code, "test description"))
	if s := Code(err); s != code {
		t.Fatalf("Code(%v) = %v; want <Code()=%s>", err, s, code)
	}
}

func (s) TestCodeImplementsInterfaceWrapped(t *testing.T) {
	const code = codes.Internal
	err := fmt.Errorf("wrapped: %w", customError{Code: code, Message: "test description"})
	if s := Code(err); s != code {
		t.Fatalf("Code(%v) = %v; want <Code()=%s>", err, s, code)
	}
}

func (s) TestConvertKnownError(t *testing.T) {
	code, message := codes.Internal, "test description"
	err := Error(code, message)
	s := Convert(err)
	if s.Code() != code || s.Message() != message {
		t.Fatalf("Convert(%v) = %v; want <Code()=%s, Message()=%q>", err, s, code, message)
	}
}

func (s) TestConvertUnknownError(t *testing.T) {
	code, message := codes.Unknown, "unknown error"
	err := errors.New("unknown error")
	s := Convert(err)
	if s.Code() != code || s.Message() != message {
		t.Fatalf("Convert(%v) = %v; want <Code()=%s, Message()=%q>", err, s, code, message)
	}
}

func (s) TestStatus_ErrorDetails(t *testing.T) {
	tests := []struct {
		code    codes.Code
		details []protoadapt.MessageV1
	}{
		{
			code:    codes.NotFound,
			details: nil,
		},
		{
			code: codes.NotFound,
			details: []protoadapt.MessageV1{
				&epb.ResourceInfo{
					ResourceType: "book",
					ResourceName: "projects/1234/books/5678",
					Owner:        "User",
				},
			},
		},
		{
			code: codes.Internal,
			details: []protoadapt.MessageV1{
				&epb.DebugInfo{
					StackEntries: []string{
						"first stack",
						"second stack",
					},
				},
			},
		},
		{
			code: codes.Unavailable,
			details: []protoadapt.MessageV1{
				&epb.RetryInfo{
					RetryDelay: &durationpb.Duration{Seconds: 60},
				},
				&epb.ResourceInfo{
					ResourceType: "book",
					ResourceName: "projects/1234/books/5678",
					Owner:        "User",
				},
			},
		},
	}

	for _, tc := range tests {
		s, err := New(tc.code, "").WithDetails(tc.details...)
		if err != nil {
			t.Fatalf("(%v).WithDetails(%+v) failed: %v", str(s), tc.details, err)
		}
		details := s.Details()
		if len(details) != len(tc.details) {
			t.Fatalf("len(s.Details()) = %d, want = %d.", len(details), len(tc.details))
		}
		for i := range details {
			if !proto.Equal(details[i].(protoreflect.ProtoMessage), tc.details[i].(protoreflect.ProtoMessage)) {
				t.Fatalf("(%v).Details()[%d] = %+v, want %+v", str(s), i, details[i], tc.details[i])
			}
		}
	}
}

func (s) TestStatus_WithDetails_Fail(t *testing.T) {
	tests := []*Status{
		nil,
		FromProto(nil),
		New(codes.OK, ""),
	}
	for _, s := range tests {
		if s, err := s.WithDetails(); err == nil || s != nil {
			t.Fatalf("(%v).WithDetails(%+v) = %v, %v; want nil, non-nil", str(s), []proto.Message{}, s, err)
		}
	}
}

func (s) TestStatus_ErrorDetails_Fail(t *testing.T) {
	tests := []struct {
		s    *Status
		want []any
	}{
		{
			s:    nil,
			want: nil,
		},
		{
			s:    FromProto(nil),
			want: nil,
		},
		{
			s:    New(codes.OK, ""),
			want: []any{},
		},
		{
			s: FromProto(&spb.Status{
				Code: int32(cpb.Code_CANCELLED),
				Details: []*anypb.Any{
					{
						TypeUrl: "",
						Value:   []byte{},
					},
					mustMarshalAny(&epb.ResourceInfo{
						ResourceType: "book",
						ResourceName: "projects/1234/books/5678",
						Owner:        "User",
					}),
				},
			}),
			want: []any{
				errors.New("invalid empty type URL"),
				&epb.ResourceInfo{
					ResourceType: "book",
					ResourceName: "projects/1234/books/5678",
					Owner:        "User",
				},
			},
		},
	}
	for _, tc := range tests {
		details := tc.s.Details()
		if len(details) != len(tc.want) {
			t.Fatalf("len(s.Details()) = %d, want = %d.", len(details), len(tc.want))
		}
		for i, d := range details {
			// s.Details can either contain an error or a proto message.  We
			// want to do a compare the proto message for an Equal match, and
			// for errors only check for presence.
			if _, ok := d.(error); ok {
				if (d != nil) != (tc.want[i] != nil) {
					t.Fatalf("s.Details()[%v] was %v; want %v", i, d, tc.want[i])
				}
				continue
			}
			if !cmp.Equal(d, tc.want[i], cmp.Comparer(proto.Equal)) {
				t.Fatalf("s.Details()[%v] was %v; want %v", i, d, tc.want[i])
			}
		}
	}
}

func str(s *Status) string {
	if s == nil {
		return "nil"
	}
	if s.Proto() == nil {
		return "<Code=OK>"
	}
	return fmt.Sprintf("<Code=%v, Message=%q, Details=%+v>", s.Code(), s.Message(), s.Details())
}

// mustMarshalAny converts a protobuf message to an any.
func mustMarshalAny(msg proto.Message) *anypb.Any {
	m, err := anypb.New(msg)
	if err != nil {
		panic(fmt.Sprintf("anypb.New(%+v) failed: %v", msg, err))
	}
	return m
}

func (s) TestFromContextError(t *testing.T) {
	testCases := []struct {
		in   error
		want *Status
	}{
		{in: nil, want: New(codes.OK, "")},
		{in: context.DeadlineExceeded, want: New(codes.DeadlineExceeded, context.DeadlineExceeded.Error())},
		{in: context.Canceled, want: New(codes.Canceled, context.Canceled.Error())},
		{in: errors.New("other"), want: New(codes.Unknown, "other")},
		{in: fmt.Errorf("wrapped: %w", context.DeadlineExceeded), want: New(codes.DeadlineExceeded, "wrapped: "+context.DeadlineExceeded.Error())},
		{in: fmt.Errorf("wrapped: %w", context.Canceled), want: New(codes.Canceled, "wrapped: "+context.Canceled.Error())},
	}
	for _, tc := range testCases {
		got := FromContextError(tc.in)
		if got.Code() != tc.want.Code() || got.Message() != tc.want.Message() {
			t.Errorf("FromContextError(%v) = %v; want %v", tc.in, got, tc.want)
		}
	}
}
