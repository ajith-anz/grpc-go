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

package grpcsync

import (
	"testing"

	"github.com/ajith-anz/grpc-go/internal/grpctest"
)

type s struct {
	grpctest.Tester
}

func Test(t *testing.T) {
	grpctest.RunSubTests(t, s{})
}

func (s) TestEventHasFired(t *testing.T) {
	e := NewEvent()
	if e.HasFired() {
		t.Fatal("e.HasFired() = true; want false")
	}
	if !e.Fire() {
		t.Fatal("e.Fire() = false; want true")
	}
	if !e.HasFired() {
		t.Fatal("e.HasFired() = false; want true")
	}
}

func (s) TestEventDoneChannel(t *testing.T) {
	e := NewEvent()
	select {
	case <-e.Done():
		t.Fatal("e.HasFired() = true; want false")
	default:
	}
	if !e.Fire() {
		t.Fatal("e.Fire() = false; want true")
	}
	select {
	case <-e.Done():
	default:
		t.Fatal("e.HasFired() = false; want true")
	}
}

func (s) TestEventMultipleFires(t *testing.T) {
	e := NewEvent()
	if e.HasFired() {
		t.Fatal("e.HasFired() = true; want false")
	}
	if !e.Fire() {
		t.Fatal("e.Fire() = false; want true")
	}
	for i := 0; i < 3; i++ {
		if !e.HasFired() {
			t.Fatal("e.HasFired() = false; want true")
		}
		if e.Fire() {
			t.Fatal("e.Fire() = true; want false")
		}
	}
}
