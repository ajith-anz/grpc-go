/*
 *
 * Copyright 2014 gRPC authors.
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

package grpc

import (
	"testing"

	"github.com/ajith-anz/grpc-go/encoding"
	"github.com/ajith-anz/grpc-go/encoding/proto"
)

func (s) TestGetCodecForProtoIsNotNil(t *testing.T) {
	if encoding.GetCodecV2(proto.Name) == nil {
		t.Fatalf("encoding.GetCodec(%q) must not be nil by default", proto.Name)
	}
}
