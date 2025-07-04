/*
 *
 * Copyright 2021 gRPC authors.
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

package priority

import (
	"sync/atomic"

	"github.com/ajith-anz/grpc-go/balancer"
	"github.com/ajith-anz/grpc-go/resolver"
)

// ignoreResolveNowClientConn wraps a balancer.ClientConn and overrides the
// ResolveNow() method to ignore those calls if the ignoreResolveNow bit is set.
type ignoreResolveNowClientConn struct {
	balancer.ClientConn
	ignoreResolveNow *uint32
}

func newIgnoreResolveNowClientConn(cc balancer.ClientConn, ignore bool) *ignoreResolveNowClientConn {
	ret := &ignoreResolveNowClientConn{
		ClientConn:       cc,
		ignoreResolveNow: new(uint32),
	}
	ret.updateIgnoreResolveNow(ignore)
	return ret
}

func (i *ignoreResolveNowClientConn) updateIgnoreResolveNow(b bool) {
	if b {
		atomic.StoreUint32(i.ignoreResolveNow, 1)
		return
	}
	atomic.StoreUint32(i.ignoreResolveNow, 0)

}

func (i ignoreResolveNowClientConn) ResolveNow(o resolver.ResolveNowOptions) {
	if atomic.LoadUint32(i.ignoreResolveNow) != 0 {
		return
	}
	i.ClientConn.ResolveNow(o)
}
