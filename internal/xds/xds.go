/*
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
 */

// Package xds contains methods to Get/Set handshake cluster names. It is separated
// out from the top level /internal package to avoid circular dependencies.
package xds

import (
	"github.com/ajith-anz/grpc-go/attributes"
	"github.com/ajith-anz/grpc-go/resolver"
)

// handshakeClusterNameKey is the type used as the key to store cluster name in
// the Attributes field of resolver.Address.
type handshakeClusterNameKey struct{}

// SetXDSHandshakeClusterName returns a copy of addr in which the Attributes field
// is updated with the cluster name.
func SetXDSHandshakeClusterName(addr resolver.Address, clusterName string) resolver.Address {
	addr.Attributes = addr.Attributes.WithValue(handshakeClusterNameKey{}, clusterName)
	return addr
}

// GetXDSHandshakeClusterName returns cluster name stored in attr.
func GetXDSHandshakeClusterName(attr *attributes.Attributes) (string, bool) {
	v := attr.Value(handshakeClusterNameKey{})
	name, ok := v.(string)
	return name, ok
}
