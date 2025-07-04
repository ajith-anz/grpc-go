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
 */

package xdsclient

import (
	"github.com/ajith-anz/grpc-go/internal/xds/bootstrap"
	"github.com/ajith-anz/grpc-go/xds/internal/xdsclient/load"
)

// ReportLoad starts a load reporting stream to the given server. All load
// reports to the same server share the LRS stream.
//
// It returns a Store for the user to report loads, a function to cancel the
// load reporting stream.
func (c *clientImpl) ReportLoad(server *bootstrap.ServerConfig) (*load.Store, func()) {
	xc, releaseChannelRef, err := c.getChannelForLRS(server)
	if err != nil {
		c.logger.Warningf("Failed to create a channel to the management server to report load: %v", server, err)
		return nil, func() {}
	}
	load, stopLoadReporting := xc.reportLoad()
	return load, func() {
		stopLoadReporting()
		releaseChannelRef()
	}
}
