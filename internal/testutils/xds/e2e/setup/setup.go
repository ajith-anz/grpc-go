/*
 *
 * Copyright 2024 gRPC authors.
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

// Package setup implements setup helpers for xDS e2e tests.
package setup

import (
	"testing"

	"github.com/google/uuid"
	"github.com/ajith-anz/grpc-go/internal"
	"github.com/ajith-anz/grpc-go/internal/testutils/xds/e2e"
	"github.com/ajith-anz/grpc-go/resolver"
	_ "github.com/ajith-anz/grpc-go/xds" // Register the xds_resolver.
)

// ManagementServerAndResolver sets up an xDS management server, creates
// bootstrap configuration pointing to that server and creates an xDS resolver
// using that configuration.
//
// Registers a cleanup function on t to stop the management server.
//
// Returns the following:
// - the xDS management server
// - the node ID to use when talking to this management server
// - bootstrap configuration to use (if creating an xDS-enabled gRPC server)
// - xDS resolver builder (if creating an xDS-enabled gRPC client)
func ManagementServerAndResolver(t *testing.T) (*e2e.ManagementServer, string, []byte, resolver.Builder) {
	// Start an xDS management server.
	xdsServer := e2e.StartManagementServer(t, e2e.ManagementServerOptions{AllowResourceSubset: true})

	// Create bootstrap configuration pointing to the above management server.
	nodeID := uuid.New().String()
	bc := e2e.DefaultBootstrapContents(t, nodeID, xdsServer.Address)

	// Create an xDS resolver with the above bootstrap configuration.
	if internal.NewXDSResolverWithConfigForTesting == nil {
		t.Fatalf("internal.NewXDSResolverWithConfigForTesting is nil")
	}
	r, err := internal.NewXDSResolverWithConfigForTesting.(func([]byte) (resolver.Builder, error))(bc)
	if err != nil {
		t.Fatalf("Failed to create xDS resolver for testing: %v", err)
	}

	return xdsServer, nodeID, bc, r
}

// ManagementServerAndResolverWithSPIFFE is exactly the same as
// ManagementServerAndResolver, except that it uses a bootstrap configuration
// containing certificate providers utilizing SPIFFE test certificates.
func ManagementServerAndResolverWithSPIFFE(t *testing.T) (*e2e.ManagementServer,
	string, []byte, resolver.Builder) {
	// Start an xDS management server.
	xdsServer := e2e.StartManagementServer(t, e2e.ManagementServerOptions{AllowResourceSubset: true})

	// Create bootstrap configuration pointing to the above management server.
	nodeID := uuid.New().String()
	bc := e2e.SPIFFEBootstrapContents(t, nodeID, xdsServer.Address)

	// Create an xDS resolver with the above bootstrap configuration.
	if internal.NewXDSResolverWithConfigForTesting == nil {
		t.Fatalf("internal.NewXDSResolverWithConfigForTesting is nil")
	}
	r, err := internal.NewXDSResolverWithConfigForTesting.(func([]byte) (resolver.Builder, error))(bc)
	if err != nil {
		t.Fatalf("Failed to create xDS resolver for testing: %v", err)
	}

	return xdsServer, nodeID, bc, r
}
