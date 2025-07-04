/*
 *
 * Copyright 2022 gRPC authors.
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

package xdsclient

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	v3statuspb "github.com/envoyproxy/go-control-plane/envoy/service/status/v3"
	estats "github.com/ajith-anz/grpc-go/experimental/stats"
	"github.com/ajith-anz/grpc-go/internal"
	"github.com/ajith-anz/grpc-go/internal/backoff"
	"github.com/ajith-anz/grpc-go/internal/grpclog"
	"github.com/ajith-anz/grpc-go/internal/grpcsync"
	"github.com/ajith-anz/grpc-go/internal/xds/bootstrap"
	xdsclientinternal "github.com/ajith-anz/grpc-go/xds/internal/xdsclient/internal"
	"github.com/ajith-anz/grpc-go/xds/internal/xdsclient/transport"
	"github.com/ajith-anz/grpc-go/xds/internal/xdsclient/transport/ads"
	"github.com/ajith-anz/grpc-go/xds/internal/xdsclient/transport/grpctransport"
	"github.com/ajith-anz/grpc-go/xds/internal/xdsclient/xdsresource"
)

const (
	// NameForServer represents the value to be passed as name when creating an xDS
	// client from xDS-enabled gRPC servers. This is a well-known dedicated key
	// value, and is defined in gRFC A71.
	NameForServer = "#server"

	defaultWatchExpiryTimeout = 15 * time.Second
)

var (
	_ XDSClient = &clientImpl{}

	// ErrClientClosed is returned when the xDS client is closed.
	ErrClientClosed = errors.New("xds: the xDS client is closed")

	// The following functions are no-ops in the actual code, but can be
	// overridden in tests to give them visibility into certain events.
	xdsClientImplCreateHook = func(string) {}
	xdsClientImplCloseHook  = func(string) {}

	defaultExponentialBackoff = backoff.DefaultExponential.Backoff

	xdsClientResourceUpdatesValidMetric = estats.RegisterInt64Count(estats.MetricDescriptor{
		Name:        "grpc.xds_client.resource_updates_valid",
		Description: "A counter of resources received that were considered valid. The counter will be incremented even for resources that have not changed.",
		Unit:        "resource",
		Labels:      []string{"grpc.target", "grpc.xds.server", "grpc.xds.resource_type"},
		Default:     false,
	})
	xdsClientResourceUpdatesInvalidMetric = estats.RegisterInt64Count(estats.MetricDescriptor{
		Name:        "grpc.xds_client.resource_updates_invalid",
		Description: "A counter of resources received that were considered invalid.",
		Unit:        "resource",
		Labels:      []string{"grpc.target", "grpc.xds.server", "grpc.xds.resource_type"},
		Default:     false,
	})
	xdsClientServerFailureMetric = estats.RegisterInt64Count(estats.MetricDescriptor{
		Name:        "grpc.xds_client.server_failure",
		Description: "A counter of xDS servers going from healthy to unhealthy. A server goes unhealthy when we have a connectivity failure or when the ADS stream fails without seeing a response message, as per gRFC A57.",
		Unit:        "failure",
		Labels:      []string{"grpc.target", "grpc.xds.server"},
		Default:     false,
	})
)

// clientImpl is the real implementation of the xDS client. The exported Client
// is a wrapper of this struct with a ref count.
type clientImpl struct {
	// The following fields are initialized at creation time and are read-only
	// after that, and therefore can be accessed without a mutex.
	done               *grpcsync.Event              // Fired when the client is closed.
	topLevelAuthority  *authority                   // The top-level authority, used only for old-style names without an authority.
	authorities        map[string]*authority        // Map from authority names in bootstrap to authority struct.
	config             *bootstrap.Config            // Complete bootstrap configuration.
	watchExpiryTimeout time.Duration                // Expiry timeout for ADS watch.
	backoff            func(int) time.Duration      // Backoff for ADS and LRS stream failures.
	transportBuilder   transport.Builder            // Builder to create transports to xDS server.
	resourceTypes      *resourceTypeRegistry        // Registry of resource types, for parsing incoming ADS responses.
	serializer         *grpcsync.CallbackSerializer // Serializer for invoking resource watcher callbacks.
	serializerClose    func()                       // Function to close the serializer.
	logger             *grpclog.PrefixLogger        // Logger for this client.
	metricsRecorder    estats.MetricsRecorder       // Metrics recorder for metrics.
	target             string                       // The gRPC target for this client.

	// The clientImpl owns a bunch of channels to individual xDS servers
	// specified in the bootstrap configuration. Authorities acquire references
	// to these channels based on server configs within the authority config.
	// The clientImpl maintains a list of interested authorities for each of
	// these channels, and forwards updates from the channels to each of these
	// authorities.
	//
	// Once all references to a channel are dropped, the channel is closed.
	channelsMu        sync.Mutex
	xdsActiveChannels map[string]*channelState // Map from server config to in-use xdsChannels.
}

func init() {
	internal.TriggerXDSResourceNotFoundForTesting = triggerXDSResourceNotFoundForTesting
	xdsclientinternal.ResourceWatchStateForTesting = resourceWatchStateForTesting

	DefaultPool = &Pool{clients: make(map[string]*clientRefCounted)}
}

// newClientImpl returns a new xdsClient with the given config.
func newClientImpl(config *bootstrap.Config, watchExpiryTimeout time.Duration, streamBackoff func(int) time.Duration, mr estats.MetricsRecorder, target string) (*clientImpl, error) {
	ctx, cancel := context.WithCancel(context.Background())
	c := &clientImpl{
		metricsRecorder:    mr,
		target:             target,
		done:               grpcsync.NewEvent(),
		authorities:        make(map[string]*authority),
		config:             config,
		watchExpiryTimeout: watchExpiryTimeout,
		backoff:            streamBackoff,
		serializer:         grpcsync.NewCallbackSerializer(ctx),
		serializerClose:    cancel,
		transportBuilder:   &grpctransport.Builder{},
		resourceTypes:      newResourceTypeRegistry(),
		xdsActiveChannels:  make(map[string]*channelState),
	}

	for name, cfg := range config.Authorities() {
		// If server configs are specified in the authorities map, use that.
		// Else, use the top-level server configs.
		serverCfg := config.XDSServers()
		if len(cfg.XDSServers) >= 1 {
			serverCfg = cfg.XDSServers
		}
		c.authorities[name] = newAuthority(authorityBuildOptions{
			serverConfigs:    serverCfg,
			name:             name,
			serializer:       c.serializer,
			getChannelForADS: c.getChannelForADS,
			logPrefix:        clientPrefix(c),
			target:           target,
			metricsRecorder:  c.metricsRecorder,
		})
	}
	c.topLevelAuthority = newAuthority(authorityBuildOptions{
		serverConfigs:    config.XDSServers(),
		name:             "",
		serializer:       c.serializer,
		getChannelForADS: c.getChannelForADS,
		logPrefix:        clientPrefix(c),
		target:           target,
		metricsRecorder:  c.metricsRecorder,
	})
	c.logger = prefixLogger(c)
	return c, nil
}

// BootstrapConfig returns the configuration read from the bootstrap file.
// Callers must treat the return value as read-only.
func (c *clientImpl) BootstrapConfig() *bootstrap.Config {
	return c.config
}

// close closes the xDS client and releases all resources.
func (c *clientImpl) close() {
	if c.done.HasFired() {
		return
	}
	c.done.Fire()

	c.topLevelAuthority.close()
	for _, a := range c.authorities {
		a.close()
	}

	// Channel close cannot be invoked with the lock held, because it can race
	// with stream failure happening at the same time. The latter will callback
	// into the clientImpl and will attempt to grab the lock. This will result
	// in a deadlock. So instead, we release the lock and wait for all active
	// channels to be closed.
	var channelsToClose []*xdsChannel
	c.channelsMu.Lock()
	for _, cs := range c.xdsActiveChannels {
		channelsToClose = append(channelsToClose, cs.channel)
	}
	c.xdsActiveChannels = nil
	c.channelsMu.Unlock()
	for _, c := range channelsToClose {
		c.close()
	}

	c.serializerClose()
	<-c.serializer.Done()

	for _, s := range c.config.XDSServers() {
		for _, f := range s.Cleanups() {
			f()
		}
	}
	for _, a := range c.config.Authorities() {
		for _, s := range a.XDSServers {
			for _, f := range s.Cleanups() {
				f()
			}
		}
	}
	c.logger.Infof("Shutdown")
}

// getChannelForADS returns an xdsChannel for the given server configuration.
//
// If an xdsChannel exists for the given server configuration, it is returned.
// Else a new one is created. It also ensures that the calling authority is
// added to the set of interested authorities for the returned channel.
//
// It returns the xdsChannel and a function to release the calling authority's
// reference on the channel. The caller must call the cancel function when it is
// no longer interested in this channel.
//
// A non-nil error is returned if an xdsChannel was not created.
func (c *clientImpl) getChannelForADS(serverConfig *bootstrap.ServerConfig, callingAuthority *authority) (*xdsChannel, func(), error) {
	if c.done.HasFired() {
		return nil, nil, ErrClientClosed
	}

	initLocked := func(s *channelState) {
		if c.logger.V(2) {
			c.logger.Infof("Adding authority %q to the set of interested authorities for channel [%p]", callingAuthority.name, s.channel)
		}
		s.interestedAuthorities[callingAuthority] = true
	}
	deInitLocked := func(s *channelState) {
		if c.logger.V(2) {
			c.logger.Infof("Removing authority %q from the set of interested authorities for channel [%p]", callingAuthority.name, s.channel)
		}
		delete(s.interestedAuthorities, callingAuthority)
	}

	return c.getOrCreateChannel(serverConfig, initLocked, deInitLocked)
}

// getChannelForLRS returns an xdsChannel for the given server configuration.
//
// If an xdsChannel exists for the given server configuration, it is returned.
// Else a new one is created. A reference count that tracks the number of LRS
// calls on the returned channel is incremented before returning the channel.
//
// It returns the xdsChannel and a function to decrement the reference count
// that tracks the number of LRS calls on the returned channel. The caller must
// call the cancel function when it is no longer interested in this channel.
//
// A non-nil error is returned if an xdsChannel was not created.
func (c *clientImpl) getChannelForLRS(serverConfig *bootstrap.ServerConfig) (*xdsChannel, func(), error) {
	if c.done.HasFired() {
		return nil, nil, ErrClientClosed
	}

	initLocked := func(s *channelState) { s.lrsRefs++ }
	deInitLocked := func(s *channelState) { s.lrsRefs-- }

	return c.getOrCreateChannel(serverConfig, initLocked, deInitLocked)
}

// getOrCreateChannel returns an xdsChannel for the given server configuration.
//
// If an active xdsChannel exists for the given server configuration, it is
// returned. If an idle xdsChannel exists for the given server configuration, it
// is revived from the idle cache and returned. Else a new one is created.
//
// The initLocked function runs some initialization logic before the channel is
// returned. This includes adding the calling authority to the set of interested
// authorities for the channel or incrementing the count of the number of LRS
// calls on the channel.
//
// The deInitLocked function runs some cleanup logic when the returned cleanup
// function is called. This involves removing the calling authority from the set
// of interested authorities for the channel or decrementing the count of the
// number of LRS calls on the channel.
//
// Both initLocked and deInitLocked are called with the c.channelsMu held.
//
// Returns the xdsChannel and a cleanup function to be invoked when the channel
// is no longer required. A non-nil error is returned if an xdsChannel was not
// created.
func (c *clientImpl) getOrCreateChannel(serverConfig *bootstrap.ServerConfig, initLocked, deInitLocked func(*channelState)) (*xdsChannel, func(), error) {
	c.channelsMu.Lock()
	defer c.channelsMu.Unlock()

	if c.logger.V(2) {
		c.logger.Infof("Received request for a reference to an xdsChannel for server config %q", serverConfig)
	}

	// Use an existing channel, if one exists for this server config.
	if state, ok := c.xdsActiveChannels[serverConfig.String()]; ok {
		if c.logger.V(2) {
			c.logger.Infof("Reusing an existing xdsChannel for server config %q", serverConfig)
		}
		initLocked(state)
		return state.channel, c.releaseChannel(serverConfig, state, deInitLocked), nil
	}

	if c.logger.V(2) {
		c.logger.Infof("Creating a new xdsChannel for server config %q", serverConfig)
	}

	// Create a new transport and create a new xdsChannel, and add it to the
	// map of xdsChannels.
	tr, err := c.transportBuilder.Build(transport.BuildOptions{ServerConfig: serverConfig})
	if err != nil {
		return nil, func() {}, fmt.Errorf("xds: failed to create transport for server config %s: %v", serverConfig, err)
	}
	state := &channelState{
		parent:                c,
		serverConfig:          serverConfig,
		interestedAuthorities: make(map[*authority]bool),
	}
	channel, err := newXDSChannel(xdsChannelOpts{
		transport:          tr,
		serverConfig:       serverConfig,
		bootstrapConfig:    c.config,
		resourceTypeGetter: c.resourceTypes.get,
		eventHandler:       state,
		backoff:            c.backoff,
		watchExpiryTimeout: c.watchExpiryTimeout,
		logPrefix:          clientPrefix(c),
	})
	if err != nil {
		return nil, func() {}, fmt.Errorf("xds: failed to create xdsChannel for server config %s: %v", serverConfig, err)
	}
	state.channel = channel
	c.xdsActiveChannels[serverConfig.String()] = state
	initLocked(state)
	return state.channel, c.releaseChannel(serverConfig, state, deInitLocked), nil
}

// releaseChannel is a function that is called when a reference to an xdsChannel
// needs to be released. It handles closing channels with no active references.
//
// The function takes the following parameters:
// - serverConfig: the server configuration for the xdsChannel
// - state: the state of the xdsChannel
// - deInitLocked: a function that performs any necessary cleanup for the xdsChannel
//
// The function returns another function that can be called to release the
// reference to the xdsChannel. This returned function is idempotent, meaning
// it can be called multiple times without any additional effect.
func (c *clientImpl) releaseChannel(serverConfig *bootstrap.ServerConfig, state *channelState, deInitLocked func(*channelState)) func() {
	return sync.OnceFunc(func() {
		c.channelsMu.Lock()

		if c.logger.V(2) {
			c.logger.Infof("Received request to release a reference to an xdsChannel for server config %q", serverConfig)
		}
		deInitLocked(state)

		// The channel has active users. Do nothing and return.
		if state.lrsRefs != 0 || len(state.interestedAuthorities) != 0 {
			if c.logger.V(2) {
				c.logger.Infof("xdsChannel %p has other active references", state.channel)
			}
			c.channelsMu.Unlock()
			return
		}

		delete(c.xdsActiveChannels, serverConfig.String())
		if c.logger.V(2) {
			c.logger.Infof("Closing xdsChannel [%p] for server config %s", state.channel, serverConfig)
		}
		channelToClose := state.channel
		c.channelsMu.Unlock()

		channelToClose.close()
	})
}

// dumpResources returns the status and contents of all xDS resources.
func (c *clientImpl) dumpResources() *v3statuspb.ClientConfig {
	retCfg := c.topLevelAuthority.dumpResources()
	for _, a := range c.authorities {
		retCfg = append(retCfg, a.dumpResources()...)
	}

	return &v3statuspb.ClientConfig{
		Node:              c.config.Node(),
		GenericXdsConfigs: retCfg,
	}
}

// channelState represents the state of an xDS channel. It tracks the number of
// LRS references, the authorities interested in the channel, and the server
// configuration used for the channel.
//
// It receives callbacks for events on the underlying ADS stream and invokes
// corresponding callbacks on interested authorities.
type channelState struct {
	parent       *clientImpl
	serverConfig *bootstrap.ServerConfig

	// Access to the following fields should be protected by the parent's
	// channelsMu.
	channel               *xdsChannel
	lrsRefs               int
	interestedAuthorities map[*authority]bool
}

func (cs *channelState) adsStreamFailure(err error) {
	if cs.parent.done.HasFired() {
		return
	}

	if xdsresource.ErrType(err) != xdsresource.ErrTypeStreamFailedAfterRecv {
		xdsClientServerFailureMetric.Record(cs.parent.metricsRecorder, 1, cs.parent.target, cs.serverConfig.ServerURI())
	}

	cs.parent.channelsMu.Lock()
	defer cs.parent.channelsMu.Unlock()
	for authority := range cs.interestedAuthorities {
		authority.adsStreamFailure(cs.serverConfig, err)
	}
}

func (cs *channelState) adsResourceUpdate(typ xdsresource.Type, updates map[string]ads.DataAndErrTuple, md xdsresource.UpdateMetadata, onDone func()) {
	if cs.parent.done.HasFired() {
		return
	}

	cs.parent.channelsMu.Lock()
	defer cs.parent.channelsMu.Unlock()

	if len(cs.interestedAuthorities) == 0 {
		onDone()
		return
	}

	authorityCnt := new(atomic.Int64)
	authorityCnt.Add(int64(len(cs.interestedAuthorities)))
	done := func() {
		if authorityCnt.Add(-1) == 0 {
			onDone()
		}
	}
	for authority := range cs.interestedAuthorities {
		authority.adsResourceUpdate(cs.serverConfig, typ, updates, md, done)
	}
}

func (cs *channelState) adsResourceDoesNotExist(typ xdsresource.Type, resourceName string) {
	if cs.parent.done.HasFired() {
		return
	}

	cs.parent.channelsMu.Lock()
	defer cs.parent.channelsMu.Unlock()
	for authority := range cs.interestedAuthorities {
		authority.adsResourceDoesNotExist(typ, resourceName)
	}
}

// clientRefCounted is ref-counted, and to be shared by the xds resolver and
// balancer implementations, across multiple ClientConns and Servers.
type clientRefCounted struct {
	*clientImpl

	refCount int32 // accessed atomically
}

func (c *clientRefCounted) incrRef() int32 {
	return atomic.AddInt32(&c.refCount, 1)
}

func (c *clientRefCounted) decrRef() int32 {
	return atomic.AddInt32(&c.refCount, -1)
}

func triggerXDSResourceNotFoundForTesting(client XDSClient, typ xdsresource.Type, name string) error {
	crc, ok := client.(*clientRefCounted)
	if !ok {
		return fmt.Errorf("xds: xDS client is of type %T, want %T", client, &clientRefCounted{})
	}
	return crc.clientImpl.triggerResourceNotFoundForTesting(typ, name)
}

func resourceWatchStateForTesting(client XDSClient, typ xdsresource.Type, name string) (ads.ResourceWatchState, error) {
	crc, ok := client.(*clientRefCounted)
	if !ok {
		return ads.ResourceWatchState{}, fmt.Errorf("xds: xDS client is of type %T, want %T", client, &clientRefCounted{})
	}
	return crc.clientImpl.resourceWatchStateForTesting(typ, name)
}
