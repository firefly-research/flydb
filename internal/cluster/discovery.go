/*
 * Copyright (c) 2026 FlyDB Authors.
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

/*
Package cluster provides mDNS/DNS-SD service discovery for FlyDB nodes.

OVERVIEW:
=========
FlyDB uses mDNS (multicast DNS) for automatic node discovery on local networks.
This enables zero-configuration cluster setup where nodes can find each other
without manual peer configuration.

SERVICE TYPE:
=============
FlyDB advertises itself as: _flydb._tcp.local.

Each node publishes:
- Instance name: <node-id>._flydb._tcp.local.
- Port: cluster port (default 7946)
- TXT records: node_id, cluster_id, raft_port, http_port, version

DISCOVERY MODES:
================
1. mDNS (Bonjour/Avahi) - Automatic discovery on local network
2. DNS-SD - Service discovery via DNS SRV records
3. Static peers - Manual peer configuration (fallback)

USAGE:
======
	discovery := NewDiscoveryService(config)
	discovery.Start()
	defer discovery.Stop()

	// Find other nodes
	nodes := discovery.DiscoverNodes(5 * time.Second)
*/
package cluster

import (
	"context"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/mdns"
)

const (
	// ServiceType is the mDNS service type for FlyDB
	ServiceType = "_flydb._tcp"

	// DefaultDiscoveryTimeout is the default timeout for node discovery
	DefaultDiscoveryTimeout = 5 * time.Second
)

// DiscoveredNode represents a node found via service discovery.
type DiscoveredNode struct {
	NodeID       string
	ClusterID    string
	ClusterAddr  string // host:port for cluster communication (gossip)
	RaftAddr     string // host:port for Raft consensus
	HTTPAddr     string // host:port for HTTP API
	Version      string
	DiscoveredAt time.Time
}

// DiscoveryConfig holds configuration for service discovery.
type DiscoveryConfig struct {
	NodeID      string
	ClusterID   string
	ClusterAddr string // Address to advertise for cluster traffic (gossip)
	RaftAddr    string // Address to advertise for Raft traffic
	HTTPAddr    string // Address to advertise for HTTP API
	Version     string
	Enabled     bool
}

// DiscoveryService handles mDNS service advertisement and discovery.
type DiscoveryService struct {
	config  DiscoveryConfig
	server  *mdns.Server
	mu      sync.RWMutex
	nodes   map[string]*DiscoveredNode
	stopCh  chan struct{}
	running bool
}

// NewDiscoveryService creates a new discovery service.
func NewDiscoveryService(config DiscoveryConfig) *DiscoveryService {
	return &DiscoveryService{
		config: config,
		nodes:  make(map[string]*DiscoveredNode),
		stopCh: make(chan struct{}),
	}
}

// Start begins advertising this node and listening for other nodes.
func (d *DiscoveryService) Start() error {
	if !d.config.Enabled {
		log.Println("[discovery] Service discovery disabled")
		return nil
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.running {
		return nil
	}

	// Parse cluster address to get host and port
	host, portStr, err := net.SplitHostPort(d.config.ClusterAddr)
	if err != nil {
		return fmt.Errorf("invalid cluster address: %w", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return fmt.Errorf("invalid cluster port: %w", err)
	}

	// If host is empty or 0.0.0.0, use all interfaces
	var ips []net.IP
	if host == "" || host == "0.0.0.0" {
		ips = getLocalIPs()
	} else {
		ip := net.ParseIP(host)
		if ip != nil {
			ips = []net.IP{ip}
		}
	}

	// Build TXT records with node metadata
	txtRecords := []string{
		fmt.Sprintf("node_id=%s", d.config.NodeID),
		fmt.Sprintf("cluster_id=%s", d.config.ClusterID),
		fmt.Sprintf("raft_addr=%s", d.config.RaftAddr),
		fmt.Sprintf("http_addr=%s", d.config.HTTPAddr),
		fmt.Sprintf("version=%s", d.config.Version),
	}

	// Create mDNS service
	service, err := mdns.NewMDNSService(
		d.config.NodeID, // Instance name
		ServiceType,     // Service type
		"",              // Domain (empty = .local)
		"",              // Host name (empty = auto)
		port,            // Port
		ips,             // IPs to advertise
		txtRecords,      // TXT records
	)
	if err != nil {
		return fmt.Errorf("failed to create mDNS service: %w", err)
	}

	// Create mDNS server
	server, err := mdns.NewServer(&mdns.Config{Zone: service})
	if err != nil {
		return fmt.Errorf("failed to create mDNS server: %w", err)
	}
	d.server = server
	d.running = true

	log.Printf("[discovery] Service discovery started: node_id=%s cluster_addr=%s service_type=%s",
		d.config.NodeID, d.config.ClusterAddr, ServiceType)

	return nil
}

// Stop stops the discovery service.
func (d *DiscoveryService) Stop() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if !d.running {
		return nil
	}

	close(d.stopCh)

	if d.server != nil {
		d.server.Shutdown()
		d.server = nil
	}

	d.running = false
	log.Println("[discovery] Service discovery stopped")
	return nil
}

// DiscoverNodes discovers FlyDB nodes on the network.
func (d *DiscoveryService) DiscoverNodes(timeout time.Duration) ([]*DiscoveredNode, error) {
	if timeout == 0 {
		timeout = DefaultDiscoveryTimeout
	}

	entriesCh := make(chan *mdns.ServiceEntry, 10)
	var nodes []*DiscoveredNode
	var mu sync.Mutex

	// Start a goroutine to collect entries
	go func() {
		for entry := range entriesCh {
			node := parseServiceEntry(entry)
			if node != nil && node.NodeID != d.config.NodeID {
				mu.Lock()
				nodes = append(nodes, node)
				d.nodes[node.NodeID] = node
				mu.Unlock()
			}
		}
	}()

	// Perform the lookup
	params := &mdns.QueryParam{
		Service:             ServiceType,
		Domain:              "local",
		Timeout:             timeout,
		Entries:             entriesCh,
		WantUnicastResponse: true,
	}

	if err := mdns.Query(params); err != nil {
		return nil, fmt.Errorf("mDNS query failed: %w", err)
	}

	close(entriesCh)

	return nodes, nil
}

// DiscoverNodesWithContext discovers nodes with context cancellation support.
func (d *DiscoveryService) DiscoverNodesWithContext(ctx context.Context, timeout time.Duration) ([]*DiscoveredNode, error) {
	resultCh := make(chan []*DiscoveredNode, 1)
	errCh := make(chan error, 1)

	go func() {
		nodes, err := d.DiscoverNodes(timeout)
		if err != nil {
			errCh <- err
			return
		}
		resultCh <- nodes
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err := <-errCh:
		return nil, err
	case nodes := <-resultCh:
		return nodes, nil
	}
}

// GetCachedNodes returns previously discovered nodes.
func (d *DiscoveryService) GetCachedNodes() []*DiscoveredNode {
	d.mu.RLock()
	defer d.mu.RUnlock()

	nodes := make([]*DiscoveredNode, 0, len(d.nodes))
	for _, node := range d.nodes {
		nodes = append(nodes, node)
	}
	return nodes
}

// parseServiceEntry parses an mDNS service entry into a DiscoveredNode.
func parseServiceEntry(entry *mdns.ServiceEntry) *DiscoveredNode {
	if entry == nil {
		return nil
	}

	node := &DiscoveredNode{
		DiscoveredAt: time.Now(),
	}

	// Get IP address
	var ip string
	if entry.AddrV4 != nil {
		ip = entry.AddrV4.String()
	} else if entry.AddrV6 != nil {
		ip = entry.AddrV6.String()
	}

	if ip == "" {
		return nil
	}

	node.ClusterAddr = fmt.Sprintf("%s:%d", ip, entry.Port)

	// Parse TXT records
	for _, txt := range entry.InfoFields {
		parts := strings.SplitN(txt, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key, value := parts[0], parts[1]
		switch key {
		case "node_id":
			node.NodeID = value
		case "cluster_id":
			node.ClusterID = value
		case "raft_addr":
			node.RaftAddr = value
		case "http_addr":
			node.HTTPAddr = value
		case "version":
			node.Version = value
		}
	}

	// Use instance name as fallback for node ID
	if node.NodeID == "" {
		node.NodeID = entry.Name
	}

	return node
}

// getLocalIPs returns all non-loopback IPv4 addresses.
func getLocalIPs() []net.IP {
	var ips []net.IP

	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ips
	}

	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok {
			if ipnet.IP.IsLoopback() {
				continue
			}
			if ipnet.IP.To4() != nil {
				ips = append(ips, ipnet.IP)
			}
		}
	}

	return ips
}

// IsRunning returns whether the discovery service is running.
func (d *DiscoveryService) IsRunning() bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.running
}

