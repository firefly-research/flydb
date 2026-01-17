/*
 * Copyright (c) 2026 Firefly Software Solutions Inc.
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

package cluster

import (
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// ============================================================================
// Cluster Metadata
// ============================================================================

// ClusterMetadataStore manages all cluster metadata with versioning and persistence
type ClusterMetadataStore struct {
	// Version for optimistic concurrency control
	version uint64

	// Node metadata
	nodes map[string]*NodeMetadata
	nodesMu sync.RWMutex

	// Partition metadata
	partitions map[int]*PartitionMetadata
	partitionsMu sync.RWMutex

	// Routing table for fast lookups
	routingTable *RoutingTableMetadata
	routingMu sync.RWMutex

	// Cluster configuration
	config ClusterConfig

	// Persistence
	dataDir string
	
	// Last update timestamp
	lastUpdated time.Time
	
	// Checksum for integrity verification
	checksum uint64
}

// NewClusterMetadataStore creates a new metadata store
func NewClusterMetadataStore(config ClusterConfig, dataDir string) *ClusterMetadataStore {
	return &ClusterMetadataStore{
		version:     0,
		nodes:       make(map[string]*NodeMetadata),
		partitions:  make(map[int]*PartitionMetadata),
		routingTable: NewRoutingTableMetadata(),
		config:      config,
		dataDir:     dataDir,
		lastUpdated: time.Now(),
	}
}

// ============================================================================
// Node Metadata
// ============================================================================

// NodeMetadata contains comprehensive information about a cluster node
type NodeMetadata struct {
	// Basic info
	ID       string `json:"id"`
	Addr     string `json:"addr"`
	DataPort int    `json:"data_port"`

	// Node capabilities
	Capacity NodeCapacity `json:"capacity"`

	// Current load metrics
	Load NodeLoad `json:"load"`

	// Topology information for locality-aware routing
	Datacenter string `json:"datacenter"`
	Rack       string `json:"rack"`
	Zone       string `json:"zone"`

	// Node state
	State  NodeState    `json:"state"`
	Health HealthStatus `json:"health"`

	// Partition ownership
	PrimaryPartitions []int `json:"primary_partitions"`
	ReplicaPartitions []int `json:"replica_partitions"`

	// Version for updates
	Version uint64 `json:"version"`

	// Last heartbeat
	LastHeartbeat time.Time `json:"last_heartbeat"`
	
	// Join time
	JoinedAt time.Time `json:"joined_at"`
	
	// Metadata
	Labels map[string]string `json:"labels,omitempty"`
}

// NodeCapacity represents the hardware capacity of a node
type NodeCapacity struct {
	CPU     int    `json:"cpu"`      // CPU cores
	Memory  uint64 `json:"memory"`   // Bytes
	Disk    uint64 `json:"disk"`     // Bytes
	Network uint64 `json:"network"`  // Bytes/sec
}

// NodeLoad represents the current load on a node
type NodeLoad struct {
	CPUUsage     float64 `json:"cpu_usage"`      // 0.0 - 1.0
	MemoryUsage  uint64  `json:"memory_usage"`   // Bytes
	DiskUsage    uint64  `json:"disk_usage"`     // Bytes
	NetworkUsage uint64  `json:"network_usage"`  // Bytes/sec
	Connections  int     `json:"connections"`
	QPS          int     `json:"qps"`
	
	// Derived metrics
	LoadScore float64   `json:"load_score"` // Composite score
	UpdatedAt time.Time `json:"updated_at"`
}

// HealthStatus represents the health of a node
type HealthStatus int

const (
	HealthUnknown HealthStatus = iota
	HealthHealthy
	HealthDegraded
	HealthUnhealthy
)

func (h HealthStatus) String() string {
	switch h {
	case HealthHealthy:
		return "HEALTHY"
	case HealthDegraded:
		return "DEGRADED"
	case HealthUnhealthy:
		return "UNHEALTHY"
	default:
		return "UNKNOWN"
	}
}

// UpdateLoad updates the node's load metrics
func (nm *NodeMetadata) UpdateLoad(load NodeLoad) {
	nm.Load = load
	nm.Load.UpdatedAt = time.Now()
	
	// Calculate composite load score
	// Score = 0.4*CPU + 0.3*Memory + 0.2*Connections + 0.1*QPS
	nm.Load.LoadScore = 0.4*load.CPUUsage +
		0.3*float64(load.MemoryUsage)/float64(nm.Capacity.Memory) +
		0.2*float64(load.Connections)/1000.0 +
		0.1*float64(load.QPS)/10000.0
}

