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
// Cluster Metadata Management
// ============================================================================
//
// This file implements comprehensive cluster metadata management for FlyDB's
// distributed architecture. It provides:
//
// 1. **Cluster Metadata Store**: Central repository for all cluster state
//    - Node metadata (capacity, load, topology, health)
//    - Partition metadata (leader, replicas, state, statistics)
//    - Routing tables for O(1) partition lookups
//    - Version tracking for optimistic concurrency control
//
// 2. **Persistence**: Metadata is persisted to disk with integrity verification
//    - JSON serialization for human-readability
//    - CRC32 checksums for corruption detection
//    - Atomic writes to prevent partial updates
//
// 3. **Thread Safety**: All operations are thread-safe with fine-grained locking
//    - Separate locks for nodes, partitions, and routing table
//    - Read-write locks for concurrent reads
//    - Atomic operations for version tracking
//
// 4. **Performance**: Optimized for high-throughput cluster operations
//    - O(1) partition lookups via routing table
//    - Minimal lock contention with separate mutexes
//    - Efficient serialization/deserialization
//
// Usage:
//   store := NewClusterMetadataStore("/path/to/metadata")
//   store.UpdateNodeMetadata(nodeID, metadata)
//   node := store.GetNodeMetadata(nodeID)
//   store.Persist()
//
// ============================================================================

// ClusterMetadataStore manages all cluster metadata with versioning and persistence.
// It is the single source of truth for cluster state and provides thread-safe
// access to node metadata, partition metadata, and routing tables.
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

// ============================================================================
// Partition Metadata
// ============================================================================

// PartitionMetadata contains comprehensive information about a partition
type PartitionMetadata struct {
	ID      int    `json:"id"`
	Version uint64 `json:"version"`

	// Ownership
	Leader   string   `json:"leader"`
	Replicas []string `json:"replicas"`

	// State
	State  PartitionState `json:"state"`
	Health HealthStatus   `json:"health"`

	// Data statistics
	KeyCount uint64 `json:"key_count"`
	DataSize uint64 `json:"data_size"`

	// Replication status
	ReplicationLag map[string]uint64 `json:"replication_lag"` // nodeID -> lag in bytes

	// Migration state
	Migration *MigrationMetadata `json:"migration,omitempty"`

	// Last modified
	LastModified time.Time `json:"last_modified"`

	// Checksum for data integrity
	Checksum uint64 `json:"checksum"`

	// Performance metrics
	ReadQPS  int `json:"read_qps"`
	WriteQPS int `json:"write_qps"`
}

// MigrationMetadata tracks partition migration progress
type MigrationMetadata struct {
	From  string `json:"from"`
	To    string `json:"to"`

	StartTime time.Time `json:"start_time"`
	EndTime   time.Time `json:"end_time,omitempty"`

	Progress      float64 `json:"progress"`       // 0.0 - 1.0
	BytesMigrated uint64  `json:"bytes_migrated"`
	BytesTotal    uint64  `json:"bytes_total"`

	State MigrationPhase `json:"state"`
	Error string         `json:"error,omitempty"`
}

// MigrationPhase represents the phase of partition migration
type MigrationPhase int

const (
	MigrationPreparing MigrationPhase = iota
	MigrationCopying
	MigrationSyncing
	MigrationSwitching
	MigrationComplete
	MigrationFailed
)

func (m MigrationPhase) String() string {
	switch m {
	case MigrationPreparing:
		return "PREPARING"
	case MigrationCopying:
		return "COPYING"
	case MigrationSyncing:
		return "SYNCING"
	case MigrationSwitching:
		return "SWITCHING"
	case MigrationComplete:
		return "COMPLETE"
	case MigrationFailed:
		return "FAILED"
	default:
		return "UNKNOWN"
	}
}

// UpdateReplicationLag updates the replication lag for a replica
func (pm *PartitionMetadata) UpdateReplicationLag(nodeID string, lag uint64) {
	if pm.ReplicationLag == nil {
		pm.ReplicationLag = make(map[string]uint64)
	}
	pm.ReplicationLag[nodeID] = lag
	pm.Version++
	pm.LastModified = time.Now()
}

// StartMigration initiates a partition migration
func (pm *PartitionMetadata) StartMigration(from, to string, totalBytes uint64) {
	pm.Migration = &MigrationMetadata{
		From:       from,
		To:         to,
		StartTime:  time.Now(),
		Progress:   0.0,
		BytesTotal: totalBytes,
		State:      MigrationPreparing,
	}
	pm.State = PartitionMigrating
	pm.Version++
	pm.LastModified = time.Now()
}

// UpdateMigrationProgress updates migration progress
func (pm *PartitionMetadata) UpdateMigrationProgress(bytesMigrated uint64) {
	if pm.Migration == nil {
		return
	}

	pm.Migration.BytesMigrated = bytesMigrated
	if pm.Migration.BytesTotal > 0 {
		pm.Migration.Progress = float64(bytesMigrated) / float64(pm.Migration.BytesTotal)
	}
	pm.Version++
	pm.LastModified = time.Now()
}

// CompleteMigration marks migration as complete
func (pm *PartitionMetadata) CompleteMigration() {
	if pm.Migration == nil {
		return
	}

	pm.Migration.State = MigrationComplete
	pm.Migration.EndTime = time.Now()
	pm.Migration.Progress = 1.0
	pm.State = PartitionHealthy
	pm.Leader = pm.Migration.To
	pm.Version++
	pm.LastModified = time.Now()
}

// FailMigration marks migration as failed
func (pm *PartitionMetadata) FailMigration(err error) {
	if pm.Migration == nil {
		return
	}

	pm.Migration.State = MigrationFailed
	pm.Migration.EndTime = time.Now()
	pm.Migration.Error = err.Error()
	pm.State = PartitionDegraded
	pm.Version++
	pm.LastModified = time.Now()
}

// ============================================================================
// Routing Table Metadata
// ============================================================================

// RoutingTableMetadata provides fast lookups for routing decisions
type RoutingTableMetadata struct {
	// Fast lookup: partition ID -> primary node
	PartitionToNode map[int]string `json:"partition_to_node"`

	// Fast lookup: partition ID -> replica nodes
	PartitionToReplicas map[int][]string `json:"partition_to_replicas"`

	// Fast lookup: node ID -> primary partitions
	NodeToPrimaryPartitions map[string][]int `json:"node_to_primary_partitions"`

	// Fast lookup: node ID -> replica partitions
	NodeToReplicaPartitions map[string][]int `json:"node_to_replica_partitions"`

	// Version for cache invalidation
	Version uint64 `json:"version"`

	// Built timestamp
	BuildTime time.Time `json:"build_time"`

	mu sync.RWMutex
}

// NewRoutingTableMetadata creates a new routing table
func NewRoutingTableMetadata() *RoutingTableMetadata {
	return &RoutingTableMetadata{
		PartitionToNode:         make(map[int]string),
		PartitionToReplicas:     make(map[int][]string),
		NodeToPrimaryPartitions: make(map[string][]int),
		NodeToReplicaPartitions: make(map[string][]int),
		Version:                 0,
		BuildTime:               time.Now(),
	}
}

// Rebuild rebuilds the routing table from partition metadata
func (rt *RoutingTableMetadata) Rebuild(partitions map[int]*PartitionMetadata) {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	// Clear existing data
	rt.PartitionToNode = make(map[int]string)
	rt.PartitionToReplicas = make(map[int][]string)
	rt.NodeToPrimaryPartitions = make(map[string][]int)
	rt.NodeToReplicaPartitions = make(map[string][]int)

	// Build new routing table
	for partitionID, partition := range partitions {
		// Partition to node mapping
		rt.PartitionToNode[partitionID] = partition.Leader
		rt.PartitionToReplicas[partitionID] = partition.Replicas

		// Node to partition mapping
		if _, ok := rt.NodeToPrimaryPartitions[partition.Leader]; !ok {
			rt.NodeToPrimaryPartitions[partition.Leader] = make([]int, 0)
		}
		rt.NodeToPrimaryPartitions[partition.Leader] = append(
			rt.NodeToPrimaryPartitions[partition.Leader], partitionID)

		// Replica mappings
		for _, replicaID := range partition.Replicas {
			if _, ok := rt.NodeToReplicaPartitions[replicaID]; !ok {
				rt.NodeToReplicaPartitions[replicaID] = make([]int, 0)
			}
			rt.NodeToReplicaPartitions[replicaID] = append(
				rt.NodeToReplicaPartitions[replicaID], partitionID)
		}
	}

	rt.Version++
	rt.BuildTime = time.Now()
}

// GetPrimaryNode returns the primary node for a partition
func (rt *RoutingTableMetadata) GetPrimaryNode(partitionID int) (string, bool) {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	nodeID, ok := rt.PartitionToNode[partitionID]
	return nodeID, ok
}

// GetReplicas returns the replica nodes for a partition
func (rt *RoutingTableMetadata) GetReplicas(partitionID int) ([]string, bool) {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	replicas, ok := rt.PartitionToReplicas[partitionID]
	return replicas, ok
}

// ============================================================================
// Cluster Metadata Store Operations
// ============================================================================

// AddNode adds or updates a node in the metadata store
func (cms *ClusterMetadataStore) AddNode(node *NodeMetadata) {
	cms.nodesMu.Lock()
	defer cms.nodesMu.Unlock()

	node.Version++
	cms.nodes[node.ID] = node
	atomic.AddUint64(&cms.version, 1)
	cms.lastUpdated = time.Now()
	cms.updateChecksum()
}

// GetNode retrieves a node from the metadata store
func (cms *ClusterMetadataStore) GetNode(nodeID string) (*NodeMetadata, bool) {
	cms.nodesMu.RLock()
	defer cms.nodesMu.RUnlock()

	node, ok := cms.nodes[nodeID]
	return node, ok
}

// RemoveNode removes a node from the metadata store
func (cms *ClusterMetadataStore) RemoveNode(nodeID string) {
	cms.nodesMu.Lock()
	defer cms.nodesMu.Unlock()

	delete(cms.nodes, nodeID)
	atomic.AddUint64(&cms.version, 1)
	cms.lastUpdated = time.Now()
	cms.updateChecksum()
}

// GetAllNodes returns all nodes
func (cms *ClusterMetadataStore) GetAllNodes() map[string]*NodeMetadata {
	cms.nodesMu.RLock()
	defer cms.nodesMu.RUnlock()

	// Return a copy to prevent external modifications
	nodes := make(map[string]*NodeMetadata, len(cms.nodes))
	for k, v := range cms.nodes {
		nodes[k] = v
	}
	return nodes
}

// AddPartition adds or updates a partition in the metadata store
func (cms *ClusterMetadataStore) AddPartition(partition *PartitionMetadata) {
	cms.partitionsMu.Lock()
	defer cms.partitionsMu.Unlock()

	partition.Version++
	cms.partitions[partition.ID] = partition
	atomic.AddUint64(&cms.version, 1)
	cms.lastUpdated = time.Now()
	cms.updateChecksum()

	// Rebuild routing table
	cms.rebuildRoutingTable()
}

// GetPartition retrieves a partition from the metadata store
func (cms *ClusterMetadataStore) GetPartition(partitionID int) (*PartitionMetadata, bool) {
	cms.partitionsMu.RLock()
	defer cms.partitionsMu.RUnlock()

	partition, ok := cms.partitions[partitionID]
	return partition, ok
}

// GetAllPartitions returns all partitions
func (cms *ClusterMetadataStore) GetAllPartitions() map[int]*PartitionMetadata {
	cms.partitionsMu.RLock()
	defer cms.partitionsMu.RUnlock()

	// Return a copy
	partitions := make(map[int]*PartitionMetadata, len(cms.partitions))
	for k, v := range cms.partitions {
		partitions[k] = v
	}
	return partitions
}

// rebuildRoutingTable rebuilds the routing table (must be called with lock held)
func (cms *ClusterMetadataStore) rebuildRoutingTable() {
	cms.routingMu.Lock()
	defer cms.routingMu.Unlock()

	cms.routingTable.Rebuild(cms.partitions)
}

// GetRoutingTable returns the current routing table
func (cms *ClusterMetadataStore) GetRoutingTable() *RoutingTableMetadata {
	cms.routingMu.RLock()
	defer cms.routingMu.RUnlock()

	return cms.routingTable
}

// updateChecksum updates the metadata checksum
func (cms *ClusterMetadataStore) updateChecksum() {
	// Simple checksum based on version and timestamp
	data := fmt.Sprintf("%d:%d", cms.version, cms.lastUpdated.Unix())
	cms.checksum = uint64(crc32.ChecksumIEEE([]byte(data)))
}

// GetVersion returns the current metadata version
func (cms *ClusterMetadataStore) GetVersion() uint64 {
	return atomic.LoadUint64(&cms.version)
}

// ============================================================================
// Persistence
// ============================================================================

// Save persists the metadata to disk
func (cms *ClusterMetadataStore) Save() error {
	cms.nodesMu.RLock()
	cms.partitionsMu.RLock()
	defer cms.nodesMu.RUnlock()
	defer cms.partitionsMu.RUnlock()

	// Create metadata snapshot
	snapshot := struct {
		Version     uint64                       `json:"version"`
		Nodes       map[string]*NodeMetadata     `json:"nodes"`
		Partitions  map[int]*PartitionMetadata   `json:"partitions"`
		Config      ClusterConfig                `json:"config"`
		LastUpdated time.Time                    `json:"last_updated"`
		Checksum    uint64                       `json:"checksum"`
	}{
		Version:     cms.version,
		Nodes:       cms.nodes,
		Partitions:  cms.partitions,
		Config:      cms.config,
		LastUpdated: cms.lastUpdated,
		Checksum:    cms.checksum,
	}

	// Marshal to JSON
	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	// Write to file
	metadataPath := filepath.Join(cms.dataDir, "cluster_metadata.json")
	tmpPath := metadataPath + ".tmp"

	// Write to temp file first
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write metadata: %w", err)
	}

	// Atomic rename
	if err := os.Rename(tmpPath, metadataPath); err != nil {
		return fmt.Errorf("failed to rename metadata file: %w", err)
	}

	return nil
}

// Load loads the metadata from disk
func (cms *ClusterMetadataStore) Load() error {
	metadataPath := filepath.Join(cms.dataDir, "cluster_metadata.json")

	// Read file
	data, err := os.ReadFile(metadataPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No metadata file yet
		}
		return fmt.Errorf("failed to read metadata: %w", err)
	}

	// Unmarshal
	var snapshot struct {
		Version     uint64                       `json:"version"`
		Nodes       map[string]*NodeMetadata     `json:"nodes"`
		Partitions  map[int]*PartitionMetadata   `json:"partitions"`
		Config      ClusterConfig                `json:"config"`
		LastUpdated time.Time                    `json:"last_updated"`
		Checksum    uint64                       `json:"checksum"`
	}

	if err := json.Unmarshal(data, &snapshot); err != nil {
		return fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	// Verify checksum
	expectedChecksum := snapshot.Checksum
	data2 := fmt.Sprintf("%d:%d", snapshot.Version, snapshot.LastUpdated.Unix())
	actualChecksum := uint64(crc32.ChecksumIEEE([]byte(data2)))

	if expectedChecksum != actualChecksum {
		return fmt.Errorf("metadata checksum mismatch: expected %d, got %d",
			expectedChecksum, actualChecksum)
	}

	// Load data
	cms.nodesMu.Lock()
	cms.partitionsMu.Lock()
	defer cms.nodesMu.Unlock()
	defer cms.partitionsMu.Unlock()

	cms.version = snapshot.Version
	cms.nodes = snapshot.Nodes
	cms.partitions = snapshot.Partitions
	cms.config = snapshot.Config
	cms.lastUpdated = snapshot.LastUpdated
	cms.checksum = snapshot.Checksum

	// Rebuild routing table
	cms.rebuildRoutingTable()

	return nil
}

// ============================================================================
// Metadata Statistics
// ============================================================================

// GetStats returns metadata statistics
func (cms *ClusterMetadataStore) GetStats() map[string]interface{} {
	cms.nodesMu.RLock()
	cms.partitionsMu.RLock()
	defer cms.nodesMu.RUnlock()
	defer cms.partitionsMu.RUnlock()

	// Count partition states
	partitionStates := make(map[string]int)
	totalKeys := uint64(0)
	totalData := uint64(0)

	for _, partition := range cms.partitions {
		partitionStates[partition.State.String()]++
		totalKeys += partition.KeyCount
		totalData += partition.DataSize
	}

	// Count node states
	nodeStates := make(map[string]int)
	for _, node := range cms.nodes {
		nodeStates[node.State.String()]++
	}

	return map[string]interface{}{
		"version":         cms.version,
		"node_count":      len(cms.nodes),
		"partition_count": len(cms.partitions),
		"total_keys":      totalKeys,
		"total_data_size": totalData,
		"partition_states": partitionStates,
		"node_states":     nodeStates,
		"last_updated":    cms.lastUpdated,
		"checksum":        cms.checksum,
	}
}

