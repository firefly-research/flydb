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

/*
Package cluster provides a unified cluster-replication architecture for FlyDB.

Unified Architecture Overview:
==============================

This package integrates cluster management and replication into a single cohesive
system that provides:

  - True distributed data storage with consistent hashing
  - Automatic data sharding across nodes
  - Integrated leader election with Raft-inspired consensus
  - Seamless failover with data consistency guarantees
  - Horizontal scaling through partition management

Architecture:
=============

	┌──────────────────────────────────────────────────────────────────┐
	│                    Unified Cluster Manager                       │
	│  ┌─────────────────────────────────────────────────────────────┐ │
	│  │                  Partition Manager                          │ │
	│  │  - Consistent hash ring for data distribution               │ │
	│  │  - Partition ownership tracking                             │ │
	│  │  - Automatic rebalancing on node changes                    │ │
	│  └─────────────────────────────────────────────────────────────┘ │
	│  ┌─────────────────────────────────────────────────────────────┐ │
	│  │                  Consensus Engine                           │ │
	│  │  - Leader election with term-based fencing                  │ │
	│  │  - Log replication for metadata changes                     │ │
	│  │  - Quorum-based decision making                             │ │
	│  └─────────────────────────────────────────────────────────────┘ │
	│  ┌─────────────────────────────────────────────────────────────┐ │
	│  │                  Replication Controller                     │ │
	│  │  - Per-partition replication                                │ │
	│  │  - Configurable replication factor                          │ │
	│  │  - Sync/async/semi-sync modes                               │ │
	│  └─────────────────────────────────────────────────────────────┘ │
	└──────────────────────────────────────────────────────────────────┘

Key Concepts:
=============

1. Partitions: Data is divided into partitions using consistent hashing.
   Each partition has a primary owner and configurable replicas.

2. Partition Groups: Related partitions are grouped for efficient management.
   Each group has a leader responsible for coordinating writes.

3. Replication Factor: Configurable number of copies for each partition.
   Default is 3 for production deployments.

4. Consistency Levels:
   - EVENTUAL: Writes return immediately, replicated asynchronously
   - QUORUM: Writes wait for majority acknowledgment
   - ALL: Writes wait for all replicas to acknowledge

5. Failover: Automatic leader election within partition groups.
   Data remains available as long as quorum is maintained.
*/
package cluster

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// ConsistencyLevel defines the consistency guarantee for operations
type ConsistencyLevel int

const (
	// ConsistencyEventual returns immediately, replicates asynchronously
	ConsistencyEventual ConsistencyLevel = iota
	// ConsistencyOne waits for at least one replica to acknowledge
	ConsistencyOne
	// ConsistencyQuorum waits for majority acknowledgment
	ConsistencyQuorum
	// ConsistencyAll waits for all replicas to acknowledge
	ConsistencyAll
)

func (c ConsistencyLevel) String() string {
	switch c {
	case ConsistencyEventual:
		return "EVENTUAL"
	case ConsistencyOne:
		return "ONE"
	case ConsistencyQuorum:
		return "QUORUM"
	case ConsistencyAll:
		return "ALL"
	default:
		return "UNKNOWN"
	}
}

// NodeRole represents the role of a node in the cluster
type NodeRole int

const (
	// RoleFollower is a node that follows a leader
	RoleFollower NodeRole = iota
	// RoleCandidate is a node running for leader election
	RoleCandidate
	// RoleLeader is the elected leader of a partition group
	RoleLeader
)

func (r NodeRole) String() string {
	switch r {
	case RoleFollower:
		return "FOLLOWER"
	case RoleCandidate:
		return "CANDIDATE"
	case RoleLeader:
		return "LEADER"
	default:
		return "UNKNOWN"
	}
}

// Default configuration values
const (
	DefaultPartitionCount    = 256
	DefaultReplicationFactor = 3
	DefaultVirtualNodes      = 150
	DefaultHeartbeatInterval = 500 * time.Millisecond
	DefaultElectionTimeout   = 2 * time.Second
	DefaultSyncTimeout       = 5 * time.Second
)

// ClusterConfig holds the configuration for the unified cluster
type ClusterConfig struct {
	// NodeID is the unique identifier for this node
	NodeID string `json:"node_id"`

	// NodeAddr is the address this node listens on
	NodeAddr string `json:"node_addr"`

	// ClusterPort is the port for cluster communication
	ClusterPort int `json:"cluster_port"`

	// DataPort is the port for data replication
	DataPort int `json:"data_port"`

	// Seeds are the initial nodes to contact for cluster discovery
	Seeds []string `json:"seeds"`

	// PartitionCount is the number of partitions (should be power of 2)
	PartitionCount int `json:"partition_count"`

	// ReplicationFactor is the number of replicas per partition
	ReplicationFactor int `json:"replication_factor"`

	// VirtualNodes is the number of virtual nodes per physical node
	VirtualNodes int `json:"virtual_nodes"`

	// DefaultConsistency is the default consistency level for operations
	DefaultConsistency ConsistencyLevel `json:"default_consistency"`

	// HeartbeatInterval is how often to send heartbeats
	HeartbeatInterval time.Duration `json:"heartbeat_interval"`

	// ElectionTimeout is how long to wait before starting election
	ElectionTimeout time.Duration `json:"election_timeout"`

	// SyncTimeout is how long to wait for sync acknowledgments
	SyncTimeout time.Duration `json:"sync_timeout"`

	// EnableAutoRebalance enables automatic partition rebalancing
	EnableAutoRebalance bool `json:"enable_auto_rebalance"`

	// DataDir is the directory for storing cluster metadata
	DataDir string `json:"data_dir"`

	// EncryptionKeyHash is the SHA-256 hash of the encryption key
	// Used to validate that all cluster nodes use the same encryption key
	EncryptionKeyHash string `json:"encryption_key_hash,omitempty"`
}

// DefaultClusterConfig returns a ClusterConfig with sensible defaults
func DefaultClusterConfig(nodeID, nodeAddr string) ClusterConfig {
	return ClusterConfig{
		NodeID:              nodeID,
		NodeAddr:            nodeAddr,
		ClusterPort:         7946,
		DataPort:            7947,
		Seeds:               []string{},
		PartitionCount:      DefaultPartitionCount,
		ReplicationFactor:   DefaultReplicationFactor,
		VirtualNodes:        DefaultVirtualNodes,
		DefaultConsistency:  ConsistencyQuorum,
		HeartbeatInterval:   DefaultHeartbeatInterval,
		ElectionTimeout:     DefaultElectionTimeout,
		SyncTimeout:         DefaultSyncTimeout,
		EnableAutoRebalance: true,
		DataDir:             "./data/cluster",
	}
}

// Partition represents a data partition in the cluster
type Partition struct {
	// ID is the partition identifier (0 to PartitionCount-1)
	ID int `json:"id"`

	// Leader is the node ID of the current leader
	Leader string `json:"leader"`

	// Replicas are the node IDs holding replicas (including leader)
	Replicas []string `json:"replicas"`

	// Version is incremented on each ownership change
	Version uint64 `json:"version"`

	// State is the current state of the partition
	State PartitionState `json:"state"`

	// LastModified is when the partition was last modified
	LastModified time.Time `json:"last_modified"`
}

// PartitionState represents the state of a partition
type PartitionState int

const (
	// PartitionHealthy means the partition is fully replicated and available
	PartitionHealthy PartitionState = iota
	// PartitionRecovering means the partition is recovering from a failure
	PartitionRecovering
	// PartitionRebalancing means the partition is being moved to new nodes
	PartitionRebalancing
	// PartitionUnavailable means the partition has lost quorum
	PartitionUnavailable
	// PartitionMigrating means the partition is being migrated to another node
	PartitionMigrating
	// PartitionDegraded means the partition is available but under-replicated
	PartitionDegraded
)

func (s PartitionState) String() string {
	switch s {
	case PartitionHealthy:
		return "HEALTHY"
	case PartitionRecovering:
		return "RECOVERING"
	case PartitionRebalancing:
		return "REBALANCING"
	case PartitionUnavailable:
		return "UNAVAILABLE"
	case PartitionMigrating:
		return "MIGRATING"
	case PartitionDegraded:
		return "DEGRADED"
	default:
		return "UNKNOWN"
	}
}

// ClusterNode represents a node in the cluster
type ClusterNode struct {
	// ID is the unique node identifier
	ID string `json:"id"`

	// Addr is the node's address
	Addr string `json:"addr"`

	// ClusterPort is the port for cluster communication
	ClusterPort int `json:"cluster_port"`

	// DataPort is the port for data replication
	DataPort int `json:"data_port"`

	// State is the current state of the node
	State NodeState `json:"state"`

	// Role is the node's role in the cluster
	Role NodeRole `json:"role"`

	// JoinedAt is when the node joined the cluster
	JoinedAt time.Time `json:"joined_at"`

	// LastHeartbeat is the last time we received a heartbeat
	LastHeartbeat time.Time `json:"last_heartbeat"`

	// Partitions are the partition IDs this node owns
	Partitions []int `json:"partitions"`

	// Load is the current load on this node (0.0 to 1.0)
	Load float64 `json:"load"`

	// Metadata holds additional node metadata
	Metadata map[string]string `json:"metadata"`

	// EncryptionKeyHash is the SHA-256 hash of the encryption key (for validation)
	// All nodes in a cluster MUST have the same encryption key hash.
	// Empty string means encryption is disabled.
	EncryptionKeyHash string `json:"encryption_key_hash,omitempty"`
}

// NodeState represents the state of a cluster node
type NodeState int

const (
	// NodeAlive means the node is healthy and responding
	NodeAlive NodeState = iota
	// NodeSuspect means the node may be failing
	NodeSuspect
	// NodeDead means the node is confirmed dead
	NodeDead
	// NodeLeaving means the node is gracefully leaving
	NodeLeaving
)

func (s NodeState) String() string {
	switch s {
	case NodeAlive:
		return "ALIVE"
	case NodeSuspect:
		return "SUSPECT"
	case NodeDead:
		return "DEAD"
	case NodeLeaving:
		return "LEAVING"
	default:
		return "UNKNOWN"
	}
}


// HashRing implements consistent hashing for partition distribution
type HashRing struct {
	mu           sync.RWMutex
	nodes        map[string]*ClusterNode
	ring         []hashEntry
	virtualNodes int
}

// hashEntry represents a point on the hash ring
type hashEntry struct {
	hash   uint32
	nodeID string
}

// NewHashRing creates a new consistent hash ring
func NewHashRing(virtualNodes int) *HashRing {
	if virtualNodes <= 0 {
		virtualNodes = DefaultVirtualNodes
	}
	return &HashRing{
		nodes:        make(map[string]*ClusterNode),
		ring:         make([]hashEntry, 0),
		virtualNodes: virtualNodes,
	}
}

// AddNode adds a node to the hash ring
func (h *HashRing) AddNode(node *ClusterNode) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.nodes[node.ID] = node

	// Add virtual nodes
	for i := 0; i < h.virtualNodes; i++ {
		key := fmt.Sprintf("%s-%d", node.ID, i)
		hash := h.hash(key)
		h.ring = append(h.ring, hashEntry{hash: hash, nodeID: node.ID})
	}

	// Sort the ring
	sort.Slice(h.ring, func(i, j int) bool {
		return h.ring[i].hash < h.ring[j].hash
	})
}

// RemoveNode removes a node from the hash ring
func (h *HashRing) RemoveNode(nodeID string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	delete(h.nodes, nodeID)

	// Remove virtual nodes
	newRing := make([]hashEntry, 0, len(h.ring))
	for _, entry := range h.ring {
		if entry.nodeID != nodeID {
			newRing = append(newRing, entry)
		}
	}
	h.ring = newRing
}

// GetNode returns the node responsible for a key
func (h *HashRing) GetNode(key string) *ClusterNode {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.ring) == 0 {
		return nil
	}

	hash := h.hash(key)

	// Binary search for the first node with hash >= key hash
	idx := sort.Search(len(h.ring), func(i int) bool {
		return h.ring[i].hash >= hash
	})

	// Wrap around if necessary
	if idx >= len(h.ring) {
		idx = 0
	}

	return h.nodes[h.ring[idx].nodeID]
}

// GetNodes returns N nodes responsible for a key (for replication)
func (h *HashRing) GetNodes(key string, n int) []*ClusterNode {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.ring) == 0 || n <= 0 {
		return nil
	}

	hash := h.hash(key)

	// Binary search for the first node
	idx := sort.Search(len(h.ring), func(i int) bool {
		return h.ring[i].hash >= hash
	})

	if idx >= len(h.ring) {
		idx = 0
	}

	// Collect unique nodes
	seen := make(map[string]bool)
	nodes := make([]*ClusterNode, 0, n)

	for i := 0; i < len(h.ring) && len(nodes) < n; i++ {
		entry := h.ring[(idx+i)%len(h.ring)]
		if !seen[entry.nodeID] {
			seen[entry.nodeID] = true
			if node, ok := h.nodes[entry.nodeID]; ok {
				nodes = append(nodes, node)
			}
		}
	}

	return nodes
}

// GetPartition returns the partition ID for a key
func (h *HashRing) GetPartition(key string, partitionCount int) int {
	hash := h.hash(key)
	return int(hash) % partitionCount
}

// hash computes the hash of a key
func (h *HashRing) hash(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

// GetNodeCount returns the number of nodes in the ring
func (h *HashRing) GetNodeCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.nodes)
}

// GetAllNodes returns all nodes in the ring
func (h *HashRing) GetAllNodes() []*ClusterNode {
	h.mu.RLock()
	defer h.mu.RUnlock()

	nodes := make([]*ClusterNode, 0, len(h.nodes))
	for _, node := range h.nodes {
		nodes = append(nodes, node)
	}
	return nodes
}


// ClusterEventType represents the type of cluster event
type ClusterEventType string

const (
	EventNodeJoined        ClusterEventType = "NODE_JOINED"
	EventNodeLeft          ClusterEventType = "NODE_LEFT"
	EventNodeFailed        ClusterEventType = "NODE_FAILED"
	EventLeaderElected     ClusterEventType = "LEADER_ELECTED"
	EventPartitionMoved    ClusterEventType = "PARTITION_MOVED"
	EventPartitionMigrating ClusterEventType = "PARTITION_MIGRATING"
	EventRebalanceStarted  ClusterEventType = "REBALANCE_STARTED"
	EventRebalanceComplete ClusterEventType = "REBALANCE_COMPLETE"
	EventQuorumLost        ClusterEventType = "QUORUM_LOST"
	EventQuorumRestored    ClusterEventType = "QUORUM_RESTORED"
)

// ClusterEvent represents a cluster state change event
type ClusterEvent struct {
	Type        ClusterEventType `json:"type"`
	NodeID      string           `json:"node_id"`
	PartitionID int              `json:"partition_id,omitempty"`
	Term        uint64           `json:"term"`
	Timestamp   time.Time        `json:"timestamp"`
	Details     string           `json:"details"`
}

// ClusterMetrics holds cluster-wide metrics
type ClusterMetrics struct {
	mu sync.RWMutex

	// Term is the current election term
	Term uint64 `json:"term"`

	// TotalNodes is the total number of nodes
	TotalNodes int `json:"total_nodes"`

	// HealthyNodes is the number of healthy nodes
	HealthyNodes int `json:"healthy_nodes"`

	// TotalPartitions is the total number of partitions
	TotalPartitions int `json:"total_partitions"`

	// HealthyPartitions is the number of healthy partitions
	HealthyPartitions int `json:"healthy_partitions"`

	// ReplicationLag is the average replication lag
	ReplicationLag time.Duration `json:"replication_lag"`

	// OperationsPerSecond is the current throughput
	OperationsPerSecond float64 `json:"operations_per_second"`

	// BytesReplicated is the total bytes replicated
	BytesReplicated int64 `json:"bytes_replicated"`
}

// WALInterface defines the interface for WAL operations.
// This allows the unified cluster manager to work with the WAL for replication.
type WALInterface interface {
	// Write appends an operation to the WAL
	Write(op byte, key string, value []byte) error
	// Sync flushes pending writes to disk
	Sync() error
	// Size returns the current WAL size in bytes
	Size() (int64, error)
	// Replay reads the WAL from startOffset and invokes fn for each record
	Replay(startOffset int64, fn func(op byte, key string, value []byte)) error
	// ReplayWithPosition reads WAL from startOffset and returns the new file position
	// This is essential for replication to track exact file positions in encrypted WALs
	ReplayWithPosition(startOffset int64, fn func(op byte, key string, value []byte)) (int64, error)
	// Close closes the WAL
	Close() error
}

// StorageEngine defines the interface for storage operations.
// This allows the unified cluster manager to apply replicated operations.
type StorageEngine interface {
	// Put stores a value (writes to WAL then storage)
	Put(key string, value []byte) error
	// Delete removes a key (writes to WAL then storage)
	Delete(key string) error
	// Get retrieves a value
	Get(key string) ([]byte, error)
}

// ReplicatedStorageEngine extends StorageEngine with methods for cluster replication.
// This allows followers to apply replicated changes without re-writing to WAL,
// following the MySQL/PostgreSQL replication pattern.
type ReplicatedStorageEngine interface {
	StorageEngine
	// ApplyReplicatedPut applies a replicated PUT without writing to WAL
	// (the WAL entry is written separately via WriteReplicatedWAL)
	ApplyReplicatedPut(key string, value []byte) error
	// ApplyReplicatedDelete applies a replicated DELETE without writing to WAL
	ApplyReplicatedDelete(key string) error
	// WriteReplicatedWAL writes a replicated WAL entry for crash recovery
	WriteReplicatedWAL(op byte, key string, value []byte) error
}

// WAL operation types (byte constants for wire protocol)
const (
	WALOpPut    byte = 1
	WALOpDelete byte = 2
)

// FollowerReplicationState tracks the replication state of a follower
type FollowerReplicationState struct {
	Address     string
	WALOffset   int64
	LastAckTime time.Time
	Lag         time.Duration
	IsHealthy   bool
	FailedSends int
	conn        net.Conn
	mu          sync.Mutex
}

// UnifiedClusterManager integrates cluster management and replication
type UnifiedClusterManager struct {
	config ClusterConfig

	// Node identity
	nodeID   string
	nodeAddr string
	role     NodeRole
	roleMu   sync.RWMutex

	// Term-based leader election
	term uint64

	// Raft consensus engine (replaces Bully algorithm)
	raftNode    *RaftNode
	useRaft     bool // Enable Raft-based consensus

	// Hash ring for consistent hashing
	ring *HashRing

	// Partition management
	partitions   map[int]*Partition
	partitionsMu sync.RWMutex

	// Node management
	nodes   map[string]*ClusterNode
	nodesMu sync.RWMutex

	// Replication state per partition
	replicationState   map[int]*PartitionReplicationState
	replicationStateMu sync.RWMutex

	// WAL-based replication
	wal              WALInterface
	store            StorageEngine
	followers        map[string]*FollowerReplicationState
	followersMu      sync.RWMutex
	replListener     net.Listener
	replPollInterval time.Duration
	replActive       bool           // true when this node is leader and accepting follower connections
	followerStopCh   chan struct{}  // channel to stop follower replication loop on leader change
	followerStopMu   sync.Mutex     // protects followerStopCh

	// Metrics
	metrics *ClusterMetrics

	// Event handling
	eventCh        chan ClusterEvent
	eventCallbacks []func(ClusterEvent)
	eventMu        sync.RWMutex

	// Network
	clusterListener net.Listener
	dataListener    net.Listener

	// Lifecycle
	stopCh chan struct{}
	wg     sync.WaitGroup
}

// PartitionReplicationState tracks replication state for a partition
type PartitionReplicationState struct {
	PartitionID   int                      `json:"partition_id"`
	Leader        string                   `json:"leader"`
	Replicas      map[string]*ReplicaState `json:"replicas"`
	LastCommitted int64                    `json:"last_committed"`
	LastApplied   int64                    `json:"last_applied"`
	mu            sync.RWMutex
}

// ReplicaState tracks the state of a replica
type ReplicaState struct {
	NodeID      string        `json:"node_id"`
	Offset      int64         `json:"offset"`
	Lag         time.Duration `json:"lag"`
	IsHealthy   bool          `json:"is_healthy"`
	LastAckTime time.Time     `json:"last_ack_time"`
}

// NewUnifiedClusterManager creates a new unified cluster manager
func NewUnifiedClusterManager(config ClusterConfig) *UnifiedClusterManager {
	if config.PartitionCount <= 0 {
		config.PartitionCount = DefaultPartitionCount
	}
	if config.ReplicationFactor <= 0 {
		config.ReplicationFactor = DefaultReplicationFactor
	}
	if config.VirtualNodes <= 0 {
		config.VirtualNodes = DefaultVirtualNodes
	}

	ucm := &UnifiedClusterManager{
		config:           config,
		nodeID:           config.NodeID,
		nodeAddr:         config.NodeAddr,
		role:             RoleFollower,
		term:             0,
		useRaft:          true, // Enable Raft by default
		ring:             NewHashRing(config.VirtualNodes),
		partitions:       make(map[int]*Partition),
		nodes:            make(map[string]*ClusterNode),
		replicationState: make(map[int]*PartitionReplicationState),
		followers:        make(map[string]*FollowerReplicationState),
		replPollInterval: 100 * time.Millisecond,
		metrics:          &ClusterMetrics{TotalPartitions: config.PartitionCount},
		eventCh:          make(chan ClusterEvent, 1000),
		stopCh:           make(chan struct{}),
	}

	// Initialize partitions
	for i := 0; i < config.PartitionCount; i++ {
		ucm.partitions[i] = &Partition{
			ID:           i,
			State:        PartitionUnavailable,
			LastModified: time.Now(),
		}
	}

	// Add self to nodes
	selfNode := &ClusterNode{
		ID:                config.NodeID,
		Addr:              config.NodeAddr,
		ClusterPort:       config.ClusterPort,
		DataPort:          config.DataPort,
		State:             NodeAlive,
		Role:              RoleFollower,
		JoinedAt:          time.Now(),
		Metadata:          make(map[string]string),
		EncryptionKeyHash: config.EncryptionKeyHash, // Include encryption key hash
	}
	ucm.nodes[config.NodeID] = selfNode
	ucm.ring.AddNode(selfNode)

	return ucm
}

// SetWAL sets the WAL for replication
func (ucm *UnifiedClusterManager) SetWAL(wal WALInterface) {
	ucm.wal = wal
}

// SetStore sets the storage engine for applying replicated operations
func (ucm *UnifiedClusterManager) SetStore(store StorageEngine) {
	ucm.store = store
}

// SetRaftNode sets the Raft consensus node for leader election
func (ucm *UnifiedClusterManager) SetRaftNode(raft *RaftNode) {
	ucm.raftNode = raft
	ucm.useRaft = raft != nil
}

// EnableRaft enables or disables Raft-based consensus
func (ucm *UnifiedClusterManager) EnableRaft(enable bool) {
	ucm.useRaft = enable
}

// GetRaftNode returns the Raft node if configured
func (ucm *UnifiedClusterManager) GetRaftNode() *RaftNode {
	return ucm.raftNode
}

// IsRaftEnabled returns true if Raft consensus is enabled
func (ucm *UnifiedClusterManager) IsRaftEnabled() bool {
	return ucm.useRaft && ucm.raftNode != nil
}

// Start initializes and starts the cluster manager
func (ucm *UnifiedClusterManager) Start() error {
	// Start cluster communication listener
	clusterAddr := fmt.Sprintf(":%d", ucm.config.ClusterPort)
	ln, err := net.Listen("tcp", clusterAddr)
	if err != nil {
		return fmt.Errorf("failed to start cluster listener: %w", err)
	}
	ucm.clusterListener = ln

	// Start data replication listener
	dataAddr := fmt.Sprintf(":%d", ucm.config.DataPort)
	dataLn, err := net.Listen("tcp", dataAddr)
	if err != nil {
		ucm.clusterListener.Close()
		return fmt.Errorf("failed to start data listener: %w", err)
	}
	ucm.dataListener = dataLn

	fmt.Printf("Unified Cluster Manager started (Node: %s, Cluster: %s, Data: %s)\n",
		ucm.nodeID, clusterAddr, dataAddr)

	// Start Raft consensus if enabled
	if ucm.useRaft && ucm.raftNode != nil {
		if err := ucm.raftNode.Start(); err != nil {
			ucm.clusterListener.Close()
			ucm.dataListener.Close()
			return fmt.Errorf("failed to start Raft node: %w", err)
		}
		fmt.Printf("Raft consensus enabled for node %s\n", ucm.nodeID)

		// Subscribe to Raft state changes via callbacks
		ucm.raftNode.SetLeaderCallback(func() {
			ucm.handleRaftStateChange(StateLeader)
		})
		ucm.raftNode.SetFollowerCallback(func(leaderID string) {
			ucm.handleRaftStateChange(StateFollower)
		})
	}

	// Start background goroutines
	ucm.wg.Add(4)
	go ucm.acceptClusterConnections()
	go ucm.acceptDataConnections()
	go ucm.heartbeatLoop()
	go ucm.eventDispatcher()

	// Join cluster if seeds are provided
	if len(ucm.config.Seeds) > 0 {
		go ucm.joinCluster()
	} else {
		// Bootstrap as single node cluster
		ucm.bootstrapSingleNode()
	}

	return nil
}

// Stop gracefully shuts down the cluster manager
func (ucm *UnifiedClusterManager) Stop() error {
	close(ucm.stopCh)

	// Stop Raft node if enabled
	if ucm.raftNode != nil {
		ucm.raftNode.Stop()
	}

	if ucm.clusterListener != nil {
		ucm.clusterListener.Close()
	}
	if ucm.dataListener != nil {
		ucm.dataListener.Close()
	}

	ucm.wg.Wait()
	return nil
}

// handleRaftStateChange handles Raft state transitions
func (ucm *UnifiedClusterManager) handleRaftStateChange(state RaftState) {
	ucm.roleMu.Lock()
	defer ucm.roleMu.Unlock()

	var newRole NodeRole
	switch state {
	case StateLeader:
		newRole = RoleLeader
	case StateCandidate:
		newRole = RoleCandidate
	case StateFollower:
		newRole = RoleFollower
	default:
		return
	}

	if ucm.role != newRole {
		oldRole := ucm.role
		ucm.role = newRole

		// Update term from Raft
		if ucm.raftNode != nil {
			atomic.StoreUint64(&ucm.term, ucm.raftNode.GetTerm())
		}

		// Emit role change event
		ucm.emitEvent(ClusterEvent{
			Type:      EventLeaderElected,
			NodeID:    ucm.nodeID,
			Term:      ucm.term,
			Timestamp: time.Now(),
			Details:   fmt.Sprintf("role changed: %s -> %s", oldRole.String(), newRole.String()),
		})

		fmt.Printf("Node %s role changed: %s -> %s (term %d)\n",
			ucm.nodeID, oldRole, newRole, ucm.term)
	}
}

// GetTerm returns the current election term
func (ucm *UnifiedClusterManager) GetTerm() uint64 {
	return atomic.LoadUint64(&ucm.term)
}

// IncrementTerm atomically increments and returns the new term
func (ucm *UnifiedClusterManager) IncrementTerm() uint64 {
	return atomic.AddUint64(&ucm.term, 1)
}

// IsLeader returns true if this node is the cluster leader
func (ucm *UnifiedClusterManager) IsLeader() bool {
	// Use Raft state if enabled
	if ucm.useRaft && ucm.raftNode != nil {
		return ucm.raftNode.IsLeader()
	}
	ucm.roleMu.RLock()
	defer ucm.roleMu.RUnlock()
	return ucm.role == RoleLeader
}

// GetRole returns the current role of this node
func (ucm *UnifiedClusterManager) GetRole() NodeRole {
	ucm.roleMu.RLock()
	defer ucm.roleMu.RUnlock()
	return ucm.role
}

// GetNodeID returns this node's ID
func (ucm *UnifiedClusterManager) GetNodeID() string {
	return ucm.nodeID
}

// GetMetrics returns a copy of the current cluster metrics
func (ucm *UnifiedClusterManager) GetMetrics() ClusterMetrics {
	ucm.metrics.mu.RLock()
	defer ucm.metrics.mu.RUnlock()
	return *ucm.metrics
}

// GetPartitionForKey returns the partition ID for a given key
func (ucm *UnifiedClusterManager) GetPartitionForKey(key string) int {
	return ucm.ring.GetPartition(key, ucm.config.PartitionCount)
}

// GetNodesForKey returns the nodes responsible for a key
func (ucm *UnifiedClusterManager) GetNodesForKey(key string) []*ClusterNode {
	return ucm.ring.GetNodes(key, ucm.config.ReplicationFactor)
}

// GetPartition returns the partition with the given ID
func (ucm *UnifiedClusterManager) GetPartition(id int) *Partition {
	ucm.partitionsMu.RLock()
	defer ucm.partitionsMu.RUnlock()
	return ucm.partitions[id]
}

// GetAllPartitions returns all partitions
func (ucm *UnifiedClusterManager) GetAllPartitions() []*Partition {
	ucm.partitionsMu.RLock()
	defer ucm.partitionsMu.RUnlock()

	partitions := make([]*Partition, 0, len(ucm.partitions))
	for _, p := range ucm.partitions {
		partitions = append(partitions, p)
	}
	return partitions
}

// GetNode returns the node with the given ID
func (ucm *UnifiedClusterManager) GetNode(id string) *ClusterNode {
	ucm.nodesMu.RLock()
	defer ucm.nodesMu.RUnlock()
	return ucm.nodes[id]
}

// GetAllNodes returns all nodes in the cluster
func (ucm *UnifiedClusterManager) GetAllNodes() []*ClusterNode {
	ucm.nodesMu.RLock()
	defer ucm.nodesMu.RUnlock()

	nodes := make([]*ClusterNode, 0, len(ucm.nodes))
	for _, n := range ucm.nodes {
		nodes = append(nodes, n)
	}
	return nodes
}

// GetAliveNodes returns all alive nodes in the cluster
func (ucm *UnifiedClusterManager) GetAliveNodes() []*ClusterNode {
	ucm.nodesMu.RLock()
	defer ucm.nodesMu.RUnlock()

	nodes := make([]*ClusterNode, 0, len(ucm.nodes))
	for _, n := range ucm.nodes {
		if n.State == NodeAlive {
			nodes = append(nodes, n)
		}
	}
	return nodes
}

// OnEvent registers a callback for cluster events
func (ucm *UnifiedClusterManager) OnEvent(callback func(ClusterEvent)) {
	ucm.eventMu.Lock()
	defer ucm.eventMu.Unlock()
	ucm.eventCallbacks = append(ucm.eventCallbacks, callback)
}

// emitEvent sends an event to all registered callbacks
func (ucm *UnifiedClusterManager) emitEvent(event ClusterEvent) {
	event.Term = ucm.GetTerm()
	event.Timestamp = time.Now()

	select {
	case ucm.eventCh <- event:
	default:
		// Channel full, drop event
	}
}

// eventDispatcher dispatches events to registered callbacks
func (ucm *UnifiedClusterManager) eventDispatcher() {
	defer ucm.wg.Done()

	for {
		select {
		case <-ucm.stopCh:
			return
		case event := <-ucm.eventCh:
			ucm.eventMu.RLock()
			callbacks := make([]func(ClusterEvent), len(ucm.eventCallbacks))
			copy(callbacks, ucm.eventCallbacks)
			ucm.eventMu.RUnlock()

			for _, cb := range callbacks {
				go cb(event)
			}
		}
	}
}

// acceptClusterConnections handles incoming cluster connections
func (ucm *UnifiedClusterManager) acceptClusterConnections() {
	defer ucm.wg.Done()

	for {
		select {
		case <-ucm.stopCh:
			return
		default:
		}

		ucm.clusterListener.(*net.TCPListener).SetDeadline(time.Now().Add(1 * time.Second))
		conn, err := ucm.clusterListener.Accept()
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			continue
		}

		go ucm.handleClusterConnection(conn)
	}
}

// acceptDataConnections handles incoming data replication connections
func (ucm *UnifiedClusterManager) acceptDataConnections() {
	defer ucm.wg.Done()

	for {
		select {
		case <-ucm.stopCh:
			return
		default:
		}

		ucm.dataListener.(*net.TCPListener).SetDeadline(time.Now().Add(1 * time.Second))
		conn, err := ucm.dataListener.Accept()
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			continue
		}

		go ucm.handleDataConnection(conn)
	}
}

// handleClusterConnection processes cluster protocol messages
func (ucm *UnifiedClusterManager) handleClusterConnection(conn net.Conn) {
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(10 * time.Second))

	// Read message type
	msgType := make([]byte, 1)
	if _, err := conn.Read(msgType); err != nil {
		return
	}

	switch msgType[0] {
	case msgHeartbeat:
		ucm.handleHeartbeat(conn)
	case msgVoteRequest:
		ucm.handleVoteRequest(conn)
	case msgVoteResponse:
		ucm.handleVoteResponse(conn)
	case msgJoinRequest:
		ucm.handleJoinRequest(conn)
	case msgPartitionSync:
		ucm.handlePartitionSync(conn)
	}
}

// handleDataConnection processes data replication messages and forwarded requests
func (ucm *UnifiedClusterManager) handleDataConnection(conn net.Conn) {
	// Try to peek at the first message to determine type
	reader := bufio.NewReader(conn)
	firstByte, err := reader.Peek(1)
	if err != nil {
		conn.Close()
		return
	}

	// If it starts with '{', it's a JSON forwarded request
	if firstByte[0] == '{' {
		ucm.handleForwardedRequest(conn, reader)
		return
	}

	// Otherwise, if replication is active (this node is leader), handle as follower connection
	if ucm.replActive {
		ucm.handleFollowerConnection(conn)
		return
	}

	// Otherwise close the connection - not ready for replication
	conn.Close()
}

// handleForwardedRequest handles forwarded GET/PUT/DELETE requests from other nodes
func (ucm *UnifiedClusterManager) handleForwardedRequest(conn net.Conn, reader *bufio.Reader) {
	defer conn.Close()

	var msg forwardMessage
	if err := json.NewDecoder(reader).Decode(&msg); err != nil {
		resp := forwardResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to decode request: %v", err),
		}
		json.NewEncoder(conn).Encode(resp)
		return
	}

	var resp forwardResponse

	switch msg.Type {
	case "GET":
		value, err := ucm.store.Get(msg.Key)
		if err != nil {
			resp.Success = false
			resp.Error = err.Error()
		} else {
			resp.Success = true
			resp.Value = value
		}

	case "PUT":
		if err := ucm.store.Put(msg.Key, msg.Value); err != nil {
			resp.Success = false
			resp.Error = err.Error()
		} else {
			resp.Success = true
		}

	case "DELETE":
		if err := ucm.store.Delete(msg.Key); err != nil {
			resp.Success = false
			resp.Error = err.Error()
		} else {
			resp.Success = true
		}

	default:
		resp.Success = false
		resp.Error = fmt.Sprintf("unknown operation type: %s", msg.Type)
	}

	json.NewEncoder(conn).Encode(resp)
}

// Message types for cluster protocol
const (
	msgHeartbeat     byte = 0x01
	msgVoteRequest   byte = 0x02
	msgVoteResponse  byte = 0x03
	msgJoinRequest   byte = 0x04
	msgJoinResponse  byte = 0x05
	msgJoinRejected  byte = 0x06 // Join rejected (e.g., encryption key mismatch)
	msgPartitionSync byte = 0x07
	msgDataSync      byte = 0x08
)

// heartbeatLoop sends periodic heartbeats to all nodes
func (ucm *UnifiedClusterManager) heartbeatLoop() {
	defer ucm.wg.Done()

	ticker := time.NewTicker(ucm.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ucm.stopCh:
			return
		case <-ticker.C:
			ucm.sendHeartbeats()
			ucm.checkNodeHealth()
		}
	}
}

// sendHeartbeats sends heartbeat to all known nodes
func (ucm *UnifiedClusterManager) sendHeartbeats() {
	ucm.nodesMu.RLock()
	nodes := make([]*ClusterNode, 0, len(ucm.nodes))
	for _, n := range ucm.nodes {
		if n.ID != ucm.nodeID {
			nodes = append(nodes, n)
		}
	}
	ucm.nodesMu.RUnlock()

	for _, node := range nodes {
		go ucm.sendHeartbeatTo(node)
	}
}

// sendHeartbeatTo sends a heartbeat to a specific node
func (ucm *UnifiedClusterManager) sendHeartbeatTo(node *ClusterNode) {
	addr := fmt.Sprintf("%s:%d", node.Addr, node.ClusterPort)
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		// Don't log every heartbeat failure - it gets noisy
		ucm.markNodeUnhealthy(node.ID)
		return
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(2 * time.Second))

	// Send heartbeat message
	msg := heartbeatMessage{
		NodeID: ucm.nodeID,
		Term:   ucm.GetTerm(),
		Role:   ucm.GetRole(),
	}
	data, _ := json.Marshal(msg)

	// Write message type + length + data
	conn.Write([]byte{msgHeartbeat})
	binary.Write(conn, binary.BigEndian, uint32(len(data)))
	conn.Write(data)

	// Update node health
	ucm.markNodeHealthy(node.ID)
}

// heartbeatMessage is sent between nodes for health checking
type heartbeatMessage struct {
	NodeID string   `json:"node_id"`
	Term   uint64   `json:"term"`
	Role   NodeRole `json:"role"`
}

// handleHeartbeat processes an incoming heartbeat
func (ucm *UnifiedClusterManager) handleHeartbeat(conn net.Conn) {
	var length uint32
	if err := binary.Read(conn, binary.BigEndian, &length); err != nil {
		return
	}

	data := make([]byte, length)
	if _, err := conn.Read(data); err != nil {
		return
	}

	var msg heartbeatMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return
	}

	// Check what we knew about leadership BEFORE this heartbeat
	ucm.nodesMu.Lock()
	var oldLeaderID string
	var wasLeader bool
	if node, ok := ucm.nodes[msg.NodeID]; ok {
		wasLeader = node.Role == RoleLeader
		node.Role = msg.Role
	}
	// Find current known leader (other than the one that just sent this heartbeat)
	for nodeID, node := range ucm.nodes {
		if node.Role == RoleLeader && nodeID != msg.NodeID {
			oldLeaderID = nodeID
			break
		}
	}
	// If we received a heartbeat from a new leader, clear the old leader's role
	if msg.Role == RoleLeader && oldLeaderID != "" {
		if oldNode, ok := ucm.nodes[oldLeaderID]; ok {
			oldNode.Role = RoleFollower
		}
	}
	ucm.nodesMu.Unlock()

	// If we're a follower and we received heartbeat from a (newly discovered) leader
	if msg.Role == RoleLeader && ucm.GetRole() == RoleFollower && !wasLeader {
		// This is a newly discovered leader
		leaderDataAddr := ucm.getNodeDataAddr(msg.NodeID)
		if leaderDataAddr != "" {
			if oldLeaderID != "" {
				fmt.Printf("Leader changed from %s to %s\n", oldLeaderID, msg.NodeID)
			} else {
				fmt.Printf("Leader %s discovered via heartbeat, becoming follower\n", msg.NodeID)
			}
			ucm.emitEvent(ClusterEvent{
				Type:    EventLeaderElected,
				NodeID:  msg.NodeID,
				Term:    msg.Term,
				Details: "discovered leader via heartbeat",
			})
		}
	}

	// Split-brain resolution: if we receive a heartbeat from another leader
	if msg.Role == RoleLeader && ucm.GetRole() == RoleLeader {
		// Higher term wins; if same term, higher node ID wins
		myTerm := ucm.GetTerm()
		if msg.Term > myTerm || (msg.Term == myTerm && msg.NodeID > ucm.nodeID) {
			fmt.Printf("Split-brain detected: stepping down for leader %s (term %d)\n", msg.NodeID, msg.Term)
			ucm.stepDown(msg.NodeID, msg.Term)
		}
	}

	ucm.markNodeHealthy(msg.NodeID)
}

// handleVoteRequest processes a vote request during leader election
func (ucm *UnifiedClusterManager) handleVoteRequest(conn net.Conn) {
	// Vote request handling for leader election
}

// handleVoteResponse processes a vote response
func (ucm *UnifiedClusterManager) handleVoteResponse(conn net.Conn) {
	// Vote response handling
}

// handleJoinRequest processes a node join request
func (ucm *UnifiedClusterManager) handleJoinRequest(conn net.Conn) {
	var length uint32
	if err := binary.Read(conn, binary.BigEndian, &length); err != nil {
		return
	}

	data := make([]byte, length)
	if _, err := conn.Read(data); err != nil {
		return
	}

	var node ClusterNode
	if err := json.Unmarshal(data, &node); err != nil {
		return
	}

	// CRITICAL: Validate encryption key hash
	// All nodes in a cluster MUST use the same encryption key
	if ucm.config.EncryptionKeyHash != "" || node.EncryptionKeyHash != "" {
		if ucm.config.EncryptionKeyHash != node.EncryptionKeyHash {
			// Encryption key mismatch - REJECT the join request
			fmt.Printf("❌ REJECTED node %s: Encryption key mismatch!\n", node.ID)
			fmt.Printf("   Local encryption hash:  %s\n", ucm.config.EncryptionKeyHash)
			fmt.Printf("   Remote encryption hash: %s\n", node.EncryptionKeyHash)
			fmt.Println("   All cluster nodes MUST use the same FLYDB_ENCRYPTION_PASSPHRASE")

			// Send rejection response
			conn.Write([]byte{msgJoinRejected})
			errorMsg := "Encryption key mismatch: All cluster nodes must use the same encryption passphrase"
			binary.Write(conn, binary.BigEndian, uint32(len(errorMsg)))
			conn.Write([]byte(errorMsg))
			return
		}
	}

	// Add the node to the cluster
	ucm.addNode(&node)

	// Send response with current cluster state
	response := ucm.getClusterState()
	respData, _ := json.Marshal(response)

	conn.Write([]byte{msgJoinResponse})
	binary.Write(conn, binary.BigEndian, uint32(len(respData)))
	conn.Write(respData)
}

// handlePartitionSync processes partition synchronization
func (ucm *UnifiedClusterManager) handlePartitionSync(conn net.Conn) {
	// Partition sync handling
}

// markNodeHealthy marks a node as healthy
func (ucm *UnifiedClusterManager) markNodeHealthy(nodeID string) {
	ucm.nodesMu.Lock()
	defer ucm.nodesMu.Unlock()

	if node, ok := ucm.nodes[nodeID]; ok {
		wasUnhealthy := node.State != NodeAlive
		node.State = NodeAlive
		node.LastHeartbeat = time.Now()

		if wasUnhealthy {
			ucm.emitEvent(ClusterEvent{
				Type:    EventNodeJoined,
				NodeID:  nodeID,
				Details: "node recovered",
			})
		}
	}
}

// markNodeUnhealthy marks a node as potentially unhealthy
func (ucm *UnifiedClusterManager) markNodeUnhealthy(nodeID string) {
	ucm.nodesMu.Lock()

	triggerReelection := false
	if node, ok := ucm.nodes[nodeID]; ok {
		if node.State == NodeAlive {
			node.State = NodeSuspect
			fmt.Printf("Node %s state changed: Alive -> Suspect\n", nodeID)
		} else if node.State == NodeSuspect {
			node.State = NodeDead
			fmt.Printf("Node %s state changed: Suspect -> Dead (Role: %s)\n", nodeID, node.Role)
			ucm.emitEvent(ClusterEvent{
				Type:    EventNodeFailed,
				NodeID:  nodeID,
				Details: "node failed health check",
			})
			// If this was the leader, trigger re-election
			if node.Role == RoleLeader && ucm.GetRole() == RoleFollower {
				triggerReelection = true
			}
		}
	}

	ucm.nodesMu.Unlock()

	if triggerReelection {
		fmt.Printf("Leader %s failed - triggering re-election\n", nodeID)
		go ucm.electLeaderAmongNodes()
	}
}

// checkNodeHealth checks the health of all nodes
func (ucm *UnifiedClusterManager) checkNodeHealth() {
	ucm.nodesMu.Lock()

	healthyCount := 0
	var leaderID string
	leaderDead := false

	for _, node := range ucm.nodes {
		if node.State == NodeAlive {
			healthyCount++
		}

		// Track the current known leader
		if node.Role == RoleLeader {
			leaderID = node.ID
		}

		// Check for stale heartbeats
		if node.ID != ucm.nodeID && time.Since(node.LastHeartbeat) > ucm.config.ElectionTimeout {
			if node.State == NodeAlive {
				node.State = NodeSuspect
			} else if node.State == NodeSuspect {
				// Node has been suspect for too long, mark as dead
				node.State = NodeDead
				// Check if this was the leader
				if node.Role == RoleLeader {
					leaderDead = true
					fmt.Printf("Leader %s detected as DEAD - triggering re-election\n", node.ID)
				}
			}
		}
	}

	ucm.nodesMu.Unlock()

	ucm.metrics.mu.Lock()
	ucm.metrics.TotalNodes = len(ucm.nodes)
	ucm.metrics.HealthyNodes = healthyCount
	ucm.metrics.mu.Unlock()

	// If the leader is dead and we're a follower, trigger re-election
	if leaderDead && ucm.GetRole() == RoleFollower {
		fmt.Printf("Starting re-election due to leader %s failure\n", leaderID)
		go ucm.electLeaderAmongNodes()
	}
}

// addNode adds a new node to the cluster
func (ucm *UnifiedClusterManager) addNode(node *ClusterNode) {
	ucm.nodesMu.Lock()
	defer ucm.nodesMu.Unlock()

	if _, exists := ucm.nodes[node.ID]; !exists {
		node.JoinedAt = time.Now()
		node.LastHeartbeat = time.Now()
		node.State = NodeAlive
		ucm.nodes[node.ID] = node
		ucm.ring.AddNode(node)

		ucm.emitEvent(ClusterEvent{
			Type:    EventNodeJoined,
			NodeID:  node.ID,
			Details: "new node joined",
		})

		// Trigger rebalancing if enabled
		if ucm.config.EnableAutoRebalance {
			go ucm.rebalancePartitions()
		}
	}
}

// getNodeDataAddr returns the data port address for a node
func (ucm *UnifiedClusterManager) getNodeDataAddr(nodeID string) string {
	ucm.nodesMu.RLock()
	defer ucm.nodesMu.RUnlock()

	if node, ok := ucm.nodes[nodeID]; ok {
		return fmt.Sprintf("%s:%d", node.Addr, node.DataPort)
	}
	return ""
}

// removeNode removes a node from the cluster
func (ucm *UnifiedClusterManager) removeNode(nodeID string) {
	ucm.nodesMu.Lock()
	defer ucm.nodesMu.Unlock()

	if _, exists := ucm.nodes[nodeID]; exists {
		delete(ucm.nodes, nodeID)
		ucm.ring.RemoveNode(nodeID)

		ucm.emitEvent(ClusterEvent{
			Type:    EventNodeLeft,
			NodeID:  nodeID,
			Details: "node removed",
		})

		// Trigger rebalancing if enabled
		if ucm.config.EnableAutoRebalance {
			go ucm.rebalancePartitions()
		}
	}
}

// clusterState represents the current state of the cluster
type clusterState struct {
	Term       uint64         `json:"term"`
	Nodes      []*ClusterNode `json:"nodes"`
	Partitions []*Partition   `json:"partitions"`
}

// getClusterState returns the current cluster state
func (ucm *UnifiedClusterManager) getClusterState() clusterState {
	return clusterState{
		Term:       ucm.GetTerm(),
		Nodes:      ucm.GetAllNodes(),
		Partitions: ucm.GetAllPartitions(),
	}
}

// joinCluster attempts to join an existing cluster via seed nodes with retries
func (ucm *UnifiedClusterManager) joinCluster() {
	// Build the self seed address to avoid connecting to ourselves
	selfSeed := fmt.Sprintf("%s:%d", ucm.nodeAddr, ucm.config.ClusterPort)
	selfSeedAlt := ucm.nodeID // NodeID is also in host:port format

	// Retry logic with exponential backoff
	maxRetries := 5
	retryDelay := 500 * time.Millisecond

	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			fmt.Printf("Join attempt %d/%d, waiting %v before retry...\n", attempt+1, maxRetries, retryDelay)
			time.Sleep(retryDelay)
			retryDelay *= 2 // Exponential backoff
			if retryDelay > 5*time.Second {
				retryDelay = 5 * time.Second
			}
		}

		for _, seed := range ucm.config.Seeds {
			// Skip if this seed is ourselves
			if seed == selfSeed || seed == selfSeedAlt || seed == ucm.nodeAddr {
				continue
			}

			conn, err := net.DialTimeout("tcp", seed, 5*time.Second)
			if err != nil {
				continue
			}

			// Send join request
			selfNode := ClusterNode{
				ID:                ucm.nodeID,
				Addr:              ucm.nodeAddr,
				ClusterPort:       ucm.config.ClusterPort,
				DataPort:          ucm.config.DataPort,
				State:             NodeAlive,
				Role:              RoleFollower,
				JoinedAt:          time.Now(),
				EncryptionKeyHash: ucm.config.EncryptionKeyHash, // Include encryption key hash for validation
			}
			data, _ := json.Marshal(selfNode)

			conn.Write([]byte{msgJoinRequest})
			binary.Write(conn, binary.BigEndian, uint32(len(data)))
			conn.Write(data)

			// Read response
			respType := make([]byte, 1)
			if _, err := conn.Read(respType); err != nil {
				conn.Close()
				continue
			}

			if respType[0] == msgJoinResponse {
				var length uint32
				if err := binary.Read(conn, binary.BigEndian, &length); err != nil {
					conn.Close()
					continue
				}

				respData := make([]byte, length)
				if _, err := conn.Read(respData); err != nil {
					conn.Close()
					continue
				}

				var state clusterState
				if err := json.Unmarshal(respData, &state); err != nil {
					conn.Close()
					continue
				}

				// Apply cluster state
				ucm.applyClusterState(state)
				conn.Close()
				fmt.Printf("Successfully joined cluster via %s\n", seed)
				return
			} else if respType[0] == msgJoinRejected {
				// Join was rejected - read error message
				var length uint32
				if err := binary.Read(conn, binary.BigEndian, &length); err != nil {
					conn.Close()
					continue
				}

				errorMsg := make([]byte, length)
				if _, err := conn.Read(errorMsg); err != nil {
					conn.Close()
					continue
				}

				// CRITICAL ERROR: Encryption key mismatch
				fmt.Println()
				fmt.Println("═══════════════════════════════════════════════════════════════")
				fmt.Println("❌ FATAL: Cluster join REJECTED - Encryption key mismatch!")
				fmt.Println("═══════════════════════════════════════════════════════════════")
				fmt.Println()
				fmt.Printf("  Seed node: %s\n", seed)
				fmt.Printf("  Reason: %s\n", string(errorMsg))
				fmt.Println()
				fmt.Println("  All cluster nodes MUST use the SAME encryption passphrase!")
				fmt.Println()
				fmt.Println("  To fix this:")
				fmt.Println("  1. Ensure all nodes use the same FLYDB_ENCRYPTION_PASSPHRASE")
				fmt.Println("  2. Restart this node with the correct passphrase")
				fmt.Println()
				fmt.Println("  Current encryption key hash (this node):")
				fmt.Printf("    %s\n", ucm.config.EncryptionKeyHash)
				fmt.Println()
				fmt.Println("═══════════════════════════════════════════════════════════════")
				fmt.Println()

				conn.Close()
				// Exit immediately - this is a fatal configuration error
				panic("Encryption key mismatch: Cannot join cluster with different encryption passphrase")
			}

			conn.Close()
		}
	}

	// Failed to join any seed after all retries, bootstrap as single node
	fmt.Println("Failed to join cluster after all retries, bootstrapping as single node")
	ucm.bootstrapSingleNode()
}

// applyClusterState applies a received cluster state
func (ucm *UnifiedClusterManager) applyClusterState(state clusterState) {
	// Update term if higher
	if state.Term > ucm.GetTerm() {
		atomic.StoreUint64(&ucm.term, state.Term)
	}

	// Add nodes and find the leader (excluding ourselves - we're joining as follower)
	// If there are multiple leaders (stale data), pick the one with the highest term/ID
	var leaderID string
	var leaderNode *ClusterNode
	for _, node := range state.Nodes {
		// Skip ourselves - when rejoining we should always become follower first
		if node.ID == ucm.nodeID {
			fmt.Printf("[DEBUG] applyClusterState: skipping self %s (role was %s)\n", node.ID, node.Role.String())
			continue
		}
		fmt.Printf("[DEBUG] applyClusterState: node %s role=%s state=%s\n", node.ID, node.Role.String(), node.State.String())
		ucm.addNode(node)
		// Track who is the leader - only consider alive/suspect nodes
		if node.Role == RoleLeader && (node.State == NodeAlive || node.State == NodeSuspect) {
			if leaderNode == nil || node.ID > leaderNode.ID {
				// Prefer higher node ID if there are multiple leaders
				leaderID = node.ID
				leaderNode = node
				fmt.Printf("[DEBUG] applyClusterState: found leader %s from node role\n", leaderID)
			}
		}
	}

	// Update partitions
	ucm.partitionsMu.Lock()
	for _, p := range state.Partitions {
		ucm.partitions[p.ID] = p
		// NOTE: Partition leader is NOT the same as cluster leader.
		// Partition leader is the owner of that shard for data distribution.
		// Cluster leader is elected via term-based election for replication.
	}
	ucm.partitionsMu.Unlock()

	fmt.Printf("[DEBUG] applyClusterState: final leaderID=%s, myID=%s\n", leaderID, ucm.nodeID)

	// If there's an existing leader and it's not us, become a follower
	if leaderID != "" && leaderID != ucm.nodeID {
		fmt.Printf("[DEBUG] applyClusterState: becoming follower of %s\n", leaderID)
		ucm.roleMu.Lock()
		ucm.role = RoleFollower
		ucm.roleMu.Unlock()

		// Emit event so the follower callback is triggered
		ucm.emitEvent(ClusterEvent{
			Type:    EventLeaderElected,
			NodeID:  leaderID,
			Details: "joined existing cluster as follower",
		})
	} else if leaderID == "" {
		fmt.Printf("[DEBUG] applyClusterState: no leader found, starting as follower and waiting for leader heartbeat\n")
		// No leader found yet - start as follower and wait for heartbeats
		// The leader will be discovered via heartbeat or we'll timeout and elect
		ucm.roleMu.Lock()
		ucm.role = RoleFollower
		ucm.roleMu.Unlock()
		
		// Start a goroutine to wait for leader discovery or timeout
		go ucm.waitForLeaderOrElect()
	}
}

// electLeaderAmongNodes elects a leader based on highest node ID (simple bully algorithm)
// Only alive nodes participate in the election
func (ucm *UnifiedClusterManager) electLeaderAmongNodes() {
	ucm.nodesMu.RLock()
	highestID := ucm.nodeID
	for nodeID, node := range ucm.nodes {
		// Only consider alive or suspect nodes (not dead ones)
		if node.State == NodeDead {
			continue
		}
		if nodeID > highestID {
			highestID = nodeID
		}
	}
	ucm.nodesMu.RUnlock()

	fmt.Printf("Election result: highest alive node ID is %s (self: %s)\n", highestID, ucm.nodeID)

	if highestID == ucm.nodeID {
		// We have the highest ID, become leader
		ucm.becomeLeader()
	} else {
		// Someone else should be leader
		ucm.roleMu.Lock()
		ucm.role = RoleFollower
		ucm.roleMu.Unlock()

		ucm.emitEvent(ClusterEvent{
			Type:    EventLeaderElected,
			NodeID:  highestID,
			Details: "elected by node ID comparison",
		})
	}
}

// waitForLeaderOrElect waits for a leader heartbeat or starts election after timeout
func (ucm *UnifiedClusterManager) waitForLeaderOrElect() {
	// Wait for a bit to receive heartbeats from an existing leader
	time.Sleep(ucm.config.HeartbeatInterval * 3)

	// Check if we've discovered a leader via heartbeat
	// The handleHeartbeat function will have already emitted the event if so
	ucm.nodesMu.RLock()
	var leaderFound string
	for nodeID, node := range ucm.nodes {
		// Only consider alive leaders (not dead nodes marked as leader from stale data)
		if node.Role == RoleLeader && node.State != NodeDead {
			leaderFound = nodeID
			break
		}
	}
	ucm.nodesMu.RUnlock()

	if leaderFound != "" {
		// Leader already discovered via heartbeat - handleHeartbeat would have
		// emitted the event, so we don't need to do anything here
		fmt.Printf("Leader %s was discovered during wait period, not running election\n", leaderFound)
		return
	}

	// No leader found, run election
	fmt.Printf("No leader discovered after waiting, starting election\n")
	ucm.electLeaderAmongNodes()
}

// becomeLeader transitions this node to the leader role
func (ucm *UnifiedClusterManager) becomeLeader() {
	ucm.IncrementTerm()

	ucm.roleMu.Lock()
	ucm.role = RoleLeader
	ucm.roleMu.Unlock()

	// Update self node role
	ucm.nodesMu.Lock()
	if self, ok := ucm.nodes[ucm.nodeID]; ok {
		self.Role = RoleLeader
	}
	ucm.nodesMu.Unlock()

	// Assign partitions
	ucm.rebalancePartitions()

	fmt.Printf("Node %s became LEADER (Term: %d)\n", ucm.nodeID, ucm.GetTerm())

	ucm.emitEvent(ClusterEvent{
		Type:    EventLeaderElected,
		NodeID:  ucm.nodeID,
		Details: "elected as leader",
	})
}

// stepDown transitions this node from leader to follower when another leader is detected
func (ucm *UnifiedClusterManager) stepDown(newLeaderID string, newTerm uint64) {
	ucm.roleMu.Lock()
	oldRole := ucm.role
	ucm.role = RoleFollower
	ucm.roleMu.Unlock()

	// Update term
	if newTerm > ucm.GetTerm() {
		atomic.StoreUint64(&ucm.term, newTerm)
	}

	// Update self node role
	ucm.nodesMu.Lock()
	if self, ok := ucm.nodes[ucm.nodeID]; ok {
		self.Role = RoleFollower
	}
	ucm.nodesMu.Unlock()

	// Deactivate replication leader
	ucm.replActive = false

	if oldRole == RoleLeader {
		fmt.Printf("Node %s stepped down from LEADER to FOLLOWER (new leader: %s)\n", ucm.nodeID, newLeaderID)

		// Emit event so the follower callback is triggered
		ucm.emitEvent(ClusterEvent{
			Type:    EventLeaderElected,
			NodeID:  newLeaderID,
			Details: "stepped down for new leader",
		})
	}
}

// bootstrapSingleNode initializes as a single-node cluster
func (ucm *UnifiedClusterManager) bootstrapSingleNode() {
	fmt.Printf("Bootstrapping as single-node cluster (Node: %s)\n", ucm.nodeID)
	ucm.becomeLeader()
}

// rebalancePartitions redistributes partitions across nodes
func (ucm *UnifiedClusterManager) rebalancePartitions() {
	ucm.emitEvent(ClusterEvent{
		Type:    EventRebalanceStarted,
		NodeID:  ucm.nodeID,
		Details: "partition rebalancing started",
	})

	nodes := ucm.ring.GetAllNodes()
	if len(nodes) == 0 {
		return
	}

	ucm.partitionsMu.Lock()
	defer ucm.partitionsMu.Unlock()

	// Redistribute partitions using consistent hashing
	for id, partition := range ucm.partitions {
		key := fmt.Sprintf("partition-%d", id)
		responsibleNodes := ucm.ring.GetNodes(key, ucm.config.ReplicationFactor)

		if len(responsibleNodes) > 0 {
			partition.Leader = responsibleNodes[0].ID
			partition.Replicas = make([]string, len(responsibleNodes))
			for i, n := range responsibleNodes {
				partition.Replicas[i] = n.ID
			}
			partition.State = PartitionHealthy
			partition.Version++
			partition.LastModified = time.Now()
		}
	}

	ucm.emitEvent(ClusterEvent{
		Type:    EventRebalanceComplete,
		NodeID:  ucm.nodeID,
		Details: "partition rebalancing complete",
	})
}

// ReplicateData replicates data to partition replicas with the specified consistency
func (ucm *UnifiedClusterManager) ReplicateData(partitionID int, key string, data []byte, consistency ConsistencyLevel) error {
	partition := ucm.GetPartition(partitionID)
	if partition == nil {
		return fmt.Errorf("partition %d not found", partitionID)
	}

	// Check if we're the leader for this partition
	if partition.Leader != ucm.nodeID {
		return fmt.Errorf("not leader for partition %d", partitionID)
	}

	// Determine required acknowledgments based on consistency level
	requiredAcks := 1
	switch consistency {
	case ConsistencyOne:
		requiredAcks = 1
	case ConsistencyQuorum:
		requiredAcks = (len(partition.Replicas) / 2) + 1
	case ConsistencyAll:
		requiredAcks = len(partition.Replicas)
	}

	// Replicate to followers
	ackCh := make(chan bool, len(partition.Replicas))
	ackCh <- true // Count self

	for _, replicaID := range partition.Replicas {
		if replicaID == ucm.nodeID {
			continue
		}

		go func(nodeID string) {
			if err := ucm.sendDataToReplica(nodeID, partitionID, key, data); err != nil {
				ackCh <- false
			} else {
				ackCh <- true
			}
		}(replicaID)
	}

	// Wait for required acknowledgments
	acks := 0
	timeout := time.After(ucm.config.SyncTimeout)

	for i := 0; i < len(partition.Replicas); i++ {
		select {
		case success := <-ackCh:
			if success {
				acks++
			}
			if acks >= requiredAcks {
				return nil
			}
		case <-timeout:
			if acks >= requiredAcks {
				return nil
			}
			return fmt.Errorf("replication timeout: got %d acks, need %d", acks, requiredAcks)
		}
	}

	if acks >= requiredAcks {
		return nil
	}
	return fmt.Errorf("insufficient acknowledgments: got %d, need %d", acks, requiredAcks)
}

// sendDataToReplica sends data to a specific replica
func (ucm *UnifiedClusterManager) sendDataToReplica(nodeID string, partitionID int, key string, data []byte) error {
	node := ucm.GetNode(nodeID)
	if node == nil {
		return fmt.Errorf("node %s not found", nodeID)
	}

	addr := fmt.Sprintf("%s:%d", node.Addr, node.DataPort)
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(ucm.config.SyncTimeout))

	// Send data sync message
	msg := dataSyncMessage{
		PartitionID: partitionID,
		Key:         key,
		Data:        data,
		Term:        ucm.GetTerm(),
	}
	msgData, _ := json.Marshal(msg)

	conn.Write([]byte{msgDataSync})
	binary.Write(conn, binary.BigEndian, uint32(len(msgData)))
	conn.Write(msgData)

	// Wait for acknowledgment
	ack := make([]byte, 1)
	if _, err := conn.Read(ack); err != nil {
		return err
	}

	if ack[0] != 0x01 {
		return fmt.Errorf("replication failed")
	}

	return nil
}

// dataSyncMessage is used for data replication
type dataSyncMessage struct {
	PartitionID int    `json:"partition_id"`
	Key         string `json:"key"`
	Data        []byte `json:"data"`
	Term        uint64 `json:"term"`
}

// GetPartitionLeader returns the leader node for a partition
func (ucm *UnifiedClusterManager) GetPartitionLeader(partitionID int) *ClusterNode {
	partition := ucm.GetPartition(partitionID)
	if partition == nil {
		return nil
	}
	return ucm.GetNode(partition.Leader)
}

// IsPartitionLeader returns true if this node is the leader for the partition
func (ucm *UnifiedClusterManager) IsPartitionLeader(partitionID int) bool {
	partition := ucm.GetPartition(partitionID)
	if partition == nil {
		return false
	}
	return partition.Leader == ucm.nodeID
}

// GetHealthyPartitionCount returns the number of healthy partitions
func (ucm *UnifiedClusterManager) GetHealthyPartitionCount() int {
	ucm.partitionsMu.RLock()
	defer ucm.partitionsMu.RUnlock()

	count := 0
	for _, p := range ucm.partitions {
		if p.State == PartitionHealthy {
			count++
		}
	}
	return count
}

// GetClusterHealth returns overall cluster health status
func (ucm *UnifiedClusterManager) GetClusterHealth() string {
	metrics := ucm.GetMetrics()

	if metrics.HealthyNodes == 0 {
		return "CRITICAL"
	}

	healthyRatio := float64(metrics.HealthyNodes) / float64(metrics.TotalNodes)
	partitionRatio := float64(ucm.GetHealthyPartitionCount()) / float64(metrics.TotalPartitions)

	if healthyRatio >= 1.0 && partitionRatio >= 1.0 {
		return "HEALTHY"
	} else if healthyRatio >= 0.5 && partitionRatio >= 0.5 {
		return "DEGRADED"
	}
	return "CRITICAL"
}

// ClusterStatus provides a snapshot of the cluster state for external consumers
type ClusterStatus struct {
	NodeID           string            `json:"node_id"`
	Role             string            `json:"role"`
	Term             uint64            `json:"term"`
	Health           string            `json:"health"`
	TotalNodes       int               `json:"total_nodes"`
	HealthyNodes     int               `json:"healthy_nodes"`
	TotalPartitions  int               `json:"total_partitions"`
	HealthyPartitions int              `json:"healthy_partitions"`
	LeaderPartitions int               `json:"leader_partitions"`
	Nodes            []NodeStatus      `json:"nodes"`
}

// NodeStatus provides status information for a single node
type NodeStatus struct {
	ID            string    `json:"id"`
	Addr          string    `json:"addr"`
	State         string    `json:"state"`
	Role          string    `json:"role"`
	Partitions    int       `json:"partitions"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
}

// GetStatus returns the current cluster status
func (ucm *UnifiedClusterManager) GetStatus() ClusterStatus {
	metrics := ucm.GetMetrics()

	// Count leader partitions for this node
	leaderCount := 0
	ucm.partitionsMu.RLock()
	for _, p := range ucm.partitions {
		if p.Leader == ucm.nodeID {
			leaderCount++
		}
	}
	ucm.partitionsMu.RUnlock()

	// Get node statuses
	nodes := ucm.GetAllNodes()
	nodeStatuses := make([]NodeStatus, 0, len(nodes))
	for _, n := range nodes {
		nodeStatuses = append(nodeStatuses, NodeStatus{
			ID:            n.ID,
			Addr:          n.Addr,
			State:         n.State.String(),
			Role:          n.Role.String(),
			Partitions:    len(n.Partitions),
			LastHeartbeat: n.LastHeartbeat,
		})
	}

	return ClusterStatus{
		NodeID:            ucm.nodeID,
		Role:              ucm.GetRole().String(),
		Term:              ucm.GetTerm(),
		Health:            ucm.GetClusterHealth(),
		TotalNodes:        metrics.TotalNodes,
		HealthyNodes:      metrics.HealthyNodes,
		TotalPartitions:   metrics.TotalPartitions,
		HealthyPartitions: ucm.GetHealthyPartitionCount(),
		LeaderPartitions:  leaderCount,
		Nodes:             nodeStatuses,
	}
}

// SetLeaderCallback sets a callback to be called when this node becomes leader
func (ucm *UnifiedClusterManager) SetLeaderCallback(callback func()) {
	ucm.OnEvent(func(event ClusterEvent) {
		if event.Type == EventLeaderElected && event.NodeID == ucm.nodeID {
			callback()
		}
	})
}

// SetFollowerCallback sets a callback to be called when this node becomes follower
func (ucm *UnifiedClusterManager) SetFollowerCallback(callback func(leaderID string)) {
	ucm.OnEvent(func(event ClusterEvent) {
		if event.Type == EventLeaderElected && event.NodeID != ucm.nodeID {
			callback(event.NodeID)
		}
	})
}

// GetConfig returns the cluster configuration
func (ucm *UnifiedClusterManager) GetConfig() ClusterConfig {
	return ucm.config
}

// ============================================================================
// WAL-Based Replication Methods
// ============================================================================

// StartReplicationLeader starts the WAL replication server for leader mode.
// This reuses the existing dataListener (started in Start()) and sets replActive
// so that handleDataConnection delegates to handleFollowerConnection.
func (ucm *UnifiedClusterManager) StartReplicationLeader(port string) error {
	if ucm.wal == nil {
		return fmt.Errorf("WAL not configured - call SetWAL first")
	}

	// Mark replication as active - dataListener will now handle follower connections
	ucm.replActive = true

	fmt.Printf("Replication Leader active on data port (consistency: %s)\n",
		ucm.config.DefaultConsistency)

	return nil
}

// acceptReplicationConnections accepts incoming follower connections
func (ucm *UnifiedClusterManager) acceptReplicationConnections() {
	defer ucm.wg.Done()

	for {
		select {
		case <-ucm.stopCh:
			if ucm.replListener != nil {
				ucm.replListener.Close()
			}
			return
		default:
		}

		ucm.replListener.(*net.TCPListener).SetDeadline(time.Now().Add(1 * time.Second))
		conn, err := ucm.replListener.Accept()
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			continue
		}

		go ucm.handleFollowerConnection(conn)
	}
}

// handleFollowerConnection manages a single follower connection with WAL streaming
func (ucm *UnifiedClusterManager) handleFollowerConnection(conn net.Conn) {
	defer conn.Close()

	// Read the follower's address for identification
	var addrLen uint32
	if err := binary.Read(conn, binary.BigEndian, &addrLen); err != nil {
		addrLen = 0
	}

	var followerAddr string
	if addrLen > 0 && addrLen < 1024 {
		addrBuf := make([]byte, addrLen)
		if _, err := io.ReadFull(conn, addrBuf); err != nil {
			return
		}
		followerAddr = string(addrBuf)
	} else {
		followerAddr = conn.RemoteAddr().String()
	}

	// Read the follower's current WAL offset
	var offset int64
	if err := binary.Read(conn, binary.BigEndian, &offset); err != nil {
		fmt.Printf("Failed to read follower offset: %v\n", err)
		return
	}

	fmt.Printf("Follower %s connected with offset %d\n", followerAddr, offset)

	// Register the follower
	ucm.followersMu.Lock()
	follower := &FollowerReplicationState{
		Address:     followerAddr,
		WALOffset:   offset,
		LastAckTime: time.Now(),
		IsHealthy:   true,
		conn:        conn,
	}
	ucm.followers[followerAddr] = follower
	ucm.followersMu.Unlock()

	defer func() {
		ucm.followersMu.Lock()
		delete(ucm.followers, followerAddr)
		ucm.followersMu.Unlock()
		fmt.Printf("Follower %s disconnected\n", followerAddr)
	}()

	// Start streaming WAL entries
	ticker := time.NewTicker(ucm.replPollInterval)
	defer ticker.Stop()

	currentOffset := offset
	var sendError error

	for {
		select {
		case <-ucm.stopCh:
			return
		case <-ticker.C:
			size, err := ucm.wal.Size()
			if err != nil {
				fmt.Printf("[DEBUG] WAL size error: %v\n", err)
				continue
			}

			if size > currentOffset {
				bytesReplicated := int64(0)
				sendError = nil

				// Check if WAL is configured
				if ucm.wal == nil {
					return
				}

				// Use ReplayWithPosition to get the actual new file offset
				// This is critical for encrypted WALs where file position != data size
				newOffset, err := ucm.wal.ReplayWithPosition(currentOffset, func(op byte, key string, value []byte) {
					if sendError != nil {
						return // Skip if we've already had a send error
					}

					// Serialize the WAL entry in unencrypted format for network transfer
					buf := make([]byte, 1+4+len(key)+4+len(value))
					buf[0] = op
					binary.BigEndian.PutUint32(buf[1:], uint32(len(key)))
					copy(buf[5:], []byte(key))
					off := 5 + len(key)
					binary.BigEndian.PutUint32(buf[off:], uint32(len(value)))
					copy(buf[off+4:], value)

					// Send to follower
					if _, werr := conn.Write(buf); werr != nil {
						sendError = werr
						follower.mu.Lock()
						follower.FailedSends++
						if follower.FailedSends >= 3 {
							follower.IsHealthy = false
						}
						follower.mu.Unlock()
						return
					}

					bytesReplicated += int64(len(buf))
				})

				// Handle send errors
				if sendError != nil {
					fmt.Printf("Send error for %s: %v\n", followerAddr, sendError)
					return
				}

				if err != nil && err != io.EOF {
					fmt.Printf("Replay error for %s: %v\n", followerAddr, err)
					return
				}

				// Update current offset to the actual file position returned by ReplayWithPosition
				currentOffset = newOffset

				// Update metrics
				ucm.metrics.mu.Lock()
				ucm.metrics.BytesReplicated += bytesReplicated
				ucm.metrics.mu.Unlock()

				// Update follower state
				follower.mu.Lock()
				follower.WALOffset = currentOffset
				follower.LastAckTime = time.Now()
				follower.FailedSends = 0
				follower.IsHealthy = true
				follower.mu.Unlock()
			}
		}
	}
}

// StartReplicationFollower connects to a leader and synchronizes data via WAL streaming.
func (ucm *UnifiedClusterManager) StartReplicationFollower(leaderAddr string) error {
	if ucm.wal == nil {
		return fmt.Errorf("WAL not configured - call SetWAL first")
	}
	if ucm.store == nil {
		return fmt.Errorf("Store not configured - call SetStore first")
	}

	// Stop any existing follower replication loop
	ucm.followerStopMu.Lock()
	if ucm.followerStopCh != nil {
		close(ucm.followerStopCh)
	}
	ucm.followerStopCh = make(chan struct{})
	stopCh := ucm.followerStopCh
	ucm.followerStopMu.Unlock()

	ucm.wg.Add(1)
	go ucm.followerReplicationLoop(leaderAddr, stopCh)
	return nil
}

// followerReplicationLoop continuously syncs with the leader
func (ucm *UnifiedClusterManager) followerReplicationLoop(leaderAddr string, stopCh chan struct{}) {
	defer ucm.wg.Done()

	reconnectInterval := 3 * time.Second

	for {
		select {
		case <-ucm.stopCh:
			return
		case <-stopCh:
			fmt.Printf("Follower replication to %s stopped (leader changed)\n", leaderAddr)
			return
		default:
		}

		err := ucm.connectAndSyncWithLeader(leaderAddr, stopCh)
		if err != nil {
			// Check if we should stop before reconnecting
			select {
			case <-stopCh:
				fmt.Printf("Follower replication to %s stopped (leader changed)\n", leaderAddr)
				return
			case <-ucm.stopCh:
				return
			default:
			}
			fmt.Printf("Replication connection lost: %v. Reconnecting in %v...\n",
				err, reconnectInterval)
			time.Sleep(reconnectInterval)
			continue
		}
	}
}

// connectAndSyncWithLeader establishes connection and syncs with leader
func (ucm *UnifiedClusterManager) connectAndSyncWithLeader(leaderAddr string, stopCh chan struct{}) error {
	conn, err := net.DialTimeout("tcp", leaderAddr, 5*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Send our address for identification
	selfAddr := fmt.Sprintf("%s:%d", ucm.nodeAddr, ucm.config.DataPort)
	if err := binary.Write(conn, binary.BigEndian, uint32(len(selfAddr))); err != nil {
		return err
	}
	if _, err := conn.Write([]byte(selfAddr)); err != nil {
		return err
	}

	// Request full WAL from leader starting at position 0
	// The leader will start reading from WALHeaderSize (position after header)
	// Each follower needs the complete WAL from the leader - not from its own WAL position
	// because the leader and follower have independent WAL files
	offset := int64(0)

	// Send our offset
	if err := binary.Write(conn, binary.BigEndian, offset); err != nil {
		return err
	}

	fmt.Printf("Connected to Leader at %s requesting full WAL sync\n", leaderAddr)

	reader := bufio.NewReader(conn)

	// Receive and apply WAL entries
	for {
		select {
		case <-ucm.stopCh:
			return nil
		case <-stopCh:
			return fmt.Errorf("leader changed")
		default:
		}

		// Set read deadline
		conn.SetReadDeadline(time.Now().Add(30 * time.Second))

		// Read operation type
		op, err := reader.ReadByte()
		if err != nil {
			return fmt.Errorf("failed to read operation: %w", err)
		}

		// Read key length
		var keyLen uint32
		if err := binary.Read(reader, binary.BigEndian, &keyLen); err != nil {
			return fmt.Errorf("failed to read key length: %w", err)
		}

		// Read key
		keyBuf := make([]byte, keyLen)
		if _, err := io.ReadFull(reader, keyBuf); err != nil {
			return fmt.Errorf("failed to read key: %w", err)
		}
		key := string(keyBuf)

		// Read value length
		var valLen uint32
		if err := binary.Read(reader, binary.BigEndian, &valLen); err != nil {
			return fmt.Errorf("failed to read value length: %w", err)
		}

		// Read value
		valBuf := make([]byte, valLen)
		if _, err := io.ReadFull(reader, valBuf); err != nil {
			return fmt.Errorf("failed to read value: %w", err)
		}

		// Apply the replicated operation using the proper MySQL/PostgreSQL pattern:
		// 1. Write to local WAL first (for crash recovery)
		// 2. Apply to storage WITHOUT re-writing to WAL
		//
		// Check if store supports ReplicatedStorageEngine interface
		if replStore, ok := ucm.store.(ReplicatedStorageEngine); ok {
			// Step 1: Write to local WAL for crash recovery
			if err := replStore.WriteReplicatedWAL(op, key, valBuf); err != nil {
				fmt.Printf("Failed to write replicated WAL for %s: %v\n", key, err)
				// Continue anyway - the data will still be applied
			}

			// Step 2: Apply to storage without re-writing to WAL
			switch op {
			case WALOpPut:
				if err := replStore.ApplyReplicatedPut(key, valBuf); err != nil {
					fmt.Printf("Failed to apply replicated PUT %s: %v\n", key, err)
				}
			case WALOpDelete:
				if err := replStore.ApplyReplicatedDelete(key); err != nil {
					fmt.Printf("Failed to apply replicated DELETE %s: %v\n", key, err)
				}
			}
		} else {
			// Fallback: use regular Put/Delete (will write to WAL twice)
			switch op {
			case WALOpPut:
				if err := ucm.store.Put(key, valBuf); err != nil {
					fmt.Printf("Failed to apply PUT %s: %v\n", key, err)
				}
			case WALOpDelete:
				if err := ucm.store.Delete(key); err != nil {
					fmt.Printf("Failed to apply DELETE %s: %v\n", key, err)
				}
			}
		}
	}
}

// GetFollowerStates returns the state of all connected followers
func (ucm *UnifiedClusterManager) GetFollowerStates() []FollowerReplicationState {
	ucm.followersMu.RLock()
	defer ucm.followersMu.RUnlock()

	states := make([]FollowerReplicationState, 0, len(ucm.followers))
	for _, f := range ucm.followers {
		f.mu.Lock()
		states = append(states, FollowerReplicationState{
			Address:     f.Address,
			WALOffset:   f.WALOffset,
			LastAckTime: f.LastAckTime,
			Lag:         f.Lag,
			IsHealthy:   f.IsHealthy,
			FailedSends: f.FailedSends,
		})
		f.mu.Unlock()
	}
	return states
}

// GetFollowerCount returns the number of connected followers
func (ucm *UnifiedClusterManager) GetFollowerCount() int {
	ucm.followersMu.RLock()
	defer ucm.followersMu.RUnlock()
	return len(ucm.followers)
}

// GetHealthyFollowerCount returns the number of healthy followers
func (ucm *UnifiedClusterManager) GetHealthyFollowerCount() int {
	ucm.followersMu.RLock()
	defer ucm.followersMu.RUnlock()

	count := 0
	for _, f := range ucm.followers {
		f.mu.Lock()
		if f.IsHealthy {
			count++
		}
		f.mu.Unlock()
	}
	return count
}

// WaitForReplication waits for replication to complete based on consistency level
func (ucm *UnifiedClusterManager) WaitForReplication(consistency ConsistencyLevel, timeout time.Duration) error {
	if consistency == ConsistencyEventual {
		return nil // No waiting for eventual consistency
	}

	ucm.followersMu.RLock()
	totalFollowers := len(ucm.followers)
	ucm.followersMu.RUnlock()

	if totalFollowers == 0 {
		return nil // No followers to wait for
	}

	requiredAcks := 1
	switch consistency {
	case ConsistencyOne:
		requiredAcks = 1
	case ConsistencyQuorum:
		requiredAcks = (totalFollowers / 2) + 1
	case ConsistencyAll:
		requiredAcks = totalFollowers
	}

	// Wait for required acknowledgments
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		healthyCount := ucm.GetHealthyFollowerCount()
		if healthyCount >= requiredAcks {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}

	return fmt.Errorf("replication timeout: insufficient acknowledgments")
}

// ============================================================================
// Partition-Aware Routing Layer for Horizontal Scaling
// ============================================================================

// RouteRequest represents a routing decision for a key operation
type RouteRequest struct {
	Key       string
	Operation string // "GET", "PUT", "DELETE"
}

// RouteResponse contains routing information for a request
type RouteResponse struct {
	PartitionID   int
	PrimaryNode   *ClusterNode
	ReplicaNodes  []*ClusterNode
	IsLocal       bool // true if this node is the primary
	RedirectAddr  string // address to redirect to if not local
}

// RouteKey determines which node should handle a key operation
// This is the core of horizontal scaling - it routes requests to the correct partition owner
func (ucm *UnifiedClusterManager) RouteKey(key string) (*RouteResponse, error) {
	// Get partition ID for this key using consistent hashing
	partitionID := ucm.GetPartitionForKey(key)

	// Get partition metadata
	partition := ucm.GetPartition(partitionID)
	if partition == nil {
		return nil, fmt.Errorf("partition %d not found", partitionID)
	}

	// Check partition state
	if partition.State != PartitionHealthy {
		return nil, fmt.Errorf("partition %d is not healthy (state: %s)", partitionID, partition.State)
	}

	// Get primary node for this partition
	primaryNode := ucm.GetNode(partition.Leader)
	if primaryNode == nil {
		return nil, fmt.Errorf("primary node %s not found for partition %d", partition.Leader, partitionID)
	}

	// Get replica nodes
	replicaNodes := make([]*ClusterNode, 0, len(partition.Replicas))
	for _, replicaID := range partition.Replicas {
		if replicaID == partition.Leader {
			continue // Skip primary
		}
		if node := ucm.GetNode(replicaID); node != nil {
			replicaNodes = append(replicaNodes, node)
		}
	}

	// Determine if this node is the primary
	isLocal := partition.Leader == ucm.nodeID

	// Build redirect address if not local
	redirectAddr := ""
	if !isLocal {
		redirectAddr = fmt.Sprintf("%s:%d", primaryNode.Addr, primaryNode.DataPort)
	}

	return &RouteResponse{
		PartitionID:  partitionID,
		PrimaryNode:  primaryNode,
		ReplicaNodes: replicaNodes,
		IsLocal:      isLocal,
		RedirectAddr: redirectAddr,
	}, nil
}

// Get performs a partition-aware GET operation
// If this node owns the partition, it reads locally
// Otherwise, it forwards the request to the owning node
func (ucm *UnifiedClusterManager) Get(key string) ([]byte, error) {
	route, err := ucm.RouteKey(key)
	if err != nil {
		return nil, fmt.Errorf("routing failed: %w", err)
	}

	if route.IsLocal {
		// This node owns the partition - read locally
		return ucm.store.Get(key)
	}

	// Forward to the owning node
	return ucm.forwardGet(route.RedirectAddr, key)
}

// Put performs a partition-aware PUT operation
// If this node owns the partition, it writes locally and replicates
// Otherwise, it forwards the request to the owning node
func (ucm *UnifiedClusterManager) Put(key string, value []byte) error {
	route, err := ucm.RouteKey(key)
	if err != nil {
		return fmt.Errorf("routing failed: %w", err)
	}

	if route.IsLocal {
		// This node owns the partition - write locally
		if err := ucm.store.Put(key, value); err != nil {
			return err
		}

		// Replicate to partition replicas with configured consistency
		return ucm.ReplicateData(route.PartitionID, key, value, ucm.config.DefaultConsistency)
	}

	// Forward to the owning node
	return ucm.forwardPut(route.RedirectAddr, key, value)
}

// Delete performs a partition-aware DELETE operation
func (ucm *UnifiedClusterManager) Delete(key string) error {
	route, err := ucm.RouteKey(key)
	if err != nil {
		return fmt.Errorf("routing failed: %w", err)
	}

	if route.IsLocal {
		// This node owns the partition - delete locally
		if err := ucm.store.Delete(key); err != nil {
			return err
		}

		// Replicate deletion to partition replicas
		return ucm.ReplicateData(route.PartitionID, key, nil, ucm.config.DefaultConsistency)
	}

	// Forward to the owning node
	return ucm.forwardDelete(route.RedirectAddr, key)
}

// forwardGet forwards a GET request to another node
func (ucm *UnifiedClusterManager) forwardGet(addr string, key string) ([]byte, error) {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", addr, err)
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(ucm.config.SyncTimeout))

	// Send GET request
	msg := forwardMessage{
		Type: "GET",
		Key:  key,
	}

	if err := json.NewEncoder(conn).Encode(msg); err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}

	// Read response
	var resp forwardResponse
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if !resp.Success {
		return nil, fmt.Errorf("remote error: %s", resp.Error)
	}

	return resp.Value, nil
}

// forwardPut forwards a PUT request to another node
func (ucm *UnifiedClusterManager) forwardPut(addr string, key string, value []byte) error {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %w", addr, err)
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(ucm.config.SyncTimeout))

	// Send PUT request
	msg := forwardMessage{
		Type:  "PUT",
		Key:   key,
		Value: value,
	}

	if err := json.NewEncoder(conn).Encode(msg); err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}

	// Read response
	var resp forwardResponse
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("remote error: %s", resp.Error)
	}

	return nil
}

// forwardDelete forwards a DELETE request to another node
func (ucm *UnifiedClusterManager) forwardDelete(addr string, key string) error {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %w", addr, err)
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(ucm.config.SyncTimeout))

	// Send DELETE request
	msg := forwardMessage{
		Type: "DELETE",
		Key:  key,
	}

	if err := json.NewEncoder(conn).Encode(msg); err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}

	// Read response
	var resp forwardResponse
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("remote error: %s", resp.Error)
	}

	return nil
}

// forwardMessage represents a forwarded operation
type forwardMessage struct {
	Type  string `json:"type"`
	Key   string `json:"key"`
	Value []byte `json:"value,omitempty"`
}

// forwardResponse represents a response to a forwarded operation
type forwardResponse struct {
	Success bool   `json:"success"`
	Value   []byte `json:"value,omitempty"`
	Error   string `json:"error,omitempty"`
}

// ============================================================================
// Partition Migration for Rebalancing
// ============================================================================

// MigratePartition migrates a partition's data from one node to another
// This is called during rebalancing when partition ownership changes
func (ucm *UnifiedClusterManager) MigratePartition(partitionID int, fromNode, toNode string) error {
	partition := ucm.GetPartition(partitionID)
	if partition == nil {
		return fmt.Errorf("partition %d not found", partitionID)
	}

	// Mark partition as migrating
	ucm.partitionsMu.Lock()
	partition.State = PartitionMigrating
	ucm.partitionsMu.Unlock()

	ucm.emitEvent(ClusterEvent{
		Type:        EventPartitionMigrating,
		NodeID:      ucm.nodeID,
		PartitionID: partitionID,
		Details:     fmt.Sprintf("migrating from %s to %s", fromNode, toNode),
	})

	// If this node is the source, send data to destination
	if fromNode == ucm.nodeID {
		if err := ucm.sendPartitionData(partitionID, toNode); err != nil {
			ucm.partitionsMu.Lock()
			partition.State = PartitionDegraded
			ucm.partitionsMu.Unlock()
			return fmt.Errorf("failed to send partition data: %w", err)
		}
	}

	// If this node is the destination, receive data from source
	if toNode == ucm.nodeID {
		if err := ucm.receivePartitionData(partitionID, fromNode); err != nil {
			ucm.partitionsMu.Lock()
			partition.State = PartitionDegraded
			ucm.partitionsMu.Unlock()
			return fmt.Errorf("failed to receive partition data: %w", err)
		}
	}

	// Update partition state
	ucm.partitionsMu.Lock()
	partition.State = PartitionHealthy
	partition.Leader = toNode
	partition.Version++
	partition.LastModified = time.Now()
	ucm.partitionsMu.Unlock()

	ucm.emitEvent(ClusterEvent{
		Type:        EventPartitionMoved,
		NodeID:      ucm.nodeID,
		PartitionID: partitionID,
		Details:     fmt.Sprintf("migration complete: %s -> %s", fromNode, toNode),
	})

	return nil
}

// sendPartitionData sends all data for a partition to another node
func (ucm *UnifiedClusterManager) sendPartitionData(partitionID int, toNode string) error {
	node := ucm.GetNode(toNode)
	if node == nil {
		return fmt.Errorf("destination node %s not found", toNode)
	}

	addr := fmt.Sprintf("%s:%d", node.Addr, node.DataPort)
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %w", addr, err)
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(5 * time.Minute)) // Long timeout for migration

	// Send migration start message
	startMsg := migrationMessage{
		Type:        "MIGRATION_START",
		PartitionID: partitionID,
	}

	if err := json.NewEncoder(conn).Encode(startMsg); err != nil {
		return fmt.Errorf("failed to send migration start: %w", err)
	}

	// Scan all keys in the store and send those belonging to this partition
	// Note: This is a simplified implementation. In production, you'd want to:
	// 1. Use a snapshot to ensure consistency
	// 2. Stream data in batches
	// 3. Handle errors and retries

	keys, err := ucm.getAllKeysForPartition(partitionID)
	if err != nil {
		return fmt.Errorf("failed to get partition keys: %w", err)
	}

	// Send each key-value pair
	for _, key := range keys {
		value, err := ucm.store.Get(key)
		if err != nil {
			continue // Skip keys that can't be read
		}

		dataMsg := migrationMessage{
			Type:  "MIGRATION_DATA",
			Key:   key,
			Value: value,
		}

		if err := json.NewEncoder(conn).Encode(dataMsg); err != nil {
			return fmt.Errorf("failed to send key %s: %w", key, err)
		}
	}

	// Send migration complete message
	completeMsg := migrationMessage{
		Type:        "MIGRATION_COMPLETE",
		PartitionID: partitionID,
	}

	if err := json.NewEncoder(conn).Encode(completeMsg); err != nil {
		return fmt.Errorf("failed to send migration complete: %w", err)
	}

	return nil
}

// receivePartitionData receives partition data from another node
func (ucm *UnifiedClusterManager) receivePartitionData(partitionID int, fromNode string) error {
	// This would be called by the data listener when receiving migration data
	// For now, this is a placeholder - the actual implementation would be in
	// handleDataConnection when it detects a MIGRATION_START message
	return nil
}

// getAllKeysForPartition returns all keys belonging to a partition
func (ucm *UnifiedClusterManager) getAllKeysForPartition(partitionID int) ([]string, error) {
	// This is a simplified implementation
	// In production, you'd want to use a more efficient method like:
	// 1. Maintain a per-partition key index
	// 2. Use range scans if your storage supports it
	// 3. Use snapshots for consistency

	allKeys := make([]string, 0)

	// Scan all keys in the store
	// Note: This assumes the store has a Scan method
	// You may need to adapt this based on your actual storage interface

	return allKeys, nil
}

// migrationMessage represents a partition migration message
type migrationMessage struct {
	Type        string `json:"type"` // MIGRATION_START, MIGRATION_DATA, MIGRATION_COMPLETE
	PartitionID int    `json:"partition_id,omitempty"`
	Key         string `json:"key,omitempty"`
	Value       []byte `json:"value,omitempty"`
}

// ============================================================================
// Scatter-Gather Query Support for Cross-Partition Operations
// ============================================================================

// ScatterGatherScan performs a scan across all partitions
// This is used for queries that need to access data across multiple partitions
func (ucm *UnifiedClusterManager) ScatterGatherScan(prefix string) (map[string][]byte, error) {
	results := make(map[string][]byte)
	resultsMu := sync.Mutex{}

	// Get all partitions
	ucm.partitionsMu.RLock()
	partitions := make([]*Partition, 0, len(ucm.partitions))
	for _, p := range ucm.partitions {
		partitions = append(partitions, p)
	}
	ucm.partitionsMu.RUnlock()

	// Group partitions by node
	nodePartitions := make(map[string][]int)
	for _, p := range partitions {
		nodePartitions[p.Leader] = append(nodePartitions[p.Leader], p.ID)
	}

	// Query each node in parallel
	var wg sync.WaitGroup
	errCh := make(chan error, len(nodePartitions))

	for nodeID, partitionIDs := range nodePartitions {
		wg.Add(1)
		go func(nid string, pids []int) {
			defer wg.Done()

			if nid == ucm.nodeID {
				// Local scan
				localResults, err := ucm.scanLocal(prefix)
				if err != nil {
					errCh <- fmt.Errorf("local scan failed: %w", err)
					return
				}

				resultsMu.Lock()
				for k, v := range localResults {
					results[k] = v
				}
				resultsMu.Unlock()
			} else {
				// Remote scan
				remoteResults, err := ucm.scanRemote(nid, prefix)
				if err != nil {
					errCh <- fmt.Errorf("remote scan of %s failed: %w", nid, err)
					return
				}

				resultsMu.Lock()
				for k, v := range remoteResults {
					results[k] = v
				}
				resultsMu.Unlock()
			}
		}(nodeID, partitionIDs)
	}

	wg.Wait()
	close(errCh)

	// Check for errors
	if len(errCh) > 0 {
		return nil, <-errCh
	}

	return results, nil
}

// scanLocal performs a local scan with a prefix
func (ucm *UnifiedClusterManager) scanLocal(prefix string) (map[string][]byte, error) {
	// This is a placeholder - implement based on your storage engine's scan capabilities
	results := make(map[string][]byte)
	return results, nil
}

// scanRemote performs a remote scan on another node
func (ucm *UnifiedClusterManager) scanRemote(nodeID string, prefix string) (map[string][]byte, error) {
	node := ucm.GetNode(nodeID)
	if node == nil {
		return nil, fmt.Errorf("node %s not found", nodeID)
	}

	addr := fmt.Sprintf("%s:%d", node.Addr, node.DataPort)
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", addr, err)
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(ucm.config.SyncTimeout))

	// Send scan request
	msg := forwardMessage{
		Type: "SCAN",
		Key:  prefix,
	}

	if err := json.NewEncoder(conn).Encode(msg); err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}

	// Read response
	var resp scanResponse
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if !resp.Success {
		return nil, fmt.Errorf("remote error: %s", resp.Error)
	}

	return resp.Results, nil
}

// scanResponse represents a response to a scan request
type scanResponse struct {
	Success bool              `json:"success"`
	Results map[string][]byte `json:"results,omitempty"`
	Error   string            `json:"error,omitempty"`
}
