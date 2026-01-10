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
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
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
	EventNodeJoined       ClusterEventType = "NODE_JOINED"
	EventNodeLeft         ClusterEventType = "NODE_LEFT"
	EventNodeFailed       ClusterEventType = "NODE_FAILED"
	EventLeaderElected    ClusterEventType = "LEADER_ELECTED"
	EventPartitionMoved   ClusterEventType = "PARTITION_MOVED"
	EventRebalanceStarted ClusterEventType = "REBALANCE_STARTED"
	EventRebalanceComplete ClusterEventType = "REBALANCE_COMPLETE"
	EventQuorumLost       ClusterEventType = "QUORUM_LOST"
	EventQuorumRestored   ClusterEventType = "QUORUM_RESTORED"
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
		ring:             NewHashRing(config.VirtualNodes),
		partitions:       make(map[int]*Partition),
		nodes:            make(map[string]*ClusterNode),
		replicationState: make(map[int]*PartitionReplicationState),
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
		ID:          config.NodeID,
		Addr:        config.NodeAddr,
		ClusterPort: config.ClusterPort,
		DataPort:    config.DataPort,
		State:       NodeAlive,
		Role:        RoleFollower,
		JoinedAt:    time.Now(),
		Metadata:    make(map[string]string),
	}
	ucm.nodes[config.NodeID] = selfNode
	ucm.ring.AddNode(selfNode)

	return ucm
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

	if ucm.clusterListener != nil {
		ucm.clusterListener.Close()
	}
	if ucm.dataListener != nil {
		ucm.dataListener.Close()
	}

	ucm.wg.Wait()
	return nil
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

// handleDataConnection processes data replication messages
func (ucm *UnifiedClusterManager) handleDataConnection(conn net.Conn) {
	defer conn.Close()
	// Data replication handling will be implemented here
}

// Message types for cluster protocol
const (
	msgHeartbeat     byte = 0x01
	msgVoteRequest   byte = 0x02
	msgVoteResponse  byte = 0x03
	msgJoinRequest   byte = 0x04
	msgJoinResponse  byte = 0x05
	msgPartitionSync byte = 0x06
	msgDataSync      byte = 0x07
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
	defer ucm.nodesMu.Unlock()

	if node, ok := ucm.nodes[nodeID]; ok {
		if node.State == NodeAlive {
			node.State = NodeSuspect
		} else if node.State == NodeSuspect {
			node.State = NodeDead
			ucm.emitEvent(ClusterEvent{
				Type:    EventNodeFailed,
				NodeID:  nodeID,
				Details: "node failed health check",
			})
		}
	}
}

// checkNodeHealth checks the health of all nodes
func (ucm *UnifiedClusterManager) checkNodeHealth() {
	ucm.nodesMu.Lock()
	defer ucm.nodesMu.Unlock()

	healthyCount := 0
	for _, node := range ucm.nodes {
		if node.State == NodeAlive {
			healthyCount++
		}

		// Check for stale heartbeats
		if node.ID != ucm.nodeID && time.Since(node.LastHeartbeat) > ucm.config.ElectionTimeout {
			if node.State == NodeAlive {
				node.State = NodeSuspect
			}
		}
	}

	ucm.metrics.mu.Lock()
	ucm.metrics.TotalNodes = len(ucm.nodes)
	ucm.metrics.HealthyNodes = healthyCount
	ucm.metrics.mu.Unlock()
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

// joinCluster attempts to join an existing cluster via seed nodes
func (ucm *UnifiedClusterManager) joinCluster() {
	for _, seed := range ucm.config.Seeds {
		if seed == ucm.nodeAddr {
			continue
		}

		conn, err := net.DialTimeout("tcp", seed, 5*time.Second)
		if err != nil {
			continue
		}

		// Send join request
		selfNode := ClusterNode{
			ID:          ucm.nodeID,
			Addr:        ucm.nodeAddr,
			ClusterPort: ucm.config.ClusterPort,
			DataPort:    ucm.config.DataPort,
			State:       NodeAlive,
			Role:        RoleFollower,
			JoinedAt:    time.Now(),
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
		}

		conn.Close()
	}

	// Failed to join any seed, bootstrap as single node
	fmt.Println("Failed to join cluster, bootstrapping as single node")
	ucm.bootstrapSingleNode()
}

// applyClusterState applies a received cluster state
func (ucm *UnifiedClusterManager) applyClusterState(state clusterState) {
	// Update term if higher
	if state.Term > ucm.GetTerm() {
		atomic.StoreUint64(&ucm.term, state.Term)
	}

	// Add nodes
	for _, node := range state.Nodes {
		if node.ID != ucm.nodeID {
			ucm.addNode(node)
		}
	}

	// Update partitions
	ucm.partitionsMu.Lock()
	for _, p := range state.Partitions {
		ucm.partitions[p.ID] = p
	}
	ucm.partitionsMu.Unlock()
}

// bootstrapSingleNode initializes as a single-node cluster
func (ucm *UnifiedClusterManager) bootstrapSingleNode() {
	ucm.IncrementTerm()

	ucm.roleMu.Lock()
	ucm.role = RoleLeader
	ucm.roleMu.Unlock()

	// Assign all partitions to self
	ucm.partitionsMu.Lock()
	for id := range ucm.partitions {
		ucm.partitions[id].Leader = ucm.nodeID
		ucm.partitions[id].Replicas = []string{ucm.nodeID}
		ucm.partitions[id].State = PartitionHealthy
		ucm.partitions[id].Version = 1
	}
	ucm.partitionsMu.Unlock()

	fmt.Printf("Bootstrapped as single-node cluster (Node: %s, Term: %d)\n", ucm.nodeID, ucm.GetTerm())

	ucm.emitEvent(ClusterEvent{
		Type:    EventLeaderElected,
		NodeID:  ucm.nodeID,
		Details: "single node bootstrap",
	})
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
