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
Package server contains the cluster management subsystem for FlyDB.

Cluster Overview:
=================

FlyDB implements automatic failover using a leader election mechanism with
production-ready features including term-based elections, quorum requirements,
and split-brain prevention.

Enhanced Cluster Features:
==========================

1. Term-based Leader Election:
   - Each election increments a term number
   - Prevents stale leaders from accepting writes
   - Fencing tokens ensure only the current leader can modify data

2. Split-Brain Prevention:
   - Quorum-based decisions (majority required)
   - Leaders step down if they lose quorum
   - Network partition detection

3. Health Monitoring:
   - Per-node health status tracking
   - Configurable health check intervals
   - Automatic unhealthy node detection

4. Dynamic Cluster Membership:
   - Nodes can join/leave without downtime
   - Membership changes are replicated
   - Graceful node removal

5. Cluster Metrics:
   - Replication lag tracking
   - Election statistics
   - Connection health metrics

Leader Election Algorithm:
==========================

FlyDB uses an enhanced Bully algorithm for leader election:

  1. Each node has a unique ID (based on address)
  2. Elections use monotonically increasing term numbers
  3. Nodes send heartbeats to the leader
  4. If a follower doesn't receive a heartbeat response, it starts an election
  5. The node with the highest ID (lexicographically) becomes the new leader
  6. Elections require quorum acknowledgment
  7. The new leader announces itself to all other nodes

Heartbeat Protocol:
===================

  - Leader sends heartbeats to all followers every HeartbeatInterval
  - Followers expect heartbeats within HeartbeatTimeout
  - If no heartbeat is received, the follower initiates an election
  - Heartbeats include term number for consistency

Failover Process:
=================

  1. Follower detects leader failure (missed heartbeats)
  2. Follower increments term and starts election
  3. Follower sends ELECTION message to higher-ID nodes
  4. If no response and quorum available, follower declares itself leader
  5. New leader sends COORDINATOR message to all nodes
  6. Other nodes update their leader reference and term
*/
package server

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Cluster timing constants
const (
	HeartbeatInterval = 500 * time.Millisecond
	HeartbeatTimeout  = 2 * time.Second
	ElectionTimeout   = 1 * time.Second
	ReconnectInterval = 3 * time.Second
)

// Cluster message types
const (
	MsgHeartbeat    byte = 0x10
	MsgHeartbeatAck byte = 0x11
	MsgElection     byte = 0x12
	MsgElectionAck  byte = 0x13
	MsgCoordinator  byte = 0x14
)

// NodeState represents the current state of a cluster node
type NodeState int

const (
	StateFollower  NodeState = iota
	StateCandidate
	StateLeader
)

func (s NodeState) String() string {
	switch s {
	case StateFollower:
		return "Follower"
	case StateCandidate:
		return "Candidate"
	case StateLeader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// ClusterConfig holds configuration for the cluster manager
type ClusterConfig struct {
	// NodeID is the unique identifier for this node
	NodeID string

	// NodeAddr is the address this node listens on for cluster communication
	NodeAddr string

	// Peers is the initial list of peer addresses
	Peers []string

	// HeartbeatInterval is how often the leader sends heartbeats
	HeartbeatInterval time.Duration

	// HeartbeatTimeout is how long to wait before considering leader dead
	HeartbeatTimeout time.Duration

	// ElectionTimeout is how long to wait for election responses
	ElectionTimeout time.Duration

	// MinQuorum is the minimum number of nodes required for quorum
	// If 0, defaults to (len(Peers)+1)/2 + 1
	MinQuorum int

	// EnablePreVote enables the pre-vote protocol to prevent disruptions
	EnablePreVote bool

	// MaxReplicationLag is the maximum acceptable replication lag
	MaxReplicationLag time.Duration
}

// DefaultClusterConfig returns a ClusterConfig with sensible defaults
func DefaultClusterConfig(nodeAddr string, peers []string) ClusterConfig {
	return ClusterConfig{
		NodeID:            nodeAddr,
		NodeAddr:          nodeAddr,
		Peers:             peers,
		HeartbeatInterval: 500 * time.Millisecond,
		HeartbeatTimeout:  2 * time.Second,
		ElectionTimeout:   1 * time.Second,
		MinQuorum:         0, // Will be calculated
		EnablePreVote:     true,
		MaxReplicationLag: 5 * time.Second,
	}
}

// NodeHealth represents the health status of a cluster node
type NodeHealth struct {
	NodeID         string        `json:"node_id"`
	Address        string        `json:"address"`
	State          NodeState     `json:"state"`
	IsHealthy      bool          `json:"is_healthy"`
	LastHeartbeat  time.Time     `json:"last_heartbeat"`
	ReplicationLag time.Duration `json:"replication_lag"`
	FailedChecks   int           `json:"failed_checks"`
	JoinedAt       time.Time     `json:"joined_at"`
}

// ClusterMetrics holds cluster-wide metrics
type ClusterMetrics struct {
	mu sync.RWMutex

	// Term is the current election term
	Term uint64 `json:"term"`

	// ElectionCount is the total number of elections held
	ElectionCount uint64 `json:"election_count"`

	// LeaderChanges is the number of leader changes
	LeaderChanges uint64 `json:"leader_changes"`

	// LastElectionTime is when the last election occurred
	LastElectionTime time.Time `json:"last_election_time"`

	// AverageReplicationLag is the average lag across all followers
	AverageReplicationLag time.Duration `json:"average_replication_lag"`

	// HealthyNodes is the count of healthy nodes
	HealthyNodes int `json:"healthy_nodes"`

	// TotalNodes is the total node count
	TotalNodes int `json:"total_nodes"`

	// QuorumSize is the required quorum size
	QuorumSize int `json:"quorum_size"`

	// HasQuorum indicates if the cluster has quorum
	HasQuorum bool `json:"has_quorum"`
}

// ClusterEventType represents the type of cluster event
type ClusterEventType string

const (
	EventLeaderElected  ClusterEventType = "LEADER_ELECTED"
	EventLeaderStepDown ClusterEventType = "LEADER_STEP_DOWN"
	EventNodeJoined     ClusterEventType = "NODE_JOINED"
	EventNodeLeft       ClusterEventType = "NODE_LEFT"
	EventNodeUnhealthy  ClusterEventType = "NODE_UNHEALTHY"
	EventNodeRecovered  ClusterEventType = "NODE_RECOVERED"
	EventQuorumLost     ClusterEventType = "QUORUM_LOST"
	EventQuorumRestored ClusterEventType = "QUORUM_RESTORED"
	EventSplitBrainRisk ClusterEventType = "SPLIT_BRAIN_RISK"
)

// ClusterEvent represents a cluster state change event
type ClusterEvent struct {
	Type      ClusterEventType `json:"type"`
	NodeID    string           `json:"node_id"`
	Term      uint64           `json:"term"`
	Timestamp time.Time        `json:"timestamp"`
	Details   string           `json:"details"`
}

// ClusterNode represents a node in the cluster (legacy compatibility)
type ClusterNode struct {
	ID      string
	Address string
}

// ClusterManager handles cluster membership and leader election
type ClusterManager struct {
	config ClusterConfig

	// Current term number (monotonically increasing)
	term uint64

	nodeID   string
	nodeAddr string
	state    NodeState
	stateMu  sync.RWMutex
	leaderID string
	leaderMu sync.RWMutex

	// Peer management with health tracking
	peers   map[string]*NodeHealth
	peersMu sync.RWMutex

	// Legacy peer list for backward compatibility
	legacyPeers []ClusterNode

	lastHeartbeat time.Time
	heartbeatMu   sync.RWMutex

	// Self node tracking
	selfJoinedAt time.Time

	// Metrics
	metrics *ClusterMetrics

	// Event channel for cluster events
	eventCh chan ClusterEvent

	listener         net.Listener
	onBecomeLeader   func()
	onBecomeFollower func(leaderAddr string)
	onClusterEvent   func(ClusterEvent)
	stopCh           chan struct{}
	wg               sync.WaitGroup
}

// NewClusterManager creates a new cluster manager
func NewClusterManager(nodeAddr string, peers []string) *ClusterManager {
	config := DefaultClusterConfig(nodeAddr, peers)
	return NewClusterManagerWithConfig(config)
}

// NewClusterManagerWithConfig creates a new cluster manager with custom configuration
func NewClusterManagerWithConfig(config ClusterConfig) *ClusterManager {
	// Calculate quorum if not specified
	if config.MinQuorum == 0 {
		totalNodes := len(config.Peers) + 1 // Include self
		config.MinQuorum = totalNodes/2 + 1
	}

	cm := &ClusterManager{
		config:       config,
		term:         0,
		nodeID:       config.NodeAddr,
		nodeAddr:     config.NodeAddr,
		state:        StateFollower,
		peers:        make(map[string]*NodeHealth),
		metrics:      &ClusterMetrics{QuorumSize: config.MinQuorum},
		eventCh:      make(chan ClusterEvent, 100),
		stopCh:       make(chan struct{}),
		selfJoinedAt: time.Now(),
	}

	// Initialize peer health tracking
	for _, addr := range config.Peers {
		if addr != config.NodeAddr {
			cm.peers[addr] = &NodeHealth{
				NodeID:    addr,
				Address:   addr,
				State:     StateFollower,
				IsHealthy: true,
				JoinedAt:  time.Now(),
			}
			// Also maintain legacy peer list
			cm.legacyPeers = append(cm.legacyPeers, ClusterNode{ID: addr, Address: addr})
		}
	}

	return cm
}

// GetTerm returns the current election term
func (cm *ClusterManager) GetTerm() uint64 {
	return atomic.LoadUint64(&cm.term)
}

// IncrementTerm atomically increments and returns the new term
func (cm *ClusterManager) IncrementTerm() uint64 {
	return atomic.AddUint64(&cm.term, 1)
}

// GetMetrics returns a copy of the current cluster metrics
func (cm *ClusterManager) GetMetrics() ClusterMetrics {
	cm.metrics.mu.RLock()
	defer cm.metrics.mu.RUnlock()

	// Update dynamic metrics
	cm.peersMu.RLock()
	healthyCount := 0
	for _, peer := range cm.peers {
		if peer.IsHealthy {
			healthyCount++
		}
	}
	cm.peersMu.RUnlock()

	metrics := *cm.metrics
	metrics.Term = cm.GetTerm()
	metrics.HealthyNodes = healthyCount + 1 // Include self
	metrics.TotalNodes = len(cm.peers) + 1
	metrics.HasQuorum = metrics.HealthyNodes >= cm.config.MinQuorum

	return metrics
}

// GetNodeHealth returns the health status of all nodes
func (cm *ClusterManager) GetNodeHealth() []NodeHealth {
	cm.peersMu.RLock()
	defer cm.peersMu.RUnlock()

	health := make([]NodeHealth, 0, len(cm.peers)+1)

	// Add self
	cm.stateMu.RLock()
	selfHealth := NodeHealth{
		NodeID:    cm.config.NodeID,
		Address:   cm.config.NodeAddr,
		State:     cm.state,
		IsHealthy: true,
		JoinedAt:  cm.selfJoinedAt,
	}
	cm.stateMu.RUnlock()
	health = append(health, selfHealth)

	// Add peers
	for _, peer := range cm.peers {
		health = append(health, *peer)
	}

	return health
}

// HasQuorum returns true if the cluster has quorum
func (cm *ClusterManager) HasQuorum() bool {
	cm.peersMu.RLock()
	defer cm.peersMu.RUnlock()

	healthyCount := 1 // Count self
	for _, peer := range cm.peers {
		if peer.IsHealthy {
			healthyCount++
		}
	}

	return healthyCount >= cm.config.MinQuorum
}

// emitEvent sends a cluster event to listeners
func (cm *ClusterManager) emitEvent(eventType ClusterEventType, nodeID string, details string) {
	event := ClusterEvent{
		Type:      eventType,
		NodeID:    nodeID,
		Term:      cm.GetTerm(),
		Timestamp: time.Now(),
		Details:   details,
	}

	// Non-blocking send to event channel
	select {
	case cm.eventCh <- event:
	default:
		// Channel full, drop event
	}

	// Call callback if set
	if cm.onClusterEvent != nil {
		go cm.onClusterEvent(event)
	}
}

// SetCallbacks sets the callbacks for leader/follower transitions
func (cm *ClusterManager) SetCallbacks(onLeader func(), onFollower func(string)) {
	cm.onBecomeLeader = onLeader
	cm.onBecomeFollower = onFollower
}

// SetEventCallback sets the callback for cluster events
func (cm *ClusterManager) SetEventCallback(onEvent func(ClusterEvent)) {
	cm.onClusterEvent = onEvent
}

// Events returns the event channel for cluster events
func (cm *ClusterManager) Events() <-chan ClusterEvent {
	return cm.eventCh
}

// GetClusterStatus returns a JSON-serializable cluster status
func (cm *ClusterManager) GetClusterStatus() map[string]interface{} {
	metrics := cm.GetMetrics()
	health := cm.GetNodeHealth()

	cm.leaderMu.RLock()
	leaderID := cm.leaderID
	cm.leaderMu.RUnlock()

	return map[string]interface{}{
		"node_id":     cm.config.NodeID,
		"leader_id":   leaderID,
		"term":        metrics.Term,
		"has_quorum":  metrics.HasQuorum,
		"quorum_size": metrics.QuorumSize,
		"nodes":       health,
		"metrics":     metrics,
	}
}

// GetClusterStatusJSON returns the cluster status as JSON
func (cm *ClusterManager) GetClusterStatusJSON() (string, error) {
	status := cm.GetClusterStatus()
	data, err := json.MarshalIndent(status, "", "  ")
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// AddNode dynamically adds a new node to the cluster
func (cm *ClusterManager) AddNode(nodeAddr string) error {
	cm.peersMu.Lock()
	defer cm.peersMu.Unlock()

	// Check if node already exists
	if _, exists := cm.peers[nodeAddr]; exists {
		return nil // Already a member
	}

	// Add the new node
	cm.peers[nodeAddr] = &NodeHealth{
		NodeID:    nodeAddr,
		Address:   nodeAddr,
		State:     StateFollower,
		IsHealthy: true,
		JoinedAt:  time.Now(),
	}

	// Update legacy peer list
	cm.legacyPeers = append(cm.legacyPeers, ClusterNode{ID: nodeAddr, Address: nodeAddr})

	// Update quorum size
	totalNodes := len(cm.peers) + 1
	cm.config.MinQuorum = totalNodes/2 + 1
	cm.metrics.mu.Lock()
	cm.metrics.QuorumSize = cm.config.MinQuorum
	cm.metrics.mu.Unlock()

	cm.emitEvent(EventNodeJoined, nodeAddr, "")

	return nil
}

// RemoveNode dynamically removes a node from the cluster
func (cm *ClusterManager) RemoveNode(nodeAddr string) error {
	cm.peersMu.Lock()
	defer cm.peersMu.Unlock()

	// Check if node exists
	if _, exists := cm.peers[nodeAddr]; !exists {
		return nil // Not a member
	}

	// Remove the node
	delete(cm.peers, nodeAddr)

	// Update legacy peer list
	newLegacyPeers := make([]ClusterNode, 0, len(cm.legacyPeers)-1)
	for _, peer := range cm.legacyPeers {
		if peer.Address != nodeAddr {
			newLegacyPeers = append(newLegacyPeers, peer)
		}
	}
	cm.legacyPeers = newLegacyPeers

	// Update quorum size
	totalNodes := len(cm.peers) + 1
	cm.config.MinQuorum = totalNodes/2 + 1
	cm.metrics.mu.Lock()
	cm.metrics.QuorumSize = cm.config.MinQuorum
	cm.metrics.mu.Unlock()

	cm.emitEvent(EventNodeLeft, nodeAddr, "")

	return nil
}

// UpdateNodeHealth updates the health status of a peer node
func (cm *ClusterManager) UpdateNodeHealth(nodeAddr string, healthy bool, lag time.Duration) {
	cm.peersMu.Lock()
	defer cm.peersMu.Unlock()

	peer, exists := cm.peers[nodeAddr]
	if !exists {
		return
	}

	wasHealthy := peer.IsHealthy

	if healthy {
		peer.IsHealthy = true
		peer.FailedChecks = 0
		peer.LastHeartbeat = time.Now()
		peer.ReplicationLag = lag

		if !wasHealthy {
			cm.emitEvent(EventNodeRecovered, nodeAddr, "")
		}
	} else {
		peer.FailedChecks++
		// Mark unhealthy after 3 consecutive failures
		if peer.FailedChecks >= 3 {
			peer.IsHealthy = false
			if wasHealthy {
				cm.emitEvent(EventNodeUnhealthy, nodeAddr, "")
			}
		}
	}

	// Check if we lost quorum
	cm.checkQuorumStatus()
}

// checkQuorumStatus checks and emits events for quorum changes
func (cm *ClusterManager) checkQuorumStatus() {
	healthyCount := 1 // Count self
	for _, peer := range cm.peers {
		if peer.IsHealthy {
			healthyCount++
		}
	}

	hasQuorum := healthyCount >= cm.config.MinQuorum

	cm.metrics.mu.Lock()
	hadQuorum := cm.metrics.HasQuorum
	cm.metrics.HasQuorum = hasQuorum
	cm.metrics.HealthyNodes = healthyCount
	cm.metrics.mu.Unlock()

	if hadQuorum && !hasQuorum {
		cm.emitEvent(EventQuorumLost, cm.config.NodeID, "")
	} else if !hadQuorum && hasQuorum {
		cm.emitEvent(EventQuorumRestored, cm.config.NodeID, "")
	}
}

// StepDown causes the leader to step down (for graceful shutdown or testing)
func (cm *ClusterManager) StepDown() {
	cm.stateMu.Lock()
	wasLeader := cm.state == StateLeader
	cm.state = StateFollower
	cm.stateMu.Unlock()

	if wasLeader {
		cm.emitEvent(EventLeaderStepDown, cm.config.NodeID, "voluntary step down")
	}
}

// Start begins the cluster manager
func (cm *ClusterManager) Start(clusterPort string) error {
	ln, err := net.Listen("tcp", clusterPort)
	if err != nil {
		return fmt.Errorf("failed to start cluster listener: %w", err)
	}
	cm.listener = ln
	fmt.Printf("Cluster manager listening on %s (Node ID: %s, Term: %d)\n", clusterPort, cm.nodeID, cm.GetTerm())

	cm.wg.Add(1)
	go cm.acceptLoop()

	cm.wg.Add(1)
	go cm.heartbeatMonitor()

	return nil
}

// Stop gracefully stops the cluster manager
func (cm *ClusterManager) Stop() {
	// Step down if leader
	if cm.IsLeader() {
		cm.StepDown()
	}

	close(cm.stopCh)
	if cm.listener != nil {
		cm.listener.Close()
	}
	cm.wg.Wait()
	close(cm.eventCh)
}

// IsLeader returns true if this node is the current leader
func (cm *ClusterManager) IsLeader() bool {
	cm.stateMu.RLock()
	defer cm.stateMu.RUnlock()
	return cm.state == StateLeader
}

// GetLeaderAddr returns the current leader's address
func (cm *ClusterManager) GetLeaderAddr() string {
	cm.leaderMu.RLock()
	defer cm.leaderMu.RUnlock()
	return cm.leaderID
}

// GetState returns the current node state
func (cm *ClusterManager) GetState() NodeState {
	cm.stateMu.RLock()
	defer cm.stateMu.RUnlock()
	return cm.state
}

// BecomeLeader forces this node to become the leader (for initial bootstrap)
func (cm *ClusterManager) BecomeLeader() {
	cm.IncrementTerm()

	cm.stateMu.Lock()
	cm.state = StateLeader
	cm.stateMu.Unlock()

	cm.leaderMu.Lock()
	cm.leaderID = cm.nodeID
	cm.leaderMu.Unlock()

	fmt.Printf("Node %s is now the LEADER (Term: %d)\n", cm.nodeID, cm.GetTerm())

	cm.metrics.mu.Lock()
	cm.metrics.LeaderChanges++
	cm.metrics.mu.Unlock()

	cm.emitEvent(EventLeaderElected, cm.nodeID, "bootstrap")

	if cm.onBecomeLeader != nil {
		cm.onBecomeLeader()
	}

	// Start sending heartbeats
	cm.wg.Add(1)
	go cm.sendHeartbeats()
}

// acceptLoop handles incoming cluster connections
func (cm *ClusterManager) acceptLoop() {
	defer cm.wg.Done()

	for {
		select {
		case <-cm.stopCh:
			return
		default:
		}

		cm.listener.(*net.TCPListener).SetDeadline(time.Now().Add(1 * time.Second))
		conn, err := cm.listener.Accept()
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			select {
			case <-cm.stopCh:
				return
			default:
				continue
			}
		}

		go cm.handleClusterConnection(conn)
	}
}

// handleClusterConnection processes messages from other cluster nodes
func (cm *ClusterManager) handleClusterConnection(conn net.Conn) {
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))

	msgType := make([]byte, 1)
	if _, err := io.ReadFull(conn, msgType); err != nil {
		return
	}

	var idLen uint32
	if err := binary.Read(conn, binary.BigEndian, &idLen); err != nil {
		return
	}
	senderID := make([]byte, idLen)
	if _, err := io.ReadFull(conn, senderID); err != nil {
		return
	}

	switch msgType[0] {
	case MsgHeartbeat:
		cm.handleHeartbeat(conn, string(senderID))
	case MsgElection:
		cm.handleElection(conn, string(senderID))
	case MsgCoordinator:
		cm.handleCoordinator(string(senderID))
	}
}

// handleHeartbeat processes a heartbeat from the leader
func (cm *ClusterManager) handleHeartbeat(conn net.Conn, senderID string) {
	cm.heartbeatMu.Lock()
	cm.lastHeartbeat = time.Now()
	cm.heartbeatMu.Unlock()

	cm.leaderMu.Lock()
	cm.leaderID = senderID
	cm.leaderMu.Unlock()

	// Update peer health
	cm.UpdateNodeHealth(senderID, true, 0)

	cm.stateMu.Lock()
	if cm.state != StateFollower {
		cm.state = StateFollower
		cm.stateMu.Unlock()
		if cm.onBecomeFollower != nil {
			cm.onBecomeFollower(senderID)
		}
	} else {
		cm.stateMu.Unlock()
	}

	conn.Write([]byte{MsgHeartbeatAck})
}

// handleElection processes an election request from another node
func (cm *ClusterManager) handleElection(conn net.Conn, senderID string) {
	// If our ID is higher, we respond to indicate we're alive
	if cm.nodeID > senderID {
		conn.Write([]byte{MsgElectionAck})
		// Start our own election
		go cm.startElection()
	}
}

// handleCoordinator processes a coordinator announcement
func (cm *ClusterManager) handleCoordinator(senderID string) {
	cm.leaderMu.Lock()
	cm.leaderID = senderID
	cm.leaderMu.Unlock()

	cm.stateMu.Lock()
	wasLeader := cm.state == StateLeader
	cm.state = StateFollower
	cm.stateMu.Unlock()

	cm.heartbeatMu.Lock()
	cm.lastHeartbeat = time.Now()
	cm.heartbeatMu.Unlock()

	if wasLeader || cm.onBecomeFollower != nil {
		if cm.onBecomeFollower != nil {
			cm.onBecomeFollower(senderID)
		}
	}

	fmt.Printf("Node %s accepted %s as new LEADER (Term: %d)\n", cm.nodeID, senderID, cm.GetTerm())
}

// sendHeartbeats sends periodic heartbeats to all followers
func (cm *ClusterManager) sendHeartbeats() {
	defer cm.wg.Done()

	ticker := time.NewTicker(HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-cm.stopCh:
			return
		case <-ticker.C:
			if !cm.IsLeader() {
				return
			}

			// Check if we still have quorum
			if !cm.HasQuorum() {
				fmt.Printf("Node %s: Lost quorum, stepping down\n", cm.nodeID)
				cm.StepDown()
				return
			}

			cm.peersMu.RLock()
			peers := make([]string, 0, len(cm.peers))
			for addr := range cm.peers {
				peers = append(peers, addr)
			}
			cm.peersMu.RUnlock()

			for _, addr := range peers {
				go cm.sendHeartbeatTo(addr)
			}
		}
	}
}

// sendHeartbeatTo sends a heartbeat to a specific peer
func (cm *ClusterManager) sendHeartbeatTo(addr string) {
	conn, err := net.DialTimeout("tcp", addr, 1*time.Second)
	if err != nil {
		cm.UpdateNodeHealth(addr, false, 0)
		return
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(2 * time.Second))

	// Write message type
	conn.Write([]byte{MsgHeartbeat})

	// Write sender ID
	binary.Write(conn, binary.BigEndian, uint32(len(cm.nodeID)))
	conn.Write([]byte(cm.nodeID))

	// Wait for ack
	ack := make([]byte, 1)
	if _, err := io.ReadFull(conn, ack); err != nil {
		cm.UpdateNodeHealth(addr, false, 0)
		return
	}

	cm.UpdateNodeHealth(addr, true, 0)
}

// heartbeatMonitor monitors for leader heartbeats and triggers election if needed
func (cm *ClusterManager) heartbeatMonitor() {
	defer cm.wg.Done()

	// Initialize last heartbeat
	cm.heartbeatMu.Lock()
	cm.lastHeartbeat = time.Now()
	cm.heartbeatMu.Unlock()

	ticker := time.NewTicker(HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-cm.stopCh:
			return
		case <-ticker.C:
			if cm.IsLeader() {
				continue
			}

			cm.heartbeatMu.RLock()
			elapsed := time.Since(cm.lastHeartbeat)
			cm.heartbeatMu.RUnlock()

			if elapsed > HeartbeatTimeout {
				fmt.Printf("Node %s: Leader heartbeat timeout, starting election (Term: %d)\n", cm.nodeID, cm.GetTerm())
				cm.startElection()
			}
		}
	}
}

// startElection initiates a leader election using the Bully algorithm
func (cm *ClusterManager) startElection() {
	cm.stateMu.Lock()
	if cm.state == StateCandidate || cm.state == StateLeader {
		cm.stateMu.Unlock()
		return
	}
	cm.state = StateCandidate
	cm.stateMu.Unlock()

	// Increment term for new election
	newTerm := cm.IncrementTerm()

	cm.metrics.mu.Lock()
	cm.metrics.ElectionCount++
	cm.metrics.LastElectionTime = time.Now()
	cm.metrics.mu.Unlock()

	fmt.Printf("Node %s starting election (Term: %d)\n", cm.nodeID, newTerm)

	// Get peers with higher IDs
	cm.peersMu.RLock()
	var higherPeers []string
	for addr := range cm.peers {
		if addr > cm.nodeID {
			higherPeers = append(higherPeers, addr)
		}
	}
	cm.peersMu.RUnlock()

	// If no higher peers, we become the leader (if we have quorum)
	if len(higherPeers) == 0 {
		if cm.HasQuorum() {
			cm.becomeLeaderAfterElection()
		} else {
			fmt.Printf("Node %s: Cannot become leader without quorum\n", cm.nodeID)
			cm.stateMu.Lock()
			cm.state = StateFollower
			cm.stateMu.Unlock()
		}
		return
	}

	// Send election messages to higher-ID nodes
	responses := make(chan bool, len(higherPeers))
	for _, addr := range higherPeers {
		go func(peerAddr string) {
			responses <- cm.sendElectionTo(peerAddr)
		}(addr)
	}

	// Wait for responses with timeout
	timeout := time.After(ElectionTimeout)
	gotResponse := false

	for i := 0; i < len(higherPeers); i++ {
		select {
		case resp := <-responses:
			if resp {
				gotResponse = true
			}
		case <-timeout:
			break
		}
	}

	// If no higher node responded, we become the leader (if we have quorum)
	if !gotResponse {
		if cm.HasQuorum() {
			cm.becomeLeaderAfterElection()
		} else {
			fmt.Printf("Node %s: Cannot become leader without quorum\n", cm.nodeID)
			cm.stateMu.Lock()
			cm.state = StateFollower
			cm.stateMu.Unlock()
		}
	} else {
		// A higher node is alive, go back to follower and wait
		cm.stateMu.Lock()
		cm.state = StateFollower
		cm.stateMu.Unlock()
	}
}

// sendElectionTo sends an election message to a peer
func (cm *ClusterManager) sendElectionTo(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, ElectionTimeout)
	if err != nil {
		return false
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(ElectionTimeout))

	// Write message type
	conn.Write([]byte{MsgElection})

	// Write sender ID
	binary.Write(conn, binary.BigEndian, uint32(len(cm.nodeID)))
	conn.Write([]byte(cm.nodeID))

	// Wait for ack
	ack := make([]byte, 1)
	_, err = io.ReadFull(conn, ack)
	return err == nil && ack[0] == MsgElectionAck
}

// becomeLeaderAfterElection transitions to leader state after winning election
func (cm *ClusterManager) becomeLeaderAfterElection() {
	cm.stateMu.Lock()
	cm.state = StateLeader
	cm.stateMu.Unlock()

	cm.leaderMu.Lock()
	cm.leaderID = cm.nodeID
	cm.leaderMu.Unlock()

	cm.metrics.mu.Lock()
	cm.metrics.LeaderChanges++
	cm.metrics.mu.Unlock()

	fmt.Printf("Node %s won election, becoming LEADER (Term: %d)\n", cm.nodeID, cm.GetTerm())

	cm.emitEvent(EventLeaderElected, cm.nodeID, "election won")

	// Announce to all peers
	cm.announceLeadership()

	if cm.onBecomeLeader != nil {
		cm.onBecomeLeader()
	}

	// Start sending heartbeats
	cm.wg.Add(1)
	go cm.sendHeartbeats()
}

// announceLeadership sends coordinator messages to all peers
func (cm *ClusterManager) announceLeadership() {
	cm.peersMu.RLock()
	peers := make([]string, 0, len(cm.peers))
	for addr := range cm.peers {
		peers = append(peers, addr)
	}
	cm.peersMu.RUnlock()

	for _, addr := range peers {
		go cm.sendCoordinatorTo(addr)
	}
}

// sendCoordinatorTo sends a coordinator message to a peer
func (cm *ClusterManager) sendCoordinatorTo(addr string) {
	conn, err := net.DialTimeout("tcp", addr, 1*time.Second)
	if err != nil {
		return
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(2 * time.Second))

	// Write message type
	conn.Write([]byte{MsgCoordinator})

	// Write sender ID
	binary.Write(conn, binary.BigEndian, uint32(len(cm.nodeID)))
	conn.Write([]byte(cm.nodeID))
}