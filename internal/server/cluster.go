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

FlyDB implements automatic failover using a leader election mechanism.
When the leader fails, followers detect the failure and elect a new leader.

Leader Election Algorithm:
==========================

FlyDB uses a simplified Bully algorithm for leader election:

  1. Each node has a unique ID (based on address)
  2. Nodes send heartbeats to the leader
  3. If a follower doesn't receive a heartbeat response, it starts an election
  4. The node with the highest ID (lexicographically) becomes the new leader
  5. The new leader announces itself to all other nodes

Heartbeat Protocol:
===================

  - Leader sends heartbeats to all followers every HeartbeatInterval
  - Followers expect heartbeats within HeartbeatTimeout
  - If no heartbeat is received, the follower initiates an election

Failover Process:
=================

  1. Follower detects leader failure (missed heartbeats)
  2. Follower starts election by sending ELECTION message to higher-ID nodes
  3. If no response, follower declares itself leader
  4. New leader sends COORDINATOR message to all nodes
  5. Other nodes update their leader reference
*/
package server

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
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

// ClusterNode represents a node in the cluster
type ClusterNode struct {
	ID      string
	Address string
}

// ClusterManager handles cluster membership and leader election
type ClusterManager struct {
	nodeID       string
	nodeAddr     string
	state        NodeState
	stateMu      sync.RWMutex
	leaderID     string
	leaderAddr   string
	leaderMu     sync.RWMutex
	peers        []ClusterNode
	peersMu      sync.RWMutex
	lastHeartbeat time.Time
	heartbeatMu  sync.RWMutex
	listener     net.Listener
	onBecomeLeader   func()
	onBecomeFollower func(leaderAddr string)
	stopCh       chan struct{}
	wg           sync.WaitGroup
}

// NewClusterManager creates a new cluster manager
func NewClusterManager(nodeAddr string, peers []string) *ClusterManager {
	cm := &ClusterManager{
		nodeID:   nodeAddr,
		nodeAddr: nodeAddr,
		state:    StateFollower,
		stopCh:   make(chan struct{}),
	}
	for _, addr := range peers {
		if addr != nodeAddr {
			cm.peers = append(cm.peers, ClusterNode{ID: addr, Address: addr})
		}
	}
	return cm
}

// SetCallbacks sets the callbacks for leader/follower transitions
func (cm *ClusterManager) SetCallbacks(onLeader func(), onFollower func(string)) {
	cm.onBecomeLeader = onLeader
	cm.onBecomeFollower = onFollower
}

// Start begins the cluster manager
func (cm *ClusterManager) Start(clusterPort string) error {
	ln, err := net.Listen("tcp", clusterPort)
	if err != nil {
		return fmt.Errorf("failed to start cluster listener: %w", err)
	}
	cm.listener = ln
	fmt.Printf("Cluster manager listening on %s (Node ID: %s)\n", clusterPort, cm.nodeID)

	cm.wg.Add(1)
	go cm.acceptLoop()

	cm.wg.Add(1)
	go cm.heartbeatMonitor()

	return nil
}

// Stop gracefully stops the cluster manager
func (cm *ClusterManager) Stop() {
	close(cm.stopCh)
	if cm.listener != nil {
		cm.listener.Close()
	}
	cm.wg.Wait()
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
	return cm.leaderAddr
}

// GetState returns the current node state
func (cm *ClusterManager) GetState() NodeState {
	cm.stateMu.RLock()
	defer cm.stateMu.RUnlock()
	return cm.state
}

// BecomeLeader forces this node to become the leader (for initial bootstrap)
func (cm *ClusterManager) BecomeLeader() {
	cm.stateMu.Lock()
	cm.state = StateLeader
	cm.stateMu.Unlock()

	cm.leaderMu.Lock()
	cm.leaderID = cm.nodeID
	cm.leaderAddr = cm.nodeAddr
	cm.leaderMu.Unlock()

	fmt.Printf("Node %s is now the LEADER\n", cm.nodeID)

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
	cm.leaderAddr = senderID
	cm.leaderMu.Unlock()

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
	cm.leaderAddr = senderID
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

	fmt.Printf("Node %s accepted %s as new LEADER\n", cm.nodeID, senderID)
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
			cm.peersMu.RLock()
			peers := make([]ClusterNode, len(cm.peers))
			copy(peers, cm.peers)
			cm.peersMu.RUnlock()

			for _, peer := range peers {
				go cm.sendHeartbeatTo(peer.Address)
			}
		}
	}
}

// sendHeartbeatTo sends a heartbeat to a specific peer
func (cm *ClusterManager) sendHeartbeatTo(addr string) {
	conn, err := net.DialTimeout("tcp", addr, 1*time.Second)
	if err != nil {
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
	io.ReadFull(conn, ack)
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
				fmt.Printf("Node %s: Leader heartbeat timeout, starting election\n", cm.nodeID)
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

	fmt.Printf("Node %s starting election\n", cm.nodeID)

	// Get peers with higher IDs
	cm.peersMu.RLock()
	var higherPeers []ClusterNode
	for _, peer := range cm.peers {
		if peer.ID > cm.nodeID {
			higherPeers = append(higherPeers, peer)
		}
	}
	cm.peersMu.RUnlock()

	// If no higher peers, we become the leader
	if len(higherPeers) == 0 {
		cm.becomeLeaderAfterElection()
		return
	}

	// Send election messages to higher-ID nodes
	responses := make(chan bool, len(higherPeers))
	for _, peer := range higherPeers {
		go func(addr string) {
			responses <- cm.sendElectionTo(addr)
		}(peer.Address)
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

	// If no higher node responded, we become the leader
	if !gotResponse {
		cm.becomeLeaderAfterElection()
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
	cm.leaderAddr = cm.nodeAddr
	cm.leaderMu.Unlock()

	fmt.Printf("Node %s won election, becoming LEADER\n", cm.nodeID)

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
	peers := make([]ClusterNode, len(cm.peers))
	copy(peers, cm.peers)
	cm.peersMu.RUnlock()

	for _, peer := range peers {
		go cm.sendCoordinatorTo(peer.Address)
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