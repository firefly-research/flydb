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
	"testing"
	"time"
)

// TestConsistencyLevelString tests the String method of ConsistencyLevel
func TestConsistencyLevelString(t *testing.T) {
	tests := []struct {
		level    ConsistencyLevel
		expected string
	}{
		{ConsistencyEventual, "EVENTUAL"},
		{ConsistencyOne, "ONE"},
		{ConsistencyQuorum, "QUORUM"},
		{ConsistencyAll, "ALL"},
		{ConsistencyLevel(99), "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			if got := tt.level.String(); got != tt.expected {
				t.Errorf("ConsistencyLevel.String() = %v, want %v", got, tt.expected)
			}
		})
	}
}

// TestNodeRoleString tests the String method of NodeRole
func TestNodeRoleString(t *testing.T) {
	tests := []struct {
		role     NodeRole
		expected string
	}{
		{RoleFollower, "FOLLOWER"},
		{RoleCandidate, "CANDIDATE"},
		{RoleLeader, "LEADER"},
		{NodeRole(99), "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			if got := tt.role.String(); got != tt.expected {
				t.Errorf("NodeRole.String() = %v, want %v", got, tt.expected)
			}
		})
	}
}

// TestNodeStateString tests the String method of NodeState
func TestNodeStateString(t *testing.T) {
	tests := []struct {
		state    NodeState
		expected string
	}{
		{NodeAlive, "ALIVE"},
		{NodeSuspect, "SUSPECT"},
		{NodeDead, "DEAD"},
		{NodeState(99), "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			if got := tt.state.String(); got != tt.expected {
				t.Errorf("NodeState.String() = %v, want %v", got, tt.expected)
			}
		})
	}
}

// TestPartitionStateString tests the String method of PartitionState
func TestPartitionStateString(t *testing.T) {
	tests := []struct {
		state    PartitionState
		expected string
	}{
		{PartitionHealthy, "HEALTHY"},
		{PartitionRecovering, "RECOVERING"},
		{PartitionUnavailable, "UNAVAILABLE"},
		{PartitionState(99), "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			if got := tt.state.String(); got != tt.expected {
				t.Errorf("PartitionState.String() = %v, want %v", got, tt.expected)
			}
		})
	}
}

// TestNewHashRing tests the creation of a new hash ring
func TestNewHashRing(t *testing.T) {
	ring := NewHashRing(100)
	if ring == nil {
		t.Fatal("NewHashRing returned nil")
	}
	if ring.virtualNodes != 100 {
		t.Errorf("Expected 100 virtual nodes, got %d", ring.virtualNodes)
	}
}

// TestHashRingAddRemoveNode tests adding and removing nodes from the hash ring
func TestHashRingAddRemoveNode(t *testing.T) {
	ring := NewHashRing(10)

	node1 := &ClusterNode{ID: "node1", Addr: "localhost", ClusterPort: 9001}
	node2 := &ClusterNode{ID: "node2", Addr: "localhost", ClusterPort: 9002}

	ring.AddNode(node1)
	ring.AddNode(node2)

	// Should have 20 virtual nodes (10 per node)
	if len(ring.ring) != 20 {
		t.Errorf("Expected 20 virtual nodes, got %d", len(ring.ring))
	}

	ring.RemoveNode("node1")

	// Should have 10 virtual nodes remaining
	if len(ring.ring) != 10 {
		t.Errorf("Expected 10 virtual nodes after removal, got %d", len(ring.ring))
	}
}

// TestHashRingGetNode tests getting a node for a key
func TestHashRingGetNode(t *testing.T) {
	ring := NewHashRing(10)

	node1 := &ClusterNode{ID: "node1", Addr: "localhost", ClusterPort: 9001}
	node2 := &ClusterNode{ID: "node2", Addr: "localhost", ClusterPort: 9002}

	ring.AddNode(node1)
	ring.AddNode(node2)

	// Get node for a key - should return consistently
	node := ring.GetNode("test-key")
	if node == nil {
		t.Fatal("GetNode returned nil")
	}

	// Same key should return same node
	node2Result := ring.GetNode("test-key")
	if node2Result.ID != node.ID {
		t.Error("GetNode returned different nodes for same key")
	}
}

// TestHashRingGetNodes tests getting multiple nodes for replication
func TestHashRingGetNodes(t *testing.T) {
	ring := NewHashRing(10)

	node1 := &ClusterNode{ID: "node1", Addr: "localhost", ClusterPort: 9001}
	node2 := &ClusterNode{ID: "node2", Addr: "localhost", ClusterPort: 9002}
	node3 := &ClusterNode{ID: "node3", Addr: "localhost", ClusterPort: 9003}

	ring.AddNode(node1)
	ring.AddNode(node2)
	ring.AddNode(node3)

	// Get 2 nodes for replication
	nodes := ring.GetNodes("test-key", 2)
	if len(nodes) != 2 {
		t.Errorf("Expected 2 nodes, got %d", len(nodes))
	}

	// Nodes should be unique
	if nodes[0].ID == nodes[1].ID {
		t.Error("GetNodes returned duplicate nodes")
	}
}

// TestNewUnifiedClusterManager tests creating a new unified cluster manager
func TestNewUnifiedClusterManager(t *testing.T) {
	config := ClusterConfig{
		NodeID:             "test-node",
		NodeAddr:           "localhost",
		ClusterPort:        0, // Use random port
		DataPort:           0,
		PartitionCount:     16,
		ReplicationFactor:  3,
		VirtualNodes:       100,
		HeartbeatInterval:  time.Second,
		ElectionTimeout:    5 * time.Second,
		SyncTimeout:        10 * time.Second,
		EnableAutoRebalance: true,
	}

	ucm := NewUnifiedClusterManager(config)
	if ucm == nil {
		t.Fatal("NewUnifiedClusterManager returned nil")
	}

	if ucm.nodeID != "test-node" {
		t.Errorf("Expected nodeID 'test-node', got '%s'", ucm.nodeID)
	}

	if ucm.GetRole() != RoleFollower {
		t.Errorf("Expected initial role FOLLOWER, got %s", ucm.GetRole())
	}

	if ucm.GetTerm() != 0 {
		t.Errorf("Expected initial term 0, got %d", ucm.GetTerm())
	}
}

// TestUnifiedClusterManagerGetPartition tests partition retrieval
func TestUnifiedClusterManagerGetPartition(t *testing.T) {
	config := ClusterConfig{
		NodeID:         "test-node",
		NodeAddr:       "localhost",
		PartitionCount: 16,
		VirtualNodes:   100,
	}

	ucm := NewUnifiedClusterManager(config)

	// Get existing partition
	partition := ucm.GetPartition(0)
	if partition == nil {
		t.Fatal("GetPartition returned nil for existing partition")
	}
	if partition.ID != 0 {
		t.Errorf("Expected partition ID 0, got %d", partition.ID)
	}

	// Get non-existing partition
	partition = ucm.GetPartition(100)
	if partition != nil {
		t.Error("GetPartition should return nil for non-existing partition")
	}
}

// TestUnifiedClusterManagerGetPartitionForKey tests key-to-partition mapping
func TestUnifiedClusterManagerGetPartitionForKey(t *testing.T) {
	config := ClusterConfig{
		NodeID:         "test-node",
		NodeAddr:       "localhost",
		PartitionCount: 16,
		VirtualNodes:   100,
	}

	ucm := NewUnifiedClusterManager(config)

	// Same key should always map to same partition
	partitionID1 := ucm.GetPartitionForKey("test-key")
	partitionID2 := ucm.GetPartitionForKey("test-key")

	if partitionID1 != partitionID2 {
		t.Error("Same key mapped to different partitions")
	}

	// Partition ID should be within range
	if partitionID1 < 0 || partitionID1 >= 16 {
		t.Errorf("Partition ID %d out of range [0, 16)", partitionID1)
	}
}

// TestClusterEventTypes tests cluster event type constants
func TestClusterEventTypes(t *testing.T) {
	events := []struct {
		eventType ClusterEventType
		name      string
	}{
		{EventNodeJoined, "EventNodeJoined"},
		{EventNodeFailed, "EventNodeFailed"},
		{EventLeaderElected, "EventLeaderElected"},
		{EventPartitionMoved, "EventPartitionMoved"},
		{EventRebalanceComplete, "EventRebalanceComplete"},
	}

	for _, e := range events {
		if e.eventType == "" {
			t.Errorf("Event type %s has empty value", e.name)
		}
	}
}

// TestClusterMetrics tests cluster metrics initialization
func TestClusterMetrics(t *testing.T) {
	config := ClusterConfig{
		NodeID:         "test-node",
		NodeAddr:       "localhost",
		PartitionCount: 16,
		VirtualNodes:   100,
	}

	ucm := NewUnifiedClusterManager(config)
	metrics := ucm.GetMetrics()

	if metrics.TotalPartitions != 16 {
		t.Errorf("Expected 16 total partitions, got %d", metrics.TotalPartitions)
	}
}
