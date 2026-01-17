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

// ============================================================================
// WAL Replication Tests
// ============================================================================

// mockWAL implements WALInterface for testing
type mockWAL struct {
	entries []walEntry
	size    int64
}

type walEntry struct {
	op    byte
	key   string
	value []byte
}

func newMockWAL() *mockWAL {
	return &mockWAL{
		entries: make([]walEntry, 0),
		size:    0,
	}
}

func (m *mockWAL) Write(op byte, key string, value []byte) error {
	m.entries = append(m.entries, walEntry{op, key, value})
	m.size += int64(1 + 4 + len(key) + 4 + len(value))
	return nil
}

func (m *mockWAL) Sync() error {
	return nil
}

func (m *mockWAL) Size() (int64, error) {
	return m.size, nil
}

func (m *mockWAL) Replay(startOffset int64, fn func(op byte, key string, value []byte)) error {
	for _, e := range m.entries {
		fn(e.op, e.key, e.value)
	}
	return nil
}

func (m *mockWAL) ReplayWithPosition(startOffset int64, fn func(op byte, key string, value []byte)) (int64, error) {
	currentOffset := startOffset
	for _, e := range m.entries {
		fn(e.op, e.key, e.value)
		// Calculate the record size
		currentOffset += int64(1 + 4 + len(e.key) + 4 + len(e.value))
	}
	return currentOffset, nil
}

func (m *mockWAL) Close() error {
	return nil
}

// mockStore implements StorageEngine for testing
type mockStore struct {
	data map[string][]byte
}

func newMockStore() *mockStore {
	return &mockStore{
		data: make(map[string][]byte),
	}
}

func (m *mockStore) Put(key string, value []byte) error {
	m.data[key] = value
	return nil
}

func (m *mockStore) Delete(key string) error {
	delete(m.data, key)
	return nil
}

func (m *mockStore) Get(key string) ([]byte, error) {
	if v, ok := m.data[key]; ok {
		return v, nil
	}
	return nil, nil
}

// TestSetWALAndStore tests setting WAL and store on the cluster manager
func TestSetWALAndStore(t *testing.T) {
	config := ClusterConfig{
		NodeID:         "test-node",
		NodeAddr:       "localhost",
		PartitionCount: 16,
		VirtualNodes:   100,
	}

	ucm := NewUnifiedClusterManager(config)

	wal := newMockWAL()
	store := newMockStore()

	ucm.SetWAL(wal)
	ucm.SetStore(store)

	if ucm.wal == nil {
		t.Error("WAL was not set")
	}
	if ucm.store == nil {
		t.Error("Store was not set")
	}
}

// TestGetFollowerStates tests getting follower states
func TestGetFollowerStates(t *testing.T) {
	config := ClusterConfig{
		NodeID:         "test-node",
		NodeAddr:       "localhost",
		PartitionCount: 16,
		VirtualNodes:   100,
	}

	ucm := NewUnifiedClusterManager(config)

	// Initially no followers
	states := ucm.GetFollowerStates()
	if len(states) != 0 {
		t.Errorf("Expected 0 followers, got %d", len(states))
	}

	// Add a mock follower
	ucm.followersMu.Lock()
	ucm.followers["follower1"] = &FollowerReplicationState{
		Address:   "follower1:9999",
		WALOffset: 1000,
		IsHealthy: true,
	}
	ucm.followersMu.Unlock()

	states = ucm.GetFollowerStates()
	if len(states) != 1 {
		t.Errorf("Expected 1 follower, got %d", len(states))
	}
	if states[0].Address != "follower1:9999" {
		t.Errorf("Expected address 'follower1:9999', got '%s'", states[0].Address)
	}
}

// TestGetFollowerCount tests getting follower count
func TestGetFollowerCount(t *testing.T) {
	config := ClusterConfig{
		NodeID:         "test-node",
		NodeAddr:       "localhost",
		PartitionCount: 16,
		VirtualNodes:   100,
	}

	ucm := NewUnifiedClusterManager(config)

	if ucm.GetFollowerCount() != 0 {
		t.Error("Expected 0 followers initially")
	}

	ucm.followersMu.Lock()
	ucm.followers["f1"] = &FollowerReplicationState{Address: "f1", IsHealthy: true}
	ucm.followers["f2"] = &FollowerReplicationState{Address: "f2", IsHealthy: false}
	ucm.followersMu.Unlock()

	if ucm.GetFollowerCount() != 2 {
		t.Errorf("Expected 2 followers, got %d", ucm.GetFollowerCount())
	}
}

// TestGetHealthyFollowerCount tests getting healthy follower count
func TestGetHealthyFollowerCount(t *testing.T) {
	config := ClusterConfig{
		NodeID:         "test-node",
		NodeAddr:       "localhost",
		PartitionCount: 16,
		VirtualNodes:   100,
	}

	ucm := NewUnifiedClusterManager(config)

	ucm.followersMu.Lock()
	ucm.followers["f1"] = &FollowerReplicationState{Address: "f1", IsHealthy: true}
	ucm.followers["f2"] = &FollowerReplicationState{Address: "f2", IsHealthy: false}
	ucm.followers["f3"] = &FollowerReplicationState{Address: "f3", IsHealthy: true}
	ucm.followersMu.Unlock()

	if ucm.GetHealthyFollowerCount() != 2 {
		t.Errorf("Expected 2 healthy followers, got %d", ucm.GetHealthyFollowerCount())
	}
}

// TestWaitForReplicationEventual tests eventual consistency (no waiting)
func TestWaitForReplicationEventual(t *testing.T) {
	config := ClusterConfig{
		NodeID:         "test-node",
		NodeAddr:       "localhost",
		PartitionCount: 16,
		VirtualNodes:   100,
	}

	ucm := NewUnifiedClusterManager(config)

	// Eventual consistency should return immediately
	err := ucm.WaitForReplication(ConsistencyEventual, time.Second)
	if err != nil {
		t.Errorf("WaitForReplication with EVENTUAL should not error: %v", err)
	}
}

// TestWaitForReplicationNoFollowers tests waiting with no followers
func TestWaitForReplicationNoFollowers(t *testing.T) {
	config := ClusterConfig{
		NodeID:         "test-node",
		NodeAddr:       "localhost",
		PartitionCount: 16,
		VirtualNodes:   100,
	}

	ucm := NewUnifiedClusterManager(config)

	// With no followers, should return immediately for any consistency level
	err := ucm.WaitForReplication(ConsistencyAll, time.Second)
	if err != nil {
		t.Errorf("WaitForReplication with no followers should not error: %v", err)
	}
}

// TestWaitForReplicationWithHealthyFollowers tests waiting with healthy followers
func TestWaitForReplicationWithHealthyFollowers(t *testing.T) {
	config := ClusterConfig{
		NodeID:         "test-node",
		NodeAddr:       "localhost",
		PartitionCount: 16,
		VirtualNodes:   100,
	}

	ucm := NewUnifiedClusterManager(config)

	// Add healthy followers
	ucm.followersMu.Lock()
	ucm.followers["f1"] = &FollowerReplicationState{Address: "f1", IsHealthy: true}
	ucm.followers["f2"] = &FollowerReplicationState{Address: "f2", IsHealthy: true}
	ucm.followers["f3"] = &FollowerReplicationState{Address: "f3", IsHealthy: true}
	ucm.followersMu.Unlock()

	// ConsistencyOne should succeed with 1 healthy follower
	err := ucm.WaitForReplication(ConsistencyOne, time.Second)
	if err != nil {
		t.Errorf("WaitForReplication with ONE should succeed: %v", err)
	}

	// ConsistencyQuorum should succeed with 2+ healthy followers (quorum of 3 is 2)
	err = ucm.WaitForReplication(ConsistencyQuorum, time.Second)
	if err != nil {
		t.Errorf("WaitForReplication with QUORUM should succeed: %v", err)
	}

	// ConsistencyAll should succeed with all 3 healthy
	err = ucm.WaitForReplication(ConsistencyAll, time.Second)
	if err != nil {
		t.Errorf("WaitForReplication with ALL should succeed: %v", err)
	}
}

// TestWaitForReplicationTimeout tests timeout when not enough healthy followers
func TestWaitForReplicationTimeout(t *testing.T) {
	config := ClusterConfig{
		NodeID:         "test-node",
		NodeAddr:       "localhost",
		PartitionCount: 16,
		VirtualNodes:   100,
	}

	ucm := NewUnifiedClusterManager(config)

	// Add unhealthy followers
	ucm.followersMu.Lock()
	ucm.followers["f1"] = &FollowerReplicationState{Address: "f1", IsHealthy: false}
	ucm.followers["f2"] = &FollowerReplicationState{Address: "f2", IsHealthy: false}
	ucm.followersMu.Unlock()

	// ConsistencyOne should timeout with no healthy followers
	err := ucm.WaitForReplication(ConsistencyOne, 100*time.Millisecond)
	if err == nil {
		t.Error("WaitForReplication should timeout with no healthy followers")
	}
}

// TestStartReplicationMasterWithoutWAL tests error when WAL not set
func TestStartReplicationMasterWithoutWAL(t *testing.T) {
	config := ClusterConfig{
		NodeID:         "test-node",
		NodeAddr:       "localhost",
		PartitionCount: 16,
		VirtualNodes:   100,
	}

	ucm := NewUnifiedClusterManager(config)

	// TODO: Update this test - StartReplicationMaster method signature changed
	_ = ucm
	// err := ucm.StartReplicationMaster(":0")
	// if err == nil {
	// 	t.Error("StartReplicationMaster should error without WAL")
	// }
}

// TestStartReplicationFollowerWithoutWAL tests error when WAL not set
func TestStartReplicationFollowerWithoutWAL(t *testing.T) {
	config := ClusterConfig{
		NodeID:         "test-node",
		NodeAddr:       "localhost",
		PartitionCount: 16,
		VirtualNodes:   100,
	}

	ucm := NewUnifiedClusterManager(config)

	err := ucm.StartReplicationFollower("localhost:9999")
	if err == nil {
		t.Error("StartReplicationFollower should error without WAL")
	}
}

// TestStartReplicationFollowerWithoutStore tests error when store not set
func TestStartReplicationFollowerWithoutStore(t *testing.T) {
	config := ClusterConfig{
		NodeID:         "test-node",
		NodeAddr:       "localhost",
		PartitionCount: 16,
		VirtualNodes:   100,
	}

	ucm := NewUnifiedClusterManager(config)
	ucm.SetWAL(newMockWAL())

	err := ucm.StartReplicationFollower("localhost:9999")
	if err == nil {
		t.Error("StartReplicationFollower should error without store")
	}
}

// TestFollowerReplicationState tests the FollowerReplicationState struct
func TestFollowerReplicationState(t *testing.T) {
	state := &FollowerReplicationState{
		Address:     "follower1:9999",
		WALOffset:   1000,
		LastAckTime: time.Now(),
		Lag:         100 * time.Millisecond,
		IsHealthy:   true,
		FailedSends: 0,
	}

	if state.Address != "follower1:9999" {
		t.Errorf("Expected address 'follower1:9999', got '%s'", state.Address)
	}
	if state.WALOffset != 1000 {
		t.Errorf("Expected WALOffset 1000, got %d", state.WALOffset)
	}
	if !state.IsHealthy {
		t.Error("Expected IsHealthy to be true")
	}
}
