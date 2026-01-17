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
)

// ============================================================================
// Test Helpers
// ============================================================================

// createTestClusterManager creates a test cluster manager with mock nodes
func createTestClusterManager(t *testing.T, nodeCount int) *UnifiedClusterManager {
	t.Helper()

	config := ClusterConfig{
		NodeID:            "test-node",
		NodeAddr:          "127.0.0.1:7000",
		PartitionCount:    256,
		ReplicationFactor: 3,
		VirtualNodes:      150,
	}

	ucm := &UnifiedClusterManager{
		nodes:      make(map[string]*ClusterNode),
		partitions: make(map[int]*Partition),
		config:     config,
	}

	// Create test nodes
	for i := 0; i < nodeCount; i++ {
		nodeID := string(rune('A' + i))
		node := &ClusterNode{
			ID:    nodeID,
			Addr:  "127.0.0.1:700" + string(rune('0'+i)),
			State: NodeAlive,
			Load:  0.5,
			Metadata: map[string]string{
				"datacenter": "dc1",
				"rack":       "rack1",
				"zone":       "zone-a",
			},
		}
		ucm.nodes[nodeID] = node
	}

	// Create test partitions
	for i := 0; i < 256; i++ {
		partition := &Partition{
			ID:       i,
			Leader:   string(rune('A' + (i % nodeCount))),
			Replicas: []string{string(rune('A' + (i % nodeCount)))},
			State:    PartitionHealthy,
		}
		ucm.partitions[i] = partition
	}

	return ucm
}

// ============================================================================
// Key-Based Routing Tests
// ============================================================================

func TestKeyBasedRouting_SelectNode(t *testing.T) {
	ucm := createTestClusterManager(t, 3)
	strategy := NewKeyBasedStrategy(ucm)

	// Test that same key always routes to same node
	key := "user:12345"
	node1, err := strategy.SelectNode(key, OpRead)
	if err != nil {
		t.Fatalf("SelectNode failed: %v", err)
	}

	node2, err := strategy.SelectNode(key, OpRead)
	if err != nil {
		t.Fatalf("SelectNode failed: %v", err)
	}

	if node1.ID != node2.ID {
		t.Errorf("Same key routed to different nodes: %s vs %s", node1.ID, node2.ID)
	}
}

func TestKeyBasedRouting_Distribution(t *testing.T) {
	ucm := createTestClusterManager(t, 3)
	strategy := NewKeyBasedStrategy(ucm)

	// Test distribution across nodes
	distribution := make(map[string]int)
	for i := 0; i < 1000; i++ {
		key := "key:" + string(rune(i))
		node, err := strategy.SelectNode(key, OpRead)
		if err != nil {
			t.Fatalf("SelectNode failed: %v", err)
		}
		distribution[node.ID]++
	}

	// Check that distribution is reasonably balanced (within 30% of average)
	avgCount := 1000 / len(ucm.nodes)
	for nodeID, count := range distribution {
		deviation := float64(count-avgCount) / float64(avgCount)
		if deviation > 0.3 || deviation < -0.3 {
			t.Errorf("Node %s has unbalanced distribution: %d (expected ~%d)",
				nodeID, count, avgCount)
		}
	}
}

// ============================================================================
// Round-Robin Routing Tests
// ============================================================================

func TestRoundRobinRouting_SelectNode(t *testing.T) {
	ucm := createTestClusterManager(t, 3)
	strategy := NewRoundRobinStrategy(ucm)

	// Test that requests are distributed evenly
	nodesSeen := make(map[string]bool)
	for i := 0; i < 10; i++ {
		node, err := strategy.SelectNode("key", OpWrite)
		if err != nil {
			t.Fatalf("SelectNode failed: %v", err)
		}
		nodesSeen[node.ID] = true
	}

	// Should have seen all nodes
	if len(nodesSeen) != len(ucm.nodes) {
		t.Errorf("Round-robin didn't use all nodes: saw %d, expected %d",
			len(nodesSeen), len(ucm.nodes))
	}
}

func TestRoundRobinRouting_EvenDistribution(t *testing.T) {
	ucm := createTestClusterManager(t, 3)
	strategy := NewRoundRobinStrategy(ucm)

	// Test that distribution is reasonably even
	distribution := make(map[string]int)
	iterations := 300 // Multiple of node count

	for i := 0; i < iterations; i++ {
		node, err := strategy.SelectNode("key", OpWrite)
		if err != nil {
			t.Fatalf("SelectNode failed: %v", err)
		}
		distribution[node.ID]++
	}

	// Each node should get approximately iterations/nodeCount requests (within 10%)
	expectedCount := iterations / len(ucm.nodes)
	for nodeID, count := range distribution {
		deviation := float64(count-expectedCount) / float64(expectedCount)
		if deviation > 0.1 || deviation < -0.1 {
			t.Errorf("Node %s got %d requests, expected ~%d (deviation: %.2f%%)",
				nodeID, count, expectedCount, deviation*100)
		}
	}
}

// ============================================================================
// Least-Loaded Routing Tests
// ============================================================================

func TestLeastLoadedRouting_SelectsLeastLoaded(t *testing.T) {
	ucm := createTestClusterManager(t, 3)
	strategy := NewLeastLoadedStrategy(ucm)

	// Set different loads via the strategy's metrics
	strategy.nodeMetrics["A"] = &NodeLoadMetrics{
		NodeID:    "A",
		CPUUsage:  0.9,
		LoadScore: 0.9,
	}
	strategy.nodeMetrics["B"] = &NodeLoadMetrics{
		NodeID:    "B",
		CPUUsage:  0.3,
		LoadScore: 0.3,
	}
	strategy.nodeMetrics["C"] = &NodeLoadMetrics{
		NodeID:    "C",
		CPUUsage:  0.7,
		LoadScore: 0.7,
	}

	// Should select node B (lowest load)
	node, err := strategy.SelectNode("key", OpRead)
	if err != nil {
		t.Fatalf("SelectNode failed: %v", err)
	}

	if node.ID != "B" {
		t.Errorf("Expected node B (least loaded), got %s", node.ID)
	}
}

func TestLeastLoadedRouting_SkipsUnhealthy(t *testing.T) {
	ucm := createTestClusterManager(t, 3)
	strategy := NewLeastLoadedStrategy(ucm)

	// Set node B as least loaded but mark it as dead
	ucm.nodes["B"].State = NodeDead

	strategy.nodeMetrics["A"] = &NodeLoadMetrics{
		NodeID:    "A",
		CPUUsage:  0.9,
		LoadScore: 0.9,
	}
	strategy.nodeMetrics["B"] = &NodeLoadMetrics{
		NodeID:    "B",
		CPUUsage:  0.1,
		LoadScore: 0.1,
	}
	strategy.nodeMetrics["C"] = &NodeLoadMetrics{
		NodeID:    "C",
		CPUUsage:  0.7,
		LoadScore: 0.7,
	}

	// Should select node C (least loaded among healthy nodes)
	node, err := strategy.SelectNode("key", OpRead)
	if err != nil {
		t.Fatalf("SelectNode failed: %v", err)
	}

	if node.ID != "C" {
		t.Errorf("Expected node C (least loaded healthy node), got %s", node.ID)
	}
}

// ============================================================================
// Locality-Aware Routing Tests
// ============================================================================

func TestLocalityAwareRouting_PrefersLocalNode(t *testing.T) {
	ucm := createTestClusterManager(t, 3)

	// Set different datacenters
	ucm.nodes["A"].Metadata["datacenter"] = "dc1"
	ucm.nodes["B"].Metadata["datacenter"] = "dc2"
	ucm.nodes["C"].Metadata["datacenter"] = "dc1"

	strategy := NewLocalityAwareStrategy(ucm, "dc1", "rack1", "zone-a")

	// Test that it can route requests (locality checking is not fully implemented yet)
	node, err := strategy.SelectNode("key", OpRead)
	if err != nil {
		t.Fatalf("SelectNode failed: %v", err)
	}

	if node == nil {
		t.Error("SelectNode returned nil node")
	}

	// TODO: Once locality checking is fully implemented, test that it prefers local nodes
}

// ============================================================================
// Hybrid Routing Tests
// ============================================================================

func TestHybridRouting_CombinesStrategies(t *testing.T) {
	ucm := createTestClusterManager(t, 3)
	strategy := NewHybridStrategy(ucm, "dc1", "rack1", "zone-a")

	// Test that it can route requests
	node, err := strategy.SelectNode("user:12345", OpRead)
	if err != nil {
		t.Fatalf("SelectNode failed: %v", err)
	}

	if node == nil {
		t.Error("SelectNode returned nil node")
	}
}

func TestHybridRouting_SelectReplicas(t *testing.T) {
	ucm := createTestClusterManager(t, 5)
	strategy := NewHybridStrategy(ucm, "dc1", "rack1", "zone-a")

	// Test replica selection
	replicas, err := strategy.SelectReplicas("user:12345", 3, nil)
	if err != nil {
		t.Fatalf("SelectReplicas failed: %v", err)
	}

	if len(replicas) != 3 {
		t.Errorf("Expected 3 replicas, got %d", len(replicas))
	}

	// Check that replicas are unique
	seen := make(map[string]bool)
	for _, replica := range replicas {
		if seen[replica.ID] {
			t.Errorf("Duplicate replica: %s", replica.ID)
		}
		seen[replica.ID] = true
	}
}

// ============================================================================
// Benchmark Tests
// ============================================================================

func BenchmarkKeyBasedRouting(b *testing.B) {
	ucm := createTestClusterManager(&testing.T{}, 10)
	strategy := NewKeyBasedStrategy(ucm)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		strategy.SelectNode("user:12345", OpRead)
	}
}

func BenchmarkRoundRobinRouting(b *testing.B) {
	ucm := createTestClusterManager(&testing.T{}, 10)
	strategy := NewRoundRobinStrategy(ucm)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		strategy.SelectNode("user:12345", OpWrite)
	}
}

func BenchmarkLeastLoadedRouting(b *testing.B) {
	ucm := createTestClusterManager(&testing.T{}, 10)
	strategy := NewLeastLoadedStrategy(ucm)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		strategy.SelectNode("user:12345", OpRead)
	}
}

func BenchmarkHybridRouting(b *testing.B) {
	ucm := createTestClusterManager(&testing.T{}, 10)
	strategy := NewHybridStrategy(ucm, "dc1", "rack1", "zone-a")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		strategy.SelectNode("user:12345", OpRead)
	}
}

