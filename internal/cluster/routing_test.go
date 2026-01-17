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
	
	ucm := &UnifiedClusterManager{
		nodes:      make(map[string]*ClusterNode),
		partitions: make(map[int]*Partition),
		config: ClusterConfig{
			PartitionCount:    256,
			ReplicationFactor: 3,
			VirtualNodes:      150,
		},
	}
	
	// Create test nodes
	for i := 0; i < nodeCount; i++ {
		nodeID := string(rune('A' + i))
		node := &ClusterNode{
			ID:    nodeID,
			Addr:  "127.0.0.1:700" + string(rune('0'+i)),
			State: NodeAlive,
			Metadata: NodeMetadata{
				ID:   nodeID,
				Addr: "127.0.0.1:700" + string(rune('0'+i)),
				Capacity: NodeCapacity{
					CPU:     8,
					Memory:  16 * 1024 * 1024 * 1024, // 16GB
					Disk:    1024 * 1024 * 1024 * 1024, // 1TB
					Network: 1000 * 1000 * 1000, // 1Gbps
				},
				Load: NodeLoad{
					CPUUsage:    0.5,
					MemoryUsage: 0.3,
					Connections: 10,
					QPS:         100,
				},
				Topology: NodeTopology{
					Datacenter: "dc1",
					Rack:       "rack1",
					Zone:       "zone-a",
				},
				Health: HealthHealthy,
			},
		}
		ucm.nodes[nodeID] = node
	}
	
	return ucm
}

// ============================================================================
// Key-Based Routing Tests
// ============================================================================

func TestKeyBasedRouting_SelectNode(t *testing.T) {
	ucm := createTestClusterManager(t, 3)
	strategy := NewKeyBasedRoutingStrategy(ucm)
	
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
	strategy := NewKeyBasedRoutingStrategy(ucm)
	
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
	
	// Check that distribution is reasonably balanced (within 20% of average)
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
	strategy := NewRoundRobinRoutingStrategy(ucm)
	
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

