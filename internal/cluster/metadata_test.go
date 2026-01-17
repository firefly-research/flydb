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
// Cluster Metadata Store Tests
// ============================================================================

func TestNewClusterMetadataStore(t *testing.T) {
	tmpDir := t.TempDir()
	config := DefaultClusterConfig("node1", "127.0.0.1:7000")
	store := NewClusterMetadataStore(config, tmpDir)
	
	if store == nil {
		t.Fatal("NewClusterMetadataStore returned nil")
	}
	
	if store.GetVersion() != 0 {
		t.Errorf("Expected initial version 0, got %d", store.GetVersion())
	}
}

func TestClusterMetadataStore_NodeMetadata(t *testing.T) {
	tmpDir := t.TempDir()
	config := DefaultClusterConfig("node1", "127.0.0.1:7000")
	store := NewClusterMetadataStore(config, tmpDir)
	
	// Create node metadata
	nodeMetadata := &NodeMetadata{
		ID:   "node1",
		Addr: "127.0.0.1:7000",
		Capacity: NodeCapacity{
			CPU:     8,
			Memory:  16 * 1024 * 1024 * 1024,
			Disk:    1024 * 1024 * 1024 * 1024,
			Network: 1000 * 1000 * 1000,
		},
		Load: NodeLoad{
			CPUUsage:    0.5,
			MemoryUsage: 5 * 1024 * 1024 * 1024, // 5 GB
			Connections: 100,
			QPS:         1000,
		},
		Datacenter: "us-east-1",
		Rack:       "rack-1",
		Zone:       "zone-a",
		Health:     HealthHealthy,
		State:      NodeAlive,
	}
	
	// Add node metadata
	store.AddNode(nodeMetadata)

	// Retrieve node metadata
	retrieved, ok := store.GetNode("node1")
	if !ok {
		t.Fatal("GetNode returned false")
	}
	if retrieved == nil {
		t.Fatal("GetNode returned nil")
	}
	
	if retrieved.ID != "node1" {
		t.Errorf("Expected node ID 'node1', got '%s'", retrieved.ID)
	}
	
	if retrieved.Datacenter != "us-east-1" {
		t.Errorf("Expected datacenter 'us-east-1', got '%s'", retrieved.Datacenter)
	}
}

func TestClusterMetadataStore_PartitionMetadata(t *testing.T) {
	tmpDir := t.TempDir()
	config := DefaultClusterConfig("node1", "127.0.0.1:7000")
	store := NewClusterMetadataStore(config, tmpDir)
	
	// Create partition metadata
	partitionMetadata := &PartitionMetadata{
		ID:       0,
		Version:  1,
		Leader:   "node1",
		Replicas: []string{"node1", "node2", "node3"},
		State:    PartitionHealthy,
		Health:   HealthHealthy,
		KeyCount: 1000,
		DataSize: 1024 * 1024,
	}
	
	// Add partition metadata
	store.AddPartition(partitionMetadata)

	// Retrieve partition metadata
	retrieved, ok := store.GetPartition(0)
	if !ok {
		t.Fatal("GetPartition returned false")
	}
	if retrieved == nil {
		t.Fatal("GetPartition returned nil")
	}
	
	if retrieved.ID != 0 {
		t.Errorf("Expected partition ID 0, got %d", retrieved.ID)
	}
	
	if retrieved.Leader != "node1" {
		t.Errorf("Expected leader 'node1', got '%s'", retrieved.Leader)
	}
}

func TestClusterMetadataStore_Versioning(t *testing.T) {
	tmpDir := t.TempDir()
	config := DefaultClusterConfig("node1", "127.0.0.1:7000")
	store := NewClusterMetadataStore(config, tmpDir)
	
	initialVersion := store.GetVersion()
	
	// Add should increment version
	nodeMetadata := &NodeMetadata{
		ID:   "node1",
		Addr: "127.0.0.1:7000",
	}
	store.AddNode(nodeMetadata)
	
	newVersion := store.GetVersion()
	if newVersion <= initialVersion {
		t.Errorf("Version not incremented: %d -> %d", initialVersion, newVersion)
	}
}

func TestClusterMetadataStore_RoutingTable(t *testing.T) {
	tmpDir := t.TempDir()
	config := DefaultClusterConfig("node1", "127.0.0.1:7000")
	store := NewClusterMetadataStore(config, tmpDir)

	// Add a partition which will update the routing table
	partitionMetadata := &PartitionMetadata{
		ID:       0,
		Version:  1,
		Leader:   "node1",
		Replicas: []string{"node1", "node2"},
		State:    PartitionHealthy,
		Health:   HealthHealthy,
	}
	store.AddPartition(partitionMetadata)

	// Get routing table
	rt := store.GetRoutingTable()
	if rt == nil {
		t.Fatal("GetRoutingTable returned nil")
	}

	// Get primary node
	primary, ok := rt.GetPrimaryNode(0)
	if !ok {
		t.Fatal("GetPrimaryNode returned false")
	}
	if primary != "node1" {
		t.Errorf("Expected primary 'node1', got '%s'", primary)
	}

	// Get replica nodes
	replicas, ok := rt.GetReplicas(0)
	if !ok {
		t.Fatal("GetReplicas returned false")
	}
	if len(replicas) != 2 {
		t.Errorf("Expected 2 replicas, got %d", len(replicas))
	}
}

