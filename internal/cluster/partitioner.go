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
Package cluster provides intelligent partition reassignment for FlyDB.

Partition Reassignment Overview:
================================

This module implements automatic partition rebalancing when cluster topology changes:
- Consistent hashing for partition assignment
- Automatic rebalancing on node join/leave
- Minimal data movement during rebalancing
- Partition migration with data transfer

Consistent Hashing:
===================

Uses a hash ring with virtual nodes:
1. Each physical node has multiple virtual nodes
2. Keys are hashed to positions on the ring
3. Partitions are assigned to the next node clockwise

Rebalancing Strategy:
=====================

When topology changes:
1. Calculate new partition assignments
2. Identify partitions that need to move
3. Prioritize moves to minimize disruption
4. Execute migrations in parallel with throttling
*/
package cluster

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
	"time"
)

// PartitionerConfig holds configuration for the partitioner
type PartitionerConfig struct {
	NumPartitions     int           `json:"num_partitions"`
	ReplicationFactor int           `json:"replication_factor"`
	VirtualNodes      int           `json:"virtual_nodes"`
	RebalanceDelay    time.Duration `json:"rebalance_delay"`
	MigrationBatch    int           `json:"migration_batch"`
}

// DefaultPartitionerConfig returns sensible defaults
func DefaultPartitionerConfig() PartitionerConfig {
	return PartitionerConfig{
		NumPartitions:     256,
		ReplicationFactor: 3,
		VirtualNodes:      150,
		RebalanceDelay:    5 * time.Second,
		MigrationBatch:    10,
	}
}

// Note: Partition and PartitionState types are defined in unified.go
// This file uses those types for consistency

// VirtualNode represents a virtual node on the hash ring
type VirtualNode struct {
	Hash   uint64
	NodeID string
	Index  int
}

// Partitioner manages partition assignment and rebalancing
type Partitioner struct {
	config PartitionerConfig
	mu     sync.RWMutex

	// Hash ring
	ring     []VirtualNode
	nodeMap  map[string]bool

	// Partition assignments
	partitions map[int]*Partition

	// Migration state
	migrations   map[int]*Migration
	migrationsMu sync.RWMutex

	// Callbacks
	onPartitionAssigned func(partition *Partition)
	onMigrationStart    func(migration *Migration)
	onMigrationComplete func(migration *Migration)

	// Control
	stopCh chan struct{}
	wg     sync.WaitGroup
}

// Migration represents a partition migration
type Migration struct {
	PartitionID int            `json:"partition_id"`
	FromNode    string         `json:"from_node"`
	ToNode      string         `json:"to_node"`
	StartedAt   time.Time      `json:"started_at"`
	Progress    float64        `json:"progress"`
	State       MigrationState `json:"state"`
}

// MigrationState represents the state of a migration
type MigrationState int

const (
	MigrationPending MigrationState = iota
	MigrationInProgress
	MigrationCompleted
	MigrationFailedOld // Renamed to avoid conflict with metadata.go
)

// NewPartitioner creates a new partitioner
func NewPartitioner(config PartitionerConfig) *Partitioner {
	p := &Partitioner{
		config:     config,
		ring:       make([]VirtualNode, 0),
		nodeMap:    make(map[string]bool),
		partitions: make(map[int]*Partition),
		migrations: make(map[int]*Migration),
		stopCh:     make(chan struct{}),
	}

	// Initialize partitions
	for i := 0; i < config.NumPartitions; i++ {
		p.partitions[i] = &Partition{
			ID:           i,
			Leader:       "",
			Replicas:     []string{},
			State:        PartitionUnavailable,
			Version:      0,
			LastModified: time.Now(),
		}
	}

	return p
}

// AddNode adds a node to the hash ring
func (p *Partitioner) AddNode(nodeID string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.nodeMap[nodeID] {
		return // Already exists
	}

	p.nodeMap[nodeID] = true

	// Add virtual nodes
	for i := 0; i < p.config.VirtualNodes; i++ {
		hash := p.hashKey(fmt.Sprintf("%s:%d", nodeID, i))
		vn := VirtualNode{
			Hash:   hash,
			NodeID: nodeID,
			Index:  i,
		}
		p.ring = append(p.ring, vn)
	}

	// Sort ring by hash
	sort.Slice(p.ring, func(i, j int) bool {
		return p.ring[i].Hash < p.ring[j].Hash
	})
}

// RemoveNode removes a node from the hash ring
func (p *Partitioner) RemoveNode(nodeID string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.nodeMap[nodeID] {
		return
	}

	delete(p.nodeMap, nodeID)

	// Remove virtual nodes
	newRing := make([]VirtualNode, 0, len(p.ring)-p.config.VirtualNodes)
	for _, vn := range p.ring {
		if vn.NodeID != nodeID {
			newRing = append(newRing, vn)
		}
	}
	p.ring = newRing
}

// GetPartitionForKey returns the partition ID for a key
func (p *Partitioner) GetPartitionForKey(key []byte) int {
	hash := p.hashKey(string(key))
	return int(hash % uint64(p.config.NumPartitions))
}

// GetNodeForPartition returns the leader node for a partition
func (p *Partitioner) GetNodeForPartition(partitionID int) string {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if partition, ok := p.partitions[partitionID]; ok {
		return partition.Leader
	}
	return ""
}

// GetReplicasForPartition returns the replica nodes for a partition
func (p *Partitioner) GetReplicasForPartition(partitionID int) []string {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if partition, ok := p.partitions[partitionID]; ok {
		return partition.Replicas
	}
	return nil
}

// Rebalance recalculates partition assignments based on current ring
func (p *Partitioner) Rebalance() []Migration {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.ring) == 0 {
		return nil
	}

	migrations := make([]Migration, 0)

	for partitionID := 0; partitionID < p.config.NumPartitions; partitionID++ {
		partition := p.partitions[partitionID]
		newLeader, newReplicas := p.calculateAssignment(partitionID)

		// Check if leader changed
		if partition.Leader != newLeader && partition.Leader != "" {
			migrations = append(migrations, Migration{
				PartitionID: partitionID,
				FromNode:    partition.Leader,
				ToNode:      newLeader,
				State:       MigrationPending,
			})
		}

		// Update assignment
		partition.Leader = newLeader
		partition.Replicas = newReplicas
		partition.Version++

		if partition.State == PartitionUnavailable && newLeader != "" {
			partition.State = PartitionHealthy
			partition.LastModified = time.Now()
		}

		if p.onPartitionAssigned != nil {
			go p.onPartitionAssigned(partition)
		}
	}

	return migrations
}

// calculateAssignment calculates the node assignment for a partition
func (p *Partitioner) calculateAssignment(partitionID int) (string, []string) {
	if len(p.ring) == 0 {
		return "", nil
	}

	// Hash the partition ID to find position on ring
	partitionHash := p.hashKey(fmt.Sprintf("partition:%d", partitionID))

	// Find the first node clockwise from this position
	startIdx := p.findNodeIndex(partitionHash)

	// Get leader and replicas (distinct nodes)
	nodes := make([]string, 0, p.config.ReplicationFactor)
	seen := make(map[string]bool)

	for i := 0; i < len(p.ring) && len(nodes) < p.config.ReplicationFactor; i++ {
		idx := (startIdx + i) % len(p.ring)
		nodeID := p.ring[idx].NodeID

		if !seen[nodeID] {
			seen[nodeID] = true
			nodes = append(nodes, nodeID)
		}
	}

	if len(nodes) == 0 {
		return "", nil
	}

	leader := nodes[0]
	replicas := nodes[1:]

	return leader, replicas
}

// findNodeIndex finds the index of the first node clockwise from a hash
func (p *Partitioner) findNodeIndex(hash uint64) int {
	// Binary search for the first node with hash >= target
	idx := sort.Search(len(p.ring), func(i int) bool {
		return p.ring[i].Hash >= hash
	})

	// Wrap around if we're past the end
	if idx >= len(p.ring) {
		idx = 0
	}

	return idx
}

// hashKey computes a hash for a key
func (p *Partitioner) hashKey(key string) uint64 {
	h := sha256.Sum256([]byte(key))
	return binary.BigEndian.Uint64(h[:8])
}

// GetPartition returns a partition by ID
func (p *Partitioner) GetPartition(partitionID int) *Partition {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.partitions[partitionID]
}

// GetAllPartitions returns all partitions
func (p *Partitioner) GetAllPartitions() []*Partition {
	p.mu.RLock()
	defer p.mu.RUnlock()

	partitions := make([]*Partition, 0, len(p.partitions))
	for _, p := range p.partitions {
		partitions = append(partitions, p)
	}
	return partitions
}

// GetPartitionsForNode returns all partitions where the node is leader or replica
func (p *Partitioner) GetPartitionsForNode(nodeID string) []*Partition {
	p.mu.RLock()
	defer p.mu.RUnlock()

	partitions := make([]*Partition, 0)
	for _, partition := range p.partitions {
		if partition.Leader == nodeID {
			partitions = append(partitions, partition)
			continue
		}
		for _, replica := range partition.Replicas {
			if replica == nodeID {
				partitions = append(partitions, partition)
				break
			}
		}
	}
	return partitions
}

// SetPartitionAssignedCallback sets the callback for partition assignments
func (p *Partitioner) SetPartitionAssignedCallback(fn func(partition *Partition)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.onPartitionAssigned = fn
}

// SetMigrationStartCallback sets the callback for migration starts
func (p *Partitioner) SetMigrationStartCallback(fn func(migration *Migration)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.onMigrationStart = fn
}

// SetMigrationCompleteCallback sets the callback for migration completions
func (p *Partitioner) SetMigrationCompleteCallback(fn func(migration *Migration)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.onMigrationComplete = fn
}
