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
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// OperationType represents the type of operation being performed
type OperationType int

const (
	OpRead OperationType = iota
	OpWrite
	OpDelete
	OpScan
)

func (o OperationType) String() string {
	switch o {
	case OpRead:
		return "READ"
	case OpWrite:
		return "WRITE"
	case OpDelete:
		return "DELETE"
	case OpScan:
		return "SCAN"
	default:
		return "UNKNOWN"
	}
}

// RoutingStrategy defines the interface for different routing strategies
type RoutingStrategy interface {
	// SelectNode selects the best node for a given key and operation
	SelectNode(key string, operation OperationType) (*ClusterNode, error)

	// SelectReplicas selects replica nodes for replication
	SelectReplicas(key string, count int, excludeNodes []string) ([]*ClusterNode, error)

	// Name returns the strategy name
	Name() string

	// UpdateMetrics updates the strategy with latest cluster metrics
	UpdateMetrics(metrics *ClusterMetrics)
}

// RoutingStrategyType represents the type of routing strategy
type RoutingStrategyType string

const (
	StrategyKeyBased      RoutingStrategyType = "key_based"
	StrategyRoundRobin    RoutingStrategyType = "round_robin"
	StrategyLeastLoaded   RoutingStrategyType = "least_loaded"
	StrategyLocalityAware RoutingStrategyType = "locality_aware"
	StrategyHybrid        RoutingStrategyType = "hybrid"
)

// ============================================================================
// Key-Based Routing Strategy (Consistent Hashing)
// ============================================================================

// KeyBasedStrategy routes requests based on consistent hashing
type KeyBasedStrategy struct {
	ucm *UnifiedClusterManager
	mu  sync.RWMutex
}

func NewKeyBasedStrategy(ucm *UnifiedClusterManager) *KeyBasedStrategy {
	return &KeyBasedStrategy{
		ucm: ucm,
	}
}

func (s *KeyBasedStrategy) Name() string {
	return string(StrategyKeyBased)
}

func (s *KeyBasedStrategy) SelectNode(key string, operation OperationType) (*ClusterNode, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Get partition for key
	partitionID := s.ucm.GetPartitionForKey(key)
	partition := s.ucm.GetPartition(partitionID)
	if partition == nil {
		return nil, fmt.Errorf("partition %d not found", partitionID)
	}

	// For writes, always use primary
	if operation == OpWrite || operation == OpDelete {
		return s.ucm.GetNode(partition.Leader), nil
	}

	// For reads, can use any healthy replica
	allNodes := append([]string{partition.Leader}, partition.Replicas...)
	for _, nodeID := range allNodes {
		node := s.ucm.GetNode(nodeID)
		if node != nil && node.State == NodeAlive {
			return node, nil
		}
	}

	return nil, fmt.Errorf("no healthy nodes for partition %d", partitionID)
}

func (s *KeyBasedStrategy) SelectReplicas(key string, count int, excludeNodes []string) ([]*ClusterNode, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	partitionID := s.ucm.GetPartitionForKey(key)
	partition := s.ucm.GetPartition(partitionID)
	if partition == nil {
		return nil, fmt.Errorf("partition %d not found", partitionID)
	}

	excludeMap := make(map[string]bool)
	for _, nodeID := range excludeNodes {
		excludeMap[nodeID] = true
	}

	replicas := make([]*ClusterNode, 0, count)
	for _, nodeID := range partition.Replicas {
		if excludeMap[nodeID] {
			continue
		}
		node := s.ucm.GetNode(nodeID)
		if node != nil && node.State == NodeAlive {
			replicas = append(replicas, node)
			if len(replicas) >= count {
				break
			}
		}
	}

	return replicas, nil
}

func (s *KeyBasedStrategy) UpdateMetrics(metrics *ClusterMetrics) {
	// Key-based strategy doesn't use metrics
}

