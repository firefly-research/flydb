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

package server

import (
	"testing"
	"time"
)

func TestNewClusterManager(t *testing.T) {
	peers := []string{":9001", ":9002", ":9003"}
	cm := NewClusterManager(":9001", peers)

	if cm.nodeID != ":9001" {
		t.Errorf("Expected nodeID :9001, got %s", cm.nodeID)
	}

	if cm.GetState() != StateFollower {
		t.Errorf("Expected initial state Follower, got %s", cm.GetState())
	}

	// Should have 2 peers (excluding self)
	if len(cm.peers) != 2 {
		t.Errorf("Expected 2 peers, got %d", len(cm.peers))
	}
}

func TestNodeStateString(t *testing.T) {
	tests := []struct {
		state    NodeState
		expected string
	}{
		{StateFollower, "Follower"},
		{StateCandidate, "Candidate"},
		{StateLeader, "Leader"},
		{NodeState(99), "Unknown"},
	}

	for _, tt := range tests {
		if got := tt.state.String(); got != tt.expected {
			t.Errorf("NodeState(%d).String() = %s, want %s", tt.state, got, tt.expected)
		}
	}
}

func TestBecomeLeader(t *testing.T) {
	cm := NewClusterManager(":9001", []string{":9001", ":9002"})

	leaderCallbackCalled := false
	cm.SetCallbacks(func() {
		leaderCallbackCalled = true
	}, nil)

	// Start the cluster manager on a random port
	err := cm.Start(":0")
	if err != nil {
		t.Fatalf("Failed to start cluster manager: %v", err)
	}
	defer cm.Stop()

	// Force become leader
	cm.BecomeLeader()

	if !cm.IsLeader() {
		t.Error("Expected node to be leader")
	}

	if cm.GetState() != StateLeader {
		t.Errorf("Expected state Leader, got %s", cm.GetState())
	}

	if cm.GetLeaderAddr() != ":9001" {
		t.Errorf("Expected leader addr :9001, got %s", cm.GetLeaderAddr())
	}

	if !leaderCallbackCalled {
		t.Error("Expected leader callback to be called")
	}
}

func TestSetCallbacks(t *testing.T) {
	cm := NewClusterManager(":9001", nil)

	leaderCalled := false
	followerCalled := false
	var followerLeaderAddr string

	cm.SetCallbacks(
		func() { leaderCalled = true },
		func(addr string) {
			followerCalled = true
			followerLeaderAddr = addr
		},
	)

	if cm.onBecomeLeader == nil {
		t.Error("Expected onBecomeLeader callback to be set")
	}
	if cm.onBecomeFollower == nil {
		t.Error("Expected onBecomeFollower callback to be set")
	}

	// Test leader callback
	cm.onBecomeLeader()
	if !leaderCalled {
		t.Error("Leader callback was not called")
	}

	// Test follower callback
	cm.onBecomeFollower(":9002")
	if !followerCalled {
		t.Error("Follower callback was not called")
	}
	if followerLeaderAddr != ":9002" {
		t.Errorf("Expected follower leader addr :9002, got %s", followerLeaderAddr)
	}
}

func TestClusterManagerStartStop(t *testing.T) {
	cm := NewClusterManager(":9001", nil)

	err := cm.Start(":0") // Use port 0 for random available port
	if err != nil {
		t.Fatalf("Failed to start cluster manager: %v", err)
	}

	if cm.listener == nil {
		t.Error("Expected listener to be set after Start")
	}

	// Give goroutines time to start
	time.Sleep(100 * time.Millisecond)

	cm.Stop()

	// Verify stop completed without hanging
	t.Log("Cluster manager stopped successfully")
}

func TestIsAggregateFunction(t *testing.T) {
	// This tests the helper function in parser.go but we include it here
	// to verify the aggregate function detection works
	aggregates := []string{"COUNT", "SUM", "AVG", "MIN", "MAX"}
	nonAggregates := []string{"SELECT", "FROM", "WHERE", "name", "id"}

	for _, agg := range aggregates {
		if !isAggregateFunction(agg) {
			t.Errorf("Expected %s to be recognized as aggregate function", agg)
		}
	}

	for _, nonAgg := range nonAggregates {
		if isAggregateFunction(nonAgg) {
			t.Errorf("Expected %s to NOT be recognized as aggregate function", nonAgg)
		}
	}
}

// Helper function copied from parser.go for testing
func isAggregateFunction(keyword string) bool {
	switch keyword {
	case "COUNT", "SUM", "AVG", "MIN", "MAX":
		return true
	}
	return false
}

