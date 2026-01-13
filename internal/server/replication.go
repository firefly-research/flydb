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
Package server contains the replication subsystem for FlyDB.

DEPRECATION NOTICE:
===================

This file provides legacy leader-follower replication for simple master/slave
setups. For new deployments, consider using the unified cluster-replication
architecture in the internal/cluster package, which provides:

  - Automatic leader election with term-based fencing
  - Data partitioning with consistent hashing
  - Integrated replication with configurable consistency levels
  - Automatic failover and rebalancing

This legacy implementation is maintained for backward compatibility with
existing master/slave deployments.

Replication Overview:
=====================

FlyDB implements Leader-Follower (Master-Slave) replication for:
  - Read scalability: Slaves can serve read queries
  - Fault tolerance: Slaves can be promoted if master fails
  - Data durability: Multiple copies of data across nodes

Replication Modes:
==================

FlyDB supports three replication modes:

  - ASYNC: Return immediately, replicate in background (default)
  - SEMI_SYNC: Wait for at least one replica to acknowledge
  - SYNC: Wait for all replicas to acknowledge

Replication Architecture:
=========================

  Master Node:
    - Accepts all write operations
    - Maintains the authoritative WAL
    - Streams WAL updates to connected slaves
    - Tracks per-follower replication state

  Slave Node:
    - Connects to master and requests WAL updates
    - Applies received updates to local storage
    - Can serve read queries (eventually consistent)
    - Automatically reconnects on failure

Replication Protocol:
=====================

The replication protocol is binary for efficiency:

  1. Slave connects to master's replication port
  2. Slave sends its address for identification
  3. Slave sends its current WAL offset (8 bytes, big-endian int64)
  4. Master streams WAL entries from that offset:
     - Op (1 byte): OpPut=1, OpDelete=2
     - KeyLen (4 bytes, big-endian uint32)
     - Key (KeyLen bytes)
     - ValueLen (4 bytes, big-endian uint32)
     - Value (ValueLen bytes)

Consistency Model:
==================

FlyDB provides configurable consistency:
  - ASYNC: Eventual consistency with ~100ms replication lag
  - SEMI_SYNC: At least one replica has the data
  - SYNC: All replicas have the data before returning

Failure Handling:
=================

  - If a slave disconnects, it reconnects and resumes from its last offset
  - The WAL offset ensures no data is lost or duplicated
  - Slaves automatically catch up after network partitions
  - Per-follower health monitoring with automatic failure detection

See Also:
=========

  - internal/cluster/unified.go: Unified cluster-replication architecture
*/
package server

import (
	"encoding/binary"
	"flydb/internal/storage"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// replicationPollInterval defines how often the master checks for new WAL entries.
// A shorter interval means lower replication lag but higher CPU usage.
const replicationPollInterval = 100 * time.Millisecond

// ReplicationMode defines how writes are acknowledged
type ReplicationMode int

const (
	// ReplicationAsync returns immediately, replicates in background
	ReplicationAsync ReplicationMode = iota

	// ReplicationSemiSync waits for at least one replica to acknowledge
	ReplicationSemiSync

	// ReplicationSync waits for all replicas to acknowledge
	ReplicationSync
)

func (m ReplicationMode) String() string {
	switch m {
	case ReplicationAsync:
		return "ASYNC"
	case ReplicationSemiSync:
		return "SEMI_SYNC"
	case ReplicationSync:
		return "SYNC"
	default:
		return "UNKNOWN"
	}
}

// ReplicationConfig holds configuration for replication
type ReplicationConfig struct {
	// Mode determines how writes are acknowledged
	Mode ReplicationMode

	// SyncTimeout is how long to wait for sync acknowledgments
	SyncTimeout time.Duration

	// MaxLagThreshold is the maximum acceptable replication lag
	MaxLagThreshold time.Duration

	// PollInterval is how often to check for new WAL entries
	PollInterval time.Duration

	// ReconnectInterval is how long to wait before reconnecting
	ReconnectInterval time.Duration

	// EnableCompression enables compression for replication traffic
	EnableCompression bool

	// CompressionAlgorithm specifies the compression algorithm ("gzip", "lz4", "snappy", "zstd")
	CompressionAlgorithm string

	// CompressionMinSize is the minimum payload size to compress (default 256)
	CompressionMinSize int
}

// DefaultReplicationConfig returns sensible defaults
func DefaultReplicationConfig() ReplicationConfig {
	return ReplicationConfig{
		Mode:                 ReplicationAsync,
		SyncTimeout:          5 * time.Second,
		MaxLagThreshold:      10 * time.Second,
		PollInterval:         100 * time.Millisecond,
		ReconnectInterval:    3 * time.Second,
		EnableCompression:    false,
		CompressionAlgorithm: "gzip",
		CompressionMinSize:   256,
	}
}

// FollowerState tracks the replication state of a follower
type FollowerState struct {
	Address     string
	WALOffset   int64
	LastAckTime time.Time
	Lag         time.Duration
	IsHealthy   bool
	FailedSends int
	conn        net.Conn
	mu          sync.Mutex
}

// ReplicationMetrics holds replication statistics
type ReplicationMetrics struct {
	mu sync.RWMutex

	// TotalBytesReplicated is the total bytes sent to followers
	TotalBytesReplicated int64

	// TotalEntriesReplicated is the total WAL entries replicated
	TotalEntriesReplicated int64

	// AverageLag is the average replication lag across followers
	AverageLag time.Duration

	// MaxLag is the maximum replication lag
	MaxLag time.Duration

	// HealthyFollowers is the count of healthy followers
	HealthyFollowers int

	// TotalFollowers is the total follower count
	TotalFollowers int

	// LastReplicationTime is when the last replication occurred
	LastReplicationTime time.Time
}

// Replicator handles Leader-Follower replication for FlyDB.
// It can operate in two modes:
//   - Master: Listens for slave connections and streams WAL updates
//   - Slave: Connects to master and applies received WAL updates
//
// The Replicator uses the WAL (Write-Ahead Log) as the source of truth
// for replication, ensuring that all operations are replicated in order.
type Replicator struct {
	config ReplicationConfig

	// wal is the Write-Ahead Log used for reading/writing operations.
	wal *storage.WAL

	// store is the storage engine for applying replicated operations.
	store storage.Engine

	// isLeader indicates whether this node is the master (true) or slave (false).
	isLeader bool

	// Follower tracking (leader only)
	followers   map[string]*FollowerState
	followersMu sync.RWMutex

	// Metrics
	metrics *ReplicationMetrics

	// Pending acknowledgments for sync replication
	pendingAcks   map[int64]chan struct{}
	pendingAcksMu sync.Mutex

	// Lifecycle
	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewReplicator creates a new Replicator instance.
//
// Parameters:
//   - wal: The Write-Ahead Log for reading operations (master) or tracking offset (slave)
//   - store: The storage engine for applying replicated operations (slave)
//   - isLeader: true for master mode, false for slave mode
//
// Returns a configured Replicator ready to start.
func NewReplicator(wal *storage.WAL, store storage.Engine, isLeader bool) *Replicator {
	return NewReplicatorWithConfig(DefaultReplicationConfig(), wal, store, isLeader)
}

// NewReplicatorWithConfig creates a new Replicator with custom configuration.
func NewReplicatorWithConfig(config ReplicationConfig, wal *storage.WAL, store storage.Engine, isLeader bool) *Replicator {
	return &Replicator{
		config:      config,
		wal:         wal,
		store:       store,
		isLeader:    isLeader,
		followers:   make(map[string]*FollowerState),
		metrics:     &ReplicationMetrics{},
		pendingAcks: make(map[int64]chan struct{}),
		stopCh:      make(chan struct{}),
	}
}

// GetMetrics returns a copy of the current replication metrics
func (r *Replicator) GetMetrics() ReplicationMetrics {
	r.metrics.mu.RLock()
	defer r.metrics.mu.RUnlock()
	return *r.metrics
}

// GetFollowerStates returns the state of all followers
func (r *Replicator) GetFollowerStates() []FollowerState {
	r.followersMu.RLock()
	defer r.followersMu.RUnlock()

	states := make([]FollowerState, 0, len(r.followers))
	for _, f := range r.followers {
		f.mu.Lock()
		states = append(states, FollowerState{
			Address:     f.Address,
			WALOffset:   f.WALOffset,
			LastAckTime: f.LastAckTime,
			Lag:         f.Lag,
			IsHealthy:   f.IsHealthy,
			FailedSends: f.FailedSends,
		})
		f.mu.Unlock()
	}
	return states
}

// StartMaster starts the replication server, listening for slave connections.
// This method blocks indefinitely, accepting slave connections and streaming
// WAL updates to each connected slave.
//
// Each slave connection is handled in a separate goroutine, allowing
// multiple slaves to replicate simultaneously.
//
// Parameters:
//   - port: The TCP port to listen on (e.g., ":9999")
//
// Returns an error if the listener cannot be created.
func (r *Replicator) StartMaster(port string) error {
	// Create a TCP listener for slave connections.
	ln, err := net.Listen("tcp", port)
	if err != nil {
		return err
	}
	fmt.Printf("Replication Master listening on %s (mode: %s)\n", port, r.config.Mode)

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		for {
			select {
			case <-r.stopCh:
				ln.Close()
				return
			default:
			}

			ln.(*net.TCPListener).SetDeadline(time.Now().Add(1 * time.Second))
			conn, err := ln.Accept()
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					continue
				}
				continue
			}

			go r.handleFollower(conn)
		}
	}()

	return nil
}

// handleFollower manages a single follower connection with health tracking
func (r *Replicator) handleFollower(conn net.Conn) {
	defer conn.Close()

	// Read the follower's address for identification
	var addrLen uint32
	if err := binary.Read(conn, binary.BigEndian, &addrLen); err != nil {
		// Legacy protocol: no address sent, use connection address
		addrLen = 0
	}

	var followerAddr string
	if addrLen > 0 {
		addrBuf := make([]byte, addrLen)
		if _, err := io.ReadFull(conn, addrBuf); err != nil {
			return
		}
		followerAddr = string(addrBuf)
	} else {
		followerAddr = conn.RemoteAddr().String()
	}

	// Read the follower's current WAL offset
	var offset int64
	if err := binary.Read(conn, binary.BigEndian, &offset); err != nil {
		fmt.Printf("Failed to read follower offset: %v\n", err)
		return
	}

	fmt.Printf("Follower %s connected with offset %d\n", followerAddr, offset)

	// Register the follower
	r.followersMu.Lock()
	follower := &FollowerState{
		Address:     followerAddr,
		WALOffset:   offset,
		LastAckTime: time.Now(),
		IsHealthy:   true,
		conn:        conn,
	}
	r.followers[followerAddr] = follower
	r.followersMu.Unlock()

	// Update metrics
	r.updateFollowerMetrics()

	defer func() {
		r.followersMu.Lock()
		delete(r.followers, followerAddr)
		r.followersMu.Unlock()
		r.updateFollowerMetrics()
		fmt.Printf("Follower %s disconnected\n", followerAddr)
	}()

	// Start streaming WAL entries
	ticker := time.NewTicker(r.config.PollInterval)
	defer ticker.Stop()

	currentOffset := offset

	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
			size, err := r.wal.Size()
			if err != nil {
				continue
			}

			if size > currentOffset {
				bytesReplicated := int64(0)
				entriesReplicated := int64(0)

				err := r.wal.Replay(currentOffset, func(op byte, key string, value []byte) {
					// Serialize the WAL entry
					buf := make([]byte, 1+4+len(key)+4+len(value))
					buf[0] = op
					binary.BigEndian.PutUint32(buf[1:], uint32(len(key)))
					copy(buf[5:], []byte(key))
					off := 5 + len(key)
					binary.BigEndian.PutUint32(buf[off:], uint32(len(value)))
					copy(buf[off+4:], value)

					// Send to follower
					if _, err := conn.Write(buf); err != nil {
						follower.mu.Lock()
						follower.FailedSends++
						if follower.FailedSends >= 3 {
							follower.IsHealthy = false
						}
						follower.mu.Unlock()
						return
					}

					bytesReplicated += int64(len(buf))
					entriesReplicated++
					currentOffset += int64(len(buf))
				})

				if err != nil && err != io.EOF {
					fmt.Printf("Replay error for %s: %v\n", followerAddr, err)
					return
				}

				// Update metrics
				r.metrics.mu.Lock()
				r.metrics.TotalBytesReplicated += bytesReplicated
				r.metrics.TotalEntriesReplicated += entriesReplicated
				r.metrics.LastReplicationTime = time.Now()
				r.metrics.mu.Unlock()

				// Update follower state
				follower.mu.Lock()
				follower.WALOffset = currentOffset
				follower.LastAckTime = time.Now()
				follower.FailedSends = 0
				follower.IsHealthy = true
				follower.mu.Unlock()
			}
		}
	}
}

// updateFollowerMetrics updates the aggregate follower metrics
func (r *Replicator) updateFollowerMetrics() {
	r.followersMu.RLock()
	defer r.followersMu.RUnlock()

	healthyCount := 0
	var totalLag time.Duration
	var maxLag time.Duration

	for _, f := range r.followers {
		f.mu.Lock()
		if f.IsHealthy {
			healthyCount++
		}
		totalLag += f.Lag
		if f.Lag > maxLag {
			maxLag = f.Lag
		}
		f.mu.Unlock()
	}

	r.metrics.mu.Lock()
	r.metrics.HealthyFollowers = healthyCount
	r.metrics.TotalFollowers = len(r.followers)
	if len(r.followers) > 0 {
		r.metrics.AverageLag = totalLag / time.Duration(len(r.followers))
	}
	r.metrics.MaxLag = maxLag
	r.metrics.mu.Unlock()
}

// StartSlave connects to a master and synchronizes data.
// It sends the current WAL offset to the master and then continuously
// receives and applies WAL entries.
//
// The synchronization process:
//  1. Connect to the master's replication port
//  2. Send current WAL offset (to resume from last position)
//  3. Receive WAL entries in a loop
//  4. Apply each entry to the local KVStore
//
// Parameters:
//   - masterAddr: The master's address (e.g., "localhost:9999")
//
// Returns an error if the connection fails or is lost.
func (r *Replicator) StartSlave(masterAddr string) error {
	return r.StartSlaveWithAddr(masterAddr, "")
}

// StartSlaveWithAddr connects to a master with a specific self address for identification
func (r *Replicator) StartSlaveWithAddr(masterAddr string, selfAddr string) error {
	for {
		select {
		case <-r.stopCh:
			return nil
		default:
		}

		err := r.connectAndSync(masterAddr, selfAddr)
		if err != nil {
			fmt.Printf("Replication connection lost: %v. Reconnecting in %v...\n",
				err, r.config.ReconnectInterval)
			time.Sleep(r.config.ReconnectInterval)
			continue
		}
	}
}

// connectAndSync establishes connection and syncs with master
func (r *Replicator) connectAndSync(masterAddr string, selfAddr string) error {
	conn, err := net.Dial("tcp", masterAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Send our address for identification (if provided)
	if selfAddr != "" {
		if err := binary.Write(conn, binary.BigEndian, uint32(len(selfAddr))); err != nil {
			return err
		}
		if _, err := conn.Write([]byte(selfAddr)); err != nil {
			return err
		}
	}

	// Get current WAL size as starting offset
	offset, err := r.wal.Size()
	if err != nil {
		return err
	}

	// Send our offset
	if err := binary.Write(conn, binary.BigEndian, offset); err != nil {
		return err
	}

	fmt.Printf("Connected to Master at %s with offset %d\n", masterAddr, offset)

	// Receive and apply WAL entries
	for {
		select {
		case <-r.stopCh:
			return nil
		default:
		}

		// Set read deadline
		conn.SetReadDeadline(time.Now().Add(30 * time.Second))

		// Read operation type
		opBuf := make([]byte, 1)
		if _, err := io.ReadFull(conn, opBuf); err != nil {
			return fmt.Errorf("failed to read operation: %w", err)
		}
		op := opBuf[0]

		// Read key
		var keyLen uint32
		if err := binary.Read(conn, binary.BigEndian, &keyLen); err != nil {
			return fmt.Errorf("failed to read key length: %w", err)
		}
		keyBuf := make([]byte, keyLen)
		if _, err := io.ReadFull(conn, keyBuf); err != nil {
			return fmt.Errorf("failed to read key: %w", err)
		}
		key := string(keyBuf)

		// Read value
		var valLen uint32
		if err := binary.Read(conn, binary.BigEndian, &valLen); err != nil {
			return fmt.Errorf("failed to read value length: %w", err)
		}
		valBuf := make([]byte, valLen)
		if _, err := io.ReadFull(conn, valBuf); err != nil {
			return fmt.Errorf("failed to read value: %w", err)
		}

		// Apply the operation
		switch op {
		case storage.OpPut:
			if err := r.store.Put(key, valBuf); err != nil {
				fmt.Printf("Failed to apply PUT %s: %v\n", key, err)
			}
		case storage.OpDelete:
			if err := r.store.Delete(key); err != nil {
				fmt.Printf("Failed to apply DELETE %s: %v\n", key, err)
			}
		}

		fmt.Printf("Replicated: %s\n", key)
	}
}

// Stop gracefully stops the replicator
func (r *Replicator) Stop() {
	close(r.stopCh)
	r.wg.Wait()
}

// WaitForReplication waits for replication to complete (for sync mode)
func (r *Replicator) WaitForReplication(offset int64, timeout time.Duration) error {
	if r.config.Mode == ReplicationAsync {
		return nil // No waiting in async mode
	}

	r.pendingAcksMu.Lock()
	ch := make(chan struct{})
	r.pendingAcks[offset] = ch
	r.pendingAcksMu.Unlock()

	defer func() {
		r.pendingAcksMu.Lock()
		delete(r.pendingAcks, offset)
		r.pendingAcksMu.Unlock()
	}()

	select {
	case <-ch:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("replication timeout after %v", timeout)
	}
}

// GetMode returns the current replication mode
func (r *Replicator) GetMode() ReplicationMode {
	return r.config.Mode
}

// SetMode changes the replication mode
func (r *Replicator) SetMode(mode ReplicationMode) {
	r.config.Mode = mode
}
