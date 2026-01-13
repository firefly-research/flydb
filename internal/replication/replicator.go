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
Package replication provides enhanced log replication for FlyDB.

Enhanced Log Replication Overview:
==================================

This package implements a robust log replication system with:
- Streaming replication from leader to followers
- Conflict resolution using vector clocks
- Consistency guarantees with configurable levels
- Automatic catch-up for lagging followers

Replication Flow:
=================

1. Leader receives write request
2. Leader appends to local WAL with sequence number
3. Leader streams entry to all followers
4. Followers acknowledge receipt
5. Leader commits when quorum acknowledges
6. Leader responds to client

Conflict Resolution:
====================

When conflicts occur (e.g., during network partitions), the system uses:
- Vector clocks for causality tracking
- Last-writer-wins with configurable tie-breakers
- Conflict detection and notification hooks

Consistency Levels:
===================

- Strong: Wait for all replicas before acknowledging
- Quorum: Wait for majority of replicas
- Eventual: Acknowledge immediately, replicate async
*/
package replication

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// ConsistencyLevel defines the replication consistency guarantee
type ConsistencyLevel int

const (
	// ConsistencyEventual acknowledges writes immediately
	ConsistencyEventual ConsistencyLevel = iota
	// ConsistencyQuorum waits for majority acknowledgment
	ConsistencyQuorum
	// ConsistencyStrong waits for all replicas
	ConsistencyStrong
)

func (c ConsistencyLevel) String() string {
	switch c {
	case ConsistencyEventual:
		return "EVENTUAL"
	case ConsistencyQuorum:
		return "QUORUM"
	case ConsistencyStrong:
		return "STRONG"
	default:
		return "UNKNOWN"
	}
}

// ReplicatorConfig holds configuration for the replicator
type ReplicatorConfig struct {
	NodeID           string           `json:"node_id"`
	ListenAddr       string           `json:"listen_addr"`
	Consistency      ConsistencyLevel `json:"consistency"`
	ReplicationPort  int              `json:"replication_port"`
	MaxBatchSize     int              `json:"max_batch_size"`
	FlushInterval    time.Duration    `json:"flush_interval"`
	AckTimeout       time.Duration    `json:"ack_timeout"`
	RetryInterval    time.Duration    `json:"retry_interval"`
	MaxRetries       int              `json:"max_retries"`
}

// DefaultReplicatorConfig returns sensible defaults
func DefaultReplicatorConfig(nodeID string) ReplicatorConfig {
	return ReplicatorConfig{
		NodeID:          nodeID,
		ListenAddr:      "0.0.0.0",
		Consistency:     ConsistencyQuorum,
		ReplicationPort: 9997,
		MaxBatchSize:    1000,
		FlushInterval:   10 * time.Millisecond,
		AckTimeout:      5 * time.Second,
		RetryInterval:   100 * time.Millisecond,
		MaxRetries:      10,
	}
}

// VectorClock tracks causality across nodes
type VectorClock struct {
	Clocks map[string]uint64 `json:"clocks"`
	mu     sync.RWMutex
}

// NewVectorClock creates a new vector clock
func NewVectorClock() *VectorClock {
	return &VectorClock{
		Clocks: make(map[string]uint64),
	}
}

// Increment increments the clock for a node
func (vc *VectorClock) Increment(nodeID string) uint64 {
	vc.mu.Lock()
	defer vc.mu.Unlock()
	vc.Clocks[nodeID]++
	return vc.Clocks[nodeID]
}

// Get returns the clock value for a node
func (vc *VectorClock) Get(nodeID string) uint64 {
	vc.mu.RLock()
	defer vc.mu.RUnlock()
	return vc.Clocks[nodeID]
}

// Merge merges another vector clock into this one
func (vc *VectorClock) Merge(other *VectorClock) {
	vc.mu.Lock()
	defer vc.mu.Unlock()
	other.mu.RLock()
	defer other.mu.RUnlock()

	for nodeID, clock := range other.Clocks {
		if clock > vc.Clocks[nodeID] {
			vc.Clocks[nodeID] = clock
		}
	}
}

// Copy returns a copy of the vector clock
func (vc *VectorClock) Copy() *VectorClock {
	vc.mu.RLock()
	defer vc.mu.RUnlock()

	copy := NewVectorClock()
	for k, v := range vc.Clocks {
		copy.Clocks[k] = v
	}
	return copy
}

// HappensBefore returns true if this clock happens before other
func (vc *VectorClock) HappensBefore(other *VectorClock) bool {
	vc.mu.RLock()
	defer vc.mu.RUnlock()
	other.mu.RLock()
	defer other.mu.RUnlock()

	atLeastOneLess := false
	for nodeID, clock := range vc.Clocks {
		otherClock := other.Clocks[nodeID]
		if clock > otherClock {
			return false
		}
		if clock < otherClock {
			atLeastOneLess = true
		}
	}

	// Check for clocks in other that aren't in vc
	for nodeID, otherClock := range other.Clocks {
		if _, exists := vc.Clocks[nodeID]; !exists && otherClock > 0 {
			atLeastOneLess = true
		}
	}

	return atLeastOneLess
}

// Concurrent returns true if neither clock happens before the other
func (vc *VectorClock) Concurrent(other *VectorClock) bool {
	return !vc.HappensBefore(other) && !other.HappensBefore(vc)
}

// ReplicationEntry represents a single entry to be replicated
type ReplicationEntry struct {
	Sequence    uint64       `json:"sequence"`
	NodeID      string       `json:"node_id"`
	Operation   byte         `json:"operation"`
	Key         string       `json:"key"`
	Value       []byte       `json:"value"`
	VectorClock *VectorClock `json:"vector_clock"`
	Timestamp   int64        `json:"timestamp"`
}

// ReplicationBatch represents a batch of entries for efficient replication
type ReplicationBatch struct {
	LeaderID    string              `json:"leader_id"`
	Term        uint64              `json:"term"`
	Entries     []*ReplicationEntry `json:"entries"`
	CommitIndex uint64              `json:"commit_index"`
}

// ReplicationAck represents an acknowledgment from a follower
type ReplicationAck struct {
	FollowerID   string `json:"follower_id"`
	LastSequence uint64 `json:"last_sequence"`
	Success      bool   `json:"success"`
	Error        string `json:"error,omitempty"`
}

// Replicator manages log replication between nodes
type Replicator struct {
	config ReplicatorConfig
	mu     sync.RWMutex

	// Sequence tracking
	sequence    uint64
	commitIndex uint64

	// Vector clock for causality
	vectorClock *VectorClock

	// Pending entries awaiting acknowledgment
	pending     map[uint64]*pendingEntry
	pendingMu   sync.RWMutex

	// Follower connections
	followers   map[string]*FollowerConn
	followersMu sync.RWMutex

	// Batching
	batchBuffer []*ReplicationEntry
	batchMu     sync.Mutex
	batchCh     chan struct{}

	// Network
	listener net.Listener
	stopCh   chan struct{}
	wg       sync.WaitGroup

	// Callbacks
	onEntryCommitted func(entry *ReplicationEntry)
	onConflict       func(local, remote *ReplicationEntry) *ReplicationEntry
}

type pendingEntry struct {
	entry     *ReplicationEntry
	acks      map[string]bool
	committed bool
	doneCh    chan error
	timer     *time.Timer
}

// FollowerConn represents a connection to a follower
type FollowerConn struct {
	ID           string
	Addr         string
	conn         net.Conn
	mu           sync.Mutex
	lastSequence uint64
	lastContact  time.Time
	isHealthy    bool
	sendCh       chan *ReplicationBatch
}

// NewReplicator creates a new replicator instance
func NewReplicator(config ReplicatorConfig) *Replicator {
	return &Replicator{
		config:      config,
		sequence:    0,
		commitIndex: 0,
		vectorClock: NewVectorClock(),
		pending:     make(map[uint64]*pendingEntry),
		followers:   make(map[string]*FollowerConn),
		batchBuffer: make([]*ReplicationEntry, 0, config.MaxBatchSize),
		batchCh:     make(chan struct{}, 1),
		stopCh:      make(chan struct{}),
	}
}

// Start begins the replicator
func (r *Replicator) Start() error {
	addr := fmt.Sprintf("%s:%d", r.config.ListenAddr, r.config.ReplicationPort)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to start replicator: %w", err)
	}
	r.listener = ln

	r.wg.Add(2)
	go r.acceptConnections()
	go r.batchFlusher()

	fmt.Printf("Replicator started on %s\n", addr)
	return nil
}

// Stop gracefully shuts down the replicator
func (r *Replicator) Stop() error {
	close(r.stopCh)
	if r.listener != nil {
		r.listener.Close()
	}
	r.wg.Wait()
	return nil
}


// Replicate adds an entry to be replicated
func (r *Replicator) Replicate(op byte, key string, value []byte) error {
	seq := atomic.AddUint64(&r.sequence, 1)
	vcCopy := r.vectorClock.Copy()
	vcCopy.Increment(r.config.NodeID)

	entry := &ReplicationEntry{
		Sequence:    seq,
		NodeID:      r.config.NodeID,
		Operation:   op,
		Key:         key,
		Value:       value,
		VectorClock: vcCopy,
		Timestamp:   time.Now().UnixNano(),
	}

	// Add to batch buffer
	r.batchMu.Lock()
	r.batchBuffer = append(r.batchBuffer, entry)
	shouldFlush := len(r.batchBuffer) >= r.config.MaxBatchSize
	r.batchMu.Unlock()

	if shouldFlush {
		select {
		case r.batchCh <- struct{}{}:
		default:
		}
	}

	// Wait for acknowledgment based on consistency level
	return r.waitForAck(entry)
}

// waitForAck waits for acknowledgment based on consistency level
func (r *Replicator) waitForAck(entry *ReplicationEntry) error {
	if r.config.Consistency == ConsistencyEventual {
		return nil
	}

	doneCh := make(chan error, 1)
	pe := &pendingEntry{
		entry:     entry,
		acks:      make(map[string]bool),
		committed: false,
		doneCh:    doneCh,
	}

	r.pendingMu.Lock()
	r.pending[entry.Sequence] = pe
	r.pendingMu.Unlock()

	// Set timeout
	pe.timer = time.AfterFunc(r.config.AckTimeout, func() {
		r.pendingMu.Lock()
		if p, ok := r.pending[entry.Sequence]; ok && !p.committed {
			delete(r.pending, entry.Sequence)
			p.doneCh <- fmt.Errorf("replication timeout")
		}
		r.pendingMu.Unlock()
	})

	err := <-doneCh
	pe.timer.Stop()
	return err
}

// handleAck processes an acknowledgment from a follower
func (r *Replicator) handleAck(ack *ReplicationAck) {
	r.pendingMu.Lock()
	defer r.pendingMu.Unlock()

	pe, ok := r.pending[ack.LastSequence]
	if !ok || pe.committed {
		return
	}

	if ack.Success {
		pe.acks[ack.FollowerID] = true
	}

	// Check if we have enough acks
	r.followersMu.RLock()
	totalFollowers := len(r.followers)
	r.followersMu.RUnlock()

	acksNeeded := r.acksNeeded(totalFollowers)
	if len(pe.acks) >= acksNeeded {
		pe.committed = true
		atomic.StoreUint64(&r.commitIndex, ack.LastSequence)
		delete(r.pending, ack.LastSequence)
		pe.doneCh <- nil

		if r.onEntryCommitted != nil {
			go r.onEntryCommitted(pe.entry)
		}
	}
}

// acksNeeded returns the number of acks needed based on consistency level
func (r *Replicator) acksNeeded(totalFollowers int) int {
	switch r.config.Consistency {
	case ConsistencyStrong:
		return totalFollowers
	case ConsistencyQuorum:
		return (totalFollowers / 2) + 1
	default:
		return 0
	}
}

// acceptConnections handles incoming follower connections
func (r *Replicator) acceptConnections() {
	defer r.wg.Done()

	for {
		select {
		case <-r.stopCh:
			return
		default:
		}

		r.listener.(*net.TCPListener).SetDeadline(time.Now().Add(1 * time.Second))
		conn, err := r.listener.Accept()
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			continue
		}

		go r.handleFollowerConnection(conn)
	}
}

// handleFollowerConnection handles a connection from a follower
func (r *Replicator) handleFollowerConnection(conn net.Conn) {
	defer conn.Close()

	// Read follower ID
	lenBuf := make([]byte, 4)
	if _, err := conn.Read(lenBuf); err != nil {
		return
	}
	idLen := binary.BigEndian.Uint32(lenBuf)
	idBuf := make([]byte, idLen)
	if _, err := io.ReadFull(conn, idBuf); err != nil {
		return
	}
	followerID := string(idBuf)

	// Read last sequence
	seqBuf := make([]byte, 8)
	if _, err := conn.Read(seqBuf); err != nil {
		return
	}
	lastSeq := binary.BigEndian.Uint64(seqBuf)

	fc := &FollowerConn{
		ID:           followerID,
		Addr:         conn.RemoteAddr().String(),
		conn:         conn,
		lastSequence: lastSeq,
		lastContact:  time.Now(),
		isHealthy:    true,
		sendCh:       make(chan *ReplicationBatch, 100),
	}

	r.followersMu.Lock()
	r.followers[followerID] = fc
	r.followersMu.Unlock()

	defer func() {
		r.followersMu.Lock()
		delete(r.followers, followerID)
		r.followersMu.Unlock()
	}()

	// Start sender goroutine
	go r.sendToFollower(fc)

	// Read acks from follower
	for {
		select {
		case <-r.stopCh:
			return
		default:
		}

		conn.SetReadDeadline(time.Now().Add(30 * time.Second))

		// Read ack
		ackLenBuf := make([]byte, 4)
		if _, err := conn.Read(ackLenBuf); err != nil {
			return
		}
		ackLen := binary.BigEndian.Uint32(ackLenBuf)
		ackBuf := make([]byte, ackLen)
		if _, err := io.ReadFull(conn, ackBuf); err != nil {
			return
		}

		var ack ReplicationAck
		if err := json.Unmarshal(ackBuf, &ack); err != nil {
			continue
		}

		fc.lastSequence = ack.LastSequence
		fc.lastContact = time.Now()
		r.handleAck(&ack)
	}
}


// sendToFollower sends batches to a follower
func (r *Replicator) sendToFollower(fc *FollowerConn) {
	for {
		select {
		case <-r.stopCh:
			return
		case batch := <-fc.sendCh:
			fc.mu.Lock()
			data, err := json.Marshal(batch)
			if err != nil {
				fc.mu.Unlock()
				continue
			}

			fc.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))

			// Write length prefix
			lenBuf := make([]byte, 4)
			binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))
			if _, err := fc.conn.Write(lenBuf); err != nil {
				fc.isHealthy = false
				fc.mu.Unlock()
				return
			}

			// Write data
			if _, err := fc.conn.Write(data); err != nil {
				fc.isHealthy = false
				fc.mu.Unlock()
				return
			}

			fc.mu.Unlock()
		}
	}
}

// batchFlusher periodically flushes the batch buffer
func (r *Replicator) batchFlusher() {
	defer r.wg.Done()

	ticker := time.NewTicker(r.config.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
			r.flushBatch()
		case <-r.batchCh:
			r.flushBatch()
		}
	}
}

// flushBatch sends the current batch to all followers
func (r *Replicator) flushBatch() {
	r.batchMu.Lock()
	if len(r.batchBuffer) == 0 {
		r.batchMu.Unlock()
		return
	}

	entries := r.batchBuffer
	r.batchBuffer = make([]*ReplicationEntry, 0, r.config.MaxBatchSize)
	r.batchMu.Unlock()

	batch := &ReplicationBatch{
		LeaderID:    r.config.NodeID,
		Entries:     entries,
		CommitIndex: atomic.LoadUint64(&r.commitIndex),
	}

	r.followersMu.RLock()
	for _, fc := range r.followers {
		select {
		case fc.sendCh <- batch:
		default:
			// Channel full, follower is slow
		}
	}
	r.followersMu.RUnlock()
}

// AddFollower adds a follower to replicate to
func (r *Replicator) AddFollower(id, addr string) error {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return fmt.Errorf("failed to connect to follower: %w", err)
	}

	fc := &FollowerConn{
		ID:          id,
		Addr:        addr,
		conn:        conn,
		lastContact: time.Now(),
		isHealthy:   true,
		sendCh:      make(chan *ReplicationBatch, 100),
	}

	r.followersMu.Lock()
	r.followers[id] = fc
	r.followersMu.Unlock()

	go r.sendToFollower(fc)
	return nil
}

// RemoveFollower removes a follower
func (r *Replicator) RemoveFollower(id string) {
	r.followersMu.Lock()
	defer r.followersMu.Unlock()

	if fc, ok := r.followers[id]; ok {
		fc.conn.Close()
		delete(r.followers, id)
	}
}

// GetCommitIndex returns the current commit index
func (r *Replicator) GetCommitIndex() uint64 {
	return atomic.LoadUint64(&r.commitIndex)
}

// GetSequence returns the current sequence number
func (r *Replicator) GetSequence() uint64 {
	return atomic.LoadUint64(&r.sequence)
}

// SetCommitCallback sets the callback for committed entries
func (r *Replicator) SetCommitCallback(fn func(entry *ReplicationEntry)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.onEntryCommitted = fn
}

// SetConflictResolver sets the conflict resolution callback
func (r *Replicator) SetConflictResolver(fn func(local, remote *ReplicationEntry) *ReplicationEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.onConflict = fn
}

// ResolveConflict resolves a conflict between two entries
func (r *Replicator) ResolveConflict(local, remote *ReplicationEntry) *ReplicationEntry {
	r.mu.RLock()
	resolver := r.onConflict
	r.mu.RUnlock()

	if resolver != nil {
		return resolver(local, remote)
	}

	// Default: last-writer-wins based on timestamp
	if remote.Timestamp > local.Timestamp {
		return remote
	}
	return local
}

// GetFollowerStatus returns status of all followers
func (r *Replicator) GetFollowerStatus() map[string]interface{} {
	r.followersMu.RLock()
	defer r.followersMu.RUnlock()

	status := make(map[string]interface{})
	for id, fc := range r.followers {
		status[id] = map[string]interface{}{
			"addr":          fc.Addr,
			"last_sequence": fc.lastSequence,
			"last_contact":  fc.lastContact,
			"is_healthy":    fc.isHealthy,
		}
	}
	return status
}
