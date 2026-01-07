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

Replication Overview:
=====================

FlyDB implements Leader-Follower (Master-Slave) replication for:
  - Read scalability: Slaves can serve read queries
  - Fault tolerance: Slaves can be promoted if master fails
  - Data durability: Multiple copies of data across nodes

Replication Architecture:
=========================

  Master Node:
    - Accepts all write operations
    - Maintains the authoritative WAL
    - Streams WAL updates to connected slaves

  Slave Node:
    - Connects to master and requests WAL updates
    - Applies received updates to local storage
    - Can serve read queries (eventually consistent)

Replication Protocol:
=====================

The replication protocol is binary for efficiency:

  1. Slave connects to master's replication port
  2. Slave sends its current WAL offset (8 bytes, big-endian int64)
  3. Master streams WAL entries from that offset:
     - Op (1 byte): OpPut=1, OpDelete=2
     - KeyLen (4 bytes, big-endian uint32)
     - Key (KeyLen bytes)
     - ValueLen (4 bytes, big-endian uint32)
     - Value (ValueLen bytes)

Consistency Model:
==================

FlyDB provides eventual consistency for replicated data:
  - Writes are immediately visible on the master
  - Slaves receive updates asynchronously (100ms polling interval)
  - There may be a brief delay before slaves see new data

This model is suitable for read-heavy workloads where slight
staleness is acceptable in exchange for better performance.

Failure Handling:
=================

  - If a slave disconnects, it reconnects and resumes from its last offset
  - The WAL offset ensures no data is lost or duplicated
  - Slaves automatically catch up after network partitions
*/
package server

import (
	"encoding/binary"
	"flydb/internal/storage"
	"fmt"
	"io"
	"net"
	"time"
)

// replicationPollInterval defines how often the master checks for new WAL entries.
// A shorter interval means lower replication lag but higher CPU usage.
const replicationPollInterval = 100 * time.Millisecond

// Replicator handles Leader-Follower replication for FlyDB.
// It can operate in two modes:
//   - Master: Listens for slave connections and streams WAL updates
//   - Slave: Connects to master and applies received WAL updates
//
// The Replicator uses the WAL (Write-Ahead Log) as the source of truth
// for replication, ensuring that all operations are replicated in order.
type Replicator struct {
	// wal is the Write-Ahead Log used for reading/writing operations.
	wal *storage.WAL

	// kv is the key-value store for applying replicated operations.
	kv *storage.KVStore

	// isLeader indicates whether this node is the master (true) or slave (false).
	isLeader bool
}

// NewReplicator creates a new Replicator instance.
//
// Parameters:
//   - wal: The Write-Ahead Log for reading operations (master) or tracking offset (slave)
//   - kv: The KVStore for applying replicated operations (slave)
//   - isLeader: true for master mode, false for slave mode
//
// Returns a configured Replicator ready to start.
func NewReplicator(wal *storage.WAL, kv *storage.KVStore, isLeader bool) *Replicator {
	return &Replicator{wal: wal, kv: kv, isLeader: isLeader}
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
	fmt.Printf("Replication Master listening on %s\n", port)

	// Accept loop: continuously accept new slave connections.
	for {
		conn, err := ln.Accept()
		if err != nil {
			// Log and continue on accept errors.
			continue
		}
		// Handle each slave in a separate goroutine.
		go r.handleSlave(conn)
	}
}

// handleSlave manages a single slave connection.
// It reads the slave's current offset and streams WAL updates from that point.
//
// The streaming process:
//  1. Read the slave's current WAL offset
//  2. Poll for new WAL entries at regular intervals
//  3. Serialize and send new entries to the slave
//  4. Continue until the connection is closed
//
// Parameters:
//   - conn: The TCP connection to the slave
func (r *Replicator) handleSlave(conn net.Conn) {
	defer conn.Close()

	// Read the slave's current WAL offset.
	// This tells us where to start streaming from.
	// The offset is sent as a big-endian int64 (8 bytes).
	var offset int64
	err := binary.Read(conn, binary.BigEndian, &offset)
	if err != nil {
		fmt.Printf("Failed to read slave offset: %v\n", err)
		return
	}

	fmt.Printf("Slave connected with offset %d\n", offset)

	// Create a ticker for polling new WAL entries.
	// Polling is simpler than push-based notification and works well
	// for this educational implementation.
	ticker := time.NewTicker(replicationPollInterval)
	defer ticker.Stop()

	// Track the current position in the WAL.
	currentOffset := offset

	// Polling loop: check for new WAL entries periodically.
	for range ticker.C {
		// Get the current WAL size to check for new entries.
		size, err := r.wal.Size()
		if err != nil {
			continue
		}

		// If there are new entries, stream them to the slave.
		if size > currentOffset {
			// Replay WAL entries from the current offset.
			// For each entry, serialize it and send to the slave.
			err := r.wal.Replay(currentOffset, func(op byte, key string, value []byte) {
				// Serialize the WAL entry in the replication protocol format.
				// Format: Op(1) + KeyLen(4) + Key + ValueLen(4) + Value
				buf := make([]byte, 1+4+len(key)+4+len(value))
				buf[0] = op
				binary.BigEndian.PutUint32(buf[1:], uint32(len(key)))
				copy(buf[5:], []byte(key))
				off := 5 + len(key)
				binary.BigEndian.PutUint32(buf[off:], uint32(len(value)))
				copy(buf[off+4:], value)

				// Send the serialized entry to the slave.
				conn.Write(buf)

				// Update the offset to track progress.
				currentOffset += int64(len(buf))
			})

			if err != nil && err != io.EOF {
				fmt.Printf("Replay error: %v\n", err)
				return
			}
		}
	}
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
	// Connect to the master's replication port.
	conn, err := net.Dial("tcp", masterAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Get the current WAL size as our starting offset.
	// This ensures we only receive entries we don't already have.
	offset, err := r.wal.Size()
	if err != nil {
		return err
	}

	// Send our offset to the master.
	// The master will start streaming from this position.
	err = binary.Write(conn, binary.BigEndian, offset)
	if err != nil {
		return err
	}

	fmt.Printf("Connected to Master at %s with offset %d\n", masterAddr, offset)

	// Receive loop: continuously read and apply WAL entries.
	for {
		// Read the operation type (1 byte).
		opBuf := make([]byte, 1)
		if _, err := io.ReadFull(conn, opBuf); err != nil {
			return fmt.Errorf("failed to read operation: %w", err)
		}
		op := opBuf[0]

		// Read the key length (4 bytes, big-endian).
		var keyLen uint32
		if err := binary.Read(conn, binary.BigEndian, &keyLen); err != nil {
			return fmt.Errorf("failed to read key length: %w", err)
		}

		// Read the key bytes.
		keyBuf := make([]byte, keyLen)
		if _, err := io.ReadFull(conn, keyBuf); err != nil {
			return fmt.Errorf("failed to read key: %w", err)
		}
		key := string(keyBuf)

		// Read the value length (4 bytes, big-endian).
		var valLen uint32
		if err := binary.Read(conn, binary.BigEndian, &valLen); err != nil {
			return fmt.Errorf("failed to read value length: %w", err)
		}

		// Read the value bytes.
		valBuf := make([]byte, valLen)
		if _, err := io.ReadFull(conn, valBuf); err != nil {
			return fmt.Errorf("failed to read value: %w", err)
		}

		// Apply the operation to the local KVStore.
		// Note: This writes to the local WAL, which increases our offset.
		// This is intentional - it ensures the slave's WAL matches the master's.
		switch op {
		case storage.OpPut:
			if err := r.kv.Put(key, valBuf); err != nil {
				fmt.Printf("Failed to apply PUT %s: %v\n", key, err)
			}
		case storage.OpDelete:
			if err := r.kv.Delete(key); err != nil {
				fmt.Printf("Failed to apply DELETE %s: %v\n", key, err)
			}
		default:
			fmt.Printf("Unknown operation type: %d\n", op)
		}

		fmt.Printf("Replicated: %s\n", key)
	}
}
