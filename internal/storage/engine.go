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
Package storage provides the persistence layer for FlyDB.

Storage Engine Overview:
========================

The storage package defines the Engine interface and provides a
key-value store implementation backed by a Write-Ahead Log (WAL).

Architecture:
=============

	┌─────────────────────────────────────────────────────┐
	│                    SQL Executor                     │
	└─────────────────────────────────────────────────────┘
	                         │
	                         ▼
	┌─────────────────────────────────────────────────────┐
	│                  Engine Interface                   │
	│         (Put, Get, Delete, Scan, Close)             │
	└─────────────────────────────────────────────────────┘
	                         │
	                         ▼
	┌─────────────────────────────────────────────────────┐
	│                     KVStore                         │
	│              (In-Memory HashMap)                    │
	└─────────────────────────────────────────────────────┘
	                         │
	                         ▼
	┌─────────────────────────────────────────────────────┐
	│                Write-Ahead Log (WAL)                │
	│                  (Disk Persistence)                 │
	└─────────────────────────────────────────────────────┘

Key Conventions:
================

FlyDB uses a key prefix convention to organize data:

	row:<table>:<id>     - Table row data (JSON)
	seq:<table>          - Auto-increment sequence for table
	schema:<table>       - Table schema definition (JSON)
	user:<username>      - User credentials (JSON)
	perm:<user>:<table>  - User permissions for table (JSON)

This prefix-based organization enables efficient Scan operations
for retrieving all rows in a table or all schemas.

Durability Model:
=================

FlyDB provides durability through the Write-Ahead Log:

 1. Every Put/Delete operation is first written to the WAL
 2. The WAL is synced to disk before the operation returns
 3. On startup, the WAL is replayed to rebuild the in-memory state

This ensures that committed data survives crashes and restarts.

Thread Safety:
==============

The KVStore implementation uses a sync.RWMutex to provide
thread-safe access to the in-memory data. Multiple readers
can access data concurrently, but writes are exclusive.

Implementations:
================

  - KVStore: The primary implementation using an in-memory HashMap
    backed by a WAL for persistence.

Future implementations could include:
  - LSM-tree based storage for better write performance
  - B-tree based storage for better range query performance
  - Distributed storage for horizontal scaling
*/
package storage

import "errors"

// ErrNotFound is returned when a requested key does not exist in the store.
// This is a sentinel error that callers can check using errors.Is().
var ErrNotFound = errors.New("key not found")

// Engine defines the interface for the storage engine.
// It provides basic Key-Value operations and support for range scans.
//
// All implementations must be thread-safe for concurrent access.
// Operations should be durable - once Put returns successfully,
// the data should survive a crash.
//
// The Engine interface is designed to be simple and composable,
// allowing different storage backends to be swapped in.
type Engine interface {
	// Put stores a value associated with a key.
	// If the key already exists, the value is overwritten.
	//
	// The operation is durable - the data is persisted before returning.
	// Returns an error if the write fails.
	//
	// Example:
	//   err := engine.Put("user:alice", []byte(`{"name":"Alice"}`))
	Put(key string, value []byte) error

	// Get retrieves the value associated with a key.
	// Returns ErrNotFound if the key does not exist.
	//
	// Example:
	//   data, err := engine.Get("user:alice")
	//   if errors.Is(err, storage.ErrNotFound) {
	//       // Key doesn't exist
	//   }
	Get(key string) ([]byte, error)

	// Delete removes a key and its associated value from the store.
	// The operation is idempotent - deleting a non-existent key is not an error.
	//
	// The deletion is durable - it is persisted before returning.
	//
	// Example:
	//   err := engine.Delete("user:alice")
	Delete(key string) error

	// Scan iterates over all keys matching the given prefix.
	// Returns a map of key-value pairs where each key starts with the prefix.
	//
	// This is used for operations like:
	//   - Retrieving all rows in a table: Scan("row:users:")
	//   - Retrieving all schemas: Scan("schema:")
	//   - Retrieving all permissions for a user: Scan("perm:alice:")
	//
	// Example:
	//   rows, err := engine.Scan("row:users:")
	//   for key, value := range rows {
	//       // Process each row
	//   }
	Scan(prefix string) (map[string][]byte, error)

	// Close shuts down the storage engine and releases resources.
	// After Close is called, no other methods should be called.
	//
	// This should be called when the server is shutting down to ensure
	// all data is properly flushed and files are closed.
	//
	// Example:
	//   defer engine.Close()
	Close() error
}
