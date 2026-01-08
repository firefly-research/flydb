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
Package storage contains the KVStore implementation.

KVStore Overview:
=================

KVStore is FlyDB's primary storage engine implementation. It provides
a simple key-value interface backed by an in-memory HashMap for fast
reads and a Write-Ahead Log (WAL) for durability.

Architecture:
=============

	┌───────────────────────────────────────────────────┐
	│                     KVStore                       │
	├───────────────────────────────────────────────────┤
	│  ┌─────────────────────────────────────────────┐  │
	│  │           In-Memory HashMap                 │  │
	│  │         (map[string][]byte)                 │  │
	│  │                                             │  │
	│  │  Fast O(1) reads and writes                 │  │
	│  │  Protected by sync.RWMutex                  │  │
	│  └─────────────────────────────────────────────┘  │
	│                        │                          │
	│                        ▼                          │
	│  ┌─────────────────────────────────────────────┐  │
	│  │           Write-Ahead Log (WAL)             │  │
	│  │                                             │  │
	│  │  Append-only file for durability            │  │
	│  │  Replayed on startup to rebuild state       │  │
	│  └─────────────────────────────────────────────┘  │
	└───────────────────────────────────────────────────┘

Write Path:
===========

 1. Acquire write lock
 2. Write operation to WAL (sync to disk)
 3. Update in-memory HashMap
 4. Release lock

This ensures durability - if the process crashes after step 2,
the operation will be replayed on restart.

Read Path:
==========

 1. Acquire read lock
 2. Read from in-memory HashMap
 3. Release lock

Reads are fast O(1) operations that don't touch disk.

Startup/Recovery:
=================

 1. Open the WAL file
 2. Replay all operations from the beginning
 3. Rebuild the in-memory HashMap

This ensures that all committed data is restored after a restart.

Thread Safety:
==============

KVStore uses a sync.RWMutex for thread safety:
  - Multiple readers can access data concurrently
  - Writers have exclusive access
  - This provides good read performance for read-heavy workloads

Performance Characteristics:
============================

  - Put: O(1) amortized, with disk I/O for WAL
  - Get: O(1), in-memory only
  - Delete: O(1) amortized, with disk I/O for WAL
  - Scan: O(N) where N is total number of keys

The Scan operation is the main performance bottleneck. A production
system would use a B-tree or LSM-tree for efficient range queries.
*/
package storage

import (
	"strings"
	"sync"
)

// KVStore is an in-memory key-value store with persistence provided by a Write-Ahead Log (WAL).
// It implements the Engine interface.
//
// The store maintains all data in memory for fast access, while the WAL
// provides durability by persisting all operations to disk.
//
// Thread Safety: All methods are safe for concurrent use.
type KVStore struct {
	// data is the in-memory HashMap storing all key-value pairs.
	data map[string][]byte

	// mu protects concurrent access to the data map.
	// Uses RWMutex to allow concurrent reads.
	mu sync.RWMutex

	// wal is the Write-Ahead Log for durability.
	wal *WAL
}

// NewKVStore creates a new KVStore backed by a WAL file at walPath.
// It opens the WAL and replays existing entries to restore the state.
//
// The initialization process:
//  1. Open or create the WAL file
//  2. Create an empty in-memory HashMap
//  3. Replay all WAL entries to rebuild state
//
// Parameters:
//   - walPath: Path to the WAL file (created if it doesn't exist)
//
// Returns the initialized KVStore, or an error if initialization fails.
//
// Example:
//
//	store, err := storage.NewKVStore("/var/lib/flydb/data.fdb")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer store.Close()
func NewKVStore(walPath string) (*KVStore, error) {
	return NewKVStoreWithEncryption(walPath, EncryptionConfig{Enabled: false})
}

// NewKVStoreWithEncryption creates a new KVStore with optional encryption.
// When encryption is enabled, all WAL entries are encrypted using AES-256-GCM.
//
// Parameters:
//   - walPath: Path to the WAL file (created if it doesn't exist)
//   - encConfig: Encryption configuration
//
// Returns the initialized KVStore, or an error if initialization fails.
//
// Example:
//
//	config := storage.EncryptionConfig{
//	    Enabled:    true,
//	    Passphrase: "my-secret-passphrase",
//	}
//	store, err := storage.NewKVStoreWithEncryption("/var/lib/flydb/data.fdb", config)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer store.Close()
func NewKVStoreWithEncryption(walPath string, encConfig EncryptionConfig) (*KVStore, error) {
	// Open or create the WAL file with encryption config.
	wal, err := OpenWALWithEncryption(walPath, encConfig)
	if err != nil {
		return nil, err
	}

	// Create the store with an empty HashMap.
	store := &KVStore{
		data: make(map[string][]byte),
		wal:  wal,
	}

	// Replay WAL from the beginning (offset 0) to rebuild in-memory map.
	// This restores all committed data from previous runs.
	err = wal.Replay(0, func(op byte, key string, value []byte) {
		if op == OpPut {
			store.data[key] = value
		} else if op == OpDelete {
			delete(store.data, key)
		}
	})
	if err != nil {
		wal.Close()
		return nil, err
	}

	return store, nil
}

// IsEncrypted returns true if the KVStore is using encryption.
func (s *KVStore) IsEncrypted() bool {
	return s.wal.IsEncrypted()
}

// WAL returns the underlying Write-Ahead Log.
// This is exposed for the replication system, which needs direct
// access to the WAL for streaming updates to replicas.
//
// Returns the WAL instance used by this store.
func (s *KVStore) WAL() *WAL {
	return s.wal
}

// Put writes a key-value pair to the store.
// The operation is first written to the WAL for durability,
// then the in-memory map is updated.
//
// This method is thread-safe and blocks until the WAL write completes.
//
// Parameters:
//   - key: The key to store
//   - value: The value to associate with the key
//
// Returns an error if the WAL write fails.
func (s *KVStore) Put(key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Write to WAL first for durability.
	if err := s.wal.Write(OpPut, key, value); err != nil {
		return err
	}

	// Update in-memory map.
	s.data[key] = value
	return nil
}

// Get retrieves a value by key from the in-memory map.
// This is a fast O(1) operation that doesn't touch disk.
//
// This method is thread-safe and allows concurrent reads.
//
// Parameters:
//   - key: The key to look up
//
// Returns the value and nil error if found, or nil and ErrNotFound if not.
func (s *KVStore) Get(key string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	val, ok := s.data[key]
	if !ok {
		return nil, ErrNotFound
	}
	return val, nil
}

// Delete removes a key from the store.
// The operation is first written to the WAL for durability,
// then the key is removed from the in-memory map.
//
// This method is thread-safe and blocks until the WAL write completes.
// Deleting a non-existent key is not an error.
//
// Parameters:
//   - key: The key to delete
//
// Returns an error if the WAL write fails.
func (s *KVStore) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Write to WAL first for durability.
	if err := s.wal.Write(OpDelete, key, nil); err != nil {
		return err
	}

	// Remove from in-memory map.
	delete(s.data, key)
	return nil
}

// Scan returns all key-value pairs where the key starts with the given prefix.
// This is used for operations like retrieving all rows in a table.
//
// Performance Note: This implementation scans the entire map (O(N) where N
// is the total number of keys). A production system would use a B-tree or
// similar data structure for efficient prefix scans.
//
// This method is thread-safe and allows concurrent reads.
//
// Parameters:
//   - prefix: The key prefix to match
//
// Returns a map of matching key-value pairs.
//
// Example:
//
//	// Get all rows in the "users" table
//	rows, err := store.Scan("row:users:")
func (s *KVStore) Scan(prefix string) (map[string][]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make(map[string][]byte)
	for k, v := range s.data {
		if strings.HasPrefix(k, prefix) {
			result[k] = v
		}
	}
	return result, nil
}

// Close closes the underlying WAL file.
// After Close is called, no other methods should be called on this store.
//
// Returns an error if the WAL cannot be closed.
func (s *KVStore) Close() error {
	return s.wal.Close()
}
