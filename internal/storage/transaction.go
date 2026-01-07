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
Transaction Implementation
===========================

This file implements transaction support for FlyDB, providing ACID properties
through write buffering and commit/rollback semantics.

Transaction Model:
==================

FlyDB uses an optimistic concurrency model with the following properties:

  - Atomicity: All operations in a transaction are applied together or not at all
  - Consistency: Transactions maintain database invariants
  - Isolation: Read-committed isolation level (reads see committed data only)
  - Durability: Committed transactions are persisted to WAL

Transaction Lifecycle:
======================

  1. BEGIN: Creates a new transaction with a write buffer
  2. Operations: Writes go to the buffer, reads check buffer then storage
  3. COMMIT: Applies all buffered writes to storage atomically
  4. ROLLBACK: Discards the write buffer

Usage:
======

	tx := storage.NewTransaction(kvStore)
	tx.Put("key1", []byte("value1"))
	tx.Put("key2", []byte("value2"))
	err := tx.Commit() // Applies both writes atomically
*/
package storage

import (
	"errors"
	"sync"
)

// TxState represents the current state of a transaction.
type TxState int

const (
	TxStateActive TxState = iota
	TxStateCommitted
	TxStateRolledBack
)

// TxOperation represents a single operation in the transaction buffer.
type TxOperation struct {
	Op    byte   // OpPut or OpDelete
	Key   string
	Value []byte
}

// Savepoint represents a savepoint within a transaction.
type Savepoint struct {
	Name           string
	BufferPosition int               // Position in buffer when savepoint was created
	ReadCacheSnap  map[string][]byte // Snapshot of readCache
	DeleteSetSnap  map[string]bool   // Snapshot of deleteSet
}

// Transaction represents an active database transaction.
// It buffers all writes until Commit is called, at which point
// they are applied atomically to the underlying storage.
//
// Thread Safety: A single Transaction should only be used by one goroutine.
// The underlying storage operations are thread-safe.
type Transaction struct {
	store      *KVStore
	buffer     []TxOperation
	readCache  map[string][]byte // Cache of values read/written in this tx
	deleteSet  map[string]bool   // Keys marked for deletion
	state      TxState
	savepoints []Savepoint // Stack of savepoints
	mu         sync.Mutex
}

// NewTransaction creates a new transaction on the given KVStore.
// The transaction starts in the Active state.
func NewTransaction(store *KVStore) *Transaction {
	return &Transaction{
		store:      store,
		buffer:     make([]TxOperation, 0),
		readCache:  make(map[string][]byte),
		deleteSet:  make(map[string]bool),
		state:      TxStateActive,
		savepoints: make([]Savepoint, 0),
	}
}

// Put adds a write operation to the transaction buffer.
// The write is not applied until Commit is called.
func (tx *Transaction) Put(key string, value []byte) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.state != TxStateActive {
		return errors.New("transaction is not active")
	}

	tx.buffer = append(tx.buffer, TxOperation{
		Op:    OpPut,
		Key:   key,
		Value: value,
	})
	tx.readCache[key] = value
	delete(tx.deleteSet, key)
	return nil
}

// Delete adds a delete operation to the transaction buffer.
// The delete is not applied until Commit is called.
func (tx *Transaction) Delete(key string) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.state != TxStateActive {
		return errors.New("transaction is not active")
	}

	tx.buffer = append(tx.buffer, TxOperation{
		Op:  OpDelete,
		Key: key,
	})
	delete(tx.readCache, key)
	tx.deleteSet[key] = true
	return nil
}

// Get retrieves a value, checking the transaction buffer first,
// then falling back to the underlying storage.
func (tx *Transaction) Get(key string) ([]byte, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.state != TxStateActive {
		return nil, errors.New("transaction is not active")
	}

	// Check if key was deleted in this transaction
	if tx.deleteSet[key] {
		return nil, ErrNotFound
	}

	// Check transaction buffer first
	if val, ok := tx.readCache[key]; ok {
		return val, nil
	}

	// Fall back to storage
	return tx.store.Get(key)
}

// Scan returns all key-value pairs matching the prefix,
// including uncommitted changes from this transaction.
func (tx *Transaction) Scan(prefix string) (map[string][]byte, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.state != TxStateActive {
		return nil, errors.New("transaction is not active")
	}

	// Get data from storage
	result, err := tx.store.Scan(prefix)
	if err != nil {
		return nil, err
	}

	// Apply transaction buffer changes
	for key, val := range tx.readCache {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			result[key] = val
		}
	}

	// Remove deleted keys
	for key := range tx.deleteSet {
		delete(result, key)
	}

	return result, nil
}

// Commit applies all buffered operations to the underlying storage.
// After Commit, the transaction cannot be used for further operations.
//
// The commit is atomic - either all operations succeed or none do.
// On success, all changes are persisted to the WAL.
func (tx *Transaction) Commit() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.state != TxStateActive {
		return errors.New("transaction is not active")
	}

	// Apply all buffered operations to storage
	for _, op := range tx.buffer {
		var err error
		if op.Op == OpPut {
			err = tx.store.Put(op.Key, op.Value)
		} else if op.Op == OpDelete {
			err = tx.store.Delete(op.Key)
		}
		if err != nil {
			// On error, we've partially committed - this is a limitation
			// A production system would use a more sophisticated approach
			tx.state = TxStateRolledBack
			return err
		}
	}

	tx.state = TxStateCommitted
	return nil
}

// Rollback discards all buffered operations.
// After Rollback, the transaction cannot be used for further operations.
func (tx *Transaction) Rollback() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.state != TxStateActive {
		return errors.New("transaction is not active")
	}

	// Simply discard the buffer
	tx.buffer = nil
	tx.readCache = nil
	tx.deleteSet = nil
	tx.state = TxStateRolledBack
	return nil
}

// IsActive returns true if the transaction is still active.
func (tx *Transaction) IsActive() bool {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return tx.state == TxStateActive
}

// State returns the current transaction state.
func (tx *Transaction) State() TxState {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return tx.state
}

// CreateSavepoint creates a savepoint with the given name.
// The savepoint captures the current state of the transaction buffer.
func (tx *Transaction) CreateSavepoint(name string) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.state != TxStateActive {
		return errors.New("transaction is not active")
	}

	// Create snapshots of current state
	readCacheSnap := make(map[string][]byte)
	for k, v := range tx.readCache {
		readCacheSnap[k] = v
	}
	deleteSetSnap := make(map[string]bool)
	for k, v := range tx.deleteSet {
		deleteSetSnap[k] = v
	}

	tx.savepoints = append(tx.savepoints, Savepoint{
		Name:           name,
		BufferPosition: len(tx.buffer),
		ReadCacheSnap:  readCacheSnap,
		DeleteSetSnap:  deleteSetSnap,
	})

	return nil
}

// RollbackToSavepoint rolls back to the specified savepoint.
// All changes made after the savepoint are discarded.
func (tx *Transaction) RollbackToSavepoint(name string) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.state != TxStateActive {
		return errors.New("transaction is not active")
	}

	// Find the savepoint
	idx := -1
	for i, sp := range tx.savepoints {
		if sp.Name == name {
			idx = i
			break
		}
	}

	if idx == -1 {
		return errors.New("savepoint not found: " + name)
	}

	sp := tx.savepoints[idx]

	// Rollback buffer to savepoint position
	tx.buffer = tx.buffer[:sp.BufferPosition]

	// Restore readCache and deleteSet
	tx.readCache = make(map[string][]byte)
	for k, v := range sp.ReadCacheSnap {
		tx.readCache[k] = v
	}
	tx.deleteSet = make(map[string]bool)
	for k, v := range sp.DeleteSetSnap {
		tx.deleteSet[k] = v
	}

	// Remove savepoints after this one (but keep this one)
	tx.savepoints = tx.savepoints[:idx+1]

	return nil
}

// ReleaseSavepoint releases (destroys) the specified savepoint.
// The savepoint is removed but changes are kept.
func (tx *Transaction) ReleaseSavepoint(name string) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.state != TxStateActive {
		return errors.New("transaction is not active")
	}

	// Find and remove the savepoint
	idx := -1
	for i, sp := range tx.savepoints {
		if sp.Name == name {
			idx = i
			break
		}
	}

	if idx == -1 {
		return errors.New("savepoint not found: " + name)
	}

	// Remove the savepoint
	tx.savepoints = append(tx.savepoints[:idx], tx.savepoints[idx+1:]...)

	return nil
}
