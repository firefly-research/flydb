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
Disk Storage Engine Implementation
===================================

The DiskStorageEngine is FlyDB's production-grade storage engine that provides
persistent, durable storage with ACID guarantees. It implements the storage.Engine
interface, allowing it to be used interchangeably with the in-memory engine.

Architecture Overview:
======================

	┌──────────────────────────────────────────────────────────────────┐
	│                    DiskStorageEngine                             │
	│  ┌─────────────────────────────────────────────────────────────┐ │
	│  │                    Key Index (in-memory)                    │ │
	│  │  HashMap: key → RecordLocation{PageID, SlotID}              │ │
	│  └─────────────────────────────────────────────────────────────┘ │
	│                              │                                   │
	│                              ▼                                   │
	│  ┌─────────────────────────────────────────────────────────────┐ │
	│  │                    Buffer Pool                              │ │
	│  │  LRU-K cache of pages in memory                             │ │
	│  └─────────────────────────────────────────────────────────────┘ │
	│                              │                                   │
	│                              ▼                                   │
	│  ┌─────────────────────────────────────────────────────────────┐ │
	│  │                    Heap File                                │ │
	│  │  Slotted pages stored on disk                               │ │
	│  └─────────────────────────────────────────────────────────────┘ │
	│                                                                  │
	│  ┌─────────────────────────────────────────────────────────────┐ │
	│  │                    Write-Ahead Log (WAL)                    │ │
	│  │  Sequential log for durability                              │ │
	│  └─────────────────────────────────────────────────────────────┘ │
	└──────────────────────────────────────────────────────────────────┘

Key Components:
===============

1. Key Index (In-Memory):
  - Hash map from key to RecordLocation{PageID, SlotID}
  - Provides O(1) key lookups
  - Rebuilt from disk on startup
  - Trade-off: Uses memory but enables fast lookups

2. Buffer Pool:
  - Caches frequently accessed pages in memory
  - Uses LRU-K eviction for database-friendly caching
  - Reduces disk I/O by 90%+ for typical workloads

3. Heap File:
  - Stores data in 8KB slotted pages
  - Manages page allocation and free list
  - Provides the physical storage layer

4. Write-Ahead Log (WAL):
  - Ensures durability by logging changes before applying
  - Enables crash recovery
  - See internal/storage/wal.go for details

Record Format:
==============

Records are stored in a simple format:

	┌──────────────────────────────────────────────────────────┐
	│ Key Length (4 bytes) │ Key (variable) │ Value (variable) │
	└──────────────────────────────────────────────────────────┘

This format allows efficient key extraction during index rebuilding.

Write Path:
===========

 1. Acquire write lock
 2. Write to WAL (for durability)
 3. If key exists, delete old record
 4. Find page with space or allocate new page
 5. Insert record into page
 6. Update in-memory key index
 7. Release lock

Read Path:
==========

 1. Acquire read lock
 2. Look up key in in-memory index → RecordLocation
 3. Fetch page from buffer pool (may hit cache or disk)
 4. Read record from page using slot ID
 5. Decode and return value
 6. Release lock

Scan Optimization:
==================

The Scan operation uses several optimizations:

1. Prefix Filtering: Only fetch records matching the prefix
2. Page Sorting: Access pages in sequential order for better I/O
3. Prefetching: Load upcoming pages asynchronously

Auto-Configuration:
===================

The engine automatically configures itself for optimal performance:
  - Buffer pool size: 25% of available RAM (2MB-1GB)
  - Checkpoint interval: 60 seconds (configurable)

Thread Safety:
==============

All operations are protected by a read-write mutex:
  - Read operations (Get, Scan) use RLock for concurrency
  - Write operations (Put, Delete) use Lock for exclusivity

References:
===========

  - See buffer_pool.go for caching implementation
  - See heap_file.go for page storage
  - See page.go for slotted page format
  - See checkpoint.go for checkpoint management
  - See internal/storage/wal.go for write-ahead logging
*/
package disk

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// WALInterface defines the interface for WAL operations.
// This allows the disk engine to work with the WAL from the parent storage package.
type WALInterface interface {
	Write(op byte, key string, value []byte) error
	Sync() error
	Close() error
	Size() (int64, error)
	IsEncrypted() bool
	Replay(fromOffset int64, callback func(op byte, key string, value []byte)) error
}

// WAL operation types (must match storage.OpPut and storage.OpDelete)
const (
	OpPut    byte = 1
	OpDelete byte = 2
)

// DiskStorageEngine implements the storage.Engine interface using disk-based storage.
// It uses a heap file for data storage, a buffer pool for caching, and WAL for durability.
type DiskStorageEngine struct {
	dataDir    string
	bufferPool *BufferPool
	checkpoint *CheckpointManager
	wal        WALInterface
	mu         sync.RWMutex
	keyIndex   map[string]RecordLocation // In-memory index: key -> location
	closed     bool
	encrypted  bool

	// Statistics
	keyCount atomic.Int64
	dataSize atomic.Int64
}

// RecordLocation identifies where a record is stored on disk.
type RecordLocation struct {
	PageID PageID
	SlotID uint16
}

// DiskEngineConfig contains configuration for the disk storage engine.
type DiskEngineConfig struct {
	DataDir            string       // Directory for data files
	BufferPoolSize     int          // Number of pages in buffer pool (0 = auto)
	CheckpointInterval int          // Checkpoint interval in seconds (0 = disabled)
	WAL                WALInterface // Optional WAL for durability
	Encrypted          bool         // Whether encryption is enabled
}

// DefaultDiskEngineConfig returns default configuration with auto-sized buffer pool.
func DefaultDiskEngineConfig() DiskEngineConfig {
	return DiskEngineConfig{
		DataDir:            "./data",
		BufferPoolSize:     0,  // Auto-size based on available memory
		CheckpointInterval: 60, // 1 minute
	}
}

// NewDiskStorageEngine creates a new disk-based storage engine.
func NewDiskStorageEngine(config DiskEngineConfig) (*DiskStorageEngine, error) {
	if err := os.MkdirAll(config.DataDir, 0755); err != nil {
		return nil, err
	}

	heapPath := filepath.Join(config.DataDir, "data.db")
	var heapFile *HeapFile
	var err error

	if _, statErr := os.Stat(heapPath); os.IsNotExist(statErr) {
		heapFile, err = CreateHeapFile(heapPath)
	} else {
		heapFile, err = OpenHeapFile(heapPath)
	}
	if err != nil {
		return nil, err
	}

	// Auto-size buffer pool if not specified
	poolSize := config.BufferPoolSize
	if poolSize <= 0 {
		poolSize = CalculateOptimalPoolSize()
	}

	bufferPool := NewBufferPool(heapFile, poolSize)

	engine := &DiskStorageEngine{
		dataDir:    config.DataDir,
		bufferPool: bufferPool,
		wal:        config.WAL,
		keyIndex:   make(map[string]RecordLocation),
		encrypted:  config.Encrypted,
	}

	// Load index from disk
	if err := engine.loadIndex(); err != nil {
		bufferPool.Close()
		return nil, err
	}

	// Start checkpoint manager if interval is set
	if config.CheckpointInterval > 0 {
		checkpointConfig := CheckpointConfig{
			CheckpointDir: filepath.Join(config.DataDir, "checkpoints"),
			Interval:      time.Duration(config.CheckpointInterval) * time.Second,
		}
		var err error
		engine.checkpoint, err = NewCheckpointManager(bufferPool, checkpointConfig)
		if err != nil {
			bufferPool.Close()
			return nil, err
		}
		engine.checkpoint.Start()
	}

	return engine, nil
}

// loadIndex scans all pages to rebuild the in-memory key index.
func (e *DiskStorageEngine) loadIndex() error {
	pageCount := e.bufferPool.HeapFile().PageCount()
	var totalSize int64

	for i := uint32(1); i <= pageCount; i++ {
		pageID := PageID(i)
		page, err := e.bufferPool.FetchPage(pageID)
		if err != nil {
			continue // Skip unreadable pages
		}

		header := page.Header()
		if header.PageType != PageTypeData {
			e.bufferPool.UnpinPage(pageID, false)
			continue
		}

		// Scan slots in this page
		for slotID := uint16(0); slotID < header.SlotCount; slotID++ {
			slot := page.GetSlot(slotID)
			if slot.Offset == 0 && slot.Length == 0 {
				continue // Empty slot
			}
			record, err := page.GetRecord(slotID)
			if err != nil {
				continue
			}
			key := e.extractKeyFromRecord(record)
			if key != "" {
				e.keyIndex[key] = RecordLocation{PageID: pageID, SlotID: slotID}
				totalSize += int64(len(record))
			}
		}
		e.bufferPool.UnpinPage(pageID, false)
	}

	e.keyCount.Store(int64(len(e.keyIndex)))
	e.dataSize.Store(totalSize)
	return nil
}

func (e *DiskStorageEngine) extractKeyFromRecord(record []byte) string {
	if len(record) < 4 {
		return ""
	}
	keyLen := binary.BigEndian.Uint32(record[0:4])
	if len(record) < int(4+keyLen) {
		return ""
	}
	return string(record[4 : 4+keyLen])
}

// encodeRecord encodes a key-value pair into a record format.
// Format: [keyLen:4][key:keyLen][value:rest]
func encodeRecord(key string, value []byte) []byte {
	keyBytes := []byte(key)
	record := make([]byte, 4+len(keyBytes)+len(value))
	binary.BigEndian.PutUint32(record[0:4], uint32(len(keyBytes)))
	copy(record[4:4+len(keyBytes)], keyBytes)
	copy(record[4+len(keyBytes):], value)
	return record
}

func decodeRecord(record []byte) (string, []byte, error) {
	if len(record) < 4 {
		return "", nil, errors.New("invalid record")
	}
	keyLen := binary.BigEndian.Uint32(record[0:4])
	if len(record) < int(4+keyLen) {
		return "", nil, errors.New("invalid record")
	}
	key := string(record[4 : 4+keyLen])
	value := record[4+keyLen:]
	return key, value, nil
}

// Put stores a value associated with a key.
func (e *DiskStorageEngine) Put(key string, value []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return errors.New("engine is closed")
	}

	// Write to WAL first for durability
	if e.wal != nil {
		if err := e.wal.Write(OpPut, key, value); err != nil {
			return err
		}
	}

	record := encodeRecord(key, value)
	isNew := true

	// If key exists, try to update in place or delete old record
	if loc, exists := e.keyIndex[key]; exists {
		isNew = false
		// Delete old record first
		page, err := e.bufferPool.FetchPage(loc.PageID)
		if err == nil {
			page.DeleteRecord(loc.SlotID)
			e.bufferPool.UnpinPage(loc.PageID, true)
		}
		delete(e.keyIndex, key)
	}

	// Find a page with enough space or allocate new one
	pageID, slotID, err := e.insertRecord(record)
	if err != nil {
		return err
	}

	e.keyIndex[key] = RecordLocation{PageID: pageID, SlotID: slotID}

	// Update statistics
	if isNew {
		e.keyCount.Add(1)
	}
	e.dataSize.Add(int64(len(record)))

	return nil
}

func (e *DiskStorageEngine) insertRecord(record []byte) (PageID, uint16, error) {
	// Try to find existing page with space
	pageCount := e.bufferPool.HeapFile().PageCount()
	for i := uint32(1); i <= pageCount; i++ {
		pageID := PageID(i)
		page, err := e.bufferPool.FetchPage(pageID)
		if err != nil {
			continue
		}

		header := page.Header()
		if header.PageType != PageTypeData {
			e.bufferPool.UnpinPage(pageID, false)
			continue
		}

		slotID, err := page.InsertRecord(record)
		if err == nil {
			e.bufferPool.UnpinPage(pageID, true)
			return pageID, slotID, nil
		}
		e.bufferPool.UnpinPage(pageID, false)
	}

	// Allocate new page
	page, pageID, err := e.bufferPool.NewPage()
	if err != nil {
		return InvalidPageID, 0, err
	}

	slotID, err := page.InsertRecord(record)
	if err != nil {
		e.bufferPool.UnpinPage(pageID, true)
		return InvalidPageID, 0, err
	}

	e.bufferPool.UnpinPage(pageID, true)
	return pageID, slotID, nil
}

// Get retrieves the value associated with a key.
func (e *DiskStorageEngine) Get(key string) ([]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.closed {
		return nil, errors.New("engine is closed")
	}

	loc, exists := e.keyIndex[key]
	if !exists {
		return nil, ErrPageNotFound // Will be mapped to storage.ErrNotFound
	}

	page, err := e.bufferPool.FetchPage(loc.PageID)
	if err != nil {
		return nil, err
	}
	defer e.bufferPool.UnpinPage(loc.PageID, false)

	record, err := page.GetRecord(loc.SlotID)
	if err != nil {
		return nil, ErrPageNotFound
	}

	_, value, err := decodeRecord(record)
	return value, err
}

// Delete removes a key and its associated value.
func (e *DiskStorageEngine) Delete(key string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return errors.New("engine is closed")
	}

	loc, exists := e.keyIndex[key]
	if !exists {
		return nil // Idempotent delete
	}

	// Write to WAL first for durability
	if e.wal != nil {
		if err := e.wal.Write(OpDelete, key, nil); err != nil {
			return err
		}
	}

	page, err := e.bufferPool.FetchPage(loc.PageID)
	if err != nil {
		return err
	}

	page.DeleteRecord(loc.SlotID)
	e.bufferPool.UnpinPage(loc.PageID, true)
	delete(e.keyIndex, key)

	// Update statistics
	e.keyCount.Add(-1)

	return nil
}

// Scan returns all key-value pairs matching the given prefix.
// Uses prefetching to improve performance for sequential access patterns.
func (e *DiskStorageEngine) Scan(prefix string) (map[string][]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.closed {
		return nil, errors.New("engine is closed")
	}

	result := make(map[string][]byte)

	// Collect matching keys and their page locations
	type keyLoc struct {
		key string
		loc RecordLocation
	}
	var matchingKeys []keyLoc
	for key, loc := range e.keyIndex {
		if strings.HasPrefix(key, prefix) {
			matchingKeys = append(matchingKeys, keyLoc{key, loc})
		}
	}

	// Sort by page ID for sequential access, then by key
	sort.Slice(matchingKeys, func(i, j int) bool {
		if matchingKeys[i].loc.PageID != matchingKeys[j].loc.PageID {
			return matchingKeys[i].loc.PageID < matchingKeys[j].loc.PageID
		}
		return matchingKeys[i].key < matchingKeys[j].key
	})

	// Prefetch upcoming pages (look ahead by 4 pages)
	const prefetchAhead = 4
	for i := 0; i < len(matchingKeys) && i < prefetchAhead; i++ {
		e.bufferPool.Prefetch(matchingKeys[i].loc.PageID)
	}

	// Fetch values for matching keys
	for i, kl := range matchingKeys {
		// Prefetch next pages as we go
		if i+prefetchAhead < len(matchingKeys) {
			e.bufferPool.Prefetch(matchingKeys[i+prefetchAhead].loc.PageID)
		}

		page, err := e.bufferPool.FetchPage(kl.loc.PageID)
		if err != nil {
			continue
		}

		record, err := page.GetRecord(kl.loc.SlotID)
		if err == nil {
			_, value, decErr := decodeRecord(record)
			if decErr == nil {
				result[kl.key] = value
			}
		}
		e.bufferPool.UnpinPage(kl.loc.PageID, false)
	}

	return result, nil
}

// Close shuts down the storage engine.
func (e *DiskStorageEngine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return nil
	}
	e.closed = true

	if e.checkpoint != nil {
		e.checkpoint.Stop()
	}

	// Close WAL if we own it
	if e.wal != nil {
		e.wal.Close()
	}

	return e.bufferPool.Close()
}

// Sync forces all pending writes to be persisted to durable storage.
func (e *DiskStorageEngine) Sync() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return errors.New("engine is closed")
	}

	// Sync WAL first
	if e.wal != nil {
		if err := e.wal.Sync(); err != nil {
			return err
		}
	}

	// Flush all dirty pages
	return e.bufferPool.FlushAllPages()
}

// BufferPool returns the underlying buffer pool.
func (e *DiskStorageEngine) BufferPool() *BufferPool {
	return e.bufferPool
}

// KeyCount returns the number of keys in the store.
func (e *DiskStorageEngine) KeyCount() int64 {
	return e.keyCount.Load()
}

// DataSize returns the approximate size of all data in bytes.
func (e *DiskStorageEngine) DataSize() int64 {
	return e.dataSize.Load()
}

// IsEncrypted returns whether the engine uses encryption.
func (e *DiskStorageEngine) IsEncrypted() bool {
	return e.encrypted
}

// SetWAL sets the WAL for the engine.
func (e *DiskStorageEngine) SetWAL(wal WALInterface) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.wal = wal
}

// GetWAL returns the WAL interface.
func (e *DiskStorageEngine) GetWAL() WALInterface {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.wal
}
