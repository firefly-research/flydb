/*
 * Copyright (c) 2026 Firefly Software Solutions Inc.
 * Licensed under the Apache License, Version 2.0
 */

/*
Package storage provides the unified disk-based storage engine for FlyDB.

The storage engine uses a hybrid architecture that combines:
  - Page-based disk storage for datasets larger than RAM
  - Intelligent buffer pool with LRU-K caching for optimal performance
  - Write-Ahead Log (WAL) for durability and crash recovery
  - Automatic buffer pool sizing based on available system memory

This unified approach provides optimal performance for both small datasets
(that fit entirely in the buffer pool) and large datasets (that exceed RAM).
*/
package storage

import (
	"path/filepath"
	"time"

	"flydb/internal/storage/disk"
)

// StorageConfig contains configuration for creating a storage engine.
type StorageConfig struct {
	// DataDir is the directory for data files.
	DataDir string

	// BufferPoolSize is the number of pages in the buffer pool.
	// If 0, the buffer pool size is automatically calculated based on
	// available system memory (25% of available RAM, bounded between 2MB and 1GB).
	BufferPoolSize int

	// CheckpointInterval is the interval between checkpoints.
	// Set to 0 to disable automatic checkpoints.
	CheckpointInterval time.Duration

	// Encryption configuration.
	Encryption EncryptionConfig
}

// DefaultStorageConfig returns default storage configuration.
// The buffer pool size is automatically calculated based on available memory.
func DefaultStorageConfig() StorageConfig {
	return StorageConfig{
		DataDir:            "./data",
		BufferPoolSize:     0, // Auto-size based on available memory
		CheckpointInterval: 60 * time.Second,
	}
}

// NewStorageEngine creates a unified disk-based storage engine.
// The engine provides optimal performance for both small and large datasets
// through intelligent buffer pool caching and page-based storage.
func NewStorageEngine(config StorageConfig) (*UnifiedStorageEngine, error) {
	// Determine WAL path
	walPath := filepath.Join(config.DataDir, "wal.fdb")

	// Create WAL with encryption if enabled
	var wal *WAL
	var err error
	if config.Encryption.Enabled {
		wal, err = OpenWALWithEncryption(walPath, config.Encryption)
	} else {
		wal, err = OpenWAL(walPath)
	}
	if err != nil {
		return nil, err
	}

	// Create disk engine configuration
	diskConfig := disk.DiskEngineConfig{
		DataDir:            config.DataDir,
		BufferPoolSize:     config.BufferPoolSize, // 0 = auto-size
		CheckpointInterval: int(config.CheckpointInterval.Seconds()),
		Encrypted:          config.Encryption.Enabled,
	}

	// Create the disk engine
	diskEngine, err := disk.NewDiskStorageEngine(diskConfig)
	if err != nil {
		wal.Close()
		return nil, err
	}

	// Create WAL adapter and set it on the disk engine
	walAdapter := &walAdapter{wal: wal}
	diskEngine.SetWAL(walAdapter)

	// Create the unified engine wrapper
	engine := &UnifiedStorageEngine{
		diskEngine: diskEngine,
		wal:        wal,
		config:     config,
	}

	// Replay WAL to recover any uncommitted operations
	if err := engine.replayWAL(); err != nil {
		diskEngine.Close()
		return nil, err
	}

	return engine, nil
}

// walAdapter adapts the storage.WAL to the disk.WALInterface.
type walAdapter struct {
	wal *WAL
}

func (w *walAdapter) Write(op byte, key string, value []byte) error {
	return w.wal.Write(op, key, value)
}

func (w *walAdapter) Sync() error {
	return w.wal.Sync()
}

func (w *walAdapter) Close() error {
	return w.wal.Close()
}

func (w *walAdapter) Size() (int64, error) {
	return w.wal.Size()
}

func (w *walAdapter) IsEncrypted() bool {
	return w.wal.IsEncrypted()
}

func (w *walAdapter) Replay(fromOffset int64, callback func(op byte, key string, value []byte)) error {
	return w.wal.Replay(fromOffset, callback)
}

// UnifiedStorageEngine is the single storage engine implementation for FlyDB.
// It combines disk-based page storage with intelligent buffer pool caching
// and WAL for durability.
type UnifiedStorageEngine struct {
	diskEngine *disk.DiskStorageEngine
	wal        *WAL
	config     StorageConfig
}

// replayWAL replays the WAL to recover any operations not yet in the disk engine.
func (e *UnifiedStorageEngine) replayWAL() error {
	return e.wal.Replay(0, func(op byte, key string, value []byte) {
		switch op {
		case OpPut:
			// Check if key exists in disk engine
			_, err := e.diskEngine.Get(key)
			if err != nil {
				// Key not in disk engine, apply from WAL
				e.diskEngine.Put(key, value)
			}
		case OpDelete:
			e.diskEngine.Delete(key)
		}
	})
}

// Put stores a value associated with a key.
func (e *UnifiedStorageEngine) Put(key string, value []byte) error {
	return e.diskEngine.Put(key, value)
}

// Get retrieves the value associated with a key.
func (e *UnifiedStorageEngine) Get(key string) ([]byte, error) {
	value, err := e.diskEngine.Get(key)
	if err == disk.ErrPageNotFound {
		return nil, ErrNotFound
	}
	return value, err
}

// Delete removes a key and its associated value.
func (e *UnifiedStorageEngine) Delete(key string) error {
	return e.diskEngine.Delete(key)
}

// Scan returns all key-value pairs matching the given prefix.
func (e *UnifiedStorageEngine) Scan(prefix string) (map[string][]byte, error) {
	return e.diskEngine.Scan(prefix)
}

// Close shuts down the storage engine.
func (e *UnifiedStorageEngine) Close() error {
	// Sync before closing
	e.Sync()
	return e.diskEngine.Close()
}

// Sync forces all pending writes to be persisted to durable storage.
func (e *UnifiedStorageEngine) Sync() error {
	return e.diskEngine.Sync()
}

// Stats returns statistics about the storage engine.
func (e *UnifiedStorageEngine) Stats() EngineStats {
	bpStats := e.diskEngine.BufferPool().Stats()
	walSize, _ := e.wal.Size()

	return EngineStats{
		KeyCount:       e.diskEngine.KeyCount(),
		DataSize:       e.diskEngine.DataSize(),
		WALSize:        walSize,
		EngineType:     EngineTypeDisk,
		IsEncrypted:    e.diskEngine.IsEncrypted(),
		BufferPoolSize: int64(bpStats.PoolSize) * int64(disk.PageSize),
		BufferPoolUsed: int64(bpStats.UsedFrames) * int64(disk.PageSize),
		CacheHitRate:   bpStats.HitRate,
		DirtyPages:     int64(bpStats.DirtyPages),
	}
}

// Type returns the type of this storage engine.
func (e *UnifiedStorageEngine) Type() StorageEngineType {
	return EngineTypeDisk
}

// WAL returns the underlying Write-Ahead Log.
func (e *UnifiedStorageEngine) WAL() *WAL {
	return e.wal
}

// IsEncrypted returns true if the storage engine uses encryption.
func (e *UnifiedStorageEngine) IsEncrypted() bool {
	return e.diskEngine.IsEncrypted()
}

// BufferPoolStats returns statistics about the buffer pool.
func (e *UnifiedStorageEngine) BufferPoolStats() disk.BufferPoolStats {
	return e.diskEngine.BufferPool().Stats()
}

// DiskEngine returns the underlying disk engine for advanced operations.
func (e *UnifiedStorageEngine) DiskEngine() *disk.DiskStorageEngine {
	return e.diskEngine
}

// ApplyReplicatedPut applies a replicated PUT operation without writing to WAL.
// This is used by followers in cluster mode to apply changes received from the leader.
// The WAL entry is written separately via WriteReplicatedWAL.
func (e *UnifiedStorageEngine) ApplyReplicatedPut(key string, value []byte) error {
	return e.diskEngine.ApplyReplicatedPut(key, value)
}

// ApplyReplicatedDelete applies a replicated DELETE operation without writing to WAL.
// This is used by followers in cluster mode to apply changes received from the leader.
func (e *UnifiedStorageEngine) ApplyReplicatedDelete(key string) error {
	return e.diskEngine.ApplyReplicatedDelete(key)
}

// WriteReplicatedWAL writes a replicated WAL entry for crash recovery.
// This is called by followers before applying replicated changes to ensure
// crash recovery works correctly.
func (e *UnifiedStorageEngine) WriteReplicatedWAL(op byte, key string, value []byte) error {
	return e.diskEngine.WriteReplicatedWAL(op, key, value)
}

