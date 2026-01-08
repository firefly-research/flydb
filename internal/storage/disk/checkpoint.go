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
Checkpoint Manager Implementation
==================================

Checkpoints are a critical component of database recovery systems. They
reduce recovery time by periodically flushing all dirty pages to disk
and recording a consistent state.

Why Checkpoints Matter:
=======================

Without checkpoints, crash recovery would require replaying the entire
Write-Ahead Log (WAL) from the beginning of time. With checkpoints:

  - Recovery only needs to replay WAL entries after the last checkpoint
  - Recovery time is bounded (typically seconds, not hours)
  - WAL files before the checkpoint can be safely deleted

Checkpoint Process:
===================

 1. Acquire checkpoint lock (prevents concurrent checkpoints)

 2. Flush all dirty pages from buffer pool to disk

 3. Sync the heap file (fsync) to ensure durability

 4. Write checkpoint marker file with timestamp

 5. Update checkpoint statistics

    ┌─────────────────────────────────────────────────────────────┐
    │                    Before Checkpoint                        │
    │  Buffer Pool: [Dirty] [Dirty] [Clean] [Dirty] [Clean]       │
    │  Heap File:   [Old]   [Old]   [Current] [Old] [Current]     │
    │  WAL:         [Entry1] [Entry2] [Entry3] [Entry4] [Entry5]  │
    └─────────────────────────────────────────────────────────────┘
    │
    ▼
    ┌─────────────────────────────────────────────────────────────┐
    │                    After Checkpoint                         │
    │  Buffer Pool: [Clean] [Clean] [Clean] [Clean] [Clean]       │
    │  Heap File:   [Current] [Current] [Current] [Current] [...] │
    │  WAL:         [Can be truncated] ──────────► [Entry5]       │
    └─────────────────────────────────────────────────────────────┘

Checkpoint Marker File:
=======================

The checkpoint marker file (checkpoint.marker) contains:

	Offset  Size  Field
	------  ----  -----
	0       8     Timestamp (Unix nanoseconds)
	8       4     Page count at checkpoint time

This file allows recovery to verify the checkpoint state and determine
which WAL entries need to be replayed.

Checkpoint Interval:
====================

The checkpoint interval is a trade-off:

  - Shorter intervals: Faster recovery, more I/O overhead
  - Longer intervals: Slower recovery, less I/O overhead

FlyDB defaults to 60-second intervals, which provides:
  - Maximum 60 seconds of WAL replay on recovery
  - Minimal impact on normal operation

Fuzzy vs. Sharp Checkpoints:
============================

FlyDB uses "sharp" checkpoints that briefly pause writes to ensure
consistency. Production databases often use "fuzzy" checkpoints that
allow concurrent writes, but require more complex recovery logic.

Thread Safety:
==============

The checkpoint manager uses a mutex to prevent concurrent checkpoints.
The background checkpoint loop runs in a separate goroutine and can
be stopped gracefully with Stop().

References:
===========

  - "Database Internals" by Alex Petrov, Chapter 6: Recovery
  - PostgreSQL Documentation: Checkpoints
  - See also: internal/storage/wal.go for Write-Ahead Logging
*/
package disk

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// CheckpointManager handles periodic checkpoints for faster recovery.
// A checkpoint flushes all dirty pages to disk and records the current state.
type CheckpointManager struct {
	bufferPool      *BufferPool
	checkpointDir   string
	interval        time.Duration
	mu              sync.Mutex
	lastCheckpoint  atomic.Int64
	checkpointCount atomic.Int64
	stopCh          chan struct{}
	doneCh          chan struct{}
}

// CheckpointConfig contains configuration for the checkpoint manager.
type CheckpointConfig struct {
	CheckpointDir string        // Directory to store checkpoint files
	Interval      time.Duration // Interval between checkpoints (0 = disabled)
}

// NewCheckpointManager creates a new checkpoint manager.
func NewCheckpointManager(bp *BufferPool, config CheckpointConfig) (*CheckpointManager, error) {
	if config.CheckpointDir != "" {
		if err := os.MkdirAll(config.CheckpointDir, 0755); err != nil {
			return nil, err
		}
	}

	cm := &CheckpointManager{
		bufferPool:    bp,
		checkpointDir: config.CheckpointDir,
		interval:      config.Interval,
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
	}

	return cm, nil
}

// Start begins the background checkpoint process.
func (cm *CheckpointManager) Start() {
	if cm.interval <= 0 {
		close(cm.doneCh)
		return
	}

	go cm.checkpointLoop()
}

// Stop stops the background checkpoint process.
func (cm *CheckpointManager) Stop() {
	close(cm.stopCh)
	<-cm.doneCh
}

func (cm *CheckpointManager) checkpointLoop() {
	defer close(cm.doneCh)

	ticker := time.NewTicker(cm.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			cm.Checkpoint()
		case <-cm.stopCh:
			// Final checkpoint before stopping
			cm.Checkpoint()
			return
		}
	}
}

// Checkpoint performs a checkpoint operation.
func (cm *CheckpointManager) Checkpoint() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// Flush all dirty pages
	if err := cm.bufferPool.FlushAllPages(); err != nil {
		return err
	}

	// Sync the heap file
	if err := cm.bufferPool.HeapFile().Sync(); err != nil {
		return err
	}

	// Write checkpoint marker file
	if cm.checkpointDir != "" {
		if err := cm.writeCheckpointMarker(); err != nil {
			return err
		}
	}

	cm.lastCheckpoint.Store(time.Now().Unix())
	cm.checkpointCount.Add(1)

	return nil
}

func (cm *CheckpointManager) writeCheckpointMarker() error {
	markerPath := filepath.Join(cm.checkpointDir, "checkpoint.marker")
	f, err := os.Create(markerPath)
	if err != nil {
		return err
	}
	defer f.Close()

	// Write checkpoint info: timestamp (8 bytes) + page count (4 bytes)
	data := make([]byte, 12)
	binary.BigEndian.PutUint64(data[0:8], uint64(time.Now().UnixNano()))
	binary.BigEndian.PutUint32(data[8:12], cm.bufferPool.HeapFile().PageCount())

	if _, err := f.Write(data); err != nil {
		return err
	}
	return f.Sync()
}

// LastCheckpoint returns the Unix timestamp of the last checkpoint.
func (cm *CheckpointManager) LastCheckpoint() int64 {
	return cm.lastCheckpoint.Load()
}

// CheckpointCount returns the number of checkpoints performed.
func (cm *CheckpointManager) CheckpointCount() int64 {
	return cm.checkpointCount.Load()
}
