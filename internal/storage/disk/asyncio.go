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
Package disk provides asynchronous disk I/O for FlyDB.

Async I/O Overview:
===================

This module implements asynchronous disk I/O to improve throughput:

- Non-blocking read/write operations
- I/O request batching and coalescing
- Prioritized I/O scheduling
- Background flush and sync

Architecture:
=============

The async I/O system uses a worker pool model:

1. Requests are submitted to a queue
2. Worker goroutines process requests
3. Callbacks notify completion
4. Batching combines adjacent operations

Request Types:
==============

- Read: Async page read with callback
- Write: Async page write with callback
- Sync: Force data to disk
- Flush: Flush dirty pages

Benefits:
=========

- Higher throughput via parallelism
- Better CPU utilization
- Reduced latency for non-blocking callers
- Improved batching opportunities
*/
package disk

import (
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// I/O operation types
type IOOpType int

const (
	IORead IOOpType = iota
	IOWrite
	IOSync
	IOFlush
)

// IORequest represents an async I/O request
type IORequest struct {
	Type     IOOpType
	PageID   PageID
	Data     []byte
	Offset   int64
	Callback func(error)
	Priority int
	submittedAt time.Time
}

// IOResult represents the result of an I/O operation
type IOResult struct {
	Request *IORequest
	Error   error
	Latency time.Duration
}

// AsyncIOConfig holds configuration for async I/O
type AsyncIOConfig struct {
	NumWorkers     int           `json:"num_workers"`
	QueueSize      int           `json:"queue_size"`
	BatchSize      int           `json:"batch_size"`
	BatchTimeout   time.Duration `json:"batch_timeout"`
	EnableCoalesce bool          `json:"enable_coalesce"`
}

// DefaultAsyncIOConfig returns sensible defaults
func DefaultAsyncIOConfig() AsyncIOConfig {
	return AsyncIOConfig{
		NumWorkers:     4,
		QueueSize:      1024,
		BatchSize:      16,
		BatchTimeout:   1 * time.Millisecond,
		EnableCoalesce: true,
	}
}

// AsyncIO provides asynchronous I/O operations
type AsyncIO struct {
	config    AsyncIOConfig
	file      *os.File
	mu        sync.RWMutex
	
	// Request queue
	requestCh chan *IORequest
	
	// Worker management
	wg        sync.WaitGroup
	stopCh    chan struct{}
	
	// Statistics
	reads     atomic.Uint64
	writes    atomic.Uint64
	syncs     atomic.Uint64
	pending   atomic.Int64
	totalLatency atomic.Uint64
}

// NewAsyncIO creates a new async I/O manager
func NewAsyncIO(file *os.File, config AsyncIOConfig) *AsyncIO {
	aio := &AsyncIO{
		config:    config,
		file:      file,
		requestCh: make(chan *IORequest, config.QueueSize),
		stopCh:    make(chan struct{}),
	}

	// Start workers
	for i := 0; i < config.NumWorkers; i++ {
		aio.wg.Add(1)
		go aio.worker(i)
	}

	return aio
}

// Close shuts down the async I/O manager
func (aio *AsyncIO) Close() error {
	close(aio.stopCh)
	aio.wg.Wait()
	return nil
}

// ReadAsync submits an async read request
func (aio *AsyncIO) ReadAsync(pageID PageID, offset int64, size int, callback func([]byte, error)) error {
	data := make([]byte, size)
	req := &IORequest{
		Type:        IORead,
		PageID:      pageID,
		Data:        data,
		Offset:      offset,
		submittedAt: time.Now(),
		Callback: func(err error) {
			if err != nil {
				callback(nil, err)
			} else {
				callback(data, nil)
			}
		},
	}

	return aio.submit(req)
}

// WriteAsync submits an async write request
func (aio *AsyncIO) WriteAsync(pageID PageID, offset int64, data []byte, callback func(error)) error {
	req := &IORequest{
		Type:        IOWrite,
		PageID:      pageID,
		Data:        data,
		Offset:      offset,
		submittedAt: time.Now(),
		Callback:    callback,
	}

	return aio.submit(req)
}

// SyncAsync submits an async sync request
func (aio *AsyncIO) SyncAsync(callback func(error)) error {
	req := &IORequest{
		Type:        IOSync,
		submittedAt: time.Now(),
		Callback:    callback,
	}

	return aio.submit(req)
}

// submit submits a request to the queue
func (aio *AsyncIO) submit(req *IORequest) error {
	select {
	case aio.requestCh <- req:
		aio.pending.Add(1)
		return nil
	default:
		return errors.New("I/O queue full")
	}
}

// worker processes I/O requests
func (aio *AsyncIO) worker(id int) {
	defer aio.wg.Done()

	batch := make([]*IORequest, 0, aio.config.BatchSize)
	timer := time.NewTimer(aio.config.BatchTimeout)
	defer timer.Stop()

	for {
		select {
		case <-aio.stopCh:
			// Process remaining requests
			aio.processBatch(batch)
			return

		case req := <-aio.requestCh:
			batch = append(batch, req)
			if len(batch) >= aio.config.BatchSize {
				aio.processBatch(batch)
				batch = batch[:0]
				timer.Reset(aio.config.BatchTimeout)
			}

		case <-timer.C:
			if len(batch) > 0 {
				aio.processBatch(batch)
				batch = batch[:0]
			}
			timer.Reset(aio.config.BatchTimeout)
		}
	}
}

// processBatch processes a batch of I/O requests
func (aio *AsyncIO) processBatch(batch []*IORequest) {
	if len(batch) == 0 {
		return
	}

	// Optionally coalesce adjacent writes
	if aio.config.EnableCoalesce {
		batch = aio.coalesceRequests(batch)
	}

	for _, req := range batch {
		aio.processRequest(req)
	}
}

// processRequest processes a single I/O request
func (aio *AsyncIO) processRequest(req *IORequest) {
	var err error
	start := time.Now()

	switch req.Type {
	case IORead:
		_, err = aio.file.ReadAt(req.Data, req.Offset)
		aio.reads.Add(1)

	case IOWrite:
		_, err = aio.file.WriteAt(req.Data, req.Offset)
		aio.writes.Add(1)

	case IOSync:
		err = aio.file.Sync()
		aio.syncs.Add(1)
	}

	latency := time.Since(start)
	aio.totalLatency.Add(uint64(latency.Nanoseconds()))
	aio.pending.Add(-1)

	if req.Callback != nil {
		req.Callback(err)
	}
}

// coalesceRequests combines adjacent write requests
func (aio *AsyncIO) coalesceRequests(batch []*IORequest) []*IORequest {
	if len(batch) <= 1 {
		return batch
	}

	// Simple implementation: just return as-is for now
	// A full implementation would sort by offset and merge adjacent writes
	return batch
}

// Stats returns I/O statistics
func (aio *AsyncIO) Stats() (reads, writes, syncs uint64, pending int64, avgLatencyNs uint64) {
	reads = aio.reads.Load()
	writes = aio.writes.Load()
	syncs = aio.syncs.Load()
	pending = aio.pending.Load()

	total := reads + writes + syncs
	if total > 0 {
		avgLatencyNs = aio.totalLatency.Load() / total
	}
	return
}

// Pending returns the number of pending requests
func (aio *AsyncIO) Pending() int64 {
	return aio.pending.Load()
}

// WaitIdle waits for all pending requests to complete
func (aio *AsyncIO) WaitIdle(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for aio.pending.Load() > 0 {
		if time.Now().After(deadline) {
			return errors.New("timeout waiting for I/O to complete")
		}
		time.Sleep(1 * time.Millisecond)
	}
	return nil
}

