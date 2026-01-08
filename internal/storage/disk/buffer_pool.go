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
Buffer Pool Implementation with LRU-K Eviction
===============================================

The buffer pool is a critical component of any disk-based database system.
It manages a fixed-size cache of database pages in memory, reducing expensive
disk I/O operations by keeping frequently accessed pages in RAM.

Why Buffer Pools Matter:
========================

Disk I/O is orders of magnitude slower than memory access:
  - Memory access: ~100 nanoseconds
  - SSD random read: ~100 microseconds (1000x slower)
  - HDD random read: ~10 milliseconds (100,000x slower)

A well-designed buffer pool can achieve 90%+ cache hit rates, dramatically
improving database performance.

LRU-K Eviction Algorithm:
=========================

FlyDB uses LRU-K (specifically LRU-2) instead of simple LRU for page eviction.
This algorithm was introduced by O'Neil, O'Neil, and Weikum in 1993.

Simple LRU Problem:
  - A sequential scan can flush the entire buffer pool
  - Frequently accessed pages get evicted by one-time accesses

LRU-K Solution:
  - Track the last K access times for each page
  - Evict the page with the oldest K-th access
  - Pages accessed only once are evicted before frequently accessed pages

Example with K=2:
  - Page A: accessed at t=1, t=5 (2nd-to-last access: t=1)
  - Page B: accessed at t=3 only (2nd-to-last access: never/∞)
  - Page C: accessed at t=2, t=4 (2nd-to-last access: t=2)
  - Eviction order: B (never accessed twice), A (oldest 2nd access), C

Buffer Pool Architecture:
=========================

	┌──────────────────────────────────────────────────────────────┐
	│                      Buffer Pool                             │
	│  ┌─────────────────────────────────────────────────────────┐ │
	│  │                    Page Table                           │ │
	│  │  PageID → Frame mapping (hash table for O(1) lookup)    │ │
	│  └─────────────────────────────────────────────────────────┘ │
	│  ┌─────────────────────────────────────────────────────────┐ │
	│  │                    Frame Array                          │ │
	│  │  [Frame 0] [Frame 1] [Frame 2] ... [Frame N-1]          │ │
	│  │  Each frame holds one page + metadata (pin count, dirty)│ │
	│  └─────────────────────────────────────────────────────────┘ │
	│  ┌─────────────────────────────────────────────────────────┐ │
	│  │                    LRU-K List                           │ │
	│  │  Unpinned frames ordered by K-th access time            │ │
	│  └─────────────────────────────────────────────────────────┘ │
	└──────────────────────────────────────────────────────────────┘

Pin/Unpin Protocol:
===================

Pages must be "pinned" before use and "unpinned" when done:

 1. FetchPage(pageID): Pins the page, returns pointer
 2. Use the page (read/write)
 3. UnpinPage(pageID, dirty): Unpins, marks dirty if modified

Pinned pages cannot be evicted, preventing use-after-free bugs.

Prefetching:
============

The buffer pool supports asynchronous prefetching for sequential scans.
When the executor knows it will need pages 5, 6, 7, it can call:

	bp.Prefetch(5, 6, 7)

A background goroutine loads these pages before they're needed.

Auto-Sizing:
============

The buffer pool automatically sizes itself based on available memory:
  - Uses 25% of available RAM
  - Minimum: 2MB (256 pages)
  - Maximum: 1GB (131,072 pages)

This provides good out-of-box performance without manual tuning.

Thread Safety:
==============

All buffer pool operations are protected by a mutex. This simple approach
works well for most workloads. Production databases use more sophisticated
locking (e.g., per-frame latches) for higher concurrency.

References:
===========

  - "The LRU-K Page Replacement Algorithm" by O'Neil, O'Neil, Weikum (1993)
  - "Database Internals" by Alex Petrov, Chapter 5: Buffer Management
  - PostgreSQL Documentation: Shared Buffer Cache
  - See also: docs/architecture.md for FlyDB's storage architecture
*/
package disk

import (
	"container/list"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// LRU-K parameter: number of accesses to track for each page
const lruKValue = 2

// BufferPool manages a fixed-size pool of pages in memory with LRU-K eviction.
// LRU-K is more effective than simple LRU for database workloads because it
// considers the frequency of access, not just recency.
type BufferPool struct {
	heapFile   *HeapFile
	poolSize   int
	mu         sync.Mutex
	pageTable  map[PageID]*Frame
	lruList    *list.List
	freeFrames []*Frame
	hits       atomic.Int64
	misses     atomic.Int64
	prefetches atomic.Int64

	// Prefetching
	prefetchChan   chan PageID
	prefetchDone   chan struct{}
	prefetchActive atomic.Bool
}

// Frame represents a slot in the buffer pool that can hold a page.
type Frame struct {
	page       *Page
	pageID     PageID
	pinCount   int
	dirty      bool
	lruElement *list.Element

	// LRU-K tracking: timestamps of last K accesses
	accessHistory []time.Time
}

// BufferPoolStats contains buffer pool statistics.
type BufferPoolStats struct {
	PoolSize    int
	UsedFrames  int
	DirtyPages  int
	PinnedPages int
	Hits        int64
	Misses      int64
	Prefetches  int64
	HitRate     float64
}

var ErrBufferPoolFull = errors.New("buffer pool is full, no unpinned pages to evict")

// CalculateOptimalPoolSize determines the optimal buffer pool size based on
// available system memory. It uses a percentage of available memory, with
// sensible minimum and maximum bounds.
func CalculateOptimalPoolSize() int {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	// Use 25% of system memory for buffer pool, with bounds
	// Sys is total memory obtained from OS
	availableBytes := memStats.Sys
	if availableBytes == 0 {
		availableBytes = 1 << 30 // Default to 1GB if can't determine
	}

	// Calculate 25% of available memory
	targetBytes := availableBytes / 4

	// Convert to pages (8KB each)
	pages := int(targetBytes / PageSize)

	// Apply bounds: minimum 256 pages (2MB), maximum 131072 pages (1GB)
	const minPages = 256
	const maxPages = 131072

	if pages < minPages {
		pages = minPages
	}
	if pages > maxPages {
		pages = maxPages
	}

	return pages
}

// NewBufferPool creates a new buffer pool with the given size.
// If poolSize is 0, it automatically calculates an optimal size.
func NewBufferPool(heapFile *HeapFile, poolSize int) *BufferPool {
	if poolSize <= 0 {
		poolSize = CalculateOptimalPoolSize()
	}

	bp := &BufferPool{
		heapFile:     heapFile,
		poolSize:     poolSize,
		pageTable:    make(map[PageID]*Frame),
		lruList:      list.New(),
		freeFrames:   make([]*Frame, poolSize),
		prefetchChan: make(chan PageID, 64), // Buffer for prefetch requests
		prefetchDone: make(chan struct{}),
	}
	for i := 0; i < poolSize; i++ {
		bp.freeFrames[i] = &Frame{
			accessHistory: make([]time.Time, 0, lruKValue),
		}
	}

	// Start prefetch worker
	go bp.prefetchWorker()

	return bp
}

// prefetchWorker handles asynchronous page prefetching.
func (bp *BufferPool) prefetchWorker() {
	bp.prefetchActive.Store(true)
	for {
		select {
		case pageID := <-bp.prefetchChan:
			bp.mu.Lock()
			// Only prefetch if not already in buffer pool
			if _, ok := bp.pageTable[pageID]; !ok {
				// Try to get a frame without blocking
				if len(bp.freeFrames) > 0 || bp.lruList.Len() > 0 {
					frame, err := bp.getFrame()
					if err == nil {
						page, err := bp.heapFile.ReadPage(pageID)
						if err == nil {
							frame.page = page
							frame.pageID = pageID
							frame.pinCount = 0 // Not pinned, just cached
							frame.dirty = false
							frame.accessHistory = frame.accessHistory[:0]
							frame.lruElement = bp.lruList.PushFront(frame)
							bp.pageTable[pageID] = frame
							bp.prefetches.Add(1)
						} else {
							bp.freeFrames = append(bp.freeFrames, frame)
						}
					}
				}
			}
			bp.mu.Unlock()
		case <-bp.prefetchDone:
			bp.prefetchActive.Store(false)
			return
		}
	}
}

// Prefetch schedules pages for asynchronous loading.
// This is useful for sequential scans where we can predict which pages will be needed.
func (bp *BufferPool) Prefetch(pageIDs ...PageID) {
	if !bp.prefetchActive.Load() {
		return
	}
	for _, pageID := range pageIDs {
		select {
		case bp.prefetchChan <- pageID:
		default:
			// Channel full, skip prefetch
		}
	}
}

// FetchPage fetches a page from the buffer pool, reading from disk if needed.
// Uses LRU-K algorithm for better cache behavior with database workloads.
func (bp *BufferPool) FetchPage(pageID PageID) (*Page, error) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	if frame, ok := bp.pageTable[pageID]; ok {
		bp.hits.Add(1)
		frame.pinCount++
		// Update access history for LRU-K
		bp.updateAccessHistory(frame)
		if frame.lruElement != nil {
			bp.lruList.Remove(frame.lruElement)
			frame.lruElement = nil
		}
		return frame.page, nil
	}

	bp.misses.Add(1)
	frame, err := bp.getFrame()
	if err != nil {
		return nil, err
	}

	page, err := bp.heapFile.ReadPage(pageID)
	if err != nil {
		bp.freeFrames = append(bp.freeFrames, frame)
		return nil, err
	}

	frame.page = page
	frame.pageID = pageID
	frame.pinCount = 1
	frame.dirty = false
	frame.lruElement = nil
	frame.accessHistory = frame.accessHistory[:0]
	bp.updateAccessHistory(frame)
	bp.pageTable[pageID] = frame
	return page, nil
}

// updateAccessHistory updates the LRU-K access history for a frame.
func (bp *BufferPool) updateAccessHistory(frame *Frame) {
	now := time.Now()
	if len(frame.accessHistory) >= lruKValue {
		// Shift history left and add new access
		copy(frame.accessHistory, frame.accessHistory[1:])
		frame.accessHistory[lruKValue-1] = now
	} else {
		frame.accessHistory = append(frame.accessHistory, now)
	}
}

// getFrame returns a free frame, evicting if necessary using LRU-K policy.
func (bp *BufferPool) getFrame() (*Frame, error) {
	if len(bp.freeFrames) > 0 {
		frame := bp.freeFrames[len(bp.freeFrames)-1]
		bp.freeFrames = bp.freeFrames[:len(bp.freeFrames)-1]
		return frame, nil
	}

	// LRU-K eviction: find the page with the oldest K-th access
	var victimElement *list.Element
	var oldestKthAccess time.Time

	for e := bp.lruList.Back(); e != nil; e = e.Prev() {
		frame := e.Value.(*Frame)
		if frame.pinCount == 0 {
			var kthAccess time.Time
			if len(frame.accessHistory) >= lruKValue {
				kthAccess = frame.accessHistory[0] // K-th most recent access
			} else if len(frame.accessHistory) > 0 {
				kthAccess = frame.accessHistory[0] // Use oldest available
			} else {
				kthAccess = time.Time{} // Never accessed, highest priority for eviction
			}

			if victimElement == nil || kthAccess.Before(oldestKthAccess) {
				victimElement = e
				oldestKthAccess = kthAccess
			}
		}
	}

	if victimElement == nil {
		return nil, ErrBufferPoolFull
	}

	frame := victimElement.Value.(*Frame)
	if frame.dirty {
		if err := bp.heapFile.WritePage(frame.page); err != nil {
			return nil, err
		}
	}
	delete(bp.pageTable, frame.pageID)
	bp.lruList.Remove(victimElement)
	frame.lruElement = nil
	frame.accessHistory = frame.accessHistory[:0]
	return frame, nil
}

// UnpinPage unpins a page and optionally marks it as dirty.
func (bp *BufferPool) UnpinPage(pageID PageID, dirty bool) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	frame, ok := bp.pageTable[pageID]
	if !ok {
		return ErrPageNotFound
	}
	if frame.pinCount <= 0 {
		return nil
	}
	frame.pinCount--
	if dirty {
		frame.dirty = true
	}
	if frame.pinCount == 0 && frame.lruElement == nil {
		frame.lruElement = bp.lruList.PushFront(frame)
	}
	return nil
}

// NewPage allocates a new page and adds it to the buffer pool.
func (bp *BufferPool) NewPage() (*Page, PageID, error) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	pageID, err := bp.heapFile.AllocatePage()
	if err != nil {
		return nil, InvalidPageID, err
	}
	frame, err := bp.getFrame()
	if err != nil {
		return nil, InvalidPageID, err
	}
	page := NewPage(pageID, PageTypeData)
	frame.page = page
	frame.pageID = pageID
	frame.pinCount = 1
	frame.dirty = true
	frame.lruElement = nil
	frame.accessHistory = frame.accessHistory[:0]
	bp.updateAccessHistory(frame)
	bp.pageTable[pageID] = frame
	return page, pageID, nil
}

// FlushPage writes a specific page to disk if dirty.
func (bp *BufferPool) FlushPage(pageID PageID) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	frame, ok := bp.pageTable[pageID]
	if !ok {
		return nil
	}
	if frame.dirty {
		if err := bp.heapFile.WritePage(frame.page); err != nil {
			return err
		}
		frame.dirty = false
	}
	return nil
}

// FlushAllPages writes all dirty pages to disk.
func (bp *BufferPool) FlushAllPages() error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	for _, frame := range bp.pageTable {
		if frame.dirty {
			if err := bp.heapFile.WritePage(frame.page); err != nil {
				return err
			}
			frame.dirty = false
		}
	}
	return nil
}

// Stats returns buffer pool statistics.
func (bp *BufferPool) Stats() BufferPoolStats {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	stats := BufferPoolStats{
		PoolSize:   bp.poolSize,
		UsedFrames: len(bp.pageTable),
		Hits:       bp.hits.Load(),
		Misses:     bp.misses.Load(),
		Prefetches: bp.prefetches.Load(),
	}
	for _, frame := range bp.pageTable {
		if frame.dirty {
			stats.DirtyPages++
		}
		if frame.pinCount > 0 {
			stats.PinnedPages++
		}
	}
	total := stats.Hits + stats.Misses
	if total > 0 {
		stats.HitRate = float64(stats.Hits) / float64(total) * 100
	}
	return stats
}

// Close flushes all dirty pages and closes the buffer pool.
func (bp *BufferPool) Close() error {
	// Stop prefetch worker
	if bp.prefetchActive.Load() {
		close(bp.prefetchDone)
		// Drain prefetch channel
		for len(bp.prefetchChan) > 0 {
			<-bp.prefetchChan
		}
	}

	if err := bp.FlushAllPages(); err != nil {
		return err
	}
	return bp.heapFile.Close()
}

// HeapFile returns the underlying heap file.
func (bp *BufferPool) HeapFile() *HeapFile {
	return bp.heapFile
}

// PoolSize returns the buffer pool size in pages.
func (bp *BufferPool) PoolSize() int {
	return bp.poolSize
}
