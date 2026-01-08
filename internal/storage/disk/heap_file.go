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
Heap File Implementation
========================

A heap file is the fundamental storage structure for table data in FlyDB.
It manages a collection of pages stored sequentially in a single file,
providing efficient allocation, deallocation, and access to pages.

What is a "Heap" in Database Terms?
===================================

In database terminology, a "heap" is an unordered collection of records.
Unlike a B-tree or other indexed structure, records in a heap are stored
in no particular order - new records are simply appended to the first
page with available space.

This is different from the "heap" data structure (priority queue) used
in algorithms. The name comes from the idea of a "pile" of records.

Heap File Layout:
=================

	┌─────────────────────────────────────────────────────────────┐
	│                    File Header (8KB)                        │
	│  [Magic: "FLYD"] [Version] [PageCount] [FreeListHead]       │
	├─────────────────────────────────────────────────────────────┤
	│                    Page 1 (8KB)                             │
	├─────────────────────────────────────────────────────────────┤
	│                    Page 2 (8KB)                             │
	├─────────────────────────────────────────────────────────────┤
	│                    Page 3 (8KB)                             │
	├─────────────────────────────────────────────────────────────┤
	│                       ...                                   │
	├─────────────────────────────────────────────────────────────┤
	│                    Page N (8KB)                             │
	└─────────────────────────────────────────────────────────────┘

Free List Management:
=====================

When pages are deleted, they're added to a "free list" - a linked list
of available pages. This allows page reuse without file compaction:

	FreeListHead → Page 5 → Page 12 → Page 3 → InvalidPageID

When allocating a new page:
 1. If free list is non-empty, pop the head and reuse it
 2. Otherwise, extend the file and create a new page

This approach:
  - Avoids expensive file compaction
  - Reuses space from deleted data
  - Keeps the file from growing unboundedly

File Header Format:
===================

The first page (8KB) is reserved for file metadata:

	Offset  Size  Field
	------  ----  -----
	0       4     Magic number (0x464C5944 = "FLYD")
	4       4     Version number (currently 1)
	8       4     Total page count
	12      4     Free list head page ID

The magic number allows quick validation that a file is a valid FlyDB
heap file, preventing accidental corruption of unrelated files.

Thread Safety:
==============

All heap file operations are protected by a read-write mutex:
  - Read operations (ReadPage) use RLock for concurrent access
  - Write operations (WritePage, AllocatePage) use Lock for exclusivity

Page Addressing:
================

Pages are addressed by PageID (1-indexed). The file offset for a page is:

	offset = FileHeaderSize + (PageID - 1) * PageSize

PageID 0 (InvalidPageID) is reserved as a null/invalid marker.

Durability:
===========

The heap file provides basic durability through:
  - Sync() to flush OS buffers to disk
  - Atomic header updates (single write)

For full ACID durability, use the Write-Ahead Log (WAL) in conjunction
with the heap file. See internal/storage/wal.go.

References:
===========

  - "Database Internals" by Alex Petrov, Chapter 3: File Formats
  - PostgreSQL Documentation: Database File Layout
  - See also: page.go for the page structure
  - See also: buffer_pool.go for page caching
*/
package disk

import (
	"encoding/binary"
	"errors"
	"os"
	"sync"
)

// HeapFile manages a collection of pages stored in a file.
type HeapFile struct {
	file         *os.File
	mu           sync.RWMutex
	filePath     string
	pageCount    uint32
	freeListHead PageID
}

// File header constants
const (
	HeapFileMagic   uint32 = 0x464C5944 // "FLYD"
	HeapFileVersion uint32 = 1
	FileHeaderSize  int64  = PageSize
)

// Errors
var (
	ErrInvalidFile     = errors.New("invalid heap file")
	ErrVersionMismatch = errors.New("heap file version mismatch")
	ErrPageNotFound    = errors.New("page not found")
)

// CreateHeapFile creates a new heap file at the given path.
func CreateHeapFile(path string) (*HeapFile, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return nil, err
	}
	hf := &HeapFile{file: file, filePath: path, freeListHead: InvalidPageID}
	if err := hf.writeHeader(); err != nil {
		file.Close()
		os.Remove(path)
		return nil, err
	}
	return hf, nil
}

// OpenHeapFile opens an existing heap file.
func OpenHeapFile(path string) (*HeapFile, error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	hf := &HeapFile{file: file, filePath: path}
	if err := hf.readHeader(); err != nil {
		file.Close()
		return nil, err
	}
	return hf, nil
}

func (hf *HeapFile) writeHeader() error {
	header := make([]byte, FileHeaderSize)
	binary.BigEndian.PutUint32(header[0:4], HeapFileMagic)
	binary.BigEndian.PutUint32(header[4:8], HeapFileVersion)
	binary.BigEndian.PutUint32(header[8:12], hf.pageCount)
	binary.BigEndian.PutUint32(header[12:16], uint32(hf.freeListHead))
	_, err := hf.file.WriteAt(header, 0)
	return err
}

func (hf *HeapFile) readHeader() error {
	header := make([]byte, FileHeaderSize)
	if _, err := hf.file.ReadAt(header, 0); err != nil {
		return err
	}
	if binary.BigEndian.Uint32(header[0:4]) != HeapFileMagic {
		return ErrInvalidFile
	}
	if binary.BigEndian.Uint32(header[4:8]) != HeapFileVersion {
		return ErrVersionMismatch
	}
	hf.pageCount = binary.BigEndian.Uint32(header[8:12])
	hf.freeListHead = PageID(binary.BigEndian.Uint32(header[12:16]))
	return nil
}

// AllocatePage allocates a new page and returns its ID.
func (hf *HeapFile) AllocatePage() (PageID, error) {
	hf.mu.Lock()
	defer hf.mu.Unlock()

	if hf.freeListHead != InvalidPageID {
		pageID := hf.freeListHead
		page, err := hf.readPageLocked(pageID)
		if err != nil {
			return InvalidPageID, err
		}
		hf.freeListHead = page.Header().NextPageID
		hf.writeHeader()
		return pageID, nil
	}

	hf.pageCount++
	pageID := PageID(hf.pageCount)
	page := NewPage(pageID, PageTypeData)
	if err := hf.writePageLocked(page); err != nil {
		hf.pageCount--
		return InvalidPageID, err
	}
	hf.writeHeader()
	return pageID, nil
}

// FreePage adds a page to the free list.
func (hf *HeapFile) FreePage(pageID PageID) error {
	hf.mu.Lock()
	defer hf.mu.Unlock()
	page, err := hf.readPageLocked(pageID)
	if err != nil {
		return err
	}
	page.initHeader(pageID, PageTypeFree)
	h := page.Header()
	h.NextPageID = hf.freeListHead
	page.setHeader(h)
	if err := hf.writePageLocked(page); err != nil {
		return err
	}
	hf.freeListHead = pageID
	return hf.writeHeader()
}

func (hf *HeapFile) readPageLocked(pageID PageID) (*Page, error) {
	if pageID == InvalidPageID || uint32(pageID) > hf.pageCount {
		return nil, ErrPageNotFound
	}
	offset := hf.pageOffset(pageID)
	data := make([]byte, PageSize)
	if _, err := hf.file.ReadAt(data, offset); err != nil {
		return nil, err
	}
	page := &Page{}
	page.SetData(data)
	return page, nil
}

// WritePage writes a page to disk.
func (hf *HeapFile) WritePage(page *Page) error {
	hf.mu.Lock()
	defer hf.mu.Unlock()
	return hf.writePageLocked(page)
}

func (hf *HeapFile) writePageLocked(page *Page) error {
	pageID := page.PageID()
	if pageID == InvalidPageID {
		return ErrPageNotFound
	}
	offset := hf.pageOffset(pageID)
	if _, err := hf.file.WriteAt(page.Data(), offset); err != nil {
		return err
	}
	page.SetDirty(false)
	return nil
}

func (hf *HeapFile) pageOffset(pageID PageID) int64 {
	return FileHeaderSize + int64(pageID-1)*int64(PageSize)
}

// PageCount returns the number of allocated pages.
func (hf *HeapFile) PageCount() uint32 {
	hf.mu.RLock()
	defer hf.mu.RUnlock()
	return hf.pageCount
}

// Sync flushes all pending writes to disk.
func (hf *HeapFile) Sync() error {
	return hf.file.Sync()
}

// Close closes the heap file.
func (hf *HeapFile) Close() error {
	hf.mu.Lock()
	defer hf.mu.Unlock()
	if err := hf.writeHeader(); err != nil {
		return err
	}
	return hf.file.Close()
}

// FilePath returns the path to the heap file.
func (hf *HeapFile) FilePath() string {
	return hf.filePath
}

// ReadPage reads a page from disk.
func (hf *HeapFile) ReadPage(pageID PageID) (*Page, error) {
	hf.mu.RLock()
	defer hf.mu.RUnlock()
	return hf.readPageLocked(pageID)
}
