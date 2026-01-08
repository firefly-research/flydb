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
Package disk implements disk-based storage for FlyDB using a page-oriented
architecture similar to PostgreSQL and other production databases.

Page-Based Storage Overview:
============================

Database systems organize data into fixed-size units called "pages" (also known
as "blocks" in some systems). This design provides several key benefits:

 1. Efficient I/O: Disk I/O is performed in page-sized chunks, matching the
    operating system's block size for optimal performance.

 2. Buffer Pool Integration: Pages can be cached in memory and managed by
    the buffer pool, reducing disk I/O for frequently accessed data.

 3. Crash Recovery: Pages provide natural boundaries for write-ahead logging
    and recovery operations.

 4. Concurrency: Page-level locking enables fine-grained concurrency control.

Slotted Page Architecture:
==========================

FlyDB uses a "slotted page" layout, which is the standard approach used by
PostgreSQL, MySQL InnoDB, and most modern databases. This design efficiently
handles variable-length records.

Page Layout:

	┌─────────────────────────────────────────────────────────────────┐
	│                    Page Header (24 bytes)                       │
	│  [PageID | Type | Flags | SlotCount | FreeStart | FreeEnd | ...]│
	├─────────────────────────────────────────────────────────────────┤
	│  Slot Array (grows →)                                           │
	│  [Slot 0: offset,len] [Slot 1: offset,len] [Slot 2: offset,len] │
	├─────────────────────────────────────────────────────────────────┤
	│                                                                 │
	│                    Free Space                                   │
	│                                                                 │
	├─────────────────────────────────────────────────────────────────┤
	│  Record Data (← grows)                                          │
	│  [Record 2 data] [Record 1 data] [Record 0 data]                │
	└─────────────────────────────────────────────────────────────────┘

Key Design Decisions:

  - Slot Array grows forward from the header
  - Record Data grows backward from the end of the page
  - Free space is in the middle, allowing both to grow independently
  - Deleted records leave "holes" that can be reclaimed via compaction

Why Slotted Pages?

 1. Variable-Length Records: Unlike fixed-size record layouts, slotted pages
    efficiently store records of varying sizes without wasting space.

 2. Record Relocation: Records can be moved within a page (for compaction)
    without changing their slot number, preserving external references.

 3. Efficient Deletion: Deleting a record only requires marking its slot as
    invalid, without immediately reclaiming space.

Page Size Selection:
====================

FlyDB uses 8KB pages, matching PostgreSQL's default. This size balances:

  - I/O Efficiency: Larger pages reduce the number of I/O operations
  - Memory Efficiency: Smaller pages reduce memory waste for sparse data
  - Index Fanout: Larger pages allow more keys per B-tree node

Common page sizes in production databases:
  - PostgreSQL: 8KB (default)
  - MySQL InnoDB: 16KB (default)
  - Oracle: 8KB (default)
  - SQLite: 4KB (default)

References:
===========

  - "Database Internals" by Alex Petrov, Chapter 3: File Formats
  - PostgreSQL Documentation: Database Page Layout
  - See also: docs/architecture.md for FlyDB's overall storage design
*/
package disk

import (
	"encoding/binary"
	"errors"
)

// Page size constants
const (
	// PageSize is the size of each page in bytes (8KB like PostgreSQL).
	PageSize = 8192

	// PageHeaderSize is the size of the page header in bytes.
	PageHeaderSize = 24

	// SlotSize is the size of each slot entry in the slot array.
	SlotSize = 4 // 2 bytes offset + 2 bytes length
)

// Page types
const (
	PageTypeData  byte = 1 // Data page containing key-value records
	PageTypeFree  byte = 2 // Free page (available for allocation)
	PageTypeIndex byte = 3 // Index page (for future B-tree index)
	PageTypeMeta  byte = 4 // Metadata page
)

// PageID is a unique identifier for a page.
type PageID uint32

// InvalidPageID represents an invalid page ID.
const InvalidPageID PageID = 0

// Errors
var (
	ErrPageFull       = errors.New("page is full")
	ErrSlotNotFound   = errors.New("slot not found")
	ErrInvalidSlot    = errors.New("invalid slot")
	ErrRecordTooLarge = errors.New("record too large for page")
)

// SlotEntry represents a slot in the slot array.
type SlotEntry struct {
	Offset uint16 // Offset of record in page (0 = deleted)
	Length uint16 // Length of record
}

// PageHeader contains metadata about a page.
// Layout (24 bytes):
//   - PageID (4 bytes): Unique page identifier
//   - PageType (1 byte): Type of page (data, free, index, meta)
//   - Flags (1 byte): Page flags (dirty, pinned, etc.)
//   - SlotCount (2 bytes): Number of slots in the page
//   - FreeSpaceStart (2 bytes): Offset where free space starts
//   - FreeSpaceEnd (2 bytes): Offset where free space ends
//   - NextPageID (4 bytes): Next page in chain (for overflow)
//   - PrevPageID (4 bytes): Previous page in chain
//   - LSN (4 bytes): Log Sequence Number for recovery
type PageHeader struct {
	PageID         PageID
	PageType       byte
	Flags          byte
	SlotCount      uint16
	FreeSpaceStart uint16 // End of slot array
	FreeSpaceEnd   uint16 // Start of record data (grows down)
	NextPageID     PageID
	PrevPageID     PageID
	LSN            uint32
}

// Page represents an 8KB page in the database.
// Uses slotted page layout:
//
//	┌────────────────────────────────────────────────────────────┐
//	│                    Page Header (24 bytes)                  │
//	├────────────────────────────────────────────────────────────┤
//	│  Slot 0  │  Slot 1  │  Slot 2  │  ...  │  Free Space  →    │
//	├────────────────────────────────────────────────────────────┤
//	│                                                            │
//	│                      Free Space                            │
//	│                                                            │
//	├────────────────────────────────────────────────────────────┤
//	│  ← Record N  │  Record N-1  │  ...  │  Record 1  │ Record 0│
//	└────────────────────────────────────────────────────────────┘
type Page struct {
	data   [PageSize]byte
	dirty  bool
	pinned int // Pin count for buffer pool
}

// NewPage creates a new empty page with the given ID and type.
func NewPage(id PageID, pageType byte) *Page {
	p := &Page{}
	p.initHeader(id, pageType)
	return p
}

// initHeader initializes the page header.
func (p *Page) initHeader(id PageID, pageType byte) {
	header := PageHeader{
		PageID:         id,
		PageType:       pageType,
		SlotCount:      0,
		FreeSpaceStart: PageHeaderSize,
		FreeSpaceEnd:   PageSize,
	}
	p.setHeader(header)
}

// Header returns the page header.
func (p *Page) Header() PageHeader {
	return PageHeader{
		PageID:         PageID(binary.BigEndian.Uint32(p.data[0:4])),
		PageType:       p.data[4],
		Flags:          p.data[5],
		SlotCount:      binary.BigEndian.Uint16(p.data[6:8]),
		FreeSpaceStart: binary.BigEndian.Uint16(p.data[8:10]),
		FreeSpaceEnd:   binary.BigEndian.Uint16(p.data[10:12]),
		NextPageID:     PageID(binary.BigEndian.Uint32(p.data[12:16])),
		PrevPageID:     PageID(binary.BigEndian.Uint32(p.data[16:20])),
		LSN:            binary.BigEndian.Uint32(p.data[20:24]),
	}
}

// setHeader writes the page header to the page data.
func (p *Page) setHeader(h PageHeader) {
	binary.BigEndian.PutUint32(p.data[0:4], uint32(h.PageID))
	p.data[4] = h.PageType
	p.data[5] = h.Flags
	binary.BigEndian.PutUint16(p.data[6:8], h.SlotCount)
	binary.BigEndian.PutUint16(p.data[8:10], h.FreeSpaceStart)
	binary.BigEndian.PutUint16(p.data[10:12], h.FreeSpaceEnd)
	binary.BigEndian.PutUint32(p.data[12:16], uint32(h.NextPageID))
	binary.BigEndian.PutUint32(p.data[16:20], uint32(h.PrevPageID))
	binary.BigEndian.PutUint32(p.data[20:24], h.LSN)
}

// IsDirty returns true if the page has been modified.
func (p *Page) IsDirty() bool {
	return p.dirty
}

// SetDirty marks the page as dirty.
func (p *Page) SetDirty(dirty bool) {
	p.dirty = dirty
}

// PinCount returns the current pin count.
func (p *Page) PinCount() int {
	return p.pinned
}

// Pin increments the pin count.
func (p *Page) Pin() {
	p.pinned++
}

// Unpin decrements the pin count.
func (p *Page) Unpin() {
	if p.pinned > 0 {
		p.pinned--
	}
}

// InsertRecord inserts a record into the page and returns the slot number.
// Records are stored from the end of the page growing towards the header.
// Slots are stored after the header growing towards the records.
func (p *Page) InsertRecord(record []byte) (uint16, error) {
	recordLen := len(record)
	if recordLen > PageSize-PageHeaderSize-SlotSize {
		return 0, ErrRecordTooLarge
	}

	if p.FreeSpace() < recordLen {
		return 0, ErrPageFull
	}

	h := p.Header()

	// Calculate new record position (grows down from end)
	newFreeSpaceEnd := h.FreeSpaceEnd - uint16(recordLen)

	// Copy record data
	copy(p.data[newFreeSpaceEnd:h.FreeSpaceEnd], record)

	// Add slot entry (offset and length)
	slotOffset := h.FreeSpaceStart
	binary.BigEndian.PutUint16(p.data[slotOffset:slotOffset+2], newFreeSpaceEnd)
	binary.BigEndian.PutUint16(p.data[slotOffset+2:slotOffset+4], uint16(recordLen))

	// Update header
	h.SlotCount++
	h.FreeSpaceStart += SlotSize
	h.FreeSpaceEnd = newFreeSpaceEnd
	p.setHeader(h)
	p.dirty = true

	return h.SlotCount - 1, nil
}

// GetSlot returns the slot entry for a given slot number.
func (p *Page) GetSlot(slotNum uint16) SlotEntry {
	h := p.Header()
	if slotNum >= h.SlotCount {
		return SlotEntry{}
	}
	slotOffset := PageHeaderSize + uint16(slotNum)*SlotSize
	return SlotEntry{
		Offset: binary.BigEndian.Uint16(p.data[slotOffset : slotOffset+2]),
		Length: binary.BigEndian.Uint16(p.data[slotOffset+2 : slotOffset+4]),
	}
}

// GetRecord retrieves a record by slot number.
func (p *Page) GetRecord(slotNum uint16) ([]byte, error) {
	h := p.Header()
	if slotNum >= h.SlotCount {
		return nil, ErrSlotNotFound
	}

	// Read slot entry
	slotOffset := PageHeaderSize + uint16(slotNum)*SlotSize
	recordOffset := binary.BigEndian.Uint16(p.data[slotOffset : slotOffset+2])
	recordLen := binary.BigEndian.Uint16(p.data[slotOffset+2 : slotOffset+4])

	// Check for deleted slot (offset = 0)
	if recordOffset == 0 && recordLen == 0 {
		return nil, ErrSlotNotFound
	}

	// Copy and return record
	record := make([]byte, recordLen)
	copy(record, p.data[recordOffset:recordOffset+recordLen])
	return record, nil
}

// DeleteRecord marks a slot as deleted.
// The space is not immediately reclaimed (requires compaction).
func (p *Page) DeleteRecord(slotNum uint16) error {
	h := p.Header()
	if slotNum >= h.SlotCount {
		return ErrSlotNotFound
	}

	// Mark slot as deleted by setting offset and length to 0
	slotOffset := PageHeaderSize + uint16(slotNum)*SlotSize
	binary.BigEndian.PutUint16(p.data[slotOffset:slotOffset+2], 0)
	binary.BigEndian.PutUint16(p.data[slotOffset+2:slotOffset+4], 0)
	p.dirty = true

	return nil
}

// UpdateRecord updates a record in place if it fits, otherwise returns ErrPageFull.
func (p *Page) UpdateRecord(slotNum uint16, record []byte) error {
	h := p.Header()
	if slotNum >= h.SlotCount {
		return ErrSlotNotFound
	}

	slotOffset := PageHeaderSize + uint16(slotNum)*SlotSize
	oldOffset := binary.BigEndian.Uint16(p.data[slotOffset : slotOffset+2])
	oldLen := binary.BigEndian.Uint16(p.data[slotOffset+2 : slotOffset+4])

	// Check for deleted slot
	if oldOffset == 0 && oldLen == 0 {
		return ErrSlotNotFound
	}

	// If new record fits in old space, update in place
	if uint16(len(record)) <= oldLen {
		copy(p.data[oldOffset:], record)
		binary.BigEndian.PutUint16(p.data[slotOffset+2:slotOffset+4], uint16(len(record)))
		p.dirty = true
		return nil
	}

	// New record is larger - need to allocate new space
	if p.FreeSpace() < len(record)-int(oldLen) {
		return ErrPageFull
	}

	// Mark old slot as deleted and insert new record
	binary.BigEndian.PutUint16(p.data[slotOffset:slotOffset+2], 0)
	binary.BigEndian.PutUint16(p.data[slotOffset+2:slotOffset+4], 0)

	// Allocate new space at end
	newFreeSpaceEnd := h.FreeSpaceEnd - uint16(len(record))
	copy(p.data[newFreeSpaceEnd:h.FreeSpaceEnd], record)

	// Update slot to point to new location
	binary.BigEndian.PutUint16(p.data[slotOffset:slotOffset+2], newFreeSpaceEnd)
	binary.BigEndian.PutUint16(p.data[slotOffset+2:slotOffset+4], uint16(len(record)))

	// Update header
	h.FreeSpaceEnd = newFreeSpaceEnd
	p.setHeader(h)
	p.dirty = true

	return nil
}

// SlotCount returns the number of slots in the page.
func (p *Page) SlotCount() uint16 {
	return p.Header().SlotCount
}

// PageID returns the page ID.
func (p *Page) PageID() PageID {
	return p.Header().PageID
}
func (p *Page) FreeSpace() int {
	h := p.Header()
	return int(h.FreeSpaceEnd) - int(h.FreeSpaceStart) - SlotSize
}

// Data returns the raw page data.
func (p *Page) Data() []byte {
	return p.data[:]
}

// SetData sets the raw page data from a byte slice.
func (p *Page) SetData(data []byte) {
	copy(p.data[:], data)
}
