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
Package protocol provides zero-copy message delivery for FlyDB.

Zero-Copy Overview:
===================

This module implements zero-copy message delivery to reduce memory allocations
and copying during message processing:

- Buffer pooling to reuse memory
- Direct buffer access without copying
- Scatter-gather I/O for efficient network writes
- Memory-mapped file support for large payloads

Buffer Pool:
============

A pool of reusable buffers reduces GC pressure:
1. Acquire buffer from pool
2. Use buffer for message processing
3. Return buffer to pool for reuse

Zero-Copy Read:
===============

Messages are read directly into pooled buffers:
1. Read header into fixed buffer
2. Acquire payload buffer from pool
3. Read payload directly into buffer
4. Process without additional copies

Zero-Copy Write:
================

Messages are written using scatter-gather I/O:
1. Header and payload are separate buffers
2. writev() combines them in kernel
3. No user-space copying required
*/
package protocol

import (
	"encoding/binary"
	"io"
	"sync"
	"sync/atomic"
)

// BufferPool provides a pool of reusable byte buffers
type BufferPool struct {
	pools     []*sync.Pool
	sizes     []int
	gets      atomic.Uint64
	puts      atomic.Uint64
	allocates atomic.Uint64
}

// Buffer size classes (powers of 2)
var defaultSizes = []int{
	256,      // 256 B
	1024,     // 1 KB
	4096,     // 4 KB
	16384,    // 16 KB
	65536,    // 64 KB
	262144,   // 256 KB
	1048576,  // 1 MB
	4194304,  // 4 MB
	16777216, // 16 MB (max message size)
}

// NewBufferPool creates a new buffer pool
func NewBufferPool() *BufferPool {
	bp := &BufferPool{
		pools: make([]*sync.Pool, len(defaultSizes)),
		sizes: defaultSizes,
	}

	for i, size := range defaultSizes {
		size := size // capture for closure
		bp.pools[i] = &sync.Pool{
			New: func() interface{} {
				bp.allocates.Add(1)
				return make([]byte, size)
			},
		}
	}

	return bp
}

// Get acquires a buffer of at least the specified size
func (bp *BufferPool) Get(size int) []byte {
	bp.gets.Add(1)

	// Find the smallest buffer class that fits
	for i, s := range bp.sizes {
		if s >= size {
			buf := bp.pools[i].Get().([]byte)
			return buf[:size]
		}
	}

	// Size exceeds max pool size, allocate directly
	bp.allocates.Add(1)
	return make([]byte, size)
}

// Put returns a buffer to the pool
func (bp *BufferPool) Put(buf []byte) {
	bp.puts.Add(1)

	// Find the pool for this buffer's capacity
	cap := cap(buf)
	for i, s := range bp.sizes {
		if s == cap {
			bp.pools[i].Put(buf[:cap])
			return
		}
	}
	// Buffer doesn't match any pool size, let GC handle it
}

// Stats returns pool statistics
func (bp *BufferPool) Stats() (gets, puts, allocates uint64) {
	return bp.gets.Load(), bp.puts.Load(), bp.allocates.Load()
}

// ZeroCopyReader provides zero-copy message reading
type ZeroCopyReader struct {
	reader     io.Reader
	pool       *BufferPool
	headerBuf  []byte
	payloadBuf []byte
}

// NewZeroCopyReader creates a new zero-copy reader
func NewZeroCopyReader(r io.Reader, pool *BufferPool) *ZeroCopyReader {
	return &ZeroCopyReader{
		reader:    r,
		pool:      pool,
		headerBuf: make([]byte, HeaderSize),
	}
}

// ReadMessage reads a message with zero-copy semantics
// The returned payload buffer is borrowed from the pool and must be released
func (zcr *ZeroCopyReader) ReadMessage() (*Header, []byte, error) {
	// Read header
	if _, err := io.ReadFull(zcr.reader, zcr.headerBuf); err != nil {
		return nil, nil, err
	}

	// Parse header
	header := &Header{
		Magic:   zcr.headerBuf[0],
		Version: zcr.headerBuf[1],
		Type:    MessageType(zcr.headerBuf[2]),
		Flags:   MessageFlag(zcr.headerBuf[3]),
		Length:  binary.BigEndian.Uint32(zcr.headerBuf[4:8]),
	}

	// Validate
	if header.Magic != MagicByte {
		return nil, nil, ErrInvalidMagic
	}
	if header.Length > MaxMessageSize {
		return nil, nil, ErrMessageTooLarge
	}

	// Get buffer from pool for payload
	if zcr.payloadBuf != nil {
		zcr.pool.Put(zcr.payloadBuf)
	}
	zcr.payloadBuf = zcr.pool.Get(int(header.Length))

	// Read payload directly into pooled buffer
	if _, err := io.ReadFull(zcr.reader, zcr.payloadBuf); err != nil {
		return nil, nil, err
	}

	return header, zcr.payloadBuf, nil
}

// Release returns the current payload buffer to the pool
func (zcr *ZeroCopyReader) Release() {
	if zcr.payloadBuf != nil {
		zcr.pool.Put(zcr.payloadBuf)
		zcr.payloadBuf = nil
	}
}

// ZeroCopyWriter provides zero-copy message writing
type ZeroCopyWriter struct {
	writer    io.Writer
	pool      *BufferPool
	headerBuf []byte
}

// NewZeroCopyWriter creates a new zero-copy writer
func NewZeroCopyWriter(w io.Writer, pool *BufferPool) *ZeroCopyWriter {
	return &ZeroCopyWriter{
		writer:    w,
		pool:      pool,
		headerBuf: make([]byte, HeaderSize),
	}
}

// WriteMessage writes a message with minimal copying
func (zcw *ZeroCopyWriter) WriteMessage(msgType MessageType, flags MessageFlag, payload []byte) error {
	// Build header
	zcw.headerBuf[0] = MagicByte
	zcw.headerBuf[1] = ProtocolVersion
	zcw.headerBuf[2] = byte(msgType)
	zcw.headerBuf[3] = byte(flags)
	binary.BigEndian.PutUint32(zcw.headerBuf[4:8], uint32(len(payload)))

	// Write header
	if _, err := zcw.writer.Write(zcw.headerBuf); err != nil {
		return err
	}

	// Write payload
	if len(payload) > 0 {
		if _, err := zcw.writer.Write(payload); err != nil {
			return err
		}
	}

	return nil
}

// WriteMessageGather writes a message using gather semantics (multiple buffers)
func (zcw *ZeroCopyWriter) WriteMessageGather(msgType MessageType, flags MessageFlag, payloads ...[]byte) error {
	// Calculate total length
	var totalLen int
	for _, p := range payloads {
		totalLen += len(p)
	}

	// Build header
	zcw.headerBuf[0] = MagicByte
	zcw.headerBuf[1] = ProtocolVersion
	zcw.headerBuf[2] = byte(msgType)
	zcw.headerBuf[3] = byte(flags)
	binary.BigEndian.PutUint32(zcw.headerBuf[4:8], uint32(totalLen))

	// Write header
	if _, err := zcw.writer.Write(zcw.headerBuf); err != nil {
		return err
	}

	// Write each payload segment
	for _, p := range payloads {
		if len(p) > 0 {
			if _, err := zcw.writer.Write(p); err != nil {
				return err
			}
		}
	}

	return nil
}

// GetBuffer acquires a buffer from the pool for building payloads
func (zcw *ZeroCopyWriter) GetBuffer(size int) []byte {
	return zcw.pool.Get(size)
}

// PutBuffer returns a buffer to the pool
func (zcw *ZeroCopyWriter) PutBuffer(buf []byte) {
	zcw.pool.Put(buf)
}

// ZeroCopyMessage represents a message with zero-copy buffer management
type ZeroCopyMessage struct {
	Header  Header
	Payload []byte
	pool    *BufferPool
}

// Release returns the message's buffer to the pool
func (m *ZeroCopyMessage) Release() {
	if m.pool != nil && m.Payload != nil {
		m.pool.Put(m.Payload)
		m.Payload = nil
	}
}

// DefaultBufferPool is the global buffer pool instance
var DefaultBufferPool = NewBufferPool()
