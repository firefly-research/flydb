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

package cluster

import (
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"syscall"
)

// ============================================================================
// Zero-Copy I/O Manager
// ============================================================================

// ZeroCopyManager manages zero-copy data transfers for partition migration
type ZeroCopyManager struct {
	// Buffer pool for non-zero-copy fallback
	bufferPool *BufferPool
	
	// Statistics
	bytesSent     uint64
	bytesReceived uint64
	transferCount uint64
	
	mu sync.RWMutex
}

// NewZeroCopyManager creates a new zero-copy manager
func NewZeroCopyManager() *ZeroCopyManager {
	return &ZeroCopyManager{
		bufferPool: NewBufferPool(),
	}
}

// ============================================================================
// Buffer Pool for Efficient Memory Management
// ============================================================================

// BufferPool manages a pool of reusable buffers
type BufferPool struct {
	pools map[int]*sync.Pool // size -> pool
	mu    sync.RWMutex
}

// NewBufferPool creates a new buffer pool
func NewBufferPool() *BufferPool {
	bp := &BufferPool{
		pools: make(map[int]*sync.Pool),
	}
	
	// Pre-create pools for common sizes
	commonSizes := []int{
		4 * 1024,      // 4KB
		64 * 1024,     // 64KB
		1024 * 1024,   // 1MB
		4 * 1024 * 1024, // 4MB
	}
	
	for _, size := range commonSizes {
		bp.createPool(size)
	}
	
	return bp
}

// Get retrieves a buffer of the specified size
func (bp *BufferPool) Get(size int) []byte {
	bp.mu.RLock()
	pool, exists := bp.pools[size]
	bp.mu.RUnlock()
	
	if !exists {
		bp.createPool(size)
		bp.mu.RLock()
		pool = bp.pools[size]
		bp.mu.RUnlock()
	}
	
	buf := pool.Get().([]byte)
	return buf[:size] // Ensure correct size
}

// Put returns a buffer to the pool
func (bp *BufferPool) Put(buf []byte) {
	size := cap(buf)
	
	bp.mu.RLock()
	pool, exists := bp.pools[size]
	bp.mu.RUnlock()
	
	if exists {
		pool.Put(buf)
	}
}

// createPool creates a new pool for a specific buffer size
func (bp *BufferPool) createPool(size int) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	
	if _, exists := bp.pools[size]; exists {
		return
	}
	
	bp.pools[size] = &sync.Pool{
		New: func() interface{} {
			return make([]byte, size)
		},
	}
}

// ============================================================================
// Zero-Copy Transfer Functions
// ============================================================================

// SendFile transfers a file to a network connection using zero-copy
// Uses sendfile() on Linux for optimal performance
func (zcm *ZeroCopyManager) SendFile(conn net.Conn, filePath string) (int64, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return 0, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()
	
	fileInfo, err := file.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to stat file: %w", err)
	}
	
	fileSize := fileInfo.Size()
	
	// Try zero-copy sendfile
	bytesSent, err := zcm.sendFileZeroCopy(conn, file, fileSize)
	if err != nil {
		// Fallback to regular copy
		return zcm.sendFileRegular(conn, file, fileSize)
	}
	
	zcm.mu.Lock()
	zcm.bytesSent += uint64(bytesSent)
	zcm.transferCount++
	zcm.mu.Unlock()
	
	return bytesSent, nil
}

// sendFileZeroCopy uses platform-specific zero-copy mechanisms
func (zcm *ZeroCopyManager) sendFileZeroCopy(conn net.Conn, file *os.File, size int64) (int64, error) {
	// Get the underlying file descriptor
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return 0, fmt.Errorf("not a TCP connection")
	}
	
	// Get raw connection
	rawConn, err := tcpConn.SyscallConn()
	if err != nil {
		return 0, fmt.Errorf("failed to get raw connection: %w", err)
	}
	
	var bytesSent int64
	var sendErr error
	
	// Use sendfile syscall
	err = rawConn.Control(func(fd uintptr) {
		bytesSent, sendErr = zcm.sendfileImpl(int(fd), int(file.Fd()), size)
	})
	
	if err != nil {
		return 0, err
	}
	
	return bytesSent, sendErr
}
