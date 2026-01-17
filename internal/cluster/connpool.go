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
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// ============================================================================
// Connection Pool Manager
// ============================================================================

// ConnectionPoolManager manages connection pools to all cluster nodes
type ConnectionPoolManager struct {
	pools map[string]*NodeConnectionPool
	mu    sync.RWMutex
	
	// Configuration
	maxIdlePerNode int
	maxOpenPerNode int
	idleTimeout    time.Duration
	dialTimeout    time.Duration
	
	// Statistics
	totalConnections uint64
	activeConnections uint64
	poolHits uint64
	poolMisses uint64
}

// NewConnectionPoolManager creates a new connection pool manager
func NewConnectionPoolManager(maxIdlePerNode, maxOpenPerNode int, 
	idleTimeout, dialTimeout time.Duration) *ConnectionPoolManager {
	
	return &ConnectionPoolManager{
		pools:          make(map[string]*NodeConnectionPool),
		maxIdlePerNode: maxIdlePerNode,
		maxOpenPerNode: maxOpenPerNode,
		idleTimeout:    idleTimeout,
		dialTimeout:    dialTimeout,
	}
}

// Get retrieves a connection to the specified node
func (cpm *ConnectionPoolManager) Get(nodeID string, addr string) (net.Conn, error) {
	pool := cpm.getOrCreatePool(nodeID, addr)
	
	conn, err := pool.Get()
	if err != nil {
		atomic.AddUint64(&cpm.poolMisses, 1)
		return nil, err
	}
	
	atomic.AddUint64(&cpm.poolHits, 1)
	atomic.AddUint64(&cpm.activeConnections, 1)
	
	return conn, nil
}

// Put returns a connection to the pool
func (cpm *ConnectionPoolManager) Put(nodeID string, conn net.Conn) error {
	cpm.mu.RLock()
	pool, exists := cpm.pools[nodeID]
	cpm.mu.RUnlock()
	
	if !exists {
		conn.Close()
		return fmt.Errorf("pool for node %s not found", nodeID)
	}
	
	atomic.AddUint64(&cpm.activeConnections, ^uint64(0)) // Decrement
	
	return pool.Put(conn)
}

// getOrCreatePool gets or creates a connection pool for a node
func (cpm *ConnectionPoolManager) getOrCreatePool(nodeID string, addr string) *NodeConnectionPool {
	cpm.mu.RLock()
	pool, exists := cpm.pools[nodeID]
	cpm.mu.RUnlock()
	
	if exists {
		return pool
	}
	
	cpm.mu.Lock()
	defer cpm.mu.Unlock()
	
	// Double-check after acquiring write lock
	if pool, exists := cpm.pools[nodeID]; exists {
		return pool
	}
	
	pool = NewNodeConnectionPool(nodeID, addr, cpm.maxIdlePerNode, 
		cpm.maxOpenPerNode, cpm.idleTimeout, cpm.dialTimeout)
	cpm.pools[nodeID] = pool
	
	return pool
}

// Close closes all connection pools
func (cpm *ConnectionPoolManager) Close() error {
	cpm.mu.Lock()
	defer cpm.mu.Unlock()
	
	for _, pool := range cpm.pools {
		pool.Close()
	}
	
	cpm.pools = make(map[string]*NodeConnectionPool)
	
	return nil
}

// GetStats returns connection pool statistics
func (cpm *ConnectionPoolManager) GetStats() map[string]interface{} {
	cpm.mu.RLock()
	defer cpm.mu.RUnlock()
	
	poolStats := make(map[string]interface{})
	for nodeID, pool := range cpm.pools {
		poolStats[nodeID] = pool.GetStats()
	}
	
	return map[string]interface{}{
		"total_connections":  atomic.LoadUint64(&cpm.totalConnections),
		"active_connections": atomic.LoadUint64(&cpm.activeConnections),
		"pool_hits":          atomic.LoadUint64(&cpm.poolHits),
		"pool_misses":        atomic.LoadUint64(&cpm.poolMisses),
		"pools":              poolStats,
	}
}

// ============================================================================
// Node Connection Pool
// ============================================================================

// NodeConnectionPool manages connections to a single node
type NodeConnectionPool struct {
	nodeID string
	addr   string
	
	// Connection pool
	idle    chan *pooledConn
	maxIdle int
	maxOpen int
	
	// Timeouts
	idleTimeout time.Duration
	dialTimeout time.Duration
	
	// Statistics
	numOpen   int32
	numIdle   int32
	waitCount int64
	
	mu     sync.Mutex
	closed bool
}

// pooledConn wraps a connection with metadata
type pooledConn struct {
	conn       net.Conn
	createdAt  time.Time
	lastUsedAt time.Time
}

// NewNodeConnectionPool creates a new connection pool for a node
func NewNodeConnectionPool(nodeID, addr string, maxIdle, maxOpen int, 
	idleTimeout, dialTimeout time.Duration) *NodeConnectionPool {
	
	return &NodeConnectionPool{
		nodeID:      nodeID,
		addr:        addr,
		idle:        make(chan *pooledConn, maxIdle),
		maxIdle:     maxIdle,
		maxOpen:     maxOpen,
		idleTimeout: idleTimeout,
		dialTimeout: dialTimeout,
	}
}

// Get retrieves a connection from the pool or creates a new one
func (ncp *NodeConnectionPool) Get() (net.Conn, error) {
	ncp.mu.Lock()
	if ncp.closed {
		ncp.mu.Unlock()
		return nil, fmt.Errorf("pool is closed")
	}
	ncp.mu.Unlock()
	
	// Try to get from idle pool
	select {
	case pc := <-ncp.idle:
		atomic.AddInt32(&ncp.numIdle, -1)
		
		// Check if connection is still valid
		if time.Since(pc.lastUsedAt) > ncp.idleTimeout {
			pc.conn.Close()
			atomic.AddInt32(&ncp.numOpen, -1)
			return ncp.createConnection()
		}
		
		pc.lastUsedAt = time.Now()
		return pc.conn, nil
		
	default:
		// No idle connections, create new one
		return ncp.createConnection()
	}
}

// createConnection creates a new connection
func (ncp *NodeConnectionPool) createConnection() (net.Conn, error) {
	// Check if we've reached max open connections
	if atomic.LoadInt32(&ncp.numOpen) >= int32(ncp.maxOpen) {
		atomic.AddInt64(&ncp.waitCount, 1)
		
		// Wait for an idle connection with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		
		select {
		case pc := <-ncp.idle:
			atomic.AddInt32(&ncp.numIdle, -1)
			pc.lastUsedAt = time.Now()
			return pc.conn, nil
		case <-ctx.Done():
			return nil, fmt.Errorf("connection pool exhausted")
		}
	}
	
	// Create new connection
	conn, err := net.DialTimeout("tcp", ncp.addr, ncp.dialTimeout)
	if err != nil {
		return nil, fmt.Errorf("failed to dial %s: %w", ncp.addr, err)
	}
	
	atomic.AddInt32(&ncp.numOpen, 1)
	
	return conn, nil
}

// Put returns a connection to the pool
func (ncp *NodeConnectionPool) Put(conn net.Conn) error {
	ncp.mu.Lock()
	if ncp.closed {
		ncp.mu.Unlock()
		conn.Close()
		atomic.AddInt32(&ncp.numOpen, -1)
		return nil
	}
	ncp.mu.Unlock()
	
	pc := &pooledConn{
		conn:       conn,
		lastUsedAt: time.Now(),
	}
	
	// Try to return to idle pool
	select {
	case ncp.idle <- pc:
		atomic.AddInt32(&ncp.numIdle, 1)
		return nil
	default:
		// Pool is full, close connection
		conn.Close()
		atomic.AddInt32(&ncp.numOpen, -1)
		return nil
	}
}

// Close closes all connections in the pool
func (ncp *NodeConnectionPool) Close() error {
	ncp.mu.Lock()
	defer ncp.mu.Unlock()
	
	if ncp.closed {
		return nil
	}
	
	ncp.closed = true
	close(ncp.idle)
	
	// Close all idle connections
	for pc := range ncp.idle {
		pc.conn.Close()
		atomic.AddInt32(&ncp.numOpen, -1)
		atomic.AddInt32(&ncp.numIdle, -1)
	}
	
	return nil
}

// GetStats returns pool statistics
func (ncp *NodeConnectionPool) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"node_id":    ncp.nodeID,
		"addr":       ncp.addr,
		"num_open":   atomic.LoadInt32(&ncp.numOpen),
		"num_idle":   atomic.LoadInt32(&ncp.numIdle),
		"wait_count": atomic.LoadInt64(&ncp.waitCount),
		"max_idle":   ncp.maxIdle,
		"max_open":   ncp.maxOpen,
	}
}
