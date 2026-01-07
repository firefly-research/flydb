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
Package pool provides a connection pool for FlyDB clients.

Connection Pool Overview:
=========================

The connection pool manages a set of reusable TCP connections to a FlyDB server.
This improves performance by avoiding the overhead of establishing new connections
for each query.

Features:
=========

  - Configurable pool size (min/max connections)
  - Connection health checking
  - Automatic connection recovery
  - Thread-safe connection acquisition and release
  - Connection timeout support
  - Idle connection cleanup

Usage Example:
==============

	pool, err := pool.New(pool.Config{
		Address:     "localhost:8888",
		MinConns:    2,
		MaxConns:    10,
		IdleTimeout: 5 * time.Minute,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	conn, err := pool.Get()
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Put(conn)

	// Use conn for queries...
*/
package pool

import (
	"bufio"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"flydb/internal/logging"
)

// Package-level logger for the pool component.
var log = logging.NewLogger("pool")

// Config holds the configuration for a connection pool.
type Config struct {
	// Address is the FlyDB server address (e.g., "localhost:8888").
	Address string

	// MinConns is the minimum number of connections to maintain.
	// The pool will try to keep at least this many connections open.
	MinConns int

	// MaxConns is the maximum number of connections allowed.
	// Get() will block if all connections are in use.
	MaxConns int

	// IdleTimeout is how long a connection can be idle before being closed.
	// Set to 0 to disable idle timeout.
	IdleTimeout time.Duration

	// ConnectTimeout is the timeout for establishing new connections.
	ConnectTimeout time.Duration

	// Username and Password for automatic authentication.
	Username string
	Password string

	// TLSConfig is the TLS configuration for encrypted connections.
	// If nil, connections are unencrypted.
	TLSConfig *tls.Config
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig(address string) Config {
	return Config{
		Address:        address,
		MinConns:       2,
		MaxConns:       10,
		IdleTimeout:    5 * time.Minute,
		ConnectTimeout: 10 * time.Second,
	}
}

// PooledConn represents a connection managed by the pool.
type PooledConn struct {
	conn       net.Conn
	reader     *bufio.Reader
	lastUsed   time.Time
	pool       *Pool
	inUse      bool
	createdAt  time.Time
}

// Send sends a command to the server and returns the response.
func (pc *PooledConn) Send(command string) (string, error) {
	_, err := pc.conn.Write([]byte(command + "\n"))
	if err != nil {
		return "", err
	}

	response, err := pc.reader.ReadString('\n')
	if err != nil {
		return "", err
	}

	// Trim the trailing newline
	if len(response) > 0 && response[len(response)-1] == '\n' {
		response = response[:len(response)-1]
	}

	return response, nil
}

// Query executes a SQL query and returns the result.
func (pc *PooledConn) Query(sql string) (string, error) {
	return pc.Send("SQL " + sql)
}

// Ping checks if the connection is still alive.
func (pc *PooledConn) Ping() error {
	response, err := pc.Send("PING")
	if err != nil {
		return err
	}
	if response != "PONG" {
		return errors.New("unexpected ping response: " + response)
	}
	return nil
}

// Close returns the connection to the pool.
// Use this instead of closing the underlying connection directly.
func (pc *PooledConn) Close() {
	pc.pool.Put(pc)
}

// Pool manages a pool of connections to a FlyDB server.
type Pool struct {
	config Config

	// mu protects all pool state
	mu sync.Mutex

	// conns holds idle connections ready for use
	conns []*PooledConn

	// numOpen is the total number of open connections (idle + in use)
	numOpen int

	// closed indicates if the pool has been closed
	closed bool

	// waiters is a channel for goroutines waiting for a connection
	waiters chan struct{}
}

// New creates a new connection pool with the given configuration.
func New(config Config) (*Pool, error) {
	log.Info("Creating connection pool",
		"address", config.Address,
		"min_conns", config.MinConns,
		"max_conns", config.MaxConns,
		"idle_timeout", config.IdleTimeout,
	)

	if config.MaxConns <= 0 {
		config.MaxConns = 10
	}
	if config.MinConns < 0 {
		config.MinConns = 0
	}
	if config.MinConns > config.MaxConns {
		config.MinConns = config.MaxConns
	}
	if config.ConnectTimeout <= 0 {
		config.ConnectTimeout = 10 * time.Second
	}

	p := &Pool{
		config:  config,
		conns:   make([]*PooledConn, 0, config.MaxConns),
		waiters: make(chan struct{}, config.MaxConns),
	}

	// Pre-create minimum connections
	log.Debug("Pre-creating minimum connections", "count", config.MinConns)
	for i := 0; i < config.MinConns; i++ {
		conn, err := p.createConn()
		if err != nil {
			log.Error("Failed to create initial connection",
				"error", err,
				"connection_index", i,
			)
			// Close any connections we've already created
			p.Close()
			return nil, fmt.Errorf("failed to create initial connection: %w", err)
		}
		p.conns = append(p.conns, conn)
		log.Debug("Initial connection created",
			"connection_index", i,
			"total_connections", len(p.conns),
		)
	}

	// Start idle connection cleanup goroutine
	if config.IdleTimeout > 0 {
		log.Debug("Starting idle connection cleanup goroutine",
			"idle_timeout", config.IdleTimeout,
		)
		go p.cleanupIdleConns()
	}

	log.Info("Connection pool created successfully",
		"initial_connections", len(p.conns),
		"max_connections", config.MaxConns,
	)

	return p, nil
}

// createConn creates a new connection to the server.
func (p *Pool) createConn() (*PooledConn, error) {
	var conn net.Conn
	var err error

	log.Debug("Creating new connection",
		"address", p.config.Address,
		"tls_enabled", p.config.TLSConfig != nil,
	)

	dialer := net.Dialer{Timeout: p.config.ConnectTimeout}

	if p.config.TLSConfig != nil {
		// Use TLS connection
		conn, err = tls.DialWithDialer(&dialer, "tcp", p.config.Address, p.config.TLSConfig)
	} else {
		// Use plain TCP connection
		conn, err = dialer.Dial("tcp", p.config.Address)
	}
	if err != nil {
		log.Warn("Failed to establish connection",
			"address", p.config.Address,
			"error", err,
		)
		return nil, err
	}

	log.Debug("Connection established",
		"address", p.config.Address,
		"local_addr", conn.LocalAddr().String(),
		"remote_addr", conn.RemoteAddr().String(),
	)

	pc := &PooledConn{
		conn:      conn,
		reader:    bufio.NewReader(conn),
		lastUsed:  time.Now(),
		pool:      p,
		createdAt: time.Now(),
	}

	// Authenticate if credentials are provided
	if p.config.Username != "" {
		log.Debug("Authenticating connection", "username", p.config.Username)
		response, err := pc.Send(fmt.Sprintf("AUTH %s %s", p.config.Username, p.config.Password))
		if err != nil {
			log.Warn("Connection authentication failed",
				"username", p.config.Username,
				"error", err,
			)
			conn.Close()
			return nil, fmt.Errorf("authentication failed: %w", err)
		}
		if response != "AUTH OK" && response != "AUTH OK (admin)" {
			log.Warn("Connection authentication rejected",
				"username", p.config.Username,
				"response", response,
			)
			conn.Close()
			return nil, errors.New("authentication failed: " + response)
		}
		log.Debug("Connection authenticated successfully", "username", p.config.Username)
	}

	p.numOpen++
	log.Info("New connection added to pool",
		"total_open", p.numOpen,
		"max_conns", p.config.MaxConns,
	)
	return pc, nil
}

// Get acquires a connection from the pool.
// If no connections are available and the pool is at capacity, it blocks.
func (p *Pool) Get() (*PooledConn, error) {
	p.mu.Lock()

	if p.closed {
		log.Debug("Attempted to get connection from closed pool")
		p.mu.Unlock()
		return nil, errors.New("pool is closed")
	}

	log.Debug("Acquiring connection from pool",
		"idle_connections", len(p.conns),
		"total_open", p.numOpen,
	)

	// Try to get an idle connection
	for len(p.conns) > 0 {
		// Get the last connection (LIFO for better cache locality)
		n := len(p.conns) - 1
		conn := p.conns[n]
		p.conns = p.conns[:n]

		// Check if connection is still healthy
		if err := conn.Ping(); err != nil {
			// Connection is dead, close it and try another
			log.Debug("Removing dead connection from pool",
				"error", err,
				"remaining_idle", len(p.conns),
			)
			conn.conn.Close()
			p.numOpen--
			continue
		}

		conn.inUse = true
		conn.lastUsed = time.Now()
		log.Debug("Reusing idle connection from pool",
			"remaining_idle", len(p.conns),
			"total_open", p.numOpen,
		)
		p.mu.Unlock()
		return conn, nil
	}

	// No idle connections available
	if p.numOpen < p.config.MaxConns {
		log.Debug("No idle connections, creating new connection",
			"current_open", p.numOpen,
			"max_conns", p.config.MaxConns,
		)
		// Create a new connection
		conn, err := p.createConn()
		if err != nil {
			p.mu.Unlock()
			return nil, err
		}
		conn.inUse = true
		p.mu.Unlock()
		return conn, nil
	}

	// Pool is at capacity, wait for a connection
	log.Debug("Pool at capacity, waiting for available connection",
		"max_conns", p.config.MaxConns,
		"timeout", p.config.ConnectTimeout,
	)
	p.mu.Unlock()

	// Wait for a connection to be returned
	select {
	case <-p.waiters:
		log.Debug("Connection became available, retrying")
		return p.Get() // Retry
	case <-time.After(p.config.ConnectTimeout):
		log.Warn("Timeout waiting for connection from pool",
			"timeout", p.config.ConnectTimeout,
		)
		return nil, errors.New("timeout waiting for connection")
	}
}

// Put returns a connection to the pool.
// If the pool is closed or at capacity, the connection is closed.
func (p *Pool) Put(conn *PooledConn) {
	if conn == nil {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	conn.inUse = false
	conn.lastUsed = time.Now()

	if p.closed {
		log.Debug("Closing connection (pool is closed)")
		conn.conn.Close()
		p.numOpen--
		return
	}

	// Return to pool
	p.conns = append(p.conns, conn)
	log.Debug("Connection returned to pool",
		"idle_connections", len(p.conns),
		"total_open", p.numOpen,
	)

	// Signal any waiters
	select {
	case p.waiters <- struct{}{}:
		log.Debug("Signaled waiting goroutine about available connection")
	default:
	}
}

// Close closes all connections in the pool and prevents new connections.
func (p *Pool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		log.Debug("Pool already closed")
		return nil
	}

	log.Info("Closing connection pool",
		"idle_connections", len(p.conns),
		"total_open", p.numOpen,
	)

	p.closed = true

	// Close all idle connections
	closedCount := 0
	for _, conn := range p.conns {
		conn.conn.Close()
		closedCount++
	}
	p.conns = nil
	p.numOpen = 0

	close(p.waiters)

	log.Info("Connection pool closed",
		"connections_closed", closedCount,
	)

	return nil
}

// Stats returns current pool statistics.
type Stats struct {
	// OpenConnections is the total number of open connections.
	OpenConnections int

	// IdleConnections is the number of idle connections.
	IdleConnections int

	// InUseConnections is the number of connections currently in use.
	InUseConnections int

	// MaxConnections is the maximum allowed connections.
	MaxConnections int
}

// Stats returns the current pool statistics.
func (p *Pool) Stats() Stats {
	p.mu.Lock()
	defer p.mu.Unlock()

	idle := len(p.conns)
	return Stats{
		OpenConnections:  p.numOpen,
		IdleConnections:  idle,
		InUseConnections: p.numOpen - idle,
		MaxConnections:   p.config.MaxConns,
	}
}

// cleanupIdleConns periodically removes idle connections that have exceeded
// the idle timeout.
func (p *Pool) cleanupIdleConns() {
	log.Debug("Idle connection cleanup goroutine started",
		"check_interval", p.config.IdleTimeout/2,
	)

	ticker := time.NewTicker(p.config.IdleTimeout / 2)
	defer ticker.Stop()

	for range ticker.C {
		p.mu.Lock()

		if p.closed {
			log.Debug("Idle connection cleanup stopping (pool closed)")
			p.mu.Unlock()
			return
		}

		now := time.Now()
		newConns := make([]*PooledConn, 0, len(p.conns))
		closedCount := 0

		for _, conn := range p.conns {
			// Keep connections that are not idle too long or if we're at min
			if now.Sub(conn.lastUsed) < p.config.IdleTimeout || len(newConns) < p.config.MinConns {
				newConns = append(newConns, conn)
			} else {
				idleTime := now.Sub(conn.lastUsed)
				log.Debug("Closing idle connection",
					"idle_time", idleTime,
					"idle_timeout", p.config.IdleTimeout,
				)
				conn.conn.Close()
				p.numOpen--
				closedCount++
			}
		}

		if closedCount > 0 {
			log.Info("Idle connection cleanup completed",
				"connections_closed", closedCount,
				"remaining_idle", len(newConns),
				"total_open", p.numOpen,
			)
		}

		p.conns = newConns
		p.mu.Unlock()
	}
}
