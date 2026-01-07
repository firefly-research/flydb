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
Package cache provides query result caching for FlyDB.

Query Cache Overview:
=====================

The query cache stores the results of SELECT queries to avoid re-executing
identical queries. This can significantly improve performance for read-heavy
workloads with repeated queries.

Features:
=========

  - LRU eviction when cache is full
  - TTL-based expiration
  - Automatic invalidation on table modifications
  - Thread-safe operations
  - Configurable cache size

Cache Invalidation:
===================

The cache is automatically invalidated when:
  - INSERT, UPDATE, or DELETE is executed on a cached table
  - The TTL expires for a cached entry
  - The cache reaches its size limit (LRU eviction)

Usage Example:
==============

	cache := cache.New(cache.Config{
		MaxEntries: 1000,
		TTL:        5 * time.Minute,
	})

	// Check cache before executing query
	if result, ok := cache.Get("SELECT * FROM users"); ok {
		return result
	}

	// Execute query and cache result
	result := executeQuery("SELECT * FROM users")
	cache.Set("SELECT * FROM users", result, []string{"users"})
*/
package cache

import (
	"container/list"
	"strings"
	"sync"
	"time"
)

// Config holds the configuration for the query cache.
type Config struct {
	// MaxEntries is the maximum number of cached queries.
	// When exceeded, the least recently used entries are evicted.
	MaxEntries int

	// TTL is the time-to-live for cached entries.
	// Entries older than TTL are considered expired.
	TTL time.Duration

	// Enabled controls whether caching is active.
	Enabled bool
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		MaxEntries: 1000,
		TTL:        5 * time.Minute,
		Enabled:    true,
	}
}

// entry represents a cached query result.
type entry struct {
	key       string
	value     string
	tables    []string  // Tables referenced by this query (for invalidation)
	expiresAt time.Time
	element   *list.Element
}

// QueryCache caches query results with LRU eviction and TTL expiration.
type QueryCache struct {
	config Config

	mu sync.RWMutex

	// cache maps query strings to entries
	cache map[string]*entry

	// lru tracks access order for LRU eviction
	lru *list.List

	// tableIndex maps table names to queries that reference them
	// Used for efficient invalidation on table modifications
	tableIndex map[string]map[string]struct{}

	// stats tracks cache performance
	hits   int64
	misses int64
}

// New creates a new QueryCache with the given configuration.
func New(config Config) *QueryCache {
	if config.MaxEntries <= 0 {
		config.MaxEntries = 1000
	}
	if config.TTL <= 0 {
		config.TTL = 5 * time.Minute
	}

	qc := &QueryCache{
		config:     config,
		cache:      make(map[string]*entry),
		lru:        list.New(),
		tableIndex: make(map[string]map[string]struct{}),
	}

	// Start background cleanup goroutine
	go qc.cleanupExpired()

	return qc
}

// Get retrieves a cached query result.
// Returns the result and true if found and not expired, empty string and false otherwise.
func (qc *QueryCache) Get(query string) (string, bool) {
	if !qc.config.Enabled {
		return "", false
	}

	qc.mu.Lock()
	defer qc.mu.Unlock()

	e, ok := qc.cache[query]
	if !ok {
		qc.misses++
		return "", false
	}

	// Check if expired
	if time.Now().After(e.expiresAt) {
		qc.removeEntry(e)
		qc.misses++
		return "", false
	}

	// Move to front of LRU list
	qc.lru.MoveToFront(e.element)
	qc.hits++

	return e.value, true
}

// Set caches a query result.
// tables is the list of tables referenced by the query (for invalidation).
func (qc *QueryCache) Set(query, result string, tables []string) {
	if !qc.config.Enabled {
		return
	}

	qc.mu.Lock()
	defer qc.mu.Unlock()

	// Check if already cached
	if e, ok := qc.cache[query]; ok {
		// Update existing entry
		e.value = result
		e.expiresAt = time.Now().Add(qc.config.TTL)
		qc.lru.MoveToFront(e.element)
		return
	}

	// Evict if at capacity
	for len(qc.cache) >= qc.config.MaxEntries {
		qc.evictOldest()
	}

	// Create new entry
	e := &entry{
		key:       query,
		value:     result,
		tables:    tables,
		expiresAt: time.Now().Add(qc.config.TTL),
	}
	e.element = qc.lru.PushFront(e)
	qc.cache[query] = e

	// Update table index for invalidation
	for _, table := range tables {
		if qc.tableIndex[table] == nil {
			qc.tableIndex[table] = make(map[string]struct{})
		}
		qc.tableIndex[table][query] = struct{}{}
	}
}

// Invalidate removes all cached queries that reference the given table.
// This should be called after INSERT, UPDATE, or DELETE operations.
func (qc *QueryCache) Invalidate(table string) {
	if !qc.config.Enabled {
		return
	}

	qc.mu.Lock()
	defer qc.mu.Unlock()

	// Normalize table name
	table = strings.ToLower(table)

	queries, ok := qc.tableIndex[table]
	if !ok {
		return
	}

	// Remove all queries that reference this table
	for query := range queries {
		if e, ok := qc.cache[query]; ok {
			qc.removeEntry(e)
		}
	}

	delete(qc.tableIndex, table)
}

// InvalidateAll clears the entire cache.
func (qc *QueryCache) InvalidateAll() {
	qc.mu.Lock()
	defer qc.mu.Unlock()

	qc.cache = make(map[string]*entry)
	qc.lru = list.New()
	qc.tableIndex = make(map[string]map[string]struct{})
}

// removeEntry removes an entry from the cache (must hold lock).
func (qc *QueryCache) removeEntry(e *entry) {
	delete(qc.cache, e.key)
	qc.lru.Remove(e.element)

	// Remove from table index
	for _, table := range e.tables {
		if queries, ok := qc.tableIndex[table]; ok {
			delete(queries, e.key)
			if len(queries) == 0 {
				delete(qc.tableIndex, table)
			}
		}
	}
}

// evictOldest removes the least recently used entry (must hold lock).
func (qc *QueryCache) evictOldest() {
	elem := qc.lru.Back()
	if elem == nil {
		return
	}
	e := elem.Value.(*entry)
	qc.removeEntry(e)
}

// cleanupExpired periodically removes expired entries.
func (qc *QueryCache) cleanupExpired() {
	ticker := time.NewTicker(qc.config.TTL / 2)
	defer ticker.Stop()

	for range ticker.C {
		qc.mu.Lock()

		now := time.Now()
		for _, e := range qc.cache {
			if now.After(e.expiresAt) {
				qc.removeEntry(e)
			}
		}

		qc.mu.Unlock()
	}
}

// Stats holds cache statistics.
type Stats struct {
	Hits       int64
	Misses     int64
	Entries    int
	MaxEntries int
	HitRate    float64
}

// Stats returns current cache statistics.
func (qc *QueryCache) Stats() Stats {
	qc.mu.RLock()
	defer qc.mu.RUnlock()

	total := qc.hits + qc.misses
	hitRate := 0.0
	if total > 0 {
		hitRate = float64(qc.hits) / float64(total)
	}

	return Stats{
		Hits:       qc.hits,
		Misses:     qc.misses,
		Entries:    len(qc.cache),
		MaxEntries: qc.config.MaxEntries,
		HitRate:    hitRate,
	}
}

// SetEnabled enables or disables the cache.
func (qc *QueryCache) SetEnabled(enabled bool) {
	qc.mu.Lock()
	defer qc.mu.Unlock()
	qc.config.Enabled = enabled
}

