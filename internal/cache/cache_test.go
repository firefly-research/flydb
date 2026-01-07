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

package cache

import (
	"testing"
	"time"
)

func TestQueryCacheBasic(t *testing.T) {
	cache := New(Config{
		MaxEntries: 100,
		TTL:        1 * time.Minute,
		Enabled:    true,
	})

	// Test Set and Get
	cache.Set("SELECT * FROM users", "result1", []string{"users"})

	result, ok := cache.Get("SELECT * FROM users")
	if !ok {
		t.Fatal("Expected cache hit")
	}
	if result != "result1" {
		t.Errorf("Expected 'result1', got '%s'", result)
	}

	// Test cache miss
	_, ok = cache.Get("SELECT * FROM orders")
	if ok {
		t.Error("Expected cache miss")
	}
}

func TestQueryCacheInvalidation(t *testing.T) {
	cache := New(Config{
		MaxEntries: 100,
		TTL:        1 * time.Minute,
		Enabled:    true,
	})

	// Cache some queries
	cache.Set("SELECT * FROM users", "result1", []string{"users"})
	cache.Set("SELECT * FROM orders", "result2", []string{"orders"})
	cache.Set("SELECT u.*, o.* FROM users u JOIN orders o", "result3", []string{"users", "orders"})

	// Invalidate users table
	cache.Invalidate("users")

	// users query should be gone
	_, ok := cache.Get("SELECT * FROM users")
	if ok {
		t.Error("Expected cache miss after invalidation")
	}

	// orders query should still be there
	_, ok = cache.Get("SELECT * FROM orders")
	if !ok {
		t.Error("Expected cache hit for orders")
	}

	// join query should be gone (references users)
	_, ok = cache.Get("SELECT u.*, o.* FROM users u JOIN orders o")
	if ok {
		t.Error("Expected cache miss for join query after users invalidation")
	}
}

func TestQueryCacheLRUEviction(t *testing.T) {
	cache := New(Config{
		MaxEntries: 3,
		TTL:        1 * time.Minute,
		Enabled:    true,
	})

	// Fill the cache
	cache.Set("query1", "result1", []string{"t1"})
	cache.Set("query2", "result2", []string{"t2"})
	cache.Set("query3", "result3", []string{"t3"})

	// Access query1 to make it recently used
	cache.Get("query1")

	// Add a new entry, should evict query2 (least recently used)
	cache.Set("query4", "result4", []string{"t4"})

	// query2 should be evicted
	_, ok := cache.Get("query2")
	if ok {
		t.Error("Expected query2 to be evicted")
	}

	// query1 should still be there (was accessed)
	_, ok = cache.Get("query1")
	if !ok {
		t.Error("Expected query1 to still be cached")
	}
}

func TestQueryCacheDisabled(t *testing.T) {
	cache := New(Config{
		MaxEntries: 100,
		TTL:        1 * time.Minute,
		Enabled:    false,
	})

	cache.Set("SELECT * FROM users", "result1", []string{"users"})

	_, ok := cache.Get("SELECT * FROM users")
	if ok {
		t.Error("Expected cache miss when disabled")
	}
}

func TestQueryCacheStats(t *testing.T) {
	cache := New(Config{
		MaxEntries: 100,
		TTL:        1 * time.Minute,
		Enabled:    true,
	})

	cache.Set("query1", "result1", []string{"t1"})

	// Hit
	cache.Get("query1")
	// Miss
	cache.Get("query2")

	stats := cache.Stats()
	if stats.Hits != 1 {
		t.Errorf("Expected 1 hit, got %d", stats.Hits)
	}
	if stats.Misses != 1 {
		t.Errorf("Expected 1 miss, got %d", stats.Misses)
	}
	if stats.Entries != 1 {
		t.Errorf("Expected 1 entry, got %d", stats.Entries)
	}
}

func TestQueryCacheInvalidateAll(t *testing.T) {
	cache := New(Config{
		MaxEntries: 100,
		TTL:        1 * time.Minute,
		Enabled:    true,
	})

	cache.Set("query1", "result1", []string{"t1"})
	cache.Set("query2", "result2", []string{"t2"})

	cache.InvalidateAll()

	stats := cache.Stats()
	if stats.Entries != 0 {
		t.Errorf("Expected 0 entries after InvalidateAll, got %d", stats.Entries)
	}
}

