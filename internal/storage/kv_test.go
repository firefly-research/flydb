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

package storage

import (
	"os"
	"testing"
)

func setupTestKVStore(t *testing.T) (*KVStore, string, func()) {
	tmpDir, err := os.MkdirTemp("", "flydb_kv_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	walPath := tmpDir + "/test.wal"
	store, err := NewKVStore(walPath)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("Failed to create KVStore: %v", err)
	}

	cleanup := func() {
		store.Close()
		os.RemoveAll(tmpDir)
	}

	return store, walPath, cleanup
}

func TestKVStorePutAndGet(t *testing.T) {
	store, _, cleanup := setupTestKVStore(t)
	defer cleanup()

	// Put a value
	err := store.Put("key1", []byte("value1"))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Get the value
	val, err := store.Get("key1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(val) != "value1" {
		t.Errorf("Expected 'value1', got '%s'", string(val))
	}
}

func TestKVStoreGetNotFound(t *testing.T) {
	store, _, cleanup := setupTestKVStore(t)
	defer cleanup()

	// Get a non-existent key
	_, err := store.Get("nonexistent")
	if err != ErrNotFound {
		t.Errorf("Expected ErrNotFound, got %v", err)
	}
}

func TestKVStoreDelete(t *testing.T) {
	store, _, cleanup := setupTestKVStore(t)
	defer cleanup()

	// Put a value
	err := store.Put("key1", []byte("value1"))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Delete the key
	err = store.Delete("key1")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify it's gone
	_, err = store.Get("key1")
	if err != ErrNotFound {
		t.Errorf("Expected ErrNotFound after delete, got %v", err)
	}
}

func TestKVStoreScan(t *testing.T) {
	store, _, cleanup := setupTestKVStore(t)
	defer cleanup()

	// Put multiple values with different prefixes
	store.Put("users:alice", []byte("Alice"))
	store.Put("users:bob", []byte("Bob"))
	store.Put("orders:1", []byte("Order1"))
	store.Put("orders:2", []byte("Order2"))

	// Scan for users
	results, err := store.Scan("users:")
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("Expected 2 users, got %d", len(results))
	}

	// Scan for orders
	results, err = store.Scan("orders:")
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("Expected 2 orders, got %d", len(results))
	}

	// Scan for non-existent prefix
	results, err = store.Scan("products:")
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("Expected 0 products, got %d", len(results))
	}
}

func TestKVStorePersistence(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "flydb_kv_persist_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	walPath := tmpDir + "/test.wal"

	// Create store and write data
	store1, err := NewKVStore(walPath)
	if err != nil {
		t.Fatalf("Failed to create KVStore: %v", err)
	}
	store1.Put("persistent_key", []byte("persistent_value"))
	store1.Close()

	// Reopen store and verify data persisted
	store2, err := NewKVStore(walPath)
	if err != nil {
		t.Fatalf("Failed to reopen KVStore: %v", err)
	}
	defer store2.Close()

	val, err := store2.Get("persistent_key")
	if err != nil {
		t.Fatalf("Get failed after reopen: %v", err)
	}
	if string(val) != "persistent_value" {
		t.Errorf("Expected 'persistent_value', got '%s'", string(val))
	}
}

