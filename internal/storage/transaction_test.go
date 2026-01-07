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
	"testing"
)

// Note: setupTestKVStore is defined in kv_test.go

func TestTransactionCommit(t *testing.T) {
	store, _, cleanup := setupTestKVStore(t)
	defer cleanup()

	// Start a transaction
	tx := NewTransaction(store)

	// Write some data
	err := tx.Put("key1", []byte("value1"))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	err = tx.Put("key2", []byte("value2"))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Data should be visible within the transaction
	val, err := tx.Get("key1")
	if err != nil || string(val) != "value1" {
		t.Errorf("Expected value1, got %s (err=%v)", string(val), err)
	}

	// Data should NOT be visible in the store yet
	_, err = store.Get("key1")
	if err != ErrNotFound {
		t.Error("Expected data to not be in store before commit")
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Data should now be visible in the store
	val, err = store.Get("key1")
	if err != nil || string(val) != "value1" {
		t.Errorf("Expected value1 after commit, got %s (err=%v)", string(val), err)
	}

	val, err = store.Get("key2")
	if err != nil || string(val) != "value2" {
		t.Errorf("Expected value2 after commit, got %s (err=%v)", string(val), err)
	}
}

func TestTransactionRollback(t *testing.T) {
	store, _, cleanup := setupTestKVStore(t)
	defer cleanup()

	// Put some initial data
	store.Put("existing", []byte("data"))

	// Start a transaction
	tx := NewTransaction(store)

	// Write some data
	tx.Put("key1", []byte("value1"))
	tx.Put("key2", []byte("value2"))

	// Rollback the transaction
	err := tx.Rollback()
	if err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}

	// Data should NOT be in the store
	_, err = store.Get("key1")
	if err != ErrNotFound {
		t.Error("Expected key1 to not exist after rollback")
	}

	_, err = store.Get("key2")
	if err != ErrNotFound {
		t.Error("Expected key2 to not exist after rollback")
	}

	// Existing data should still be there
	val, err := store.Get("existing")
	if err != nil || string(val) != "data" {
		t.Error("Expected existing data to still exist")
	}
}

func TestTransactionDelete(t *testing.T) {
	store, _, cleanup := setupTestKVStore(t)
	defer cleanup()

	// Put some initial data
	store.Put("key1", []byte("value1"))

	// Start a transaction
	tx := NewTransaction(store)

	// Delete the key
	err := tx.Delete("key1")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Key should appear deleted within the transaction
	_, err = tx.Get("key1")
	if err != ErrNotFound {
		t.Error("Expected key1 to appear deleted in transaction")
	}

	// Key should still exist in the store
	val, err := store.Get("key1")
	if err != nil || string(val) != "value1" {
		t.Error("Expected key1 to still exist in store before commit")
	}

	// Commit the transaction
	tx.Commit()

	// Key should now be deleted from the store
	_, err = store.Get("key1")
	if err != ErrNotFound {
		t.Error("Expected key1 to be deleted after commit")
	}
}

